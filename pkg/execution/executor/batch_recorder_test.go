package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution/batch"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	telemetrytrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type functionsData struct {
	cqrs.Manager
	fns []inngest.Function
}

func (f functionsData) Functions(context.Context) ([]inngest.Function, error) {
	return f.fns, nil
}

// batchHarness is a two-event batch whose scheduler job runs against a real
// batch manager and a run recorder that fails its first call.
type batchHarness struct {
	bm       batch.BatchManager
	fn       inngest.Function
	batchID  ulid.ULID
	pointer  string
	item     queue.Item
	exec     *executor
	svc      *svc
	recorded []ulid.ULID
}

func newBatchHarness(t *testing.T) *batchHarness {
	t.Helper()
	ctx := context.Background()
	r := miniredis.RunT(t)
	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{r.Addr()}, DisableCache: true})
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	bm := batch.NewRedisBatchManager(redis_state.NewBatchClient(rc, redis_state.QueueDefaultKey), nil, batch.WithoutBuffer())

	fn := inngest.Function{ID: uuid.New(), FunctionVersion: 1, Name: "batched", EventBatch: &inngest.EventBatchConfig{MaxSize: 10, Timeout: "60s"}}
	accountID, wsID, appID := uuid.New(), uuid.New(), uuid.New()
	var batchID, pointer string
	for range 2 {
		id := ulid.Make()
		res, err := bm.Append(ctx, batch.BatchItem{
			AccountID: accountID, WorkspaceID: wsID, AppID: appID, FunctionID: fn.ID, FunctionVersion: 1,
			EventID: id, Event: event.Event{ID: id.String(), Name: "test/batch", Timestamp: time.Now().UnixMilli(), Data: map[string]any{}},
		}, fn)
		require.NoError(t, err)
		batchID, pointer = res.BatchID, res.BatchPointerKey
	}

	h := &batchHarness{bm: bm, fn: fn, batchID: ulid.MustParse(batchID), pointer: pointer}
	failNext := true
	h.exec = &executor{
		log:               logger.VoidLogger(),
		queue:             &idempotentQueue{},
		smv2:              &idempotentRunService{},
		batcher:           bm,
		clock:             clockwork.NewFakeClockAt(time.Now()),
		tracerProvider:    tracing.NewNoopTracerProvider(),
		conditionalTracer: telemetrytrace.NoopConditionalTracer(),
		runRecorder: func(_ context.Context, md sv2.Metadata) error {
			h.recorded = append(h.recorded, md.ID.RunID)
			if failNext {
				failNext = false
				return errors.New("insert failed")
			}
			return nil
		},
	}
	h.svc = &svc{exec: h.exec, batcher: bm, data: functionsData{fns: []inngest.Function{fn}}, log: logger.VoidLogger()}

	payload, err := json.Marshal(batch.ScheduleBatchOpts{ScheduleBatchPayload: batch.ScheduleBatchPayload{
		BatchID: h.batchID, BatchPointer: pointer, AccountID: accountID, WorkspaceID: wsID, AppID: appID, FunctionID: fn.ID, FunctionVersion: 1,
	}})
	require.NoError(t, err)
	h.item = queue.Item{
		Kind:       queue.KindScheduleBatch,
		Identifier: state.Identifier{WorkflowID: fn.ID, AccountID: accountID, WorkspaceID: wsID, AppID: appID},
		Payload:    json.RawMessage(payload),
	}
	return h
}

// firstAttemptQueuesUnrecordedRun runs the scheduler job once: the run is
// created and queued, the recorder fails, and the batch stays started.
func (h *batchHarness) firstAttemptQueuesUnrecordedRun(t *testing.T) {
	t.Helper()
	require.ErrorIs(t, h.svc.handleScheduledBatch(context.Background(), h.item), ErrRunNotRecorded)
	require.Len(t, h.recorded, 1)
	h.item.Attempt++
}

func (h *batchHarness) requireBatchDeleted(t *testing.T) {
	t.Helper()
	items, err := h.bm.RetrieveItems(context.Background(), h.fn.ID, h.batchID)
	require.NoError(t, err)
	require.Empty(t, items, "the batch keys are deleted")
}

// A batch job retried after its run was queued but not recorded finds the batch
// started; the retry must still record that run.
func TestScheduledBatchRetryRecordsQueuedRun(t *testing.T) {
	h := newBatchHarness(t)
	h.firstAttemptQueuesUnrecordedRun(t)

	require.NoError(t, h.svc.handleScheduledBatch(context.Background(), h.item))
	require.Len(t, h.recorded, 2)
	require.Equal(t, h.recorded[0], h.recorded[1], "the retry must record the run the first attempt queued")
	h.requireBatchDeleted(t)
}

// Past the idempotency period a started batch still resumes while its run's
// idempotency record exists, recording the original run.
func TestScheduledBatchLateRetryWithIdempotencyRecordRecordsOriginalRun(t *testing.T) {
	h := newBatchHarness(t)
	h.firstAttemptQueuesUnrecordedRun(t)
	h.exec.clock.(*clockwork.FakeClock).Advance(consts.FunctionIdempotencyPeriod + time.Hour)

	require.NoError(t, h.svc.handleScheduledBatch(context.Background(), h.item))
	require.Len(t, h.recorded, 2)
	require.Equal(t, h.recorded[0], h.recorded[1], "the late retry must record the original run")
	h.requireBatchDeleted(t)
}

// Once the run's idempotency record and queue dedup have expired, rescheduling a
// started batch would mint a second run; the retry only cleans up.
func TestScheduledBatchLateRetryWithoutIdempotencyRecordDropsBatch(t *testing.T) {
	h := newBatchHarness(t)
	h.firstAttemptQueuesUnrecordedRun(t)
	h.exec.clock.(*clockwork.FakeClock).Advance(consts.FunctionIdempotencyPeriod + time.Hour)
	rs, q := &idempotentRunService{}, &idempotentQueue{}
	h.exec.smv2, h.exec.queue = rs, q

	require.NoError(t, h.svc.handleScheduledBatch(context.Background(), h.item))
	require.Nil(t, rs.first, "no run state is created")
	require.Nil(t, q.owner, "no start job is queued")
	require.Len(t, h.recorded, 1)
	h.requireBatchDeleted(t)
}

// A started batch with no idempotency record inside the idempotency period never
// created its run, so the retry schedules it.
func TestScheduledBatchRetryBeforeRunCreatedSchedulesRun(t *testing.T) {
	h := newBatchHarness(t)
	status, err := h.bm.StartExecution(context.Background(), h.fn.ID, h.batchID, h.pointer)
	require.NoError(t, err)
	require.NotEqual(t, enums.BatchStatusStarted.String(), status)
	h.item.Attempt++

	require.ErrorIs(t, h.svc.handleScheduledBatch(context.Background(), h.item), ErrRunNotRecorded)
	rs := h.exec.smv2.(*idempotentRunService)
	require.NotNil(t, rs.first, "the retry creates the run")
	require.Equal(t, []ulid.ULID{rs.first.ID.RunID}, h.recorded)
}
