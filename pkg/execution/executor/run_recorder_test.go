package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	telemetrytrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// idempotentQueue accepts the first start job and reports every later one as
// a duplicate owned by the first run.
type idempotentQueue struct {
	queue.Queue
	owner *ulid.ULID
}

func (q *idempotentQueue) Enqueue(_ context.Context, item queue.Item, _ time.Time, _ queue.EnqueueOpts) error {
	if q.owner != nil {
		return queue.QueueItemExists(*item.JobID, q.owner)
	}
	runID := item.Identifier.RunID
	q.owner = &runID
	return nil
}

// idempotentRunService keeps the first run's state and returns it for every
// later create with the same idempotency key.
type idempotentRunService struct {
	sv2.RunService
	first *sv2.Metadata
}

func (r *idempotentRunService) Create(_ context.Context, s sv2.CreateState) (sv2.State, error) {
	if r.first != nil {
		return sv2.State{Metadata: *r.first}, state.ErrIdentifierExists
	}
	md := s.Metadata
	r.first = &md
	return sv2.State{Metadata: md}, nil
}

func (r *idempotentRunService) LoadMetadata(context.Context, sv2.ID, ...sv2.LoadMetadataOption) (sv2.Metadata, error) {
	return *r.first, nil
}

func (r *idempotentRunService) LookupIdempotency(_ context.Context, _ sv2.ID, key string) (*sv2.IdempotencyEntry, error) {
	if r.first == nil || r.first.Config.Idempotency != key {
		return nil, nil
	}
	return &sv2.IdempotencyEntry{RunID: r.first.ID.RunID}, nil
}

func (r *idempotentRunService) Delete(context.Context, sv2.ID, ...sv2.DeleteOption) error {
	return nil
}

func recorderScheduleRequest() execution.ScheduleRequest {
	eventID := ulid.Make()
	return execution.ScheduleRequest{
		AccountID:   uuid.New(),
		WorkspaceID: uuid.New(),
		AppID:       uuid.New(),
		Function:    inngest.Function{ID: uuid.New(), FunctionVersion: 1, Name: "recorder"},
		Events: []event.TrackedEvent{event.InternalEvent{
			ID:    eventID,
			Event: event.Event{ID: eventID.String(), Name: "test/recorder", Timestamp: time.Now().UnixMilli(), Data: map[string]any{}},
		}},
	}
}

// A schedule whose recorder fails is retried; the retry finds the start job
// already queued and records the original run instead of creating another.
func TestRunRecorderRetriedScheduleRecordsOriginalRun(t *testing.T) {
	var recorded []ulid.ULID
	failNext := true
	e := &executor{
		log:               logger.VoidLogger(),
		queue:             &idempotentQueue{},
		smv2:              &idempotentRunService{},
		tracerProvider:    tracing.NewNoopTracerProvider(),
		conditionalTracer: telemetrytrace.NoopConditionalTracer(),
		runRecorder: func(_ context.Context, md sv2.Metadata) error {
			recorded = append(recorded, md.ID.RunID)
			if failNext {
				failNext = false
				return errors.New("insert failed")
			}
			return nil
		},
	}
	req := recorderScheduleRequest()

	firstRunID, md, err := e.schedule(context.Background(), req, ulid.Make(), "key", false, nil)
	require.ErrorIs(t, err, ErrRunNotRecorded)
	require.NotNil(t, md)

	_, _, err = e.schedule(context.Background(), req, ulid.Make(), "key", false, nil)
	require.ErrorIs(t, err, state.ErrIdentifierExists)
	require.Equal(t, []ulid.ULID{*firstRunID, *firstRunID}, recorded)
}

func TestRecordExistingRunSkipsOtherOwnersAndDiscardedState(t *testing.T) {
	var recorded int
	e := &executor{runRecorder: func(context.Context, sv2.Metadata) error {
		recorded++
		return nil
	}}
	md := sv2.Metadata{ID: sv2.ID{RunID: ulid.Make()}}
	other := ulid.Make()

	require.NoError(t, e.recordExistingRun(context.Background(), queue.QueueItemExists("job", &other), md, false))
	require.NoError(t, e.recordExistingRun(context.Background(), queue.QueueItemExists("job", nil), md, true))
	require.Zero(t, recorded)

	require.NoError(t, e.recordExistingRun(context.Background(), queue.QueueItemExists("job", &md.ID.RunID), md, true))
	require.NoError(t, e.recordExistingRun(context.Background(), queue.QueueItemExists("job", nil), md, false))
	require.Equal(t, 2, recorded)
}
