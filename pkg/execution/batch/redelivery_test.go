package batch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// recordingProducer records scheduled batch job IDs and fails while failing
// is set, standing in for a process killed before the job was enqueued.
type recordingProducer struct {
	queue.Producer
	mu      sync.Mutex
	failing bool
	jobIDs  []string
}

func (p *recordingProducer) Enqueue(_ context.Context, item queue.Item, _ time.Time, _ queue.EnqueueOpts) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failing {
		return errors.New("killed")
	}
	p.jobIDs = append(p.jobIDs, *item.JobID)
	return nil
}

func (p *recordingProducer) scheduled() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.jobIDs...)
}

func newRedeliveryManager(t *testing.T, q queue.Producer) (*miniredis.Miniredis, BatchManager) {
	t.Helper()
	r := miniredis.RunT(t)
	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{r.Addr()}, DisableCache: true})
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	bm := NewRedisBatchManager(redis_state.NewBatchClient(rc, redis_state.QueueDefaultKey), q, WithBufferSettings(20*time.Millisecond, 50))
	t.Cleanup(func() { _ = bm.Close() })
	return r, bm
}

func redeliveryItem(fn inngest.Function, wsID uuid.UUID) BatchItem {
	id := ulid.Make()
	return BatchItem{WorkspaceID: wsID, FunctionID: fn.ID, EventID: id, Event: event.Event{ID: id.String(), Name: "test/batch"}}
}

// A redelivered event whose first append filled batch A (rotating the pointer
// to B) must schedule A, even though A's job was never enqueued.
func TestRedeliveredEventSchedulesTheBatchItFilled(t *testing.T) {
	q := &recordingProducer{failing: true}
	_, bm := newRedeliveryManager(t, q)
	wsID := uuid.New()
	fn := inngest.Function{ID: uuid.New(), EventBatch: &inngest.EventBatchConfig{MaxSize: 2, Timeout: "60s"}}

	first, second := redeliveryItem(fn, wsID), redeliveryItem(fn, wsID)
	var wg sync.WaitGroup
	for _, bi := range []BatchItem{first, second} {
		wg.Go(func() {
			res, err := bm.Append(context.Background(), bi, fn)
			require.Error(t, err)
			require.Nil(t, res)
		})
	}
	wg.Wait()
	require.Empty(t, q.scheduled())

	q.mu.Lock()
	q.failing = false
	q.mu.Unlock()

	_, err := bm.Append(context.Background(), second, fn)
	require.NoError(t, err)

	var full []string
	for _, jobID := range q.scheduled() {
		batchID := ulid.MustParse(strings.TrimPrefix(jobID, wsID.String()+":"))
		items, err := bm.RetrieveItems(context.Background(), fn.ID, batchID)
		require.NoError(t, err)
		if len(items) == 2 {
			full = append(full, jobID)
		}
	}
	require.Len(t, full, 1, "the full batch the duplicate was appended to must be scheduled")
}

// Concurrent seeded retries of one request append the same events at the same
// time; each event must land in exactly one batch.
func TestConcurrentRetriesAppendEachEventOnce(t *testing.T) {
	r, bm := newRedeliveryManager(t, &recordingProducer{})
	wsID := uuid.New()
	fn := inngest.Function{ID: uuid.New(), EventBatch: &inngest.EventBatchConfig{MaxSize: 7, Timeout: "60s"}}

	items := make([]BatchItem, 40)
	for i := range items {
		items[i] = redeliveryItem(fn, wsID)
	}

	var wg sync.WaitGroup
	for range 3 {
		for _, bi := range items {
			wg.Go(func() {
				_, err := bm.Append(context.Background(), bi, fn)
				require.NoError(t, err)
			})
		}
	}
	wg.Wait()

	seen := map[ulid.ULID]int{}
	for _, key := range r.Keys() {
		if !strings.Contains(key, ":batches:") || strings.HasSuffix(key, ":metadata") {
			continue
		}
		batchID := ulid.MustParse(key[strings.LastIndex(key, ":")+1:])
		got, err := bm.RetrieveItems(context.Background(), fn.ID, batchID)
		require.NoError(t, err)
		for _, it := range got {
			seen[it.EventID]++
		}
	}
	require.Len(t, seen, len(items))
	for _, bi := range items {
		require.Equal(t, 1, seen[bi.EventID], "event %s", bi.EventID)
	}
}
