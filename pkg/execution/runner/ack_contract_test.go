package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

type stubFunctionData struct {
	cqrs.Manager
	fns []inngest.Function
}

func (s stubFunctionData) Functions(context.Context) ([]inngest.Function, error) {
	return s.fns, nil
}

func (s stubFunctionData) FunctionsByTrigger(context.Context, string) ([]inngest.Function, error) {
	return nil, nil
}

func (s stubFunctionData) GetFunctionByInternalUUID(_ context.Context, id uuid.UUID) (*cqrs.Function, error) {
	return &cqrs.Function{ID: id, AppID: uuid.New()}, nil
}

type blockingScheduleExecutor struct {
	execution.Executor
	release chan struct{}
	err     error
	failErr error
	failed  chan struct{}
}

func (e *blockingScheduleExecutor) Schedule(context.Context, execution.ScheduleRequest) (*ulid.ULID, *sv2.Metadata, error) {
	<-e.release
	return nil, nil, e.err
}

func (e *blockingScheduleExecutor) InvokeFailHandler(context.Context, execution.InvokeFailHandlerOpts) error {
	close(e.failed)
	return e.failErr
}

func newInvokeEvent(fnID string) event.TrackedEvent {
	evt := event.NewInvocationEvent(event.NewInvocationEventOpts{Event: event.Event{Data: map[string]any{}}, FnID: fnID})
	return event.NewBaseTrackedEventWithID(evt.Event, ulid.Make())
}

// An invoke-only event matches no trigger; functions() must still wait for the
// invoke goroutine, or the event is acked mid-schedule and its error lost.
func TestFunctionsWaitsForInvokeScheduling(t *testing.T) {
	fn := inngest.Function{ID: uuid.New(), Slug: "app-fn", Name: "fn"}
	boom := errors.New("boom")
	exec := &blockingScheduleExecutor{release: make(chan struct{}), err: boom}
	data := stubFunctionData{fns: []inngest.Function{fn}}
	s := &svc{config: config.Config{}, cqrs: data, data: data, executor: exec, log: NewService(config.Config{}).(*svc).log}

	done := make(chan error, 1)
	go func() { done <- s.functions(context.Background(), newInvokeEvent("app-fn")) }()

	select {
	case <-done:
		t.Fatal("functions returned before the invoked run was scheduled")
	case <-time.After(100 * time.Millisecond):
	}
	close(exec.release)
	require.ErrorIs(t, <-done, boom)
}

func TestFunctionsInvokeOfMissingFunction(t *testing.T) {
	data := stubFunctionData{}
	log := NewService(config.Config{}).(*svc).log

	t.Run("failed invoker is not a redelivery", func(t *testing.T) {
		exec := &blockingScheduleExecutor{failed: make(chan struct{})}
		s := &svc{cqrs: data, data: data, executor: exec, log: log}
		require.NoError(t, s.functions(context.Background(), newInvokeEvent("missing")))
		<-exec.failed
	})

	t.Run("fail handler error is returned", func(t *testing.T) {
		failErr := errors.New("publish failed")
		exec := &blockingScheduleExecutor{failed: make(chan struct{}), failErr: failErr}
		s := &svc{cqrs: data, data: data, executor: exec, log: log}
		require.ErrorIs(t, s.functions(context.Background(), newInvokeEvent("missing")), failErr)
	})
}
