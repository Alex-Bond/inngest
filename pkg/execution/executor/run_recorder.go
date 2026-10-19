package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
)

// ErrRunNotRecorded is returned when a run was scheduled but the run recorder
// failed; retrying the schedule records it without creating a second run.
var ErrRunNotRecorded = errors.New("run scheduled but not recorded")

// RunRecorder durably records a scheduled run inline; its error fails the
// schedule so the caller retries, so it must be idempotent per run ID.
type RunRecorder func(ctx context.Context, md sv2.Metadata) error

// WithRunRecorder records every scheduled run synchronously, including when a
// retried schedule finds the run's start job already queued.
func WithRunRecorder(r RunRecorder) ExecutorOpt {
	return func(e execution.Executor) error {
		e.(*executor).runRecorder = r
		return nil
	}
}

func (e *executor) recordRun(ctx context.Context, md sv2.Metadata) error {
	if e.runRecorder == nil {
		return nil
	}
	if err := e.runRecorder(context.WithoutCancel(ctx), md); err != nil {
		return fmt.Errorf("%w: %w", ErrRunNotRecorded, err)
	}
	return nil
}

// recordExistingRun records the run behind an already-queued start job, skipping
// jobs owned by another run and fresh state that is being discarded.
func (e *executor) recordExistingRun(ctx context.Context, enqueueErr error, md sv2.Metadata, stateCreated bool) error {
	var existsErr queue.QueueItemExistsError
	hasOwner := errors.As(enqueueErr, &existsErr) && existsErr.RunID != nil
	if hasOwner && *existsErr.RunID != md.ID.RunID {
		return nil
	}
	if stateCreated && !hasOwner {
		return nil
	}
	return e.recordRun(ctx, md)
}
