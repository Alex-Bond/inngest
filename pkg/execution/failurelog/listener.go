// Package failurelog logs function runs that finish in a terminal failure, so
// failures are visible in the engine's own logs and not only in the dashboard.
package failurelog

import (
	"context"
	"encoding/json"

	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	statev1 "github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
)

// FunctionFailedEvent is a stable marker for querying failure lines in a log store.
const FunctionFailedEvent = "function.failed"

var _ execution.LifecycleListener = (*Listener)(nil)

// Listener writes one ERROR line per function run that finishes with an error.
// Listeners run asynchronously, so a run that fails from parallel steps may be
// logged more than once; consumers deduplicate on run_id.
type Listener struct {
	execution.NoopLifecyceListener

	log logger.Logger
}

func NewListener(l logger.Logger) *Listener {
	return &Listener{log: l}
}

func (l *Listener) OnFunctionFinished(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	evts []json.RawMessage,
	resp statev1.DriverResponse,
) {
	if resp.Err == nil {
		return
	}

	// StandardError reads the SDK's error from the output, which is where a function-level failure carries it.
	se := resp.StandardError()
	l.log.ErrorContext(ctx, "function run failed",
		"inngest_event", FunctionFailedEvent,
		"run_id", md.ID.RunID.String(),
		"function_id", md.ID.FunctionID.String(),
		"function_slug", md.Config.FunctionSlug(),
		"app_id", md.ID.Tenant.AppID.String(),
		"env_id", md.ID.Tenant.EnvID.String(),
		"event_id", md.Config.EventID().String(),
		"event_name", eventName(evts),
		"attempt", item.Attempt,
		"max_attempts", item.GetMaxAttempts(),
		"error", *resp.Err,
		"status_code", resp.StatusCode,
		"step_name", resp.Step.Name,
		"no_retry", resp.NoRetry,
		"error_name", se.Name,
		"error_message", se.Message,
		"error_stack", se.Stack,
	)
}

// eventName reads the name of the first triggering event; an unparseable event yields "".
func eventName(evts []json.RawMessage) string {
	if len(evts) == 0 {
		return ""
	}
	var evt struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(evts[0], &evt)
	return evt.Name
}
