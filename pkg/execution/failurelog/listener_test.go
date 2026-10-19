package failurelog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/execution/queue"
	statev1 "github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func newTestListener() (*Listener, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	l := logger.FromSlog(slog.New(slog.NewJSONHandler(buf, nil)), slog.LevelInfo)
	return NewListener(l), buf
}

func newTestMetadata() statev2.Metadata {
	md := statev2.Metadata{
		ID: statev2.ID{
			RunID:      ulid.Make(),
			FunctionID: uuid.New(),
			Tenant: statev2.Tenant{
				AppID: uuid.New(),
				EnvID: uuid.New(),
			},
		},
		Config: *statev2.InitConfig(&statev2.Config{EventIDs: []ulid.ULID{ulid.Make()}}),
	}
	md.Config.SetFunctionSlug("app-send-email")
	return md
}

func TestOnFunctionFinishedLogsFailureOnce(t *testing.T) {
	listener, buf := newTestListener()
	md := newTestMetadata()
	maxAttempts := 4
	item := queue.Item{Attempt: 3, MaxAttempts: &maxAttempts}
	evts := []json.RawMessage{json.RawMessage(`{"name":"app/email.requested","data":{}}`)}
	errStr := "Error: boom"
	resp := statev1.DriverResponse{
		Err:        &errStr,
		StatusCode: 500,
		Step:       inngest.Step{Name: "send"},
		Output:     json.RawMessage(`{"name":"Error","message":"boom","stack":"Error: boom\n    at send"}`),
	}

	listener.OnFunctionFinished(context.Background(), md, item, evts, resp)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 1)

	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &got))
	require.Equal(t, "ERROR", got["level"])
	require.Equal(t, "function run failed", got["msg"])
	require.Equal(t, FunctionFailedEvent, got["inngest_event"])
	require.Equal(t, md.ID.RunID.String(), got["run_id"])
	require.Equal(t, md.ID.FunctionID.String(), got["function_id"])
	require.Equal(t, "app-send-email", got["function_slug"])
	require.Equal(t, md.ID.Tenant.AppID.String(), got["app_id"])
	require.Equal(t, md.ID.Tenant.EnvID.String(), got["env_id"])
	require.Equal(t, md.Config.EventIDs[0].String(), got["event_id"])
	require.Equal(t, "app/email.requested", got["event_name"])
	require.EqualValues(t, 3, got["attempt"])
	require.EqualValues(t, 4, got["max_attempts"])
	require.Equal(t, errStr, got["error"])
	require.EqualValues(t, 500, got["status_code"])
	require.Equal(t, "send", got["step_name"])
	require.Equal(t, false, got["no_retry"])
	require.Equal(t, "Error", got["error_name"])
	require.Equal(t, "boom", got["error_message"])
	require.Equal(t, "Error: boom\n    at send", got["error_stack"])
}

func TestOnFunctionFinishedWithoutSDKErrorFallsBackToErr(t *testing.T) {
	listener, buf := newTestListener()
	errStr := "Unable to reach SDK URL"

	listener.OnFunctionFinished(
		context.Background(),
		newTestMetadata(),
		queue.Item{},
		nil,
		statev1.DriverResponse{Err: &errStr},
	)

	var got map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got))
	require.Equal(t, errStr, got["error"])
	require.Equal(t, "Error", got["error_name"])
	require.Equal(t, errStr, got["error_message"])
}

func TestOnFunctionFinishedIgnoresSuccess(t *testing.T) {
	listener, buf := newTestListener()

	listener.OnFunctionFinished(
		context.Background(),
		newTestMetadata(),
		queue.Item{},
		nil,
		statev1.DriverResponse{StatusCode: 200, Output: map[string]any{"ok": true}},
	)

	require.Empty(t, buf.String())
}
