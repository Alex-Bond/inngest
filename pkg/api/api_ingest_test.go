package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/inngest/inngest/pkg/coreapi/apiutil"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/stretchr/testify/require"
)

type fakeIngester struct {
	admitErr   error
	publishErr error
	ready      bool

	prepared  atomic.Int32
	published [][]pubsub.Message
}

func (f *fakeIngester) PrepareEvent(_ context.Context, e *event.Event, _ *event.SeededID) (string, pubsub.Message, error) {
	n := f.prepared.Add(1)
	return fmt.Sprintf("id-%d", n), pubsub.Message{Name: event.EventReceivedName, Data: e.Name}, nil
}

func (f *fakeIngester) PublishEvents(_ context.Context, msgs []pubsub.Message) error {
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, msgs)
	return nil
}

func (f *fakeIngester) Admit() error { return f.admitErr }
func (f *fakeIngester) Ready() bool  { return f.ready }

func postIngest(t *testing.T, ing *fakeIngester, maxBytes int64, body string) *httptest.ResponseRecorder {
	t.Helper()
	a := API{
		ingester:        ing,
		maxRequestBytes: maxBytes,
		log:             logger.StdlibLogger(t.Context()),
	}
	req := httptest.NewRequest(http.MethodPost, "/e/test-key", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("key", "test-key")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	a.ReceiveEvent(w, req)
	return w
}

func decodeEventResponse(t *testing.T, w *httptest.ResponseRecorder) apiutil.EventAPIResponse {
	t.Helper()
	resp := apiutil.EventAPIResponse{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

func TestReceiveEvent_IngesterPublishesOneBatchThenResponds(t *testing.T) {
	ing := &fakeIngester{ready: true}
	w := postIngest(t, ing, 0, `[{"name":"test/a","data":{}},{"name":"test/b","data":{}},{"name":"test/c","data":{}}]`)

	require.Equal(t, 200, w.Code, w.Body.String())
	require.Len(t, ing.published, 1)
	require.Len(t, ing.published[0], 3)
	require.Equal(t, []string{"id-1", "id-2", "id-3"}, decodeEventResponse(t, w).IDs)
}

func TestReceiveEvent_PublishFailureIs503WithRetryAfter(t *testing.T) {
	ing := &fakeIngester{ready: true, publishErr: fmt.Errorf("%w: connection refused", pubsub.ErrUnavailable)}
	w := postIngest(t, ing, 0, `[{"name":"test/a","data":{}}]`)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	require.Equal(t, http.StatusServiceUnavailable, decodeEventResponse(t, w).Status)
}

func TestReceiveEvent_BackpressureIs503BeforeParsing(t *testing.T) {
	ing := &fakeIngester{ready: true, admitErr: fmt.Errorf("%w: backlog 10 exceeds 5", pubsub.ErrBackpressure)}
	w := postIngest(t, ing, 0, `[{"name":"test/a","data":{}}]`)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	require.Zero(t, ing.prepared.Load())
	require.Empty(t, ing.published)
}

func TestReceiveEvent_InvalidEventPublishesNothing(t *testing.T) {
	ing := &fakeIngester{ready: true}
	w := postIngest(t, ing, 0, `[{"name":"test/a","data":{}},{"name":"","data":{}}]`)

	require.Equal(t, 400, w.Code, w.Body.String())
	require.Empty(t, ing.published)
	require.Empty(t, decodeEventResponse(t, w).IDs)
}

func TestReceiveEvent_RequestOverLimitIs413(t *testing.T) {
	ing := &fakeIngester{ready: true}
	body := `[{"name":"test/a","data":{"pad":"` + strings.Repeat("x", 4096) + `"}}]`
	w := postIngest(t, ing, 1024, body)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	require.Empty(t, ing.published)
}

// The size limit landing between two array elements must not publish the
// events read before it.
func TestReceiveEvent_LimitBetweenElementsIs413(t *testing.T) {
	ing := &fakeIngester{ready: true}
	first := `[{"name":"test/a","data":{}}`
	body := first + strings.Repeat(" ", 4096) + `,{"name":"test/b","data":{}}]`
	w := postIngest(t, ing, int64(len(first)+10), body)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	require.Empty(t, ing.published)
}

func TestHealthCheck_Readiness(t *testing.T) {
	ing := &fakeIngester{ready: true}
	a := API{ingester: ing, log: logger.StdlibLogger(t.Context()), shuttingDown: &atomic.Bool{}}

	check := func(h http.HandlerFunc) int {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodGet, HealthPath, nil))
		return w.Code
	}

	require.Equal(t, 200, check(a.HealthCheck))
	ing.ready = false
	require.Equal(t, http.StatusServiceUnavailable, check(a.HealthCheck))
	require.Equal(t, 200, check(a.LivenessCheck))

	ing.ready = true
	a.shuttingDown.Store(true)
	require.Equal(t, http.StatusServiceUnavailable, check(a.HealthCheck))
	require.Equal(t, 200, check(a.LivenessCheck))
}
