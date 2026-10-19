package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/coreapi/apiutil"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/eventstream"
	"github.com/inngest/inngest/pkg/headers"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/publicerr"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	itrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Constants set here are leveraged by start, dev, and, healthcheck commands
const (
	DefaultAPIPort = 8288
	HealthPath     = "/health"
	// LivenessPath always answers 200 so a liveness probe never depends on
	// the event stream backend.
	LivenessPath = "/health/live"

	// shutdownTimeout lets in-flight requests finish publishing on shutdown.
	shutdownTimeout = 20 * time.Second
)

type EventHandler func(context.Context, *event.Event, *event.SeededID) (string, error)

// EventIngester publishes all of a request's events in one batch after every
// event parsed, so a 200 is only written once all of them are published.
type EventIngester interface {
	PrepareEvent(ctx context.Context, e *event.Event, seed *event.SeededID) (string, pubsub.Message, error)
	PublishEvents(ctx context.Context, msgs []pubsub.Message) error
	// Admit rejects a request up front while the stream is unavailable or full.
	Admit() error
	// Ready reports whether the stream backend is reachable.
	Ready() bool
}

type Options struct {
	Config config.Config

	EventHandler EventHandler
	Logger       logger.Logger

	// Ingester, when set, replaces EventHandler for the event endpoint.
	Ingester EventIngester

	// MaxRequestBytes caps the body of a single event request; 0 disables.
	MaxRequestBytes int64

	// LocalEventKeys are the keys used to send events to the local event API
	// from an app. If this is set, only keys that match one of these values
	// will be accepted.
	LocalEventKeys []string

	// RequireKeys defines whether event and signing keys are required for the
	// server to function. If this is true and signing keys are not defined,
	// the server will still boot but core actions such as syncing, runs, and
	// ingesting events will not work.
	RequireKeys bool
}

func NewAPI(o Options) (chi.Router, error) {
	logger := o.Logger.With("caller", "api")

	api := &API{
		Router:          chi.NewMux(),
		config:          o.Config,
		handler:         o.EventHandler,
		ingester:        o.Ingester,
		maxRequestBytes: o.MaxRequestBytes,
		log:             logger,
		localEventKeys:  o.LocalEventKeys,
		requireKeys:     o.RequireKeys,
		shuttingDown:    &atomic.Bool{},
	}

	cors := cors.New(cors.Options{
		AllowOriginFunc:  func(r *http.Request, origin string) bool { return true },
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		ExposedHeaders:   []string{"Link", "Mcp-Session-Id"},
		AllowCredentials: false,
		MaxAge:           60 * 60, // 1 hour
	})
	api.Use(cors.Handler)
	api.Use(headers.StaticHeadersMiddleware(o.Config.GetServerKind()))

	api.Get(HealthPath, api.HealthCheck)
	api.Get(LivenessPath, api.LivenessCheck)
	api.Post("/e/{key}", api.ReceiveEvent)
	api.Post("/invoke/{slug}", api.Invoke)

	return api, nil
}

type API struct {
	chi.Router

	config config.Config

	handler  EventHandler
	ingester EventIngester
	log      logger.Logger

	maxRequestBytes int64

	server *http.Server

	// shuttingDown fails readiness once shutdown starts.
	shuttingDown *atomic.Bool

	// localEventKeys are the keys used to send events to the local event API
	// from an app. If this is set, only keys that match one of these values
	// will be accepted.
	localEventKeys []string

	// requireKeys defines whether event and signing keys are required for the
	// server to function. If this is true and signing keys are not defined,
	// the server will still boot but core actions such as syncing, runs, and
	// ingesting events will not work.
	requireKeys bool
}

func (a *API) AddRoutes() {
}

func (a *API) Start(ctx context.Context) error {
	a.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", a.config.EventAPI.Addr, a.config.EventAPI.Port),
		Handler: a.Router,
	}
	a.log.Info("starting server", "addr", a.server.Addr)

	lerrChan := make(chan error)
	go func() {
		lerrChan <- a.server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		a.log.Info("shutting down server")
		a.shuttingDown.Store(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return a.server.Shutdown(shutdownCtx)
	case err := <-lerrChan:
		return err
	}
}

func (a API) Stop(ctx context.Context) error {
	if a.server == nil {
		return nil
	}

	return a.server.Shutdown(ctx)
}

// HealthCheck is the readiness check: it fails while shutting down or while
// the event stream backend is unreachable.
func (a API) HealthCheck(w http.ResponseWriter, r *http.Request) {
	a.log.Trace("healthcheck")
	w.Header().Add("Content-Type", "application/json")
	if a.shuttingDown != nil && a.shuttingDown.Load() {
		a.writeResponse(w, apiResponse{StatusCode: http.StatusServiceUnavailable, Error: "Shutting down"})
		return
	}
	if a.ingester != nil && !a.ingester.Ready() {
		a.writeResponse(w, apiResponse{StatusCode: http.StatusServiceUnavailable, Error: "Event stream unavailable"})
		return
	}
	a.writeResponse(w, apiResponse{
		StatusCode: http.StatusOK,
		Message:    "OK",
	})
}

func (a API) LivenessCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	a.writeResponse(w, apiResponse{StatusCode: http.StatusOK, Message: "OK"})
}

func (a API) ReceiveEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()

	// If self hosting and keys are not defined, error.
	if a.requireKeys && len(a.localEventKeys) == 0 {
		a.log.Error("rejecting event; event keys are required to process events securely")
		w.Header().Add("Content-Type", "application/json")
		a.writeResponse(w, apiResponse{
			StatusCode: http.StatusServiceUnavailable,
			Error:      "Event keys are required to process events securely",
		})
		return
	}

	key := chi.URLParam(r, "key")
	if key == "" {
		a.log.Error("rejecting event; event key is required")
		w.Header().Add("Content-Type", "application/json")
		a.writeResponse(w, apiResponse{
			StatusCode: http.StatusUnauthorized,
			Error:      "Event key is required",
		})

		return
	}

	if len(a.localEventKeys) > 0 {
		var found bool
		for _, k := range a.localEventKeys {
			if k == key {
				found = true
				break
			}
		}

		if !found {
			a.log.Error("rejecting event; event key not recognized")
			w.Header().Add("Content-Type", "application/json")
			a.writeResponse(w, apiResponse{
				StatusCode: http.StatusUnauthorized,
				Error:      "Event key not found",
			})
			return
		}
	}

	if a.maxRequestBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, a.maxRequestBytes)
	}

	if a.ingester != nil {
		if err := a.ingester.Admit(); err != nil {
			a.log.Warn("rejecting events; event stream cannot accept them", "error", err)
			writeEventsUnavailable(w, nil, err)
			return
		}
	}

	ctx, cancel := context.WithCancel(ctx)

	// Create a new trace that may have a link to a previous one
	ctx = itrace.UserTracer().Propagator().Extract(ctx, propagation.HeaderCarrier(r.Header))

	// Create a new channel which receives a stream of events from the incoming HTTP request
	stream := make(chan eventstream.StreamItem)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return eventstream.ParseStream(
			ctx,
			r.Body,
			stream,
			consts.AbsoluteMaxEventSize,
			r.Header.Get("Content-Type"),
		)
	})

	// Create a new channel which holds all event IDs as a slice.
	var (
		max    int
		ids    = make([]string, consts.MaxEvents)
		idChan = make(chan struct {
			int
			string
		})
	)
	eg.Go(func() error {
		for item := range idChan {
			if max < item.int {
				max = item.int
			}
			ids[item.int] = item.string
		}
		return nil
	})

	// msgs collects the prepared events an ingester publishes after parsing.
	var msgs []pubsub.Message

	// Process those incoming events
	eg.Go(func() error {
		// Close the idChan so that we stop appending to the ID slice.
		defer close(idChan)

		index := 0
		for s := range stream {
			index++
			evt := event.Event{}
			if err := json.Unmarshal(s.Item, &evt); err != nil {
				return err
			}

			ts := time.Now()
			sessionsMetrics, err := event.NormalizeInbound(ctx, &evt, ts)
			if err != nil {
				return err
			}
			metrics.IncrEventSessionsResolvedCounter(
				ctx,
				"ingest",
				sessionsMetrics.Manual,
				sessionsMetrics.Propagated,
				sessionsMetrics.Nulling,
				metrics.CounterOpt{PkgName: metricsPkgName},
			)

			ctx, span := itrace.UserTracer().Provider().
				Tracer(consts.OtelScopeEvent).
				Start(ctx, consts.OtelSpanEvent,
					trace.WithTimestamp(ts),
					trace.WithNewRoot(),
					trace.WithLinks(trace.LinkFromContext(ctx)),
				)
			defer span.End()

			seed := event.SeededIDFromString(
				r.Header.Get(headers.HeaderEventIDSeed),
				index,
			)
			var id string
			if a.ingester != nil {
				var m pubsub.Message
				id, m, err = a.ingester.PrepareEvent(ctx, &evt, seed)
				msgs = append(msgs, m)
			} else {
				id, err = a.handler(ctx, &evt, seed)
			}
			if err != nil {
				a.log.Error("error handling event", "error", err, "event", evt.Name)
				return err
			}
			idChan <- struct {
				int
				string
			}{s.N, id}
		}

		return nil
	})

	err := eg.Wait()
	cancel()

	if max+1 > len(ids) {
		max = len(ids) - 1
	}

	if err != nil {
		status := 400
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		respIDs := ids[0 : max+1]
		if a.ingester != nil {
			// Nothing was published, so no ID is valid.
			respIDs = []string{}
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(apiutil.EventAPIResponse{
			IDs:    respIDs,
			Status: status,
			Error:  err.Error(),
		})

		return
	}

	if a.ingester != nil && len(msgs) > 0 {
		if err := a.ingester.PublishEvents(r.Context(), msgs); err != nil {
			a.log.Error("error publishing events", "error", err, "count", len(msgs))
			writeEventsUnavailable(w, ids[0:max+1], err)
			return
		}
	}

	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(apiutil.EventAPIResponse{
		IDs:    ids[0 : max+1],
		Status: 200,
	})
}

// writeEventsUnavailable answers 503 so SDKs retry the send; their retry reuses
// the ID seed, so events already published are deduplicated downstream.
func writeEventsUnavailable(w http.ResponseWriter, ids []string, err error) {
	if ids == nil {
		ids = []string{}
	}
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(apiutil.EventAPIResponse{
		IDs:    ids,
		Status: http.StatusServiceUnavailable,
		Error:  err.Error(),
	})
}

// Invoke creates an event to invoke a specific function.
func (a API) Invoke(w http.ResponseWriter, r *http.Request) {
	// XXX: In OSS self hosting, check signing keys here.

	// Get the function slug from the route parameter.   This is the function
	// we'll invoke.  Any request is passed as the event data to the function.
	slug := chi.URLParam(r, "slug")
	if slug == "" {
		_ = publicerr.WriteHTTP(w, publicerr.Errorf(400, "Function slug is required"))
		return
	}
	slug, err := url.QueryUnescape(slug)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrap(err, 400, "Unable to decode function slug"))
		return
	}

	rawEvt := event.Event{}
	if err := json.NewDecoder(r.Body).Decode(&rawEvt); err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrap(err, 400, "Unable to read post data request"))
		return
	}

	newInvOpts := event.NewInvocationEventOpts{
		Event: rawEvt,
		FnID:  slug,
	}
	evt := event.NewInvocationEvent(newInvOpts)

	// Merge the two session layers before the event is handled. The merge
	// reports the pre-merge state of both layers for adoption metrics.
	sessionsMetrics := evt.Event.Meta.ResolveSessions()
	metrics.IncrEventSessionsResolvedCounter(
		r.Context(),
		"invoke_http",
		sessionsMetrics.Manual,
		sessionsMetrics.Propagated,
		sessionsMetrics.Nulling,
		metrics.CounterOpt{PkgName: metricsPkgName},
	)

	// Validate after the merge
	if err := evt.Event.Validate(r.Context()); err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrap(err, 400, "Invalid invocation event"))
		return
	}

	seed := event.SeededIDFromString(
		r.Header.Get(headers.HeaderEventIDSeed),
		0,
	)
	evtID, err := a.handler(r.Context(), &evt.Event, seed)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(err, 500, "Unable to create invocation event: %s", err))
		return
	}

	// TODO: If await is true as a query parameter, await the data from the function.
	_ = json.NewEncoder(w).Encode(apiutil.InvokeAPIResponse{
		ID:     evtID,
		Status: 200,
	})
}
