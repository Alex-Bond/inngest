package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// fakeDocker stands in for docker compose: failOn names the command that
// fails, and restarts changes the reported start time only when set.
type fakeDocker struct {
	failOn   string
	restarts bool
	started  string
	calls    []string
}

func (f *fakeDocker) install(t *testing.T) {
	prevCompose, prevStarted := dockerCompose, inngestStartedAt
	t.Cleanup(func() { dockerCompose, inngestStartedAt = prevCompose, prevStarted })
	f.started = "t0"
	dockerCompose = func(_ driveOpts, args ...string) error {
		f.calls = append(f.calls, args[0])
		if args[0] == f.failOn {
			return errors.New("docker unavailable")
		}
		if args[0] == "start" && f.restarts {
			f.started = "t1"
		}
		return nil
	}
	inngestStartedAt = func(driveOpts) (string, error) { return f.started, nil }
}

func restartWithBacklog(t *testing.T, backlog int, killBacklog int64) restartResult {
	t.Helper()
	r := miniredis.RunT(t)
	for range backlog {
		_, err := r.XAdd(streamKey, "*", []string{"m", "x"})
		require.NoError(t, err)
	}
	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{r.Addr()}, DisableCache: true})
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(health.Close)

	done := make(chan struct{})
	close(done)
	o := driveOpts{api: health.URL, signal: "SIGKILL", killBacklog: killBacklog}
	return restart(context.Background(), o, rc, done, &atomic.Int64{})
}

func TestRestartFailsWhenDockerFails(t *testing.T) {
	for _, cmd := range []string{"kill", "start"} {
		t.Run(cmd, func(t *testing.T) {
			(&fakeDocker{failOn: cmd, restarts: true}).install(t)
			require.ErrorContains(t, restartWithBacklog(t, 20, 10).err, "docker unavailable")
		})
	}
}

func TestRestartFailsWhenContainerNeverRestarted(t *testing.T) {
	(&fakeDocker{}).install(t)
	require.ErrorContains(t, restartWithBacklog(t, 20, 10).err, "never restarted")
}

func TestRestartFailsWhenBacklogNeverReached(t *testing.T) {
	f := &fakeDocker{restarts: true}
	f.install(t)
	require.ErrorContains(t, restartWithBacklog(t, 5, 10).err, "never above")
	require.Empty(t, f.calls, "the server must not be signalled without a backlog")
}

func TestRestartSucceedsWithBacklogAndNewStart(t *testing.T) {
	f := &fakeDocker{restarts: true}
	f.install(t)
	res := restartWithBacklog(t, 20, 10)
	require.NoError(t, res.err)
	require.Equal(t, []string{"kill", "start"}, f.calls)
}

// A restart that fails while senders still have work must stop them and return,
// not leave them retrying against a stopped server.
func TestDriveReturnsRestartErrorWhileSendersHaveWork(t *testing.T) {
	(&fakeDocker{failOn: "start"}).install(t)
	var killed atomic.Bool
	failing := dockerCompose
	dockerCompose = func(o driveOpts, args ...string) error {
		if args[0] == "kill" {
			killed.Store(true)
		}
		return failing(o, args...)
	}

	r := miniredis.RunT(t)
	for range 20 {
		_, err := r.XAdd(streamKey, "*", []string{"m", "x"})
		require.NoError(t, err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if killed.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(api.Close)

	errc := make(chan error, 1)
	go func() {
		errc <- drive([]string{
			"-api", api.URL, "-redis", r.Addr(), "-postgres", "postgres://u:p@127.0.0.1:1/db",
			"-events", "100000", "-batch", "10", "-concurrency", "2", "-kill-backlog", "10",
		})
	}()
	select {
	case err := <-errc:
		require.ErrorContains(t, err, "restart: docker unavailable")
	case <-time.After(10 * time.Second):
		t.Fatal("drive did not return after the restart failed")
	}
}
