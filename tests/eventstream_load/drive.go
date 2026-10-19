package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/rueidis"
)

const streamKey = "inngest:eventstream:{events}"

type driveOpts struct {
	api         string
	redis       string
	postgres    string
	compose     string
	phase       string
	signal      string
	events      int
	batch       int
	concurrency int
	killBacklog int64
}

type sendResult struct {
	latency  time.Duration
	attempts int
	ids      []string
}

// drive sends events like the SDK (one seed per request, retried on non-200),
// restarts the server mid-backlog, waits for the drain, then checks the database.
func drive(args []string) error {
	o := driveOpts{}
	fs := flag.NewFlagSet("drive", flag.ExitOnError)
	fs.StringVar(&o.api, "api", "http://localhost:18288", "inngest server URL")
	fs.StringVar(&o.redis, "redis", "localhost:16379", "valkey address")
	fs.StringVar(&o.postgres, "postgres", "postgres://inngest:inngest@localhost:15432/inngest?sslmode=disable", "postgres URI")
	fs.StringVar(&o.compose, "compose", "docker-compose.yml", "compose file")
	fs.StringVar(&o.phase, "phase", "kill", "event ID prefix for this phase")
	fs.StringVar(&o.signal, "signal", "SIGKILL", "SIGKILL, SIGTERM or none")
	fs.IntVar(&o.events, "events", 50_000, "events to send")
	fs.IntVar(&o.batch, "batch", 500, "events per request")
	fs.IntVar(&o.concurrency, "concurrency", 4, "concurrent senders")
	fs.Int64Var(&o.killBacklog, "kill-backlog", 10_000, "restart the server once the stream holds this many entries")
	_ = fs.Parse(args)

	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{o.redis}, DisableCache: true})
	if err != nil {
		return err
	}
	defer rc.Close()
	db, err := sql.Open("pgx", o.postgres)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	batches := (o.events + o.batch - 1) / o.batch
	work := make(chan int, batches)
	for i := range batches {
		work <- i
	}
	close(work)

	var (
		mu      sync.Mutex
		results []sendResult
		sent    atomic.Int64
		sendEnd time.Time
	)
	// Senders retry until accepted, so a failed restart must cancel them or drive never returns.
	sendCtx, cancelSends := context.WithCancel(ctx)
	defer cancelSends()
	start := time.Now()
	sendersDone := make(chan struct{})
	var wg sync.WaitGroup
	for range o.concurrency {
		wg.Go(func() {
			for b := range work {
				res, ok := sendBatch(sendCtx, o, b)
				if !ok {
					return
				}
				sent.Add(int64(len(res.ids)))
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}
		})
	}
	go func() {
		wg.Wait()
		sendEnd = time.Now()
		close(sendersDone)
	}()

	restarted := restartOnce(ctx, o, rc, sendersDone, &sent)
	var res restartResult
	select {
	case res = <-restarted:
	case <-sendersDone:
		res = <-restarted
	}
	if res.err != nil {
		cancelSends()
		<-sendersDone
		return fmt.Errorf("restart: %w", res.err)
	}
	<-sendersDone
	sendDuration := sendEnd.Sub(start)
	restartInfo := res.info

	if err := waitDrained(ctx, rc, 20*time.Minute); err != nil {
		return err
	}
	drained := time.Since(start)

	acked := make([]string, 0, o.events)
	latencies := make([]time.Duration, 0, len(results))
	retried := 0
	for _, r := range results {
		acked = append(acked, r.ids...)
		latencies = append(latencies, r.latency)
		if r.attempts > 1 {
			retried++
		}
	}
	report, err := verify(ctx, db, rc, o.phase, acked)
	if err != nil {
		return err
	}
	report["phase"] = o.phase
	report["signal"] = o.signal
	report["events_sent"] = o.events
	report["requests"] = len(results)
	report["requests_retried"] = retried
	report["send_p50_ms"] = percentile(latencies, 0.50).Milliseconds()
	report["send_p99_ms"] = percentile(latencies, 0.99).Milliseconds()
	report["send_duration_s"] = sendDuration.Seconds()
	report["drained_after_s"] = drained.Seconds()
	report["restart"] = restartInfo

	out, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(out))
	if report["ok"] != true {
		return fmt.Errorf("phase %s failed verification", o.phase)
	}
	return nil
}

// sendBatch posts batch b until it is accepted; ok is false when ctx ends first.
func sendBatch(ctx context.Context, o driveOpts, b int) (res sendResult, ok bool) {
	from, to := b*o.batch, min((b+1)*o.batch, o.events)
	evts := make([]map[string]any, 0, to-from)
	ids := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		id := fmt.Sprintf("%s-%d", o.phase, i)
		ids = append(ids, id)
		evts = append(evts, map[string]any{"name": eventName, "id": id, "data": map[string]any{"phase": o.phase, "i": i}})
	}
	body, _ := json.Marshal(evts)
	seed := newSeed()

	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, o.api+"/e/"+eventKey, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-inngest-event-id-seed", seed)
		t := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return sendResult{latency: time.Since(t), attempts: attempt, ids: ids}, true
			}
		}
		select {
		case <-ctx.Done():
			return sendResult{}, false
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 2*time.Second)
	}
}

// newSeed returns an x-inngest-event-id-seed value: millis and 10 bytes of
// base64 entropy, reused across retries so they produce the same IDs.
func newSeed() string {
	entropy := make([]byte, 10)
	_, _ = rand.Read(entropy)
	return fmt.Sprintf("%d,%s", time.Now().UnixMilli(), base64.StdEncoding.EncodeToString(entropy))
}

// dockerCompose and inngestStartedAt are variables so tests can stand in for
// Docker.
var (
	dockerCompose    = compose
	inngestStartedAt = containerStartedAt
)

type restartResult struct {
	info map[string]any
	err  error
}

// restartOnce signals and restarts the server once the backlog passes killBacklog,
// failing unless the container demonstrably restarted and became healthy.
func restartOnce(ctx context.Context, o driveOpts, rc rueidis.Client, sendersDone <-chan struct{}, sent *atomic.Int64) <-chan restartResult {
	out := make(chan restartResult, 1)
	if o.signal == "none" {
		out <- restartResult{info: map[string]any{"signal": "none"}}
		return out
	}
	go func() {
		out <- restart(ctx, o, rc, sendersDone, sent)
	}()
	return out
}

func restart(ctx context.Context, o driveOpts, rc rueidis.Client, sendersDone <-chan struct{}, sent *atomic.Int64) restartResult {
	var backlog int64
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
wait:
	for {
		if n, err := rc.Do(ctx, rc.B().Xlen().Key(streamKey).Build()).AsInt64(); err == nil {
			backlog = max(backlog, n)
			if n > o.killBacklog {
				break
			}
		}
		select {
		case <-sendersDone:
			break wait
		case <-ticker.C:
		}
	}

	info := map[string]any{"signal": o.signal, "stream_len_at_signal": backlog, "events_acked_at_signal": sent.Load()}
	if backlog <= o.killBacklog {
		return restartResult{info: info, err: fmt.Errorf("stream backlog peaked at %d, never above %d", backlog, o.killBacklog)}
	}

	startedBefore, err := inngestStartedAt(o)
	if err != nil {
		return restartResult{info: info, err: err}
	}
	t := time.Now()
	if o.signal == "SIGKILL" {
		err = dockerCompose(o, "kill", "-s", "SIGKILL", "inngest")
	} else {
		err = dockerCompose(o, "stop", "-t", "180", "inngest")
	}
	if err != nil {
		return restartResult{info: info, err: err}
	}
	info["stop_s"] = time.Since(t).Seconds()
	if err := dockerCompose(o, "start", "inngest"); err != nil {
		return restartResult{info: info, err: err}
	}

	startedAfter, err := inngestStartedAt(o)
	if err != nil {
		return restartResult{info: info, err: err}
	}
	if startedAfter == startedBefore {
		return restartResult{info: info, err: fmt.Errorf("container start time unchanged (%s); it never restarted", startedAfter)}
	}
	info["started_before"], info["started_after"] = startedBefore, startedAfter
	if err := waitHealthy(o.api, 2*time.Minute); err != nil {
		return restartResult{info: info, err: err}
	}
	return restartResult{info: info}
}

func compose(o driveOpts, args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose", "-f", o.compose}, args...)...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %v: %w", args, err)
	}
	return nil
}

func containerStartedAt(o driveOpts) (string, error) {
	id, err := exec.Command("docker", "compose", "-f", o.compose, "ps", "-a", "-q", "inngest").Output()
	if err != nil {
		return "", fmt.Errorf("finding the inngest container: %w", err)
	}
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.StartedAt}}", strings.TrimSpace(string(id))).Output()
	if err != nil {
		return "", fmt.Errorf("inspecting the inngest container: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func waitHealthy(api string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(api + "/health"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("server not healthy within %s after restart", timeout)
}

// waitDrained waits until the stream holds no entries and nothing is pending.
func waitDrained(ctx context.Context, rc rueidis.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := rc.Do(ctx, rc.B().Xlen().Key(streamKey).Build()).AsInt64()
		if err == nil && n == 0 {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("stream did not drain within %s", timeout)
}

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(int(float64(len(s))*p), len(s)-1)]
}
