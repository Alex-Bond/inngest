package pubsub

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

const (
	streamReadCount       = 100
	streamReadBlock       = 2 * time.Second
	streamReadGrace       = 3 * time.Second
	streamReclaimInterval = 30 * time.Second
	streamReadMaxBackoff  = 5 * time.Second
	streamAckAttempts     = 3
	streamAckRetryDelay   = 100 * time.Millisecond
)

// streamHandlerRetryDelays are the waits between in-process handler attempts;
// after the last attempt the entry stays pending for the reclaimer.
var streamHandlerRetryDelays = []time.Duration{200 * time.Millisecond, time.Second, 5 * time.Second}

// streamConsumer is one subscription's read loop, reclaimer and heartbeat.
type streamConsumer struct {
	s      *redisStream
	key    string
	dlqKey string
	run    PerformFunc

	// slots bounds in-flight handlers and is taken before reading, so nothing read waits.
	slots chan struct{}
	wg    sync.WaitGroup

	mu       sync.Mutex
	inflight map[string]struct{}

	// subCtx is cancelled on shutdown; handlerCtx never is, so handlers finish.
	subCtx     context.Context
	handlerCtx context.Context
}

type pendingEntry struct {
	id         string
	consumer   string
	idle       time.Duration
	deliveries int64
}

func (c *streamConsumer) consume() {
	l := c.s.log.With("stream", c.key, "consumer", c.s.consumer)
	l.Info("consuming event stream")

	heartbeatCtx, stopHeartbeat := context.WithCancel(c.handlerCtx)
	defer stopHeartbeat()
	go c.heartbeat(heartbeatCtx)

	// Entries this consumer held before a restart are retried first.
	c.reclaim(true)

	reclaimDone := make(chan struct{})
	go func() {
		defer close(reclaimDone)
		ticker := time.NewTicker(c.s.reclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.subCtx.Done():
				return
			case <-ticker.C:
				c.reclaim(false)
			}
		}
	}()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		c.readLoop()
	}()

	<-c.subCtx.Done()
	// The drain budget starts at shutdown and also covers a last read held by a hung Redis.
	deadline := time.After(c.s.drainTimeout)
	done := make(chan struct{})
	go func() {
		<-readDone
		<-reclaimDone
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		l.Info("event stream drained")
	case <-deadline:
		l.Warn("event stream drain timed out; unfinished entries stay pending", "in_flight", c.inflightCount())
	}
}

func (c *streamConsumer) readLoop() {
	backoff := time.Duration(0)
	for c.subCtx.Err() == nil {
		if !c.acquire() {
			return
		}
		n := 1 + c.acquireMore(streamReadCount-1)

		// Bounded (rueidis sets no timeout on blocking reads); a timed-out read's entries stay in our PEL for the reclaimer.
		cmd := c.s.rc.B().Xreadgroup().Group(streamGroup, c.s.consumer).
			Count(int64(n)).Block(streamReadBlock.Milliseconds()).
			Streams().Key(c.key).Id(">").Build()
		readCtx, cancel := context.WithTimeout(context.Background(), streamReadBlock+streamReadGrace)
		entries, err := parseStreamRead(c.s.rc.Do(readCtx, cmd), c.key)
		cancel()
		if err != nil {
			c.release(n)
			if rueidis.IsRedisNil(err) {
				backoff = 0
				continue
			}
			if strings.Contains(err.Error(), "NOGROUP") {
				c.s.groups.Delete(c.key)
				_ = c.s.ensureGroup(c.handlerCtx, c.key)
			}
			backoff = min(max(2*backoff, 100*time.Millisecond), streamReadMaxBackoff)
			c.s.log.Error("error reading event stream", "error", err, "retry_in", backoff)
			select {
			case <-c.subCtx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0

		c.release(n - len(entries))
		for _, e := range entries {
			c.dispatch(e, 1)
		}
	}
}

// reclaim claims this consumer's pending entries when own is set, otherwise any idle
// for minIdle; entries delivered too often are dead-lettered instead.
func (c *streamConsumer) reclaim(own bool) {
	start := "-"
	for c.subCtx.Err() == nil {
		b := c.s.rc.B().Xpending().Key(c.key).Group(streamGroup)
		var cmd rueidis.Completed
		if own {
			cmd = b.Start(start).End("+").Count(streamReadCount).Consumer(c.s.consumer).Build()
		} else {
			cmd = b.Idle(c.s.minIdle.Milliseconds()).Start(start).End("+").Count(streamReadCount).Build()
		}
		pending, err := parsePending(c.s.rc.Do(c.subCtx, cmd))
		if err != nil {
			if c.subCtx.Err() == nil {
				c.s.log.Error("error listing pending event stream entries", "error", err)
			}
			return
		}
		if len(pending) == 0 {
			return
		}

		for _, p := range pending {
			if c.isInflight(p.id) {
				continue
			}
			if p.deliveries >= c.s.maxDeliveries {
				c.deadLetter(p.id, "max_deliveries")
				continue
			}
			if !c.acquire() {
				return
			}
			minIdle := c.s.minIdle
			if own {
				minIdle = 0
			}
			// XCLAIM re-checks idle time, so an entry a heartbeat just refreshed is left alone.
			claimed, err := parseClaim(c.s.rc.Do(c.subCtx, c.s.rc.B().Xclaim().Key(c.key).Group(streamGroup).
				Consumer(c.s.consumer).MinIdleTime(strconv.FormatInt(minIdle.Milliseconds(), 10)).
				Id(p.id).Build()))
			if err != nil || len(claimed) == 0 {
				c.release(1)
				if err != nil && c.subCtx.Err() == nil {
					c.s.log.Error("error claiming event stream entry", "error", err, "id", p.id)
				}
				continue
			}
			c.s.log.Info("reclaimed event stream entry", "id", p.id, "from", p.consumer, "deliveries", p.deliveries+1)
			c.dispatch(claimed[0], p.deliveries+1)
		}
		start = nextStreamID(pending[len(pending)-1].id)
	}
}

func (c *streamConsumer) dispatch(e rueidis.XRangeEntry, deliveries int64) {
	c.mu.Lock()
	c.inflight[e.ID] = struct{}{}
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.release(1)
		defer func() {
			c.mu.Lock()
			delete(c.inflight, e.ID)
			c.mu.Unlock()
		}()
		c.handle(e, deliveries)
	}()
}

func (c *streamConsumer) handle(e rueidis.XRangeEntry, deliveries int64) {
	raw, ok := e.FieldValues[streamFieldMessage]
	if !ok {
		c.deadLetter(e.ID, "missing_message")
		return
	}
	m := Message{}
	if err := m.Decode([]byte(raw)); err != nil {
		c.s.log.Error("error decoding event stream entry", "error", err, "id", e.ID)
		c.deadLetter(e.ID, "decode")
		return
	}

	// Dedup keys expire, so replaying past that horizon could run functions twice.
	if deliveries >= 2 && time.Since(streamEntryTime(e.ID)) > c.s.maxReplayAge {
		c.deadLetter(e.ID, "replay_age")
		return
	}

	if err := c.runWithRetry(m); err != nil {
		c.s.log.Warn("event stream handler failed; entry stays pending", "error", err, "id", e.ID, "event", m.Name)
		return
	}
	c.ack(e.ID)
}

func (c *streamConsumer) runWithRetry(m Message) error {
	var err error
	for attempt := 0; ; attempt++ {
		if err = c.safeRun(m); err == nil {
			return nil
		}
		if attempt >= len(c.s.retryDelays) {
			return err
		}
		select {
		case <-c.subCtx.Done():
			// Shutting down: leave the entry pending for the next consumer.
			return err
		case <-time.After(c.s.retryDelays[attempt]):
		}
	}
}

func (c *streamConsumer) safeRun(m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("event stream handler panicked: %v", r)
			c.s.log.Error("event stream handler panicked", "error", r, "stack", string(debug.Stack()))
		}
	}()
	return c.run(c.handlerCtx, m)
}

func (c *streamConsumer) ack(id string) {
	var err error
	for attempt := 0; attempt < streamAckAttempts; attempt++ {
		err = streamAckScript.Exec(c.handlerCtx, c.s.rc, []string{c.key}, []string{streamGroup, id}).Error()
		if err == nil {
			return
		}
		time.Sleep(streamAckRetryDelay)
	}
	c.s.log.Error("error acknowledging event stream entry; it will be redelivered", "error", err, "id", id)
}

func (c *streamConsumer) deadLetter(id, reason string) {
	res, err := streamDeadLetterScript.Exec(
		c.handlerCtx,
		c.s.rc,
		[]string{c.key, c.dlqKey},
		[]string{streamGroup, id, c.s.consumer, strconv.FormatInt(c.s.minIdle.Milliseconds(), 10), reason},
	).AsInt64()
	if err != nil {
		c.s.log.Error("error dead-lettering event stream entry", "error", err, "id", id, "reason", reason)
		return
	}
	if res == 1 {
		c.s.log.Warn("moved event stream entry to the dead-letter stream", "id", id, "reason", reason, "dlq", c.dlqKey)
	}
}

// heartbeat refreshes in-flight entries every minIdle/3 until ctx is done; it
// keeps running through the drain.
func (c *streamConsumer) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(c.s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		c.mu.Lock()
		ids := make([]string, 0, len(c.inflight))
		for id := range c.inflight {
			ids = append(ids, id)
		}
		c.mu.Unlock()

		for start := 0; start < len(ids); start += streamReadCount {
			chunk := ids[start:min(start+streamReadCount, len(ids))]
			args := append([]string{streamGroup, c.s.consumer}, chunk...)
			if err := streamHeartbeatScript.Exec(ctx, c.s.rc, []string{c.key}, args).Error(); err != nil && ctx.Err() == nil {
				c.s.log.Error("error refreshing in-flight event stream entries", "error", err, "count", len(chunk))
			}
		}
	}
}

func (c *streamConsumer) acquire() bool {
	select {
	case c.slots <- struct{}{}:
		return true
	case <-c.subCtx.Done():
		return false
	}
}

func (c *streamConsumer) acquireMore(limit int) int {
	n := 0
	for n < limit {
		select {
		case c.slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func (c *streamConsumer) release(n int) {
	for range n {
		<-c.slots
	}
}

func (c *streamConsumer) isInflight(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.inflight[id]
	return ok
}

func (c *streamConsumer) inflightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight)
}

// nextStreamID returns the smallest ID after id, for paging through XPENDING
// without exclusive ranges.
func nextStreamID(id string) string {
	ms, seq, _ := strings.Cut(id, "-")
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return id
	}
	return fmt.Sprintf("%s-%d", ms, n+1)
}

// streamEntryTime returns the XADD time encoded in an auto-generated entry ID.
func streamEntryTime(id string) time.Time {
	ms, _, _ := strings.Cut(id, "-")
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(n)
}

func parseStreamRead(resp rueidis.RedisResult, key string) ([]rueidis.XRangeEntry, error) {
	streams, err := resp.AsXRead()
	if err != nil {
		return nil, err
	}
	return streams[key], nil
}

func parsePending(resp rueidis.RedisResult) ([]pendingEntry, error) {
	rows, err := resp.ToArray()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]pendingEntry, 0, len(rows))
	for _, row := range rows {
		fields, err := row.ToArray()
		if err != nil || len(fields) != 4 {
			return nil, fmt.Errorf("unexpected XPENDING row: %w", err)
		}
		id, err := fields[0].ToString()
		if err != nil {
			return nil, err
		}
		consumer, err := fields[1].ToString()
		if err != nil {
			return nil, err
		}
		idle, err := fields[2].AsInt64()
		if err != nil {
			return nil, err
		}
		deliveries, err := fields[3].AsInt64()
		if err != nil {
			return nil, err
		}
		out = append(out, pendingEntry{id: id, consumer: consumer, idle: time.Duration(idle) * time.Millisecond, deliveries: deliveries})
	}
	return out, nil
}

// parseClaim skips entries deleted from the stream while still pending.
func parseClaim(resp rueidis.RedisResult) ([]rueidis.XRangeEntry, error) {
	rows, err := resp.ToArray()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]rueidis.XRangeEntry, 0, len(rows))
	for _, row := range rows {
		if row.IsNil() {
			continue
		}
		e, err := row.AsXRangeEntry()
		if err != nil {
			return nil, err
		}
		if e.FieldValues == nil {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}
