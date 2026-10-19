package pubsub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/inngest/inngest/pkg/config"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

const testTopic = "events"

func newTestStreamRedis(t *testing.T) (*miniredis.Miniredis, rueidis.Client) {
	t.Helper()
	r := miniredis.RunT(t)
	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	t.Cleanup(rc.Close)
	return r, rc
}

func newTestStream(t *testing.T, rc rueidis.Client, consumer string, mutate ...func(*config.RedisMessaging)) *redisStream {
	t.Helper()
	conf := &config.RedisMessaging{
		Topic:    testTopic,
		Client:   rc,
		Consumer: consumer,
		MinIdle:  time.Second,
	}
	for _, m := range mutate {
		m(conf)
	}
	c := &config.MessagingService{}
	c.Set(conf)
	ps, err := NewPublishSubscriber(context.Background(), *c)
	require.NoError(t, err)
	s := ps.(*redisStream)
	s.retryDelays = []time.Duration{10 * time.Millisecond}
	s.reclaimInterval = 100 * time.Millisecond
	s.heartbeatInterval = s.minIdle / 3
	return s
}

func subscribe(t *testing.T, s *redisStream, run PerformFunc, concurrency int64) (cancel func(), done <-chan struct{}) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		require.NoError(t, s.SubscribeN(ctx, testTopic, run, concurrency))
	}()
	t.Cleanup(func() {
		cancelFn()
		<-ch
	})
	return cancelFn, ch
}

func streamLen(t *testing.T, rc rueidis.Client, key string) int64 {
	t.Helper()
	n, err := rc.Do(context.Background(), rc.B().Xlen().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	return n
}

func pendingCount(t *testing.T, rc rueidis.Client, key string) int {
	t.Helper()
	p, err := parsePending(rc.Do(context.Background(), rc.B().Xpending().Key(key).Group(streamGroup).Start("-").End("+").Count(1000).Build()))
	require.NoError(t, err)
	return len(p)
}

func msg(i int) Message {
	return Message{Name: "event/received", Data: fmt.Sprintf(`{"n":%d}`, i), Timestamp: time.UnixMilli(int64(i)).UTC()}
}

func TestRedisStreamBatchPublishConsumeAckDeletes(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a")
	ctx := context.Background()

	msgs := make([]Message, 1200)
	for i := range msgs {
		msgs[i] = msg(i)
	}
	require.NoError(t, s.PublishBatch(ctx, testTopic, msgs))
	require.EqualValues(t, len(msgs), streamLen(t, rc, s.streamKey(testTopic)))

	var mu sync.Mutex
	seen := map[string]int{}
	subscribe(t, s, func(_ context.Context, m Message) error {
		mu.Lock()
		seen[m.Data]++
		mu.Unlock()
		return nil
	}, 50)

	require.Eventually(t, func() bool {
		return streamLen(t, rc, s.streamKey(testTopic)) == 0
	}, 10*time.Second, 20*time.Millisecond)
	require.Zero(t, pendingCount(t, rc, s.streamKey(testTopic)))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, len(msgs))
	for _, m := range msgs {
		require.Equal(t, 1, seen[m.Data])
	}
}

func TestRedisStreamDeliversEntriesPublishedBeforeSubscribe(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	// Written before any consumer group exists, as after a group was lost.
	body, err := msg(1).Encode()
	require.NoError(t, err)
	require.NoError(t, rc.Do(context.Background(), rc.B().Xadd().Key(defaultStreamKeyPrefix+":{"+testTopic+"}").Id("*").
		FieldValue().FieldValue(streamFieldMessage, string(body)).Build()).Error())

	got := make(chan Message, 1)
	subscribe(t, newTestStream(t, rc, "runner"), func(_ context.Context, m Message) error {
		got <- m
		return nil
	}, 1)

	select {
	case m := <-got:
		require.Equal(t, msg(1).Data, m.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("entry published before the subscriber was never delivered")
	}
}

func TestRedisStreamFailedHandlerStaysPendingAndIsReclaimed(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.MinIdle = 300 * time.Millisecond })
	require.NoError(t, s.Publish(context.Background(), testTopic, msg(1)))

	var calls atomic.Int32
	subscribe(t, s, func(_ context.Context, m Message) error {
		if calls.Add(1) <= 2 {
			return errors.New("transient")
		}
		return nil
	}, 1)

	// Both in-process attempts fail; the reclaimer redelivers the entry after minIdle.
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, pendingCount(t, rc, s.streamKey(testTopic)))
	require.Eventually(t, func() bool {
		return calls.Load() == 3 && streamLen(t, rc, s.streamKey(testTopic)) == 0
	}, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, pendingCount(t, rc, s.streamKey(testTopic)))
}

func TestRedisStreamMaxDeliveriesMovesToDeadLetter(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) {
		c.MinIdle = 100 * time.Millisecond
		c.MaxDeliveries = 3
	})
	require.NoError(t, s.Publish(context.Background(), testTopic, msg(1)))

	var calls atomic.Int32
	subscribe(t, s, func(context.Context, Message) error {
		calls.Add(1)
		return errors.New("permanent")
	}, 1)

	require.Eventually(t, func() bool {
		return streamLen(t, rc, s.deadLetterKey(testTopic)) == 1
	}, 10*time.Second, 20*time.Millisecond)
	require.Zero(t, streamLen(t, rc, s.streamKey(testTopic)))
	require.Zero(t, pendingCount(t, rc, s.streamKey(testTopic)))

	entries, err := rc.Do(context.Background(), rc.B().Xrange().Key(s.deadLetterKey(testTopic)).Start("-").End("+").Build()).AsXRange()
	require.NoError(t, err)
	require.Equal(t, "max_deliveries", entries[0].FieldValues["reason"])
	decoded := Message{}
	require.NoError(t, decoded.Decode([]byte(entries[0].FieldValues[streamFieldMessage])))
	require.Equal(t, msg(1).Data, decoded.Data)
}

func TestRedisStreamDecodeErrorMovesToDeadLetter(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a")
	key := s.streamKey(testTopic)
	require.NoError(t, rc.Do(context.Background(), rc.B().Xadd().Key(key).Id("*").FieldValue().FieldValue(streamFieldMessage, "{not json").Build()).Error())

	var calls atomic.Int32
	subscribe(t, s, func(context.Context, Message) error {
		calls.Add(1)
		return nil
	}, 1)

	require.Eventually(t, func() bool {
		return streamLen(t, rc, s.deadLetterKey(testTopic)) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, streamLen(t, rc, key))
	require.Zero(t, pendingCount(t, rc, key))
	require.Zero(t, calls.Load())
}

func TestRedisStreamShutdownWaitsAndLeavesUnfinishedPending(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.DrainTimeout = 300 * time.Millisecond })
	ctx := context.Background()
	require.NoError(t, s.PublishBatch(ctx, testTopic, []Message{msg(1), msg(2)}))

	release := make(chan struct{})
	var started, finished atomic.Int32
	cancel, done := subscribe(t, s, func(_ context.Context, m Message) error {
		started.Add(1)
		if m.Data == msg(2).Data {
			<-release
		} else {
			time.Sleep(100 * time.Millisecond)
		}
		finished.Add(1)
		return nil
	}, 2)
	defer close(release)

	require.Eventually(t, func() bool { return started.Load() == 2 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	<-done

	// The quick handler was acked during the drain; the stuck one stays pending.
	require.EqualValues(t, 1, finished.Load())
	require.EqualValues(t, 1, streamLen(t, rc, s.streamKey(testTopic)))
	require.Equal(t, 1, pendingCount(t, rc, s.streamKey(testTopic)))
}

func TestRedisStreamRestartRetriesOwnPending(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "pod-a", func(c *config.RedisMessaging) { c.MinIdle = time.Hour })
	key := s.streamKey(testTopic)
	require.NoError(t, s.Publish(context.Background(), testTopic, msg(1)))

	// Simulate a crash: the entry is delivered to pod-a and never acked.
	_, err := parseStreamRead(rc.Do(context.Background(), rc.B().Xreadgroup().Group(streamGroup, "pod-a").Count(10).Streams().Key(key).Id(">").Build()), key)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount(t, rc, key))

	got := make(chan Message, 1)
	subscribe(t, newTestStream(t, rc, "pod-a", func(c *config.RedisMessaging) { c.MinIdle = time.Hour }), func(_ context.Context, m Message) error {
		got <- m
		return nil
	}, 1)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("own pending entry was not retried on restart")
	}
	require.Eventually(t, func() bool { return streamLen(t, rc, key) == 0 }, 5*time.Second, 10*time.Millisecond)
}

func TestRedisStreamHeartbeatPreventsReclaim(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	minIdle := 300 * time.Millisecond
	a := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.MinIdle = minIdle })
	require.NoError(t, a.Publish(context.Background(), testTopic, msg(1)))

	release := make(chan struct{})
	var aCalls, bCalls atomic.Int32
	subscribe(t, a, func(context.Context, Message) error {
		aCalls.Add(1)
		<-release
		return nil
	}, 1)
	require.Eventually(t, func() bool { return aCalls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	b := newTestStream(t, rc, "b", func(c *config.RedisMessaging) { c.MinIdle = minIdle })
	subscribe(t, b, func(context.Context, Message) error {
		bCalls.Add(1)
		return nil
	}, 1)

	// Outlast several minIdle windows plus two of B's blocking reads, which hold B's slots.
	time.Sleep(4*minIdle + 2*streamReadBlock)
	require.Zero(t, bCalls.Load())
	close(release)
	require.Eventually(t, func() bool { return streamLen(t, rc, a.streamKey(testTopic)) == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, bCalls.Load())
}

func TestRedisStreamStalledHandlerIsReclaimed(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	minIdle := 200 * time.Millisecond
	a := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.MinIdle = minIdle })
	// A stalled process sends no heartbeats.
	a.heartbeatInterval = time.Hour
	require.NoError(t, a.Publish(context.Background(), testTopic, msg(1)))

	stall := make(chan struct{})
	defer close(stall)
	var aCalls atomic.Int32
	subscribe(t, a, func(context.Context, Message) error {
		aCalls.Add(1)
		<-stall
		return nil
	}, 1)
	require.Eventually(t, func() bool { return aCalls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	got := make(chan Message, 1)
	subscribe(t, newTestStream(t, rc, "b", func(c *config.RedisMessaging) { c.MinIdle = minIdle }), func(_ context.Context, m Message) error {
		got <- m
		return nil
	}, 1)
	select {
	case m := <-got:
		require.Equal(t, msg(1).Data, m.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("entry held by a stalled consumer was not reclaimed")
	}
	require.Eventually(t, func() bool { return streamLen(t, rc, a.streamKey(testTopic)) == 0 }, 5*time.Second, 10*time.Millisecond)
}

// The restart pass retries more own pending entries than one XPENDING page holds.
func TestRedisStreamReclaimPagesThroughPending(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "b", func(c *config.RedisMessaging) { c.MinIdle = time.Hour })
	s.reclaimInterval = time.Hour
	ctx := context.Background()
	key := s.streamKey(testTopic)

	msgs := make([]Message, streamReadCount+50)
	for i := range msgs {
		msgs[i] = msg(i)
	}
	require.NoError(t, s.PublishBatch(ctx, testTopic, msgs))
	_, err := parseStreamRead(rc.Do(ctx, rc.B().Xreadgroup().Group(streamGroup, "b").Count(int64(len(msgs))).Streams().Key(key).Id(">").Build()), key)
	require.NoError(t, err)

	var handled atomic.Int32
	subscribe(t, s, func(context.Context, Message) error {
		handled.Add(1)
		return nil
	}, int64(len(msgs)))
	require.Eventually(t, func() bool { return streamLen(t, rc, key) == 0 }, 10*time.Second, 20*time.Millisecond)
	require.EqualValues(t, len(msgs), handled.Load())
}

func TestRedisStreamAckScriptLeavesNoOrphan(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a")
	ctx := context.Background()
	key := s.streamKey(testTopic)
	require.NoError(t, s.PublishBatch(ctx, testTopic, []Message{msg(1), msg(2), msg(3)}))

	entries, err := parseStreamRead(rc.Do(ctx, rc.B().Xreadgroup().Group(streamGroup, "a").Count(10).Streams().Key(key).Id(">").Build()), key)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	acked, err := streamAckScript.Exec(ctx, rc, []string{key}, []string{streamGroup, entries[0].ID, entries[1].ID}).AsInt64()
	require.NoError(t, err)
	require.EqualValues(t, 2, acked)

	// Acked entries leave both the stream and the PEL; the unacked one stays in both.
	require.EqualValues(t, 1, streamLen(t, rc, key))
	require.Equal(t, 1, pendingCount(t, rc, key))
	left, err := rc.Do(ctx, rc.B().Xrange().Key(key).Start("-").End("+").Build()).AsXRange()
	require.NoError(t, err)
	require.Equal(t, entries[2].ID, left[0].ID)
}

func TestRedisStreamDeadLetterSkipsLiveOwner(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.MinIdle = time.Hour })
	ctx := context.Background()
	key := s.streamKey(testTopic)
	require.NoError(t, s.Publish(ctx, testTopic, msg(1)))
	entries, err := parseStreamRead(rc.Do(ctx, rc.B().Xreadgroup().Group(streamGroup, "owner").Count(1).Streams().Key(key).Id(">").Build()), key)
	require.NoError(t, err)

	res, err := streamDeadLetterScript.Exec(ctx, rc, []string{key, s.deadLetterKey(testTopic)},
		[]string{streamGroup, entries[0].ID, "a", fmt.Sprintf("%d", time.Hour.Milliseconds()), "max_deliveries"}).AsInt64()
	require.NoError(t, err)
	require.EqualValues(t, -1, res)
	require.EqualValues(t, 1, streamLen(t, rc, key))
	require.Zero(t, streamLen(t, rc, s.deadLetterKey(testTopic)))
}

func TestRedisStreamReplayAgeMovesRedeliveryToDeadLetter(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) {
		c.MinIdle = 100 * time.Millisecond
		c.MaxReplayAge = time.Minute
	})
	ctx := context.Background()
	key := s.streamKey(testTopic)
	body, err := msg(1).Encode()
	require.NoError(t, err)
	old := fmt.Sprintf("%d-0", time.Now().Add(-time.Hour).UnixMilli())
	require.NoError(t, rc.Do(ctx, rc.B().Xadd().Key(key).Id(old).FieldValue().FieldValue(streamFieldMessage, string(body)).Build()).Error())
	// First delivery to a consumer that crashed.
	_, err = parseStreamRead(rc.Do(ctx, rc.B().Xreadgroup().Group(streamGroup, "crashed").Count(1).Streams().Key(key).Id(">").Build()), key)
	require.NoError(t, err)

	var calls atomic.Int32
	subscribe(t, s, func(context.Context, Message) error {
		calls.Add(1)
		return nil
	}, 1)
	require.Eventually(t, func() bool {
		return streamLen(t, rc, s.deadLetterKey(testTopic)) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, calls.Load())
	require.Zero(t, streamLen(t, rc, key))
}

func TestClassifyStreamError(t *testing.T) {
	require.ErrorIs(t, classifyStreamError(errors.New("OOM command not allowed when used memory > 'maxmemory'.")), ErrBackpressure)
	require.ErrorIs(t, classifyStreamError(errors.New("dial tcp: connection refused")), ErrUnavailable)
}

// freezeProxy relays TCP to addr until frozen, then stops relaying without
// closing, like a Redis that hangs.
type freezeProxy struct {
	ln     net.Listener
	frozen atomic.Bool
}

func newFreezeProxy(t *testing.T, addr string) *freezeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &freezeProxy{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", addr)
			if err != nil {
				_ = client.Close()
				continue
			}
			go p.relay(client, server)
			go p.relay(server, client)
		}
	}()
	return p
}

func (p *freezeProxy) relay(from, to net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := from.Read(buf)
		if err != nil {
			return
		}
		for p.frozen.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := to.Write(buf[:n]); err != nil {
			return
		}
	}
}

// A Redis that stops answering mid-read must not stop shutdown: SubscribeN
// returns within the drain timeout.
func TestRedisStreamShutdownWithinDrainTimeoutWhenRedisHangs(t *testing.T) {
	r := miniredis.RunT(t)
	proxy := newFreezeProxy(t, r.Addr())
	rc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{proxy.ln.Addr().String()}, DisableCache: true})
	require.NoError(t, err)
	t.Cleanup(rc.Close)

	drain := 500 * time.Millisecond
	s := newTestStream(t, rc, "a", func(c *config.RedisMessaging) { c.DrainTimeout = drain })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.SubscribeN(ctx, testTopic, func(context.Context, Message) error { return nil }, 1)
	}()

	time.Sleep(200 * time.Millisecond)
	proxy.frozen.Store(true)
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case <-done:
		require.Less(t, time.Since(start), drain+500*time.Millisecond)
	case <-time.After(15 * time.Second):
		t.Fatal("SubscribeN never returned while redis was hung")
	}
	proxy.frozen.Store(false)
}
