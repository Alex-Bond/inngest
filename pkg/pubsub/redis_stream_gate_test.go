package pubsub

import (
	"context"
	"testing"

	"github.com/inngest/inngest/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRedisStreamBacklogCapRejects(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "api", func(c *config.RedisMessaging) { c.MaxBacklog = 5 })
	ctx := context.Background()

	s.sample(ctx)
	require.NoError(t, s.Admit())
	require.True(t, s.Ready())

	msgs := make([]Message, 10)
	for i := range msgs {
		msgs[i] = msg(i)
	}
	require.NoError(t, s.PublishBatch(ctx, testTopic, msgs))
	s.sample(ctx)
	require.ErrorIs(t, s.Admit(), ErrBackpressure)
}

func TestRedisStreamUnreachableIsUnavailable(t *testing.T) {
	r, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "api")
	r.Close()

	s.sample(context.Background())
	require.False(t, s.Ready())
	require.ErrorIs(t, s.Admit(), ErrUnavailable)
	require.ErrorIs(t, s.Publish(context.Background(), testTopic, msg(1)), ErrUnavailable)
}

func TestRedisStreamMemoryAdmission(t *testing.T) {
	_, rc := newTestStreamRedis(t)
	s := newTestStream(t, rc, "api", func(c *config.RedisMessaging) { c.MaxMemoryRatio = 0.7 })
	s.sampled.Store(true)
	s.ready.Store(true)

	used, limit := parseMemoryInfo("# Memory\r\nused_memory:800\r\nused_memory_human:800B\r\nmaxmemory:1000\r\nmaxmemory_policy:noeviction\r\n")
	require.EqualValues(t, 800, used)
	require.EqualValues(t, 1000, limit)

	s.usedMemory.Store(used)
	s.maxMemory.Store(limit)
	require.ErrorIs(t, s.Admit(), ErrBackpressure)

	s.usedMemory.Store(600)
	require.NoError(t, s.Admit())

	// maxmemory 0 means unlimited, which disables the check.
	s.usedMemory.Store(1 << 40)
	s.maxMemory.Store(0)
	require.NoError(t, s.Admit())
}
