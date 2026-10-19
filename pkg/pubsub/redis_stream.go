package pubsub

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/redis/rueidis"
)

const (
	streamGroup        = "runner"
	streamFieldMessage = "m"
	streamFieldName    = "n"

	defaultStreamKeyPrefix     = "inngest:eventstream"
	defaultStreamMinIdle       = 5 * time.Minute
	defaultStreamDrainTimeout  = 45 * time.Second
	defaultStreamMaxDeliveries = 10
	defaultStreamMaxReplayAge  = 55 * time.Minute

	// DefaultStreamConcurrency is the default number of in-flight stream handlers.
	DefaultStreamConcurrency = 200

	// streamPublishChunk* bound one pipelined XADD batch.
	streamPublishChunkEntries = 500
	streamPublishChunkBytes   = 8 << 20
)

//go:embed lua/*.lua
var streamLua embed.FS

var (
	streamAckScript        = mustStreamScript("stream_ack")
	streamDeadLetterScript = mustStreamScript("stream_dead_letter")
	streamHeartbeatScript  = mustStreamScript("stream_heartbeat")
)

func mustStreamScript(name string) *rueidis.Lua {
	byt, err := streamLua.ReadFile("lua/" + name + ".lua")
	if err != nil {
		panic(fmt.Errorf("error reading stream lua script %s: %w", name, err))
	}
	return rueidis.NewLuaScript(string(byt))
}

// redisStream is a durable PublishSubscriber on Redis Streams: an entry stays
// pending until its handler succeeds, so a crash redelivers it instead of losing it.
type redisStream struct {
	rc    rueidis.Client
	log   logger.Logger
	topic string

	keyPrefix     string
	consumer      string
	minIdle       time.Duration
	drainTimeout  time.Duration
	maxDeliveries int64
	maxReplayAge  time.Duration

	maxBacklog     int64
	maxMemoryRatio float64

	reclaimInterval   time.Duration
	heartbeatInterval time.Duration
	retryDelays       []time.Duration

	// groups caches stream keys whose consumer group is known to exist.
	groups sync.Map

	// Cached samples written by Watch and read by Admit and Ready.
	sampled    atomic.Bool
	ready      atomic.Bool
	backlog    atomic.Int64
	usedMemory atomic.Int64
	maxMemory  atomic.Int64
}

func newRedisStream(ctx context.Context, c *config.RedisMessaging) (*redisStream, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("redis event stream requires a redis client")
	}

	s := &redisStream{
		rc:             c.Client,
		log:            logger.StdlibLogger(ctx).With("caller", "eventstream"),
		topic:          c.Topic,
		keyPrefix:      c.KeyPrefix,
		consumer:       c.Consumer,
		minIdle:        c.MinIdle,
		drainTimeout:   c.DrainTimeout,
		maxDeliveries:  c.MaxDeliveries,
		maxReplayAge:   c.MaxReplayAge,
		maxBacklog:     c.MaxBacklog,
		maxMemoryRatio: c.MaxMemoryRatio,
	}
	if s.keyPrefix == "" {
		s.keyPrefix = defaultStreamKeyPrefix
	}
	if s.consumer == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "inngest"
		}
		s.consumer = host
	}
	if s.minIdle <= 0 {
		s.minIdle = defaultStreamMinIdle
	}
	if s.drainTimeout <= 0 {
		s.drainTimeout = defaultStreamDrainTimeout
	}
	if s.maxDeliveries <= 0 {
		s.maxDeliveries = defaultStreamMaxDeliveries
	}
	if s.maxReplayAge <= 0 {
		s.maxReplayAge = defaultStreamMaxReplayAge
	}
	s.reclaimInterval = c.ReclaimInterval
	if s.reclaimInterval <= 0 {
		s.reclaimInterval = streamReclaimInterval
	}
	s.heartbeatInterval = s.minIdle / 3
	s.retryDelays = streamHandlerRetryDelays

	if err := s.ensureGroup(ctx, s.streamKey(s.topic)); err != nil {
		return nil, err
	}
	return s, nil
}

// streamKey hash-tags the topic so a stream and its dead-letter stream share a
// cluster slot, which the Lua scripts require.
func (s *redisStream) streamKey(topic string) string {
	return fmt.Sprintf("%s:{%s}", s.keyPrefix, topic)
}

func (s *redisStream) deadLetterKey(topic string) string {
	return s.streamKey(topic) + ":dlq"
}

// ensureGroup creates the consumer group at id 0 so entries published before
// the first subscriber are still delivered.
func (s *redisStream) ensureGroup(ctx context.Context, key string) error {
	if _, ok := s.groups.Load(key); ok {
		return nil
	}
	err := s.rc.Do(ctx, s.rc.B().XgroupCreate().Key(key).Group(streamGroup).Id("0").Mkstream().Build()).Error()
	if err != nil && !rueidis.IsRedisBusyGroup(err) {
		return fmt.Errorf("error creating event stream consumer group: %w", err)
	}
	s.groups.Store(key, struct{}{})
	return nil
}

func (s *redisStream) Publish(ctx context.Context, topic string, m Message) error {
	return s.PublishBatch(ctx, topic, []Message{m})
}

// PublishBatch returns nil only once Redis accepted every XADD; a partial failure is
// safe to retry because consumers deduplicate on the event's internal ID.
func (s *redisStream) PublishBatch(ctx context.Context, topic string, msgs []Message) error {
	key := s.streamKey(topic)
	if err := s.ensureGroup(ctx, key); err != nil {
		return classifyStreamError(err)
	}

	cmds := make(rueidis.Commands, 0, min(len(msgs), streamPublishChunkEntries))
	size := 0
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		for _, resp := range s.rc.DoMulti(ctx, cmds...) {
			if err := resp.Error(); err != nil {
				return classifyStreamError(err)
			}
		}
		cmds = cmds[:0]
		size = 0
		return nil
	}

	for _, m := range msgs {
		body, err := m.Encode()
		if err != nil {
			return fmt.Errorf("error encoding message: %w", err)
		}
		cmds = append(cmds, s.rc.B().Xadd().Key(key).Id("*").FieldValue().
			FieldValue(streamFieldMessage, rueidis.BinaryString(body)).
			FieldValue(streamFieldName, m.Name).
			Build())
		size += len(body)
		if len(cmds) >= streamPublishChunkEntries || size >= streamPublishChunkBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (s *redisStream) Subscribe(ctx context.Context, topic string, run PerformFunc) error {
	return s.SubscribeN(ctx, topic, run, 1)
}

// SubscribeN consumes until ctx is cancelled, then waits up to the drain timeout
// for in-flight handlers; unfinished entries stay pending and are redelivered.
func (s *redisStream) SubscribeN(ctx context.Context, topic string, run PerformFunc, concurrency int64) error {
	if concurrency <= 0 {
		concurrency = 1
	}
	key := s.streamKey(topic)
	if err := s.ensureGroup(ctx, key); err != nil {
		return err
	}
	c := &streamConsumer{
		s:          s,
		key:        key,
		dlqKey:     s.deadLetterKey(topic),
		run:        run,
		slots:      make(chan struct{}, concurrency),
		inflight:   map[string]struct{}{},
		subCtx:     ctx,
		handlerCtx: context.WithoutCancel(ctx),
	}
	c.consume()
	return nil
}

// classifyStreamError maps a write failure onto the sentinel errors callers
// translate into retryable responses.
func classifyStreamError(err error) error {
	if errors.Is(err, ErrBackpressure) || errors.Is(err, ErrUnavailable) {
		return err
	}
	if strings.HasPrefix(err.Error(), "OOM") {
		return fmt.Errorf("%w: %w", ErrBackpressure, err)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}
