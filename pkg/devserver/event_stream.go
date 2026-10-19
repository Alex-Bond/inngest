package devserver

import (
	"fmt"
	"time"

	"github.com/inngest/inngest/pkg/config"
	"github.com/redis/rueidis"
)

const (
	DefaultEventStreamMaxBacklog       = 500_000
	DefaultEventStreamMaxMemoryPercent = 70
	DefaultEventStreamDrainSeconds     = 45
	DefaultEventStreamMaxRequestBytes  = 64 << 20

	// durableDedupTTL outlives the stream's replay horizon so redeliveries still hit dedup keys.
	durableDedupTTL = 2 * time.Hour
)

// EventStreamOpts selects and tunes the event stream between the event API and
// the runner.
type EventStreamOpts struct {
	// Backend is config.MessagingInMemory (the default) or config.MessagingRedis.
	Backend         string
	Concurrency     int64
	MaxBacklog      int64
	MaxMemoryRatio  float64
	DrainTimeout    time.Duration
	MaxRequestBytes int64
}

func (o EventStreamOpts) durable() bool {
	return o.Backend == config.MessagingRedis
}

// configureEventStream points the config at a durable Redis stream on its own
// client, or returns nil when the in-memory stream is selected.
func configureEventStream(opts *StartOpts) (rueidis.Client, error) {
	if !opts.EventStream.durable() {
		return nil, nil
	}
	if opts.RedisURI == "" {
		return nil, fmt.Errorf("the redis event stream requires an external redis uri")
	}

	rc, err := connectToOrCreateRedis(opts.RedisURI)
	if err != nil {
		return nil, fmt.Errorf("error connecting the event stream to redis: %w", err)
	}
	opts.Config.EventStream.Service.Set(&config.RedisMessaging{
		Topic:          opts.Config.EventStream.Service.TopicName(),
		Client:         rc,
		Concurrency:    opts.EventStream.Concurrency,
		DrainTimeout:   opts.EventStream.DrainTimeout,
		MaxBacklog:     opts.EventStream.MaxBacklog,
		MaxMemoryRatio: opts.EventStream.MaxMemoryRatio,
	})
	return rc, nil
}
