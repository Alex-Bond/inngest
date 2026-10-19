package start

import (
	"fmt"
	"time"

	localconfig "github.com/inngest/inngest/cmd/internal/config"
	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/devserver"
	"github.com/inngest/inngest/pkg/execution/batch"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/urfave/cli/v3"
)

// eventStreamOpts reads and validates the event stream flags.
func eventStreamOpts(cmd *cli.Command, redisURI string) (devserver.EventStreamOpts, error) {
	opts := devserver.EventStreamOpts{
		Backend:         localconfig.GetValue(cmd, "event-stream", config.MessagingInMemory),
		Concurrency:     int64(localconfig.GetIntValue(cmd, "event-stream-concurrency", pubsub.DefaultStreamConcurrency)),
		MaxBacklog:      int64(localconfig.GetIntValue(cmd, "event-stream-max-backlog", devserver.DefaultEventStreamMaxBacklog)),
		MaxMemoryRatio:  float64(localconfig.GetIntValue(cmd, "event-stream-max-memory-percent", devserver.DefaultEventStreamMaxMemoryPercent)) / 100,
		DrainTimeout:    time.Duration(localconfig.GetIntValue(cmd, "event-stream-drain-timeout", devserver.DefaultEventStreamDrainSeconds)) * time.Second,
		MaxRequestBytes: int64(localconfig.GetIntValue(cmd, "event-stream-max-request-bytes", devserver.DefaultEventStreamMaxRequestBytes)),
	}
	return opts, validateEventStreamOpts(opts, redisURI)
}

func validateEventStreamOpts(opts devserver.EventStreamOpts, redisURI string) error {
	switch opts.Backend {
	case config.MessagingInMemory:
		return nil
	case config.MessagingRedis:
	default:
		return fmt.Errorf("event-stream must be %q or %q, got %q", config.MessagingInMemory, config.MessagingRedis, opts.Backend)
	}

	if redisURI == "" {
		return fmt.Errorf("event-stream %q requires redis-uri", config.MessagingRedis)
	}
	// Batch appends block until a buffer flushes, so fewer handlers than one buffer stall batching.
	if opts.Concurrency < batch.DefaultMaxBufferSize {
		return fmt.Errorf("event-stream-concurrency (%d) must be at least %d", opts.Concurrency, batch.DefaultMaxBufferSize)
	}
	if opts.MaxBacklog < 0 || opts.MaxMemoryRatio < 0 || opts.MaxMemoryRatio > 1 || opts.MaxRequestBytes < 0 {
		return fmt.Errorf("event-stream limits must be non-negative and max-memory-percent at most 100")
	}
	if opts.DrainTimeout <= 0 {
		return fmt.Errorf("event-stream-drain-timeout must be positive")
	}
	return nil
}
