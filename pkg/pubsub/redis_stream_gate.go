package pubsub

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

const streamGateInterval = time.Second

var _ IngestGate = (*redisStream)(nil)

// Admit rejects external events while Redis is unreachable or the backlog or memory
// limit is exceeded; the memory bound leaves consumers headroom under noeviction.
func (s *redisStream) Admit() error {
	if !s.sampled.Load() {
		return nil
	}
	if !s.ready.Load() {
		return ErrUnavailable
	}
	if backlog := s.backlog.Load(); s.maxBacklog > 0 && backlog > s.maxBacklog {
		return fmt.Errorf("%w: backlog %d exceeds %d", ErrBackpressure, backlog, s.maxBacklog)
	}
	used, limit := s.usedMemory.Load(), s.maxMemory.Load()
	if s.maxMemoryRatio > 0 && limit > 0 && float64(used) > s.maxMemoryRatio*float64(limit) {
		return fmt.Errorf("%w: memory %d of %d bytes in use", ErrBackpressure, used, limit)
	}
	return nil
}

func (s *redisStream) Ready() bool {
	return !s.sampled.Load() || s.ready.Load()
}

// Watch probes durability once, then samples health, backlog and memory every
// second until ctx is done.
func (s *redisStream) Watch(ctx context.Context) {
	s.probeDurability(ctx)
	s.sample(ctx)

	ticker := time.NewTicker(streamGateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sample(ctx)
		}
	}
}

// probeDurability logs whether Redis fsyncs the stream: WAITAOF errors when
// appendonly is disabled.
func (s *redisStream) probeDurability(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := s.rc.B().Waitaof().Numlocal(1).Numreplicas(0).Timeout(1000).Build()
	if err := s.rc.Do(ctx, cmd).Error(); err != nil {
		s.log.Warn("event stream durability unconfirmed; accepted events may be lost if redis crashes", "error", err)
		return
	}
	s.log.Info("event stream durability confirmed: redis appendonly is enabled")
}

func (s *redisStream) sample(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, streamGateInterval)
	defer cancel()

	key := s.streamKey(s.topic)
	resps := s.rc.DoMulti(ctx,
		s.rc.B().Ping().Build(),
		s.rc.B().XinfoGroups().Key(key).Build(),
		s.rc.B().Xlen().Key(key).Build(),
		s.rc.B().Info().Section("memory").Build(),
	)
	if ctx.Err() != nil && ctx.Err() != context.DeadlineExceeded {
		return
	}

	s.ready.Store(resps[0].Error() == nil)
	s.sampled.Store(true)
	if resps[0].Error() != nil {
		return
	}

	if backlog, ok := groupBacklog(resps[1]); ok {
		s.backlog.Store(backlog)
	} else if n, err := resps[2].AsInt64(); err == nil {
		s.backlog.Store(n)
	}

	if info, err := resps[3].ToString(); err == nil {
		used, limit := parseMemoryInfo(info)
		s.usedMemory.Store(used)
		s.maxMemory.Store(limit)
	}
}

// groupBacklog returns pending + lag for the group; lag can be null, and then XLEN
// is used instead, which matches because acknowledged entries are deleted.
func groupBacklog(resp rueidis.RedisResult) (int64, bool) {
	groups, err := resp.ToArray()
	if err != nil {
		return 0, false
	}
	for _, g := range groups {
		fields, err := g.AsMap()
		if err != nil {
			continue
		}
		name, pending, lag := fields["name"], fields["pending"], fields["lag"]
		if n, _ := name.ToString(); n != streamGroup {
			continue
		}
		p, err := pending.AsInt64()
		if err != nil {
			return 0, false
		}
		lg, err := lag.AsInt64()
		if err != nil {
			return 0, false
		}
		return p + lg, true
	}
	return 0, false
}

func parseMemoryInfo(info string) (used, limit int64) {
	sc := bufio.NewScanner(strings.NewReader(info))
	for sc.Scan() {
		k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), ":")
		if !ok {
			continue
		}
		switch k {
		case "used_memory":
			used, _ = strconv.ParseInt(v, 10, 64)
		case "maxmemory":
			limit, _ = strconv.ParseInt(v, 10, 64)
		}
	}
	return used, limit
}
