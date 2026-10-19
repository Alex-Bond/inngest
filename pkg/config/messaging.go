package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"gocloud.dev/pubsub"
)

type URLType string

const (
	MessagingInMemory  = "inmemory"
	MessagingNATS      = "nats"
	MessagingGCPPubSub = "gcp-pubsub"
	MessagingSQS       = "aws-sqs"
	MessagingRedis     = "redis"

	URLTypePublish   URLType = "publish"
	URLTypeSubscribe URLType = "subscribe"
)

// TopicURLCreator creates pub/sub topic URLs for the given backend
// implementation.
type TopicURLCreator interface {
	Backend() string

	TopicURL(topic string, typ URLType) string
	TopicName() string
}

// MessagingService represents
type MessagingService struct {
	Backend  string
	Concrete TopicURLCreator
}

// Set allows users to manually override the concrete backing config,
// for use within test environments.
func (m *MessagingService) Set(to TopicURLCreator) {
	m.Backend = to.Backend()
	m.Concrete = to
}

func (m MessagingService) TopicURL(topic string, typ URLType) string {
	if m.Concrete == nil {
		return ""
	}
	return m.Concrete.TopicURL(topic, typ)
}

func (m MessagingService) TopicName() string {
	return m.Concrete.TopicName()
}

// UnmarshalJSON unmarshals the messaging service, keeping the raw bytes
// available for unmarshalling depending on the Backend type.
func (m *MessagingService) UnmarshalJSON(byt []byte) error {
	type svc struct {
		Backend string
	}

	data := &svc{}
	if err := json.Unmarshal(byt, data); err != nil {
		return err
	}

	m.Backend = data.Backend

	var concrete TopicURLCreator
	switch m.Backend {
	case MessagingInMemory:
		concrete = &InMemoryMessaging{}
	case MessagingNATS:
		concrete = &NATSMessaging{}
	case MessagingGCPPubSub:
		concrete = &GCPPubSubMessaging{}
	case MessagingSQS:
		concrete = &SQSMessaging{}
	case MessagingRedis:
		concrete = &RedisMessaging{}
	default:
		return fmt.Errorf("unknown messaging backend: %s", m.Backend)
	}

	if err := json.Unmarshal(byt, concrete); err != nil {
		return err
	}

	m.Concrete = concrete
	return nil
}

// InMemoryMessaging configures the topic for use with an in-memory
// pubsub backend.
type InMemoryMessaging struct {
	Topic string
}

func (i InMemoryMessaging) Backend() string {
	return MessagingInMemory
}

func (i InMemoryMessaging) TopicName() string {
	return i.Topic
}

func (i InMemoryMessaging) TopicURL(topic string, typ URLType) string {
	// Ensure that this topic is created locally.
	url := fmt.Sprintf("mem://%s", topic)
	_, _ = pubsub.OpenTopic(context.Background(), url)
	return url
}

// NATSMessaging configures the NATS server URL and topic for use with
// a NATS messaging backend.
type NATSMessaging struct {
	Topic     string
	ServerURL string
}

func (n NATSMessaging) Backend() string {
	return MessagingNATS
}

func (n NATSMessaging) TopicName() string {
	return n.Topic
}

func (n NATSMessaging) TopicURL(topic string, typ URLType) string {
	// Unfortunately, NATS uses an environment variable to configure the
	// remote URL.  This is hacky, but we set the remote URL prior to
	// connecting to any queues here.
	os.Setenv("NATS_SERVER_URL", n.ServerURL)
	return fmt.Sprintf("nats://%s", topic)
}

type GCPPubSubMessaging struct {
	Project string
	Topic   string
}

func (g GCPPubSubMessaging) Backend() string {
	return MessagingGCPPubSub
}

func (g GCPPubSubMessaging) TopicName() string {
	return g.Topic
}

func (g GCPPubSubMessaging) TopicURL(topic string, typ URLType) string {
	if typ == URLTypePublish {
		return fmt.Sprintf("gcppubsub://projects/%s/topics/%s", g.Project, g.Topic)
	}

	return fmt.Sprintf("gcppubsub://projects/%s/subscriptions/%s", g.Project, g.Topic)
}

type SQSMessaging struct {
	Region   string
	Topic    string
	QueueURL string
}

func (s SQSMessaging) Backend() string {
	return MessagingSQS
}

func (s SQSMessaging) TopicName() string {
	return s.Topic
}

func (s SQSMessaging) TopicURL(topic string, typ URLType) string {
	// Replace https:// with awssqs://
	url := strings.Replace(s.QueueURL, "https://", "awssqs://", 1)
	url = strings.Replace(url, "http://", "awssqs://", 1)

	if strings.Contains(url, "?") {
		return fmt.Sprintf(
			"%s&region=%s",
			url,
			s.Region,
		)
	}

	return fmt.Sprintf(
		"%s?region=%s",
		url,
		s.Region,
	)
}

// RedisMessaging configures a durable event stream on Redis Streams.  Zero
// values select the defaults documented on each field.
type RedisMessaging struct {
	Topic string
	// KeyPrefix prefixes the stream keys; defaults to "inngest:eventstream".
	KeyPrefix string
	// Client is dedicated so blocking reads never share queue or state connections.
	Client rueidis.Client `json:"-"`
	// Consumer names this process in the consumer group; defaults to the hostname.
	Consumer string
	// Concurrency bounds in-flight handlers; defaults to 200.
	Concurrency int64
	// MinIdle is how long a pending entry must be idle before it is reclaimed; defaults to 5m.
	MinIdle time.Duration
	// ReclaimInterval is how often idle pending entries are reclaimed; defaults to 30s.
	ReclaimInterval time.Duration
	// DrainTimeout bounds the shutdown wait for in-flight handlers; defaults to 45s.
	DrainTimeout time.Duration
	// MaxDeliveries dead-letters an entry delivered this many times; defaults to 10.
	MaxDeliveries int64
	// MaxReplayAge dead-letters older redeliveries, whose dedup keys may be gone; defaults to 55m.
	MaxReplayAge time.Duration
	// MaxBacklog rejects external events while the backlog exceeds it; 0 disables.
	MaxBacklog int64
	// MaxMemoryRatio rejects external events above this used/max memory ratio; 0 disables.
	MaxMemoryRatio float64
}

func (r RedisMessaging) Backend() string {
	return MessagingRedis
}

func (r RedisMessaging) TopicName() string {
	return r.Topic
}

func (r RedisMessaging) TopicURL(topic string, typ URLType) string {
	return fmt.Sprintf("redisstream://%s", topic)
}
