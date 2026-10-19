package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/db"
	dbpostgres "github.com/inngest/inngest/pkg/db/postgres"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/inngest/inngest/tests/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

const topic = "events"

func streamKey() string { return "inngest:eventstream:{" + topic + "}" }

func startValkey(t *testing.T) (*helper.ValkeyContainer, func() rueidis.Client) {
	t.Helper()
	vc, err := helper.StartValkey(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vc.Terminate(context.Background()) })
	return vc, func() rueidis.Client {
		rc, err := helper.NewValkeyClient(vc.Addr, "", vc.Password, false)
		require.NoError(t, err)
		return rc
	}
}

func newStream(t *testing.T, rc rueidis.Client, consumer string, mutate ...func(*config.RedisMessaging)) pubsub.PublishSubscriber {
	t.Helper()
	conf := &config.RedisMessaging{
		Topic:           topic,
		Client:          rc,
		Consumer:        consumer,
		MinIdle:         2 * time.Second,
		ReclaimInterval: 500 * time.Millisecond,
		DrainTimeout:    time.Second,
	}
	for _, m := range mutate {
		m(conf)
	}
	svc := config.MessagingService{}
	svc.Set(conf)
	ps, err := pubsub.NewPublishSubscriber(context.Background(), svc)
	require.NoError(t, err)
	return ps
}

func insertEvent(ctx context.Context, q db.Querier, m pubsub.Message) error {
	id, err := ulid.Parse(m.Data)
	if err != nil {
		return err
	}
	return q.InsertEvent(ctx, db.InsertEventParams{
		InternalID: id,
		EventID:    id.String(),
		EventName:  "test/stream",
		EventData:  "{}",
		EventUser:  "{}",
		EventTs:    time.Now(),
		ReceivedAt: time.Now(),
	})
}

func eventCounts(t *testing.T, conn *sql.DB) (total, distinct int) {
	t.Helper()
	require.NoError(t, conn.QueryRow("SELECT count(*), count(DISTINCT internal_id) FROM events").Scan(&total, &distinct))
	return total, distinct
}

// Consumer A records entries and dies before acking or heartbeating; consumer B
// must reclaim them, handling every entry without duplicating a row.
func TestCrashedConsumerEntriesAreReclaimed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping functional tests")
	}
	ctx := context.Background()

	_, newClient := startValkey(t)
	pc, err := testutil.StartPostgres(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Terminate(context.Background()) })
	conn, err := dbpostgres.Open(ctx, dbpostgres.Options{URI: pc.URI, ForTest: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	q := dbpostgres.New(conn).Q()

	publisherRc := newClient()
	defer publisherRc.Close()
	publisher := newStream(t, publisherRc, "api").(pubsub.BatchPublisher)

	const total = 300
	msgs := make([]pubsub.Message, total)
	for i := range msgs {
		msgs[i] = pubsub.Message{Name: "event/received", Data: ulid.Make().String(), Timestamp: time.Now()}
	}
	require.NoError(t, publisher.PublishBatch(ctx, topic, msgs))

	// A hangs between the insert and the ack, like a killed process.
	aRc := newClient()
	a := newStream(t, aRc, "pod-a")
	hang := make(chan struct{})
	var aTaken atomic.Int32
	aCtx, aCancel := context.WithCancel(ctx)
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		_ = a.SubscribeN(aCtx, topic, func(ctx context.Context, m pubsub.Message) error {
			if err := insertEvent(ctx, q, m); err != nil {
				return err
			}
			aTaken.Add(1)
			<-hang
			return nil
		}, 50)
	}()
	require.Eventually(t, func() bool { return aTaken.Load() == 50 }, 30*time.Second, 10*time.Millisecond)
	aRc.Close()
	aCancel()
	<-aDone

	bRc := newClient()
	defer bRc.Close()
	b := newStream(t, bRc, "pod-b")
	var mu sync.Mutex
	bSeen := map[string]int{}
	bCtx, bCancel := context.WithCancel(ctx)
	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		_ = b.SubscribeN(bCtx, topic, func(ctx context.Context, m pubsub.Message) error {
			mu.Lock()
			bSeen[m.Data]++
			mu.Unlock()
			return insertEvent(ctx, q, m)
		}, 50)
	}()

	require.Eventually(t, func() bool {
		n, err := publisherRc.Do(ctx, publisherRc.B().Xlen().Key(streamKey()).Build()).AsInt64()
		return err == nil && n == 0
	}, 60*time.Second, 100*time.Millisecond)
	bCancel()
	<-bDone

	mu.Lock()
	require.Len(t, bSeen, total, "B must handle every entry, including those A died holding")
	mu.Unlock()

	// A's late handlers, waking after B redelivered their entries, must not duplicate rows.
	close(hang)
	totalRows, distinct := eventCounts(t, conn)
	require.Equal(t, total, distinct)
	require.Equal(t, total, totalRows)
}

// Past the memory threshold external events are refused while internal publishers
// and consumers keep working, and admission recovers once the backlog drains.
func TestMemoryAdmissionRecoversAfterDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping functional tests")
	}
	ctx := context.Background()
	_, newClient := startValkey(t)
	rc := newClient()
	defer rc.Close()
	require.NoError(t, rc.Do(ctx, rc.B().ConfigSet().ParameterValue().ParameterValue("maxmemory", "32mb").Build()).Error())

	s := newStream(t, rc, "api", func(c *config.RedisMessaging) { c.MaxMemoryRatio = 0.5 })
	gate := s.(pubsub.IngestGate)
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go gate.Watch(watchCtx)

	payload := strings.Repeat("x", 1024)
	batch := make([]pubsub.Message, 500)
	for i := range batch {
		batch[i] = pubsub.Message{Name: "event/received", Data: payload}
	}
	require.Eventually(t, func() bool {
		require.NoError(t, s.(pubsub.BatchPublisher).PublishBatch(ctx, topic, batch))
		return errors.Is(gate.Admit(), pubsub.ErrBackpressure)
	}, 60*time.Second, 50*time.Millisecond, "admission never applied backpressure")

	// Internal publishers still have headroom below maxmemory.
	require.NoError(t, s.Publish(ctx, topic, pubsub.Message{Name: "event/received", Data: payload}))

	consumerRc := newClient()
	defer consumerRc.Close()
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = newStream(t, consumerRc, "runner").SubscribeN(subCtx, topic, func(context.Context, pubsub.Message) error { return nil }, 200)
	}()

	require.Eventually(t, func() bool {
		return gate.Admit() == nil
	}, 60*time.Second, 100*time.Millisecond, "admission did not recover after the stream drained")
}
