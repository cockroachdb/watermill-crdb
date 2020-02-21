package crdb_test

import (
	"database/sql"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/cockroachdb/watermill-crdb/pkg/crdb"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
)

func PubSubConstructor(t *testing.T, conn *sql.DB, consumerGroup string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	publisher := crdb.NewPublisher(conn, logger)
	subscriber := crdb.NewSubscriber(conn, consumerGroup, logger)

	return publisher, subscriber
}

func TestWatermillUniversal(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	tests.TestPubSub(t, tests.Features{
		// ConsumerGroups should be true, if consumer groups are supported.
		ConsumerGroups: true,

		// // ExactlyOnceDelivery should be true, if exactly-once delivery is supported.
		ExactlyOnceDelivery: true,

		// // GuaranteedOrder should be true, if order of messages is guaranteed.
		// GuaranteedOrder bool

		// // Some Pub/Subs guarantee the order only when one subscriber is subscribed at a time.
		GuaranteedOrderWithSingleSubscriber: true,

		// // Persistent should be true, if messages are persistent between multiple instancees of a Pub/Sub
		// // (in practice, only GoChannel doesn't support that).
		Persistent: true,

		// // RestartServiceCommand is a command to test reconnects. It should restart the message broker.
		// // Example: []string{"docker", "restart", "rabbitmq"}
		// RestartServiceCommand []string
		// RestartServiceCommand: []string{"bash", "-c", "docker restart cockroachlabs_cockroachdb_1 && sleep 5"},

		// // RequireSingleInstance must be true,if a PubSub requires a single instance to work properly
		// // (for example: GoChannel implementation).
		RequireSingleInstance: false,

		// // NewSubscriberReceivesOldMessages should be set to true if messages are persisted even
		// // if they are already consumed (for example, like in Kafka).
		NewSubscriberReceivesOldMessages: false,
	}, func(t *testing.T) (message.Publisher, message.Subscriber) {
		return PubSubConstructor(t, conn, "pubsub-test")
	}, func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
		return PubSubConstructor(t, conn, consumerGroup)
	})
}
