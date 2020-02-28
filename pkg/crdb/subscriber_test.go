package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
)

func TestSubscriberMissedMessages(t *testing.T) {
	topic := watermill.NewUUID()
	logger := watermill.NewStdLogger(true, true)

	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	pub := NewPublisher(conn, logger)
	sub := NewSubscriber(conn, "group", logger)

	require.NoError(t, sub.SubscribeInitialize(topic))

	t.Run(topic, func(t *testing.T) {
		t.Run("missedMessage", func(t *testing.T) {
			missedMessageID := watermill.NewUUID()

			require.NoError(t, pub.Publish(
				topic,
				message.NewMessage(missedMessageID, []byte("{}")),
			))

			tx, err := conn.BeginTx(context.Background(), nil)
			require.NoError(t, err)

			// Write a cursor that is after missedMessage published time
			require.NoError(
				t,
				sub.setCursor(context.Background(), tx, topic, time.Now()),
			)

			require.NoError(t, tx.Commit())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			out, err := sub.Subscribe(ctx, topic)
			require.NoError(t, err)

			select {
			case msg := <-out:
				require.NotNil(t, msg)
				require.Equal(t, missedMessageID, msg.UUID)
				msg.Ack()

			case <-time.After(5 * time.Second):
				require.FailNow(t, "old message not picked up")
			}
		})

		t.Run("abandonedMessage", func(t *testing.T) {
			abandonedCtx, cancel := context.WithCancel(context.Background())
			abandonedMessageID := watermill.NewUUID()

			require.NoError(t, pub.Publish(
				topic,
				message.NewMessage(abandonedMessageID, []byte("{}")),
			))

			out, err := sub.Subscribe(abandonedCtx, topic)
			require.NoError(t, err)

			select {
			case msg := <-out:
				require.NotNil(t, msg)
				require.Equal(t, abandonedMessageID, msg.UUID)

			case <-time.After(5 * time.Second):
				require.FailNow(t, "message not picked up")
			}

			// Don't ack the message, kill the subscriber to leave it in
			// an abandoned state
			cancel()

			tx, err := conn.BeginTx(context.Background(), nil)
			require.NoError(t, err)

			// Write a cursor that is after abandonedMessage published time
			require.NoError(
				t,
				sub.setCursor(context.Background(), tx, topic, time.Now()),
			)

			require.NoError(t, tx.Commit())

			// Open a new subscriber
			out, err = sub.Subscribe(context.Background(), topic)
			require.NoError(t, err)

			// We should receive the abandoned message
			select {
			case msg := <-out:
				require.NotNil(t, msg)
				require.Equal(t, abandonedMessageID, msg.UUID)
				msg.Ack()

			case <-time.After(5 * time.Second):
				require.FailNow(t, "abandoned message not picked up")
			}
		})
	})

	require.NoError(t, pub.Close())
	require.NoError(t, sub.Close())
}

func TestConsumeAfter(t *testing.T) {
	topic := watermill.NewUUID()
	logger := watermill.NewStdLogger(true, true)

	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	pub := NewPublisher(conn, logger)
	sub := NewSubscriber(conn, "group", logger)

	defer pub.Close()
	defer sub.Close()

	out, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	messageID := watermill.NewUUID()

	require.NoError(t, pub.PublishAt(
		topic,
		time.Now().Add(10 * time.Second),
		message.NewMessage(messageID, []byte("{}")),
	))

	select {
	case <-out:
		require.FailNow(t, "recieved message before timeout")
	case <-time.After(10 * time.Second):
	}

	select {
	case msg := <-out:
		require.Equal(t, messageID, msg.UUID)
	// there are some latency issues that need to be worked out on CRDB's side
	case <-time.After(60 * time.Second):
		require.FailNow(t, "did not recieve message")
	}
}
