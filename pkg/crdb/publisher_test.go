package crdb

import (
	"fmt"
	"testing"
	"context"
	"database/sql"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestPublishDB(t *testing.T) {
	topic := watermill.NewUUID()
	logger := watermill.NewStdLogger(true, testing.Verbose())

	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	pub := NewPublisher(conn, logger)
	defer pub.Close()

	require.NoError(t, pub.Publish(
		topic,
		message.NewMessage(watermill.NewUUID(), []byte("{}")),
		message.NewMessage(watermill.NewUUID(), []byte("{}")),
	))

	require.NoError(t, err)

	row := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", messageTable(topic)))

	var count int

	require.NoError(t, row.Scan(&count))
	require.Equal(t, 2, count)
}

func TestPublishTx(t *testing.T) {
	ctx := context.Background()
	topic := watermill.NewUUID()
	logger := watermill.NewStdLogger(true, testing.Verbose())

	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	require.NoError(t, InitializeMessageSchema(ctx, conn, topic, logger))

	err = crdb.ExecuteTx(ctx, conn, nil, func(tx *sql.Tx) error {
		pub := NewPublisher(tx, logger)
		defer pub.Close()

		return pub.Publish(
			topic,
			message.NewMessage(watermill.NewUUID(), []byte("{}")),
			message.NewMessage(watermill.NewUUID(), []byte("{}")),
		)
	})

	require.NoError(t, err)

	row := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", messageTable(topic)))

	var count int

	require.NoError(t, row.Scan(&count))
	require.Equal(t, 2, count)
}
