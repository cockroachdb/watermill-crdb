package crdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/watermill-crdb/pkg/crdb"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
)

func TestNewSession(t *testing.T) {
	// Nothing is initialized when creating a session
	s := crdb.NewSession(nil, nil)

	require.Equal(t, "", s.SessionID)
	select {
	case <-s.Start:
		require.FailNow(t, ".Start should block")
	default:
	}
}

func TestSessionRun(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, crdb.InitializeSessionSchema(ctx, conn))

	s := crdb.NewSession(conn, nil)

	go func() {
		require.NoError(t, s.Run(ctx))
	}()

	select {
	case <-s.Start:
	case <-time.After(time.Second):
		require.FailNow(t, "session never started")
	}

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Ensure our session is active
	require.NoError(t, s.Observe(ctx, tx))

	_ = tx.Commit()

	cancel()
	require.NoError(t, s.Close())

	tx, err = conn.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	require.EqualError(
		t,
		s.Observe(context.Background(), tx),
		fmt.Sprintf("session does not exist: %s", s.SessionID),
	)

	_ = tx.Commit()
}

func TestSessionsRemoveExpiredSessions(t *testing.T) {
	config, err := pgx.ParseConfig("postgres://root@localhost:43430/defaultdb?sslmode=disable")
	require.NoError(t, err)

	conn := stdlib.OpenDB(*config)
	ctx := context.Background()
	canceledCtx, cancel := context.WithCancel(context.Background())

	s := crdb.NewSession(conn, nil)
	expired := crdb.NewSession(conn, nil)

	go func() {
		_ = expired.Run(canceledCtx)
	}()

	<-expired.Start

	cancel()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Session exists but is not active
	require.NoError(t, expired.Observe(ctx, tx))

	_ = tx.Commit()

	go func() {
		_ = s.Run(context.Background())
	}()

	time.Sleep(5 * time.Second)

	tx, err = conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Session no longer exists
	require.EqualError(
		t,
		expired.Observe(ctx, tx),
		fmt.Sprintf("session does not exist: %s", expired.SessionID),
	)

	_ = tx.Commit()

	require.NoError(t, s.Close())
}
