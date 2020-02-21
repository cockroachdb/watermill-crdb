package crdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
)

const sessionTable = `"watermill_sessions"`
const cursorsTable = `"watermill_subscriber_cursors"`

func InitializeSessionSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		heartbeat TIMESTAMPTZ NOT NULL,
		expire_at TIMESTAMPTZ NOT NULL,
		meta JSONB
	);`, sessionTable))

	return err
}

func InitializeMessageSchema(ctx context.Context, db *sql.DB, topic string, logger watermill.LoggerAdapter) error {
	if err := validateTopicName(topic); err != nil {
		return err
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		message_id STRING NOT NULL,
		consume_after TIMESTAMPTZ NOT NULL,
		published_at TIMESTAMPTZ NOT NULL,
		payload BYTEA NOT NULL,
		meta JSONB
	);`, messageTable(topic))

	logger.Info("Initializing messages schema", watermill.LogFields{
		"query": query,
		"topic": topic,
	})

	_, err := db.ExecContext(ctx, query)

	return err
}

func InitializeClaimsSchema(ctx context.Context, db *sql.DB, topic string, consumerGroup string, logger watermill.LoggerAdapter) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		message_id UUID PRIMARY KEY NOT NULL REFERENCES %s,
		session_id UUID NOT NULL,
		acked TIMESTAMPTZ
	);`, claimsTable(topic, consumerGroup), messageTable(topic))

	logger.Info("Initializing claims schema", watermill.LogFields{
		"query": query,
	})

	_, err := db.ExecContext(ctx, query)

	return err
}

func InitializeCursorsSchema(ctx context.Context, db *sql.DB, logger watermill.LoggerAdapter) error {
	// Enabled Changefeeds
	if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`); err != nil {
		return err
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		topic STRING NOT NULL,
		consumer_group STRING NOT NULL,
		cursor TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (topic, consumer_group)
	);`, cursorsTable)

	logger.Info("Initializing cursors schema", watermill.LogFields{
		"query": query,
	})

	_, err := db.ExecContext(ctx, query)

	return err
}
