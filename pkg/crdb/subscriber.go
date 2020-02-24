package crdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var ErrSubscriberClosed = errors.New("subscriber is closed")

type CRDBEnvelope struct {
	Value ResolvedTimestamp `json:"__crdb__"`
}

type ResolvedTimestamp struct {
	Resolved string `json:"resolved"`
}

type subscriber struct {
	closed        bool
	closing       chan struct{}
	consumerGroup string
	db            *sql.DB
	errgroup      errgroup.Group
	logger        watermill.LoggerAdapter
}

var _ message.Subscriber = &subscriber{}

func NewSubscriber(db *sql.DB, consumerGroup string, logger watermill.LoggerAdapter) *subscriber {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"consumer_group": consumerGroup,
	})

	return &subscriber{
		closed:        false,
		closing:       make(chan struct{}),
		db:            db,
		logger:        logger,
		consumerGroup: consumerGroup,
	}
}

func (s *subscriber) SubscribeInitialize(topic string) error {
	ctx := context.Background()

	if err := InitializeSessionSchema(ctx, s.db); err != nil {
		return err
	}

	if err := InitializeCursorsSchema(ctx, s.db, s.logger); err != nil {
		return err
	}

	if err := InitializeMessageSchema(ctx, s.db, topic, s.logger); err != nil {
		return err
	}

	if err := InitializeClaimsSchema(ctx, s.db, topic, s.consumerGroup, s.logger); err != nil {
		return err
	}

	return nil
}

func (s *subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err := validateTopicName(topic); err != nil {
		return nil, err
	}

	if err := s.SubscribeInitialize(topic); err != nil {
		return nil, err
	}

	var cursor time.Time
	if err := crdb.ExecuteTx(ctx, s.db, nil, func(tx *sql.Tx) error {
		var err error

		cursor, err = s.Cursor(ctx, tx, topic)

		return err
	}); err != nil {
		return nil, err
	}

	out := make(chan *message.Message)
	ctx, cancel := context.WithCancel(ctx)

	rows, err := s.db.QueryContext(ctx, ChangeFeedQuery(topic, time.Second, cursor))
	if err != nil {
		cancel()
		return nil, err
	}

	session := NewSession(s.db, s.logger)

	// not technically nessecary to place this in the errgroup
	// Ensure that calling .Close cancels this context
	s.errgroup.Go(func() error {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		}
		return nil
	})

	s.errgroup.Go(func() error {
		return session.Run(ctx)
	})

	<-session.Start

	logger := s.logger.With(watermill.LogFields{
		"topic":   topic,
		"session": session.SessionID,
	})

	s.errgroup.Go(func() error {
		defer close(out)
		defer cancel()

		logger.Info("subscribing", watermill.LogFields{
			"cursor": cursor,
		})

		err := s.consume(ctx, rows, topic, session, out, logger)

		logger.Debug("subscription closed", watermill.LogFields{
			"err":    err,
		})

		// swallow errors if ctx has been canceled
		if ctx.Err() != nil {
			return nil
		}

		return err
	})

	return out, nil
}

func (s *subscriber) SetCursor(ctx context.Context, tx *sql.Tx, topic string, cursor time.Time) error {
	query := fmt.Sprintf(`
		INSERT INTO %s(topic, consumer_group, cursor)
		VALUES ($1, $2, $3)
		ON CONFLICT (topic, consumer_group)
		DO UPDATE SET cursor = excluded.cursor
	`, cursorsTable)

	// TODO check rows affected?
	_, err := tx.ExecContext(ctx, query, topic, s.consumerGroup, cursor)

	return err
}

func (s *subscriber) Cursor(ctx context.Context, tx *sql.Tx, topic string) (time.Time, error) {
	query := fmt.Sprintf(`SELECT cursor FROM %s WHERE topic = $1 AND consumer_group = $2`, cursorsTable)

	row := tx.QueryRowContext(ctx, query, topic, s.consumerGroup)

	var cursor time.Time
	if err := row.Scan(&cursor); err != nil && err != sql.ErrNoRows {
		return time.Time{}, err
	}

	return cursor, nil
}

func (s *subscriber) MissedMessageIDs(ctx context.Context, session *Session, tx *sql.Tx, topic string, cursor time.Time) ([]string, error) {
	query := fmt.Sprintf(`SELECT msg.ID FROM %s msg
		WHERE msg.consume_after <= $1
		AND NOT EXISTS (
			SELECT * FROM %s claim
			WHERE claim.acked IS NULL
			AND claim.message_id = msg.ID
			AND NOT EXISTS (
				SELECT * FROM %s session WHERE session.id = claim.session_id
			)
	);`, messageTable(topic), claimsTable(topic, s.consumerGroup), session.TableName())

	rows, err := tx.QueryContext(ctx, query, cursor)
	if err != nil {
		return nil, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (s *subscriber) consume(
	ctx context.Context,
	rows *sql.Rows,
	topic string,
	session *Session,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) error {
	var cursor time.Time

	for rows.Next() {
		var key sql.NullString
		var table sql.NullString
		var value string

		if err := rows.Scan(&table, &key, &value); err != nil {
			return err
		}

		var timestamp time.Time
		var messageID string

		// Is this a resolve timestamp or an actual change?
		isChange := table.Valid

		// unmarshal either the timestamp envelope or changed row ID
		if isChange {
			var ids []string
			if err := json.Unmarshal([]byte(key.String), &ids); err != nil {
				return err
			}
			messageID = ids[0]
		} else {
			var env CRDBEnvelope
			if err := json.Unmarshal([]byte(value), &env); err != nil {
				return err
			}

			ts, err := strconv.ParseFloat(env.Value.Resolved, 64)
			if err != nil {
				return err
			}

			timestamp = time.Unix(0, int64(ts))
		}

		var claimed *message.Message
		if err := crdb.ExecuteTx(ctx, s.db, nil, func(tx *sql.Tx) error {
			var err error
			claimed = nil

			if isChange {
				claimed, err = s.claimMessage(ctx, session, tx, topic, messageID)
				return err
			}

			missed, err := s.MissedMessageIDs(ctx, session, tx, topic, timestamp)
			if err != nil {
				return err
			}

			// If no missed messages exist, update our group's cursor
			if len(missed) == 0 {
				return s.SetCursor(ctx, tx, topic, timestamp)
			}

			claimed, err = s.claimMessage(ctx, session, tx, topic, missed[0])

			return err
		}); err != nil {
			return err
		}

		// Upgrade timestamp to our cursor if there are no missed messages
		if claimed == nil && !timestamp.IsZero() {
			cursor = timestamp
		}

		// No messages available
		if claimed == nil {
			continue
		}

		if acked := s.sendMessage(ctx, claimed, out, logger); acked {
			result, err := s.db.ExecContext(
				ctx,
				fmt.Sprintf(`UPDATE %s SET acked = NOW() WHERE message_id = $1 AND session_id = $2`, claimsTable(topic, s.consumerGroup)),
				messageID,
				session.SessionID,
			)
			if err != nil {
				return err
			}

			rowsAffected, _ := result.RowsAffected()

			logger.Debug("acked message", watermill.LogFields{
				"message_id":    claimed.UUID,
				"rows_affected": rowsAffected,
				"cursor":        cursor,
			})
		}
	}

	return rows.Err()
}

func (s *subscriber) claimMessage(ctx context.Context, session *Session, tx *sql.Tx, topic string, messageID string) (*message.Message, error) {
	var msgConsumeAfter time.Time
	var msgMeta string
	var msgPayload []byte
	var msgUUID string

	query := fmt.Sprintf(`SELECT msg.message_id, msg.payload, msg.meta, msg.consume_after FROM
	%[1]s msg
	LEFT JOIN %[2]s ack ON ack.message_id = msg.id
	WHERE msg.id = $1
	AND ack.acked IS NULL
	AND msg.consume_after <= NOW()
	AND NOT EXISTS (
		SELECT * FROM %[3]s WHERE id = ack.session_id
	);`, messageTable(topic), claimsTable(topic, s.consumerGroup), session.TableName())

	row := tx.QueryRowContext(ctx, query, messageID)
	if err := row.Scan(&msgUUID, &msgPayload, &msgMeta, &msgConsumeAfter); err != nil {
		// message either doesn't exist, has already been claimed, or isn't ready to be consumed
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	// logger.Debug("recieved message", watermill.LogFields{
	// 	"cursor": cursor,
	// 	"message_id": msgUUID,
	// 	"latency": time.Now().Sub(msgConsumeAfter),
	// })

	// Ensure our session is still valid
	if err := session.Observe(ctx, tx); err != nil {
		return nil, err
	}

	claimQuery := fmt.Sprintf(`
	INSERT INTO %s(message_id, session_id, acked)
	VALUES ($1, $2, NULL)
	ON CONFLICT (message_id)
	DO UPDATE SET session_id = excluded.session_id
	`, claimsTable(topic, s.consumerGroup))

	if _, err := tx.ExecContext(ctx, claimQuery, messageID, session.SessionID); err != nil {
		return nil, err
	}

	msg := message.NewMessage(msgUUID, msgPayload)

	if err := json.Unmarshal([]byte(msgMeta), &msg.Metadata); err != nil {
		return nil, err
	}

	return msg, nil
}

func (s *subscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {
	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()

	for {
		select {
		case out <- msg:

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			return true

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("message nacked, resending", watermill.LogFields{
				"msg_uuid": msg.UUID,
			})

			msg = msg.Copy()

			select {
			// TODO
			case <-time.After(time.Second):
				continue
			case <-ctx.Done():
				return false
			}

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	s.logger.Info("closing subscriber", nil)

	close(s.closing)

	return s.errgroup.Wait()
}
