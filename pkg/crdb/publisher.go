package crdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"
)

var ErrPublisherClosed = errors.New("publisher is closed")

type DB interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type PublisherConfig struct {
	AutoInitializeSchema bool
}

type publisher struct {
	pending            sync.WaitGroup
	logger             watermill.LoggerAdapter
	closed             bool
	closing            chan struct{}
	initializedSchemas sync.Map
	conn               DB
}

var _ message.Publisher = &publisher{}

func NewPublisher(conn DB, logger watermill.LoggerAdapter) *publisher {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &publisher{
		conn:    conn,
		closed:  false,
		closing: make(chan struct{}),
		logger:  logger,
	}
}

func (p *publisher) initializeSchema(topic string) error {
	db, ok := p.conn.(*sql.DB)
	if !ok {
		return nil
	}

	if _, ok := p.initializedSchemas.Load(topic); ok {
		return nil
	}

	p.initializedSchemas.Store(topic, true)

	if err := InitializeMessageSchema(context.Background(), db, topic, p.logger); err != nil {
		return err
	}

	return nil
}

func (p *publisher) Publish(topic string, messages ...*message.Message) error {
	return p.PublishAt(topic, time.Now(), messages...)
}

// PublishAt persists messages to CRDB but schedules them to be consumed after the specified time.Time
func (p *publisher) PublishAt(topic string, consumeAfter time.Time, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	if err := validateTopicName(topic); err != nil {
		return err
	}

	if err := p.initializeSchema(topic); err != nil {
		return err
	}

	p.pending.Add(1)
	defer p.pending.Done()

	ctx := context.Background()
	publishedAt := time.Now()

	return ensureTx(ctx, p.conn, func(tx DB) error {
		// TODO bulk insert is more efficient
		for _, m := range messages {
			meta, err := json.Marshal(m.Metadata)
			if err != nil {
				return err
			}

			if m.Payload == nil {
				m.Payload = []byte{}
			}

			insertQuery := fmt.Sprintf(`
				INSERT INTO %s (id, message_id, payload, meta, published_at, consume_after)
				VALUES (DEFAULT, $1, $2, $3, $4, $5)
			`, messageTable(topic))

			_, err = tx.ExecContext(ctx, insertQuery, m.UUID, m.Payload, meta, publishedAt, consumeAfter)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (p *publisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	p.logger.Info("closing publisher", nil)

	close(p.closing)

	p.pending.Wait()

	return nil
}

// ensureTx executes cb via crdb.ExecuteTx, if db is a *sql.DB. Otherwise cb is executed normally
func ensureTx(ctx context.Context, db DB, cb func(tx DB) error) error {
	sqlDB, ok := db.(*sql.DB)

	if !ok {
		return cb(db)
	}

	return crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *sql.Tx) error {
		return cb(tx)
	})
}
