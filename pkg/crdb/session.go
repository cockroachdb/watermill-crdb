package crdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

const sessionTable = `"watermill_sessions"`

type Session struct {
	rw                sync.Mutex
	closed            bool
	db                *sql.DB
	heartbeatInterval time.Duration
	logger            watermill.LoggerAdapter
	timeOutInterval   time.Duration
	table             string

	Start     chan struct{}
	SessionID string
}

func NewSession(db *sql.DB, logger watermill.LoggerAdapter) *Session {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Session{
		db:                db,
		logger:            logger,
		Start:             make(chan struct{}),
		table:             sessionTable,
		heartbeatInterval: 1 * time.Second,
		timeOutInterval:   4 * time.Second,
	}
}

func (s *Session) Run(ctx context.Context) error {
	if err := s.init(ctx); err != nil {
		return errors.Wrap(err, "could not initialize session")
	}

	for !s.closed {
		if err := s.heartbeat(ctx); err != nil {
			if ctx.Err() == nil {
				return err
			}

			break
		}

		select {
		case <-time.After(s.heartbeatInterval):
			continue
		case <-ctx.Done():
			break
		}
	}

	return s.Close()
}

func (s *Session) Observe(ctx context.Context, tx *sql.Tx) error {
	row := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT true FROM %s WHERE id = $1", s.table), s.SessionID)

	var exists bool
	if err := row.Scan(&exists); err != nil && err != sql.ErrNoRows {
		return err
	}

	if !exists {
		return errors.Errorf("session does not exist: %s", s.SessionID)
	}

	return nil
}

func (s *Session) TableName() string {
	return s.table
}

func (s *Session) Close() error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	res, err := s.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table), s.SessionID)
	if err != nil {
		return err
	}

	affected, _ := res.RowsAffected()

	s.logger.Info("removed session", watermill.LogFields{
		"removed": affected,
	})

	return err
}

func (s *Session) init(ctx context.Context) error {
	s.rw.Lock()
	defer s.rw.Unlock()

	if s.SessionID != "" {
		return errors.New("already initialized")
	}

	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		INSERT INTO %s(id, heartbeat, expire_at)
		VALUES (DEFAULT, NOW(), NOW()+$1)
		RETURNING id;
	`, s.table), s.timeOutInterval.String())

	if err := row.Scan(&s.SessionID); err != nil {
		return err
	}

	s.logger = s.logger.With(watermill.LogFields{"session_id": s.SessionID})

	close(s.Start)

	return nil
}

func (s *Session) heartbeat(ctx context.Context) error {
	res, err := s.db.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE %s SET heartbeat = NOW(), expire_at = NOW() + $2 WHERE id = $1`, s.table),
		s.SessionID,
		s.timeOutInterval.String(),
	)

	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if affected != 1 {
		return errors.Wrapf(err, "heartbeat did not affect exactly 1 row")
	}

	res, err = s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE expire_at < NOW()`, s.table))
	if err != nil {
		return err
	}

	if removedSessions, _ := res.RowsAffected(); removedSessions > 0 {
		s.logger.Info("removed expired sessions", watermill.LogFields{"removed": removedSessions})
	}

	return nil
}
