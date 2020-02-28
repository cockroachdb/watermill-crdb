package crdb

import (
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Scanner interface {
	Scan(...interface{}) error
}

type messageRow struct {
	ID           string
	PublishedAt  time.Time
	ConsumeAfter time.Time
	Msg          *message.Message
}

func scanMessage(row Scanner) (messageRow, error) {
	var (
		internal messageRow
		meta     string
		payload  []byte
		uuid     string
	)

	if err := row.Scan(
		&internal.ID,
		&internal.PublishedAt,
		&internal.ConsumeAfter,
		&uuid,
		&payload,
		&meta,
	); err != nil {
		return messageRow{}, err
	}

	internal.Msg = message.NewMessage(uuid, payload)

	if err := json.Unmarshal([]byte(meta), &internal.Msg.Metadata); err != nil {
		return messageRow{}, err
	}

	return internal, nil
}
