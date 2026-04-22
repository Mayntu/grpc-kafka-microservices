package domain

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type OutboxStatus string

const (
	OutboxStatusPending OutboxStatus = "pending"
	OutboxStatusSent    OutboxStatus = "sent"
	OutboxStatusFailed  OutboxStatus = "failed"
)

type OutboxEvent struct {
	ID int64
	// ?
	EventType string
	// ?
	Payload   []byte // json
	Status    OutboxStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

type OutboxRepository interface {
	CreateWithinTx(ctx context.Context, tx pgx.Tx, event *OutboxEvent) error
	FetchPending(ctx context.Context, limit int) ([]*OutboxEvent, error)
	MarkSent(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64) error
}

// ?
type TX interface {
	Exec(ctx context.Context, sql string, args ...any) (interface{}, error)
}
