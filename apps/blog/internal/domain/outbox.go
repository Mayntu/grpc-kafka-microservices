package domain

import (
	"context"
	"go-proj/pkg/events"
	"time"

	"github.com/jackc/pgx/v5"
)

type OutboxEventStatus string

const (
	Pending OutboxEventStatus = "pending"
	Sent    OutboxEventStatus = "sent"
	Failed  OutboxEventStatus = "failed"
)

type OutboxEvent struct {
	ID        string
	EventType events.EventType
	Payload   []byte
	Status    OutboxEventStatus
	CreatedAt time.Time
}

type OutboxRepo interface {
	CreateWithinTx(ctx context.Context, tx pgx.Tx, event *OutboxEvent) (*OutboxEvent, error)
	FetchPending(ctx context.Context, count int32) ([]*OutboxEvent, error)
	MarkFailed(ctx context.Context, id string) error
	MarkSent(ctx context.Context, id string) error
}
