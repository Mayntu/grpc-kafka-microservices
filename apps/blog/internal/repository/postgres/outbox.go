package postgres

import (
	"blog/internal/domain"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type outboxRepo struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) domain.OutboxRepo {
	return &outboxRepo{
		db: db,
	}
}

func (r *outboxRepo) CreateWithinTx(ctx context.Context, tx pgx.Tx, event *domain.OutboxEvent) (*domain.OutboxEvent, error) {
	query := `
		INSERT INTO outbox_events
		(id, event_type, payload, status)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at
	`

	err := tx.QueryRow(ctx, query, event.ID, event.EventType, event.Payload, event.Status).Scan(&event.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("outbox repo: failed to create outbox_event: %w", err)
	}

	return event, nil
}

func (r *outboxRepo) FetchPending(ctx context.Context, count int32) ([]*domain.OutboxEvent, error) {
	query := `
		SELECT id, event_type, payload, status, created_at
		FROM outbox_events
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`
	rows, err := r.db.Query(ctx, query, string(domain.Pending), count)
	if err != nil {
		return nil, fmt.Errorf("outbox repo: failed to fetch %d events: %w", count, err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		event := &domain.OutboxEvent{}
		err := rows.Scan(&event.ID, &event.EventType, &event.Payload, &event.Status, &event.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("outbox repo: scan event: %w", err)
		}
		events = append(events, event)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("outbox repo: rows error: %w", err)
	}
	return events, nil
}

func (r *outboxRepo) MarkFailed(ctx context.Context, id string) error {
	query := "UPDATE outbox_events SET status = $1 WHERE id = $2"
	tag, err := r.db.Exec(ctx, query, string(domain.Failed), id)
	if err != nil {
		return fmt.Errorf("outbox repo: failed to mark failed with id=%v : %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox repo: not found with id: %v", id)
	}
	return nil
}

func (r *outboxRepo) MarkSent(ctx context.Context, id string) error {
	query := "UPDATE outbox_events SET status = $1 WHERE id = $2"

	tag, err := r.db.Exec(ctx, query, string(domain.Sent), id)
	if err != nil {
		return fmt.Errorf("outbox repo: failed to mark sent with id=%v : %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox repo: outbox event with id: %v not found", id)
	}
	return nil
}
