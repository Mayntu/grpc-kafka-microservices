package postgres

import (
	"auth/internal/domain"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type outboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) domain.OutboxRepository {
	return &outboxRepository{
		db: db,
	}
}

func (r *outboxRepository) CreateWithinTx(ctx context.Context, tx pgx.Tx, event *domain.OutboxEvent) error {
	query := `INSERT INTO outbox_events (event_type, payload, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := tx.Exec(
		ctx,
		query,
		event.EventType,
		event.Payload,
		domain.OutboxStatusPending,
		time.Now(),
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("outbox repo: create: %w", err)
	}
	return nil
}

func (r *outboxRepository) FetchPending(ctx context.Context, limit int) ([]*domain.OutboxEvent, error) {
	query := `
		SELECT id, event_type, payload, status, created_at, updated_at
		FROM outbox_events
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	rows, err := r.db.Query(ctx, query, domain.OutboxStatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox repo: get all outbox events failed: %w", err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		e := &domain.OutboxEvent{}
		err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.Status, &e.CreatedAt, &e.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("outbox repo: scan outbox_event: %w", err)
		}
		events = append(events, e)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("outbox repo: rows error: %w", err)
	}
	return events, nil
}

func (r *outboxRepository) MarkSent(ctx context.Context, id int64) error {
	query := `UPDATE outbox_events SET status = $1, updated_at = $2 WHERE id = $3`
	tag, err := r.db.Exec(ctx, query, domain.OutboxStatusSent, time.Now(), id)
	if err != nil {
		return fmt.Errorf("outbox repo: failed to update outbox_event with id %v: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("repo: outbox_event with id: %v Not Found", id)
	}
	return nil
}

func (r *outboxRepository) MarkFailed(ctx context.Context, id int64) error {
	query := `UPDATE outbox_events SET status = $1, updated_at = $2 WHERE id = $3`
	tag, err := r.db.Exec(ctx, query, domain.OutboxStatusFailed, time.Now(), id)
	if err != nil {
		return fmt.Errorf("outbox repo: mark failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox repo: outbox_event with id: %v Not Found", id)
	}
	return nil
}
