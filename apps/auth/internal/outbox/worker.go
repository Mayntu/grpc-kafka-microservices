package outbox

import (
	"auth/internal/domain"
	"context"
	"encoding/json"
	"fmt"
	"go-proj/pkg/events"
	"go-proj/pkg/kafka"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Worker struct {
	outboxRepo domain.OutboxRepository
	producer   *kafka.Producer
	logger     *slog.Logger
	interval   time.Duration
}

func NewWorker(
	outboxRepo domain.OutboxRepository, producer *kafka.Producer, logger *slog.Logger, interval time.Duration,
) *Worker {
	return &Worker{
		outboxRepo: outboxRepo,
		producer:   producer,
		logger:     logger,
		interval:   interval,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("outbox worker started", "interval", w.interval)

	for {
		// ?
		select {
		// ?
		case <-ctx.Done():
			w.logger.Info("outbox worker stopped")
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				w.logger.Error("outbox worker: process batch", "error", err)
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) error {
	pending, err := w.outboxRepo.FetchPending(ctx, 50)
	if err != nil {
		return fmt.Errorf("fetch pending: %w", err)
	}

	for _, event := range pending {
		if err := w.publishEvent(ctx, event); err != nil {
			w.logger.Error("outbox error: publish event",
				"error",
				err,
			)
			// ?
			_ = w.outboxRepo.MarkFailed(ctx, event.ID)
			continue
		}

		if err := w.outboxRepo.MarkSent(ctx, event.ID); err != nil {
			w.logger.Error("outbox worker: mark sent failed", "event_id", event.ID, "error", err)
		}
	}
	return nil
}

func (w *Worker) publishEvent(ctx context.Context, event *domain.OutboxEvent) error {
	key := []byte(event.EventType)

	if err := w.producer.SendMessage(ctx, key, event.Payload); err != nil {
		return fmt.Errorf("send to kafka: %w", err)
	}
	w.logger.Info("outbox worker: event published", "event_type", event.EventType, "event_id", event.ID)
	return nil
}

func BuildUserCreatedPayload(userID string, email string) ([]byte, error) {
	evt := events.UserCreatedEvent{
		EventID:   uuid.New().String(),
		EventType: events.EventTypeUserCreated,
		UserID:    userID,
		Email:     email,
		CreatedAt: time.Now(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal user created event: %w", err)
	}
	return payload, nil
}
