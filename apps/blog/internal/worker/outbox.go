package worker

import (
	"blog/internal/domain"
	"context"
	"encoding/json"
	"fmt"
	"go-proj/pkg/events"
	"go-proj/pkg/kafka"
	"log/slog"
	"time"
)

type Worker struct {
	outboxRepo domain.OutboxRepo
	producer   *kafka.Producer
	logger     *slog.Logger
	interval   time.Duration
}

func NewWorker(outboxRepo domain.OutboxRepo, producer *kafka.Producer, logger *slog.Logger, interval time.Duration) *Worker {
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

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("outbox worker stopped")
			return
		case <-ticker.C:
			if err := w.ProcessBatch(ctx); err != nil {
				w.logger.Error("failed to process batch", "error", err)
			}
		}
	}
}

func (w *Worker) ProcessBatch(ctx context.Context) error {
	pending, err := w.outboxRepo.FetchPending(ctx, 50)
	if err != nil {
		return fmt.Errorf("failed to fetch pending: %w", err)
	}

	for _, event := range pending {
		if err := w.publishEvent(ctx, event); err != nil {
			w.logger.Error("outbox worker: failed to publish event", "error", err)

			err := w.outboxRepo.MarkFailed(ctx, event.ID)
			if err != nil {
				w.logger.Error("failed to mark failed event", "event_id", event.ID)
			}
			continue
		}
		if err := w.outboxRepo.MarkSent(ctx, event.ID); err != nil {
			w.logger.Error("failed to mark sent event", "event_id", event.ID)
		}
	}
	return nil
}

func (w *Worker) publishEvent(ctx context.Context, event *domain.OutboxEvent) error {
	key := []byte(event.EventType)
	err := w.producer.SendMessage(ctx, key, event.Payload)
	if err != nil {
		return fmt.Errorf("failed to send to kafka: %w", err)
	}

	w.logger.Info("outbox worker: kafka event published", "event_id", event.ID, "event_type", event.EventType)
	return nil
}

func BuildArticleCreatedPayload(eventID, userID, title string, articleID int64, createdAt time.Time) ([]byte, error) {
	event := events.ArticleCreatedEvent{
		EventID:   eventID,
		EventType: events.EventTypeArticleCreated,
		UserID:    userID,
		ArticleID: articleID,
		Title:     title,
		CreatedAt: createdAt,
	}

	val, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("build article created payload: %w", err)
	}
	return val, nil
}
