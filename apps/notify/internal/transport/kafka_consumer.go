package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"go-proj/pkg/events"
	"go-proj/pkg/kafka"
	"log/slog"
	"notify/internal/domain"
	"time"

	kafka_go "github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	consumer   *kafka.Consumer
	dlq        *kafka.Producer // Dead Letter Queue
	handler    domain.EventHandler
	logger     *slog.Logger
	maxRetries int
}

func NewKafkaConsumer(
	consumer *kafka.Consumer,
	dlq *kafka.Producer,
	handler domain.EventHandler,
	logger *slog.Logger,
	maxRetries int,
) *KafkaConsumer {
	return &KafkaConsumer{
		consumer:   consumer,
		dlq:        dlq,
		handler:    handler,
		logger:     logger,
		maxRetries: maxRetries,
	}
}

func (k *KafkaConsumer) Start(ctx context.Context) error {
	return k.consumer.Consume(ctx, k.handle)
}

func (k *KafkaConsumer) handle(ctx context.Context, msg kafka_go.Message) error {
	var raw map[string]interface{}
	json.Unmarshal(msg.Value, &raw)

	eventType := raw["event_type"].(string)

	switch eventType {
	case string(events.EventTypeUserCreated):
		var event events.UserCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			k.logger.Error("notify kafka consumer: failed to unmarshal event", "error", err, "offset", msg.Offset)
			return k.sendToDLQWithRetries(ctx, msg, fmt.Sprintf("unmarshal error: %s", err))
		}

		k.logger.Info("notify: received event",
			// "event_type", string(event.EventType),
			"event_id", event.EventID,
			"email", event.Email,
		)

		if err := k.handler.HandleUserCreated(ctx, event.Email); err != nil {
			k.logger.Error("notify kafka consumer: failed to handle event, adding to dlq", "error", err)
			return k.sendToDLQWithRetries(ctx, msg, fmt.Sprintf("handler error: %s", err))
		}

		return nil
	}
	return nil
}

func (k *KafkaConsumer) sendToDLQWithRetries(ctx context.Context, original kafka_go.Message, reason string) error {
	var lastError error
	for attempt := range k.maxRetries {
		dlqMessage := kafka_go.Message{
			Key:   original.Key,
			Value: original.Value,
			Headers: []kafka_go.Header{
				{Key: "dlq-reason", Value: []byte(reason)},
				{Key: "dlq-original-topic", Value: []byte(original.Topic)},
			},
		}
		err := k.dlq.SendMessage(ctx, dlqMessage.Key, dlqMessage.Value)
		if err == nil {
			k.logger.Info("notify kafka consumer: event sent to DLQ", "attemt", attempt, "original_offset", original.Offset)
			return nil
		}

		lastError = err
		k.logger.Error("notify kafka consumer: failed to send to DLQ, retrying", "attempt", attempt+1, "max_retries", k.maxRetries, "error", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("notify kafka consumer: context stopped while sending to DLQ: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond * time.Duration(1<<attempt)):

		}
	}

	k.logger.Error(
		"notify kafka consumer: DLQ unavailable after all retries - event logged for manual recovery",
		"reason", reason,
		"offset", original.Offset,
		"topic", original.Topic,
		"partition", original.Partition,
		"payload", string(original.Value),
		"error", lastError,
	)
	return nil
}

func (k *KafkaConsumer) Close() error {
	return k.consumer.Close()
}
