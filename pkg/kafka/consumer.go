package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader *kafka.Reader
}

type ConsumerOption func(*kafka.ReaderConfig)

// Из какой consumer group читать
func WithConsumerGroupID(id string) ConsumerOption {
	return func(rc *kafka.ReaderConfig) {
		rc.GroupID = id
	}
}

// сколько должно байтов накопиться, чтобы консьюмер прочитал сообщение
//	но если за период времени MaxWait не набралось, кафка прочитает
func WithConsumerMinBytes(n int) ConsumerOption {
	return func(c *kafka.ReaderConfig) {
		c.MinBytes = n
	}
}

// сколько байтов консьюмер максимум потребит
//	но если больше чем MaxBytes, то тогда кафка ВРЕМЕННО расширит MaxBytes, потребит, и вернёт назад MaxBytes
func WithConsumerMaxBytes(n int) ConsumerOption {
	return func(c *kafka.ReaderConfig) {
		c.MaxBytes = n
	}
}

func NewConsumer(brokers []string, topic string, opts ...ConsumerOption) *Consumer {
	cfg := kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		// default параметр, если не передали WithConsumerMinBytes
		MinBytes: 1,
		// 10mb default параметр, если не передали WithConsumerMaxBytes
		MaxBytes: 10e6,
		// 0 - ручной коммит, at least once, пока мы не закоммитим, сообщение не считается обработанным
		CommitInterval: 0,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Consumer{
		reader: kafka.NewReader(cfg),
	}
}

func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka consumer: fetch: %w", err)
		}

		if err := handler(ctx, msg); err != nil {
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka consumer: commit: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("kafka consumer: close: %w", err)
	}
	return nil
}
