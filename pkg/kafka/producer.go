package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

type ProducerOption func(*kafka.Writer)

// какой балансировщик нагрузки использовать:
// leastbytes - в свободную партицию
// round robin - 1-е сообщение в 0-ю партицию, 2-е сообщение в 1-ю партицию, 3-е сообщение во 2-ю партицию и тд
// hash - когда важен порядок, одинаковый хэш в одну и ту же партицию
// crc32, murmur2 - Это конкретные алгоритмы хеширования (разновидности Hash-балансировщика), для совместимости Go и Java например
func WithProducerBalancer(b kafka.Balancer) ProducerOption {
	return func(w *kafka.Writer) {
		w.Balancer = b
	}
}

// ожидать ли подтверждение от кафки, что сообщение принято (false - синхронно - ожидать) (true - асинхронно - не ожидать)
func WithProducerAsync(async bool) ProducerOption {
	return func(w *kafka.Writer) {
		w.Async = async
	}
}

// сколько ждать, перед отправкой батча
func WithProducerBatchTimeout(d time.Duration) ProducerOption {
	return func(w *kafka.Writer) {
		w.BatchTimeout = d
	}
}

func NewProducer(brokers []string, topic string, opts ...ProducerOption) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// default value: сколько ждать перед отправкой батча
		BatchTimeout: 10 * time.Millisecond,
		// как только в лидер партицию И в реплики
		RequiredAcks: kafka.RequireAll,
		// 3 раза попробовать и если кафка не доступна - ошибка
		MaxAttempts: 3,
	}

	for _, opt := range opts {
		opt(w)
	}

	return &Producer{
		writer: w,
	}
}

func (p *Producer) SendMessage(ctx context.Context, key, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	})
	if err != nil {
		return fmt.Errorf("kafka producer: send message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("kafka producer: close: %w", err)
	}
	return nil
}
