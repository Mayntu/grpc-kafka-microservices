package main

import (
	"context"
	"errors"
	"go-proj/pkg/events"
	"go-proj/pkg/kafka"
	"log"
	"log/slog"
	"notify/internal/config"
	"notify/internal/transport"
	"notify/internal/usecase"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func main() {
	cfg := config.MustLoad()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := kafka.CreateTopics(ctx, cfg.KafkaServer.Brokers[0], []kafka.TopicConfig{
		{Topic: events.TopicAuthEvents, NumPartitions: 1, ReplicationFactor: 1},
		{Topic: events.TopicAuthEventsDLQ, NumPartitions: 1, ReplicationFactor: 1},
	}); err != nil {
		logger.Error("failed to create kafka topics", "error", err)
	}

	// ? поч не закрываем консьюмера
	consumer := kafka.NewConsumer(
		cfg.KafkaServer.Brokers,
		events.TopicAuthEvents,
		kafka.WithConsumerGroupID(cfg.KafkaServer.GroupID),
	)
	dlqProducer := kafka.NewProducer(
		cfg.KafkaServer.Brokers,
		events.TopicAuthEventsDLQ,
	)
	defer dlqProducer.Close()

	notifyUsecase := usecase.NewNotifyUsecase(logger)
	kafkaConsumer := transport.NewKafkaConsumer(consumer, dlqProducer, notifyUsecase, logger, cfg.DLQMaxRetries)
	defer kafkaConsumer.Close()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		logger.Info("notify service started", "topic", events.TopicAuthEvents)
		if err := kafkaConsumer.Start(gCtx); err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		logger.Info("notify service shutting down")
		return nil
	})

	if err := g.Wait(); !errors.Is(err, context.Canceled) && err != nil {
		log.Printf("notify service stopped with error: %s", err)
	} else {
		log.Println("notify service stopped successfully")
	}
}
