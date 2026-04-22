package main

import (
	"auth/internal/config"
	"auth/internal/jwt"
	"auth/internal/outbox"
	"auth/internal/repository/postgres"
	app_http "auth/internal/transport/http"
	app_rpc "auth/internal/transport/rpc"
	"auth/internal/usecase"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-proj/pkg/events"
	"go-proj/pkg/gen/auth"
	"go-proj/pkg/kafka"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.MustLoad()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Could not make dbPool connection: %s", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("DB is not responding: %s", err)
	}

	func() {
		db, err := sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("Migrations connection failed: %s", err)
		}
		defer db.Close()

		log.Println("Running migrations...")
		if err := goose.SetDialect("postgres"); err != nil {
			log.Fatalf("Failed to set dialect to Postgres: %s", err)
		}

		err = goose.Up(db, "migrations")
		if err != nil {
			log.Fatalf("Failed to run migrations: %s", err)
		}
		log.Println("Migrations completed successfully")
	}()

	// 2. Инициализация слоев
	repo := postgres.NewUserRepository(dbPool)
	outboxRepo := postgres.NewOutboxRepository(dbPool)
	tokenManager := jwt.NewTokenManager(cfg.JWT.SecretKey, cfg.JWT.TTL)
	logic := usecase.NewUserUsecase(repo, outboxRepo, tokenManager)

	// 3. Настройка gRPC
	grpcHandler := app_rpc.NewUserRPCHandler(logic)
	grpcServer := grpc.NewServer()
	auth.RegisterAuthServiceServer(grpcServer, grpcHandler)

	// 4. Настройка HTTP
	httpHandler := app_http.NewHandler(logic)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.HTTPServer.Port),
		Handler: httpHandler.InitRoutes(),
	}
	logger.Info("kafka config", "brokers", cfg.KafkaServer.Brokers, "topic", cfg.KafkaServer.UserCreatedTopic)
	// 5. Настройка Kafka
	if err := kafka.CreateTopics(ctx, cfg.KafkaServer.Brokers[0], []kafka.TopicConfig{
		{
			Topic:             events.TopicAuthEvents,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}); err != nil {
		logger.Error("failed to create kafka topics", "error", err)
		// не fatal - топик мог уже существовать
	}

	kafkaProducer := kafka.NewProducer(
		cfg.KafkaServer.Brokers,
		events.TopicAuthEvents,
		kafka.WithProducerBatchTimeout(10*time.Millisecond),
	)
	defer kafkaProducer.Close()

	// OutboxWorker
	worker := outbox.NewWorker(
		outboxRepo,
		kafkaProducer,
		logger,
		5*time.Second,
	)
	go worker.Run(ctx)

	// 6. Запуск через ErrGroup для Graceful Shutdown
	g, gCtx := errgroup.WithContext(ctx)

	// Запуск gRPC
	g.Go(func() error {
		l, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.RPCServer.Port))
		if err != nil {
			return err
		}
		log.Printf("gRPC server starting on :%s", cfg.RPCServer.Port)
		return grpcServer.Serve(l)
	})

	// Запуск HTTP
	g.Go(func() error {
		log.Printf("HTTP server starting on :%s", cfg.HTTPServer.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// Ожидание сигнала завершения и Graceful Shutdown
	g.Go(func() error {
		<-gCtx.Done() // Ждем либо системный сигнал, либо ошибку от серверов
		log.Println("Shutting down gracefully...")

		// Останавливаем gRPC сразу
		grpcServer.GracefulStop()

		// Останавливаем HTTP с таймаутом
		shutdownCtx, timeoutCancel := context.WithTimeout(context.Background(), cfg.HTTPServer.ShutdownTimeout)
		defer timeoutCancel()

		return httpServer.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		log.Printf("Service stopped with error: %s", err)
	} else {
		log.Println("Service stopped successfully")
	}
}
