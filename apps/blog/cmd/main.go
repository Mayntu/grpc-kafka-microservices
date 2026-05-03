package main

import (
	"blog/internal/config"
	"blog/internal/repository/postgres"
	handler "blog/internal/transport/http"
	"blog/internal/usecase"
	"blog/internal/worker"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-proj/pkg/events"
	"go-proj/pkg/gen/auth"
	"go-proj/pkg/kafka"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
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
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	func() {
		db, err := sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("goose: failed to open db: %v", err)
		}
		defer db.Close()

		log.Println("Running migrations...")
		if err := goose.SetDialect("postgres"); err != nil {
			log.Fatalf("goose: failed to set dialect: %v", err)
		}

		if err := goose.Up(db, "migrations"); err != nil {
			log.Fatalf("goose: failed to run migrations: %v", err)
		}
		log.Println("Migrations completed successfully")
	}()

	conn, err := grpc.NewClient(cfg.AuthServiceAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to create grpc client: %v", err)
	}
	defer conn.Close()

	authClient := auth.NewAuthServiceClient(conn)

	repo := postgres.NewArticleRepository(dbPool)
	outboxRepo := postgres.NewOutboxRepository(dbPool)
	logic := usecase.NewArticleUsecase(repo, outboxRepo)
	h := handler.NewHandler(logic, authClient)

	router := h.InitRoutes(conn)

	httpServer := &http.Server{
		Addr:         cfg.HTTPServer.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	if len(cfg.KafkaServer.Brokers) > 0 {
		logger.Info("kafka config", "brokers", cfg.KafkaServer.Brokers, "topic", events.TopicBlogEvents)
		if err := kafka.CreateTopics(ctx, cfg.KafkaServer.Brokers[0], []kafka.TopicConfig{
			{
				Topic:             events.TopicBlogEvents,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}); err != nil {
			logger.Error("failed to create kafka topics", "error", err)
		}

		kafkaProducer := kafka.NewProducer(
			cfg.KafkaServer.Brokers,
			events.TopicBlogEvents,
			kafka.WithProducerBatchTimeout(10*time.Millisecond),
		)
		defer kafkaProducer.Close()

		outboxWorker := worker.NewWorker(outboxRepo, kafkaProducer, logger, 5*time.Second)
		go outboxWorker.Run(ctx)
	} else {
		logger.Warn("KAFKA_SERVER_BROKERS is empty; outbox worker will not publish events")
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("Starting server on %s", cfg.HTTPServer.Address)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		log.Println("Shutting down gracefully...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.HTTPServer.ShutdownTimeout)
		defer shutdownCancel()

		return httpServer.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		log.Printf("Service stopped with error: %s", err)
	} else {
		log.Println("Service stopped successfully")
	}
}
