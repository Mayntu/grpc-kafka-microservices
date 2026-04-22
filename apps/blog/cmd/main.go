package main

import (
	"blog/internal/config"
	"blog/internal/repository/postgres"
	handler "blog/internal/transport/http"
	"blog/internal/usecase"
	"context"
	"database/sql"
	"go-proj/pkg/gen/auth"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.MustLoad()

	dbPool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	func() {
		// 1. Создаем временное соединение для goose
		// Нам нужен стандартный sql.DB, goose не умеет в pgxpool напрямую
		db, err := sql.Open("pgx", cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("goose: failed to open db: %v", err)
		}
		defer db.Close()

		// 2. Запускаем миграции
		log.Println("Running migrations...")
		if err := goose.SetDialect("postgres"); err != nil {
			log.Fatalf("goose: failed to set dialect: %v", err)
		}

		// Указываем путь к папке с .sql файлами
		if err := goose.Up(db, "migrations"); err != nil {
			log.Fatalf("goose: failed to run migrations: %v", err)
		}
		log.Println("Migrations completed successfully")
	}()

	conn, err := grpc.NewClient(cfg.AuthServiceAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to create grpc client: %v", err)
	}
	authClient := auth.NewAuthServiceClient(conn)

	repo := postgres.NewArticleRepository(dbPool)
	logic := usecase.NewArticleUsecase(repo)
	h := handler.NewHandler(logic, authClient)

	router := h.InitRoutes(conn)

	srv := &http.Server{
		Addr:         cfg.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	go func() {
		log.Printf("Starting server on %s", cfg.HTTPServer.Address)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	log.Println("Shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}

// func main() {
// 	conn, err := grpc.NewClient(":50051", grpc.WithInsecure())
// 	if err != nil {
// 		log.Fatalf("Failed to connect to server: %v", err)
// 	}

// 	client := auth.NewAuthServiceClient(conn)

// 	response, err := client.ValidateJwt(context.Background(), &auth.ValidateJWTRequest{Jwt: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySUQiOiI4MGUzNjA3Mi1kYzI3LTQ4YTMtOTRhNS00ZmRiOWM0MGFhNTUiLCJFbWFpbCI6InRlc3QyQGV4YW1wbGUuY29tIiwiZXhwIjoxNzc2MTk2OTY3LCJpYXQiOjE3NzYxOTYzNjd9.X9Cdtlasob00SyeBBpdwvCgSf-1_Cyqg3gX5Yqs3pks"})
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	log.Printf("result : %v", response)
// }
