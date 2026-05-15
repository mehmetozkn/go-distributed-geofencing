package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	_ "github.com/lib/pq"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/repository"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/service"
	transportHTTP "github.com/mehmet-ozkan/go-distributed-geofencing/internal/transport/http"
	transportKafka "github.com/mehmet-ozkan/go-distributed-geofencing/internal/transport/kafka"
)

func main() {
	// ── Configuration (env vars) ──────────────────────────────────
	dbDSN := envOrDefault("DATABASE_URL", "postgres://geofencing:geofencing_secret@localhost:5432/geofencing_db?sslmode=disable")
	kafkaBrokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaGroup := envOrDefault("KAFKA_GROUP", "geofencing-consumer-group")
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")

	// ── Database ──────────────────────────────────────────────────
	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Println("connected to PostgreSQL")

	// Run migrations
	if _, err := db.Exec(repository.MigrateSQL); err != nil {
		log.Fatalf("migration failed: %v", err)
	}
	log.Println("database migrations applied")

	// ── Repositories ──────────────────────────────────────────────
	locationRepo := repository.NewLocationRepository(db)
	geofenceRepo := repository.NewGeofenceRepository(db)

	// ── Kafka Producer ────────────────────────────────────────────
	producer, err := transportKafka.NewProducer(kafkaBrokers)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	// ── Service (DI wiring) ───────────────────────────────────────
	locationService := service.NewLocationService(producer, locationRepo, geofenceRepo)

	// ── Kafka Consumer ────────────────────────────────────────────
	consumer, err := transportKafka.NewConsumer(kafkaBrokers, kafkaGroup, locationService)
	if err != nil {
		log.Fatalf("failed to create kafka consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	}()

	// ── HTTP Server ───────────────────────────────────────────────
	app := fiber.New(fiber.Config{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	})
	app.Use(recover.New())

	handler := transportHTTP.NewLocationHandler(locationService)
	handler.RegisterRoutes(app)

	go func() {
		log.Printf("HTTP server listening on %s", httpAddr)
		if err := app.Listen(httpAddr); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// ── Graceful Shutdown ─────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	cancel() // stop consumer

	if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Println("server stopped gracefully")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
