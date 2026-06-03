package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"gorm.io/gorm"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/handler"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/repository"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/service"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/db"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/transport/kafka"
	"github.com/mehmet-ozkan/go-distributed-geofencing/pkg/postgres"
)

func main() {
	// ── Configuration ─────────────────────────────────────────────
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")

	// ── PostgreSQL (GORM) ─────────────────────────────────────────
	gormDB, err := postgres.New()
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	log.Println("database connection established")

	// ── Migrations ────────────────────────────────────────────────
	if err := db.RunMigrations(); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}
	log.Println("database migrations applied")

	// ── Repository, Kafka & Service ───────────────────────────────
	locationRepo := repository.NewLocationRepository(gormDB)

	// Kafka Producer
	kafkaProducer := kafka.NewProducer([]string{kafkaBrokers}, "location-updates")

	// Kafka Consumer
	kafkaConsumer, err := kafka.NewConsumer([]string{kafkaBrokers}, "location-updates", "geofencing-group", locationRepo)
	if err != nil {
		log.Fatalf("failed to create kafka consumer: %v", err)
	}

	// Context for graceful shutdown of background workers
	ctx, cancel := context.WithCancel(context.Background())

	// Start consumer in background
	go kafkaConsumer.Start(ctx)

	// Service uses Producer
	locationService := service.NewLocationService(kafkaProducer)

	// ── HTTP Server ───────────────────────────────────────────────
	app := fiber.New(fiber.Config{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	})
	app.Use(recover.New())

	locationHandler := handler.NewLocationHandler(locationService)

	// ── Routing ───────────────────────────────────────────────────
	appRoute := api.NewRoute(locationHandler)
	appRoute.SetupRoutes(&api.RouteContext{App: app})

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

	shutdown(app, gormDB, kafkaProducer, kafkaConsumer, cancel, log.New(os.Stdout, "", 0))

	log.Println("server stopped gracefully")
}

func shutdown(app *fiber.App, gormDB *gorm.DB, kafkaProducer kafka.Producer, kafkaConsumer kafka.Consumer, cancel context.CancelFunc, logger *log.Logger) {
	// 1. Stop consumer reading gracefully
	cancel()
	if err := kafkaConsumer.Close(); err != nil {
		logger.Printf("shutdown: consumer close error: %v", err)
	}

	// 2. Close producer
	if err := kafkaProducer.Close(); err != nil {
		logger.Printf("shutdown: producer close error: %v", err)
	}

	// 3. HTTP Sunucusunu Kapat
	if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
		logger.Printf("shutdown: HTTP server error: %v", err)
	}

	// Veritabanı Bağlantısını Kapat
	sqlDB, err := gormDB.DB()
	if err == nil {
		if dbErr := sqlDB.Close(); dbErr != nil {
			logger.Printf("shutdown: database error: %v", dbErr)
		}
	} else {
		logger.Printf("shutdown: failed to retrieve database connection: %v", err)
	}

	// Loglama
	logger.Println("shutdown: completed")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
