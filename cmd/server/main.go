package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/handler"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/repository"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/service"
	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/db"
	"github.com/mehmet-ozkan/go-distributed-geofencing/pkg/postgres"
)

func main() {
	// ── Configuration ─────────────────────────────────────────────
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")

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

	// ── Repository & Service ──────────────────────────────────────
	locationRepo := repository.NewLocationRepository(gormDB)
	locationService := service.NewLocationService(locationRepo)

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
