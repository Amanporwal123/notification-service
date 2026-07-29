package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Amanporwal123/notification-service/internal/api"
	"github.com/Amanporwal123/notification-service/internal/config"
	"github.com/Amanporwal123/notification-service/internal/repository"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"go.uber.org/zap"
)

// main.go is the entry point of the application.
// It initializes config, db, and starts the Gin HTTP server.

func main() {
	if err := logger.InitLogger(); err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync()
	logger.Log.Info("Logger initialized successfully")

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Log.Fatal("failed to load config", zap.Error(err))
	}
	logger.Log.Info("Configuration loaded", zap.String("port", cfg.Server.Port))

	// Initialize Database Connection
	if err := repository.ConnectToDB(cfg.Database); err != nil {
		logger.Log.Fatal("Database connection failed", zap.Error(err))
	}

	// Initialize Router and Dependencies
	r := api.SetupRouter()

	addr := ":" + cfg.Server.Port
	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		logger.Log.Info("Starting HTTP server", zap.String("address", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("Listen failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	logger.Log.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Log.Fatal("Server forced to shutdown:", zap.Error(err))
	}

	logger.Log.Info("Server exiting")
}
