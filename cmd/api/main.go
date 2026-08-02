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
	"github.com/Amanporwal123/notification-service/internal/provider"
	"github.com/Amanporwal123/notification-service/internal/repository"
	"github.com/Amanporwal123/notification-service/internal/worker"
	"github.com/Amanporwal123/notification-service/pkg/kafka"
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

	// Initialize Kafka Producer
	producer := kafka.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Log.Error("Failed to close Kafka producer", zap.Error(err))
		}
	}()
	logger.Log.Info("Kafka Producer initialized successfully", zap.Strings("brokers", cfg.Kafka.Brokers))

	// Initialize Background Worker Dependencies
	sendgridProvider := provider.NewSendGridProvider(cfg.Providers.SendGrid.ApiKey, cfg.Providers.SendGrid.FromEmail)
	twilioProvider := provider.NewTwilioProvider(cfg.Providers.Twilio.AccountSID, cfg.Providers.Twilio.AuthToken, cfg.Providers.Twilio.FromNumber)

	consumer := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, "notification_worker_group")
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Log.Error("Failed to close Kafka consumer", zap.Error(err))
		}
	}()

	processor := worker.NewProcessor(
		consumer, 
		sendgridProvider, 
		twilioProvider, 
		repository.DB, 
		cfg.Kafka.MaxWorkers,
		cfg.Kafka.MaxRetries,
		cfg.Kafka.InitialBackoffMs,
	)
	
	// Create a context for the worker that we will cancel on shutdown
	workerCtx, workerCancel := context.WithCancel(context.Background())
	// NOTE: We don't defer workerCancel here because we need it in the main scope to manually cancel later.

	go func() {
		processor.Start(workerCtx)
	}()
	logger.Log.Info("Background Worker started successfully in a goroutine")

	// Initialize Router and Dependencies
	r := api.SetupRouter(producer, cfg.Kafka.Topic)

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
	logger.Log.Info("Shutting down server and worker...")

	// Cancel the worker context to stop the background loop
	workerCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Log.Fatal("Server forced to shutdown:", zap.Error(err))
	}

	logger.Log.Info("Server exiting")
}
