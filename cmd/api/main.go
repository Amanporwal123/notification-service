package main

import (
	"log"
	"net/http"

	"github.com/Amanporwal123/notification-service/internal/config"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// main.go is the entry point of the application.
// It initializes config, db, and starts the Gin HTTP server.

func main() {
	if err := logger.InitLogger(); err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync() // Ensure logs are flushed before the program exits
	logger.Log.Info("Logger initialized successfully")

	// Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Log.Fatal("failed to load config", zap.Error(err))
	}
	logger.Log.Info("Configuration loaded", zap.String("port", cfg.Server.Port))

	//Initialize Gin Router
	r := gin.Default()

	//Setup Basic Route (Healthcheck)
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})
	//Start HTTP Server
	addr := ":" + cfg.Server.Port
	logger.Log.Info("Starting HTTP server", zap.String("address", addr))

	if err := r.Run(addr); err != nil {
		logger.Log.Fatal("failed to start server", zap.Error(err))
	}

}
