package api

import (
	"github.com/Amanporwal123/notification-service/internal/handler"
	"github.com/Amanporwal123/notification-service/internal/repository"
	"github.com/Amanporwal123/notification-service/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
)

// SetupRouter initializes all services, handlers, and defines the HTTP routes.
// This keeps main.go clean as the application grows.
func SetupRouter() *gin.Engine {
	r := gin.Default()

	// 1. Initialize Dependencies (Dependency Injection)
	// As the app grows, you can move this into a dedicated "InitializeDependencies" function
	notificationRepo := repository.NewNotificationRepository(repository.DB)
	notificationSvc := service.NewNotificationService(notificationRepo)
	notificationHandler := handler.NewNotificationHandler(notificationSvc)

	// 2. Setup Health Check
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// 3. Setup API Groups
	api := r.Group("/api/v1")
	{
		// Notification Routes
		api.POST("/notifications", notificationHandler.HandleCreateNotification)
		
		// Future routes will go here:
		// api.GET("/notifications/:id", notificationHandler.HandleGetNotification)
		// api.POST("/users", userHandler.HandleCreateUser)
	}

	return r
}
