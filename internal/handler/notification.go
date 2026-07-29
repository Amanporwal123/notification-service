package handler

import (
	"net/http"

	"github.com/Amanporwal123/notification-service/internal/constants"
	"github.com/Amanporwal123/notification-service/internal/service"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type NotificationHandler struct {
	svc service.NotificationService
}

func NewNotificationHandler(svc service.NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}

// HandleCreateNotification processes POST /notifications
func (h *NotificationHandler) HandleCreateNotification(c *gin.Context) {
	var req service.CreateNotificationRequest

	// 1. Validate Payload
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Log.Warn("Validation failed for CreateNotification", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   constants.ErrInvalidRequestPayload,
			"details": err.Error(),
		})
		return
	}

	// 2. Execute Business Logic (Pass down the request context!)
	notification, err := h.svc.CreateNotification(c.Request.Context(), req)
	if err != nil {
		// The error from the service is safe to return
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 3. Return Success
	c.JSON(http.StatusCreated, gin.H{
		"message": constants.MsgNotificationQueued,
		"data":    notification,
	})
}
