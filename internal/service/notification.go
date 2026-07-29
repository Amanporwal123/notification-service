package service

import (
	"context"
	"errors"

	"github.com/Amanporwal123/notification-service/internal/constants"
	"github.com/Amanporwal123/notification-service/internal/model"
	"github.com/Amanporwal123/notification-service/internal/repository"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"go.uber.org/zap"
)

// CreateNotificationRequest is our strict DTO.
// Production tip: use 'oneof' to validate enums at the router level.
type CreateNotificationRequest struct {
	Type      string `json:"type" binding:"required,oneof=EMAIL SMS PUSH"`
	Recipient string `json:"recipient" binding:"required"`
	Content   string `json:"content" binding:"required"`
}

// NotificationService defines the contract.
type NotificationService interface {
	CreateNotification(ctx context.Context, req CreateNotificationRequest) (*model.Notification, error)
}

type notificationService struct {
	repo repository.NotificationRepository
}

func NewNotificationService(repo repository.NotificationRepository) NotificationService {
	return &notificationService{repo: repo}
}

// CreateNotification handles the business rules.
func (s *notificationService) CreateNotification(ctx context.Context, req CreateNotificationRequest) (*model.Notification, error) {
	notification := &model.Notification{
		Type:      req.Type,
		Recipient: req.Recipient,
		Content:   req.Content,
		Status:    constants.StatusPending,
	}

	// 2. Save to DB using our Repository Interface!
	if err := s.repo.Save(ctx, notification); err != nil {
		logger.Log.Error("Failed to insert notification into database", zap.Error(err))
		// ...but return a safe error to the layer above
		return nil, errors.New(constants.ErrInternalServer)
	}

	logger.Log.Info("Notification created successfully", zap.Uint("id", notification.ID))
	return notification, nil
}
