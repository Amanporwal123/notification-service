package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/Amanporwal123/notification-service/internal/constants"
	"github.com/Amanporwal123/notification-service/internal/model"
	"github.com/Amanporwal123/notification-service/internal/repository"
	"github.com/Amanporwal123/notification-service/pkg/kafka"
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
	repo       repository.NotificationRepository
	producer   kafka.Producer
	kafkaTopic string
}

func NewNotificationService(repo repository.NotificationRepository, producer kafka.Producer, topic string) NotificationService {
	return &notificationService{
		repo:       repo,
		producer:   producer,
		kafkaTopic: topic,
	}
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

	// 3. Publish Event to Kafka (Event-Driven Magic!)
	// We use a unique key (e.g. notification_1) so Kafka knows how to route it.
	key := fmt.Sprintf("notification_%d", notification.ID)
	if err := s.producer.PublishEvent(ctx, s.kafkaTopic, key, notification); err != nil {
		// Even if Kafka fails, we don't return an error to the user because it's already saved in the DB!
		// A robust system would have a background job to retry sending "PENDING" notifications later.
		logger.Log.Error("Failed to publish notification event to Kafka", zap.Error(err))
	}

	logger.Log.Info("Notification created successfully", zap.Uint("id", notification.ID))
	return notification, nil
}
