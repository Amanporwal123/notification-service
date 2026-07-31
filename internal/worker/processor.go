package worker

import (
	"context"
	"encoding/json"

	"github.com/Amanporwal123/notification-service/internal/constants"
	"github.com/Amanporwal123/notification-service/internal/model"
	"github.com/Amanporwal123/notification-service/internal/provider"
	"github.com/Amanporwal123/notification-service/pkg/kafka"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Processor struct {
	consumer kafka.Consumer
	email    provider.NotificationProvider
	sms      provider.NotificationProvider
	db       *gorm.DB
}

func NewProcessor(c kafka.Consumer, email provider.NotificationProvider, sms provider.NotificationProvider, db *gorm.DB) *Processor {
	return &Processor{
		consumer: c,
		email:    email,
		sms:      sms,
		db:       db,
	}
}

func (p *Processor) Start(ctx context.Context) {
	logger.Log.Info("Starting Kafka Consumer Background Worker...")

	for {
		// Read raw message
		msgBytes, err := p.consumer.ReadMessage(ctx)
		if err != nil {
			logger.Log.Error("Failed to read message from Kafka", zap.Error(err))
			continue
		}

		// Unmarshal
		var notification model.Notification
		if err := json.Unmarshal(msgBytes, &notification); err != nil {
			logger.Log.Error("Failed to unmarshal Kafka message", zap.Error(err))
			continue
		}

		logger.Log.Info("Processing notification", zap.Uint("id", notification.ID), zap.String("type", notification.Type))

		// Process it in a new goroutine to allow massive concurrency!
		go func(notif model.Notification) {
			var sendErr error
			if notif.Type == "EMAIL" {
				sendErr = p.email.SendEmail(ctx, notif.Recipient, notif.Content)
			} else if notif.Type == "SMS" {
				sendErr = p.sms.SendSMS(ctx, notif.Recipient, notif.Content)
			} else {
				logger.Log.Warn("Unknown notification type, ignoring", zap.String("type", notif.Type))
				return
			}

			// Determine final status
			newStatus := constants.StatusSent
			if sendErr != nil {
				logger.Log.Error("Failed to send notification via Provider", zap.Error(sendErr), zap.Uint("id", notif.ID))
				newStatus = constants.StatusFailed
			}

			// Update database
			if err := p.db.Model(&model.Notification{}).Where("id = ?", notif.ID).Update("status", newStatus).Error; err != nil {
				logger.Log.Error("Failed to update notification status in DB", zap.Error(err), zap.Uint("id", notif.ID))
			} else {
				logger.Log.Info("Successfully processed notification", zap.Uint("id", notif.ID), zap.String("new_status", string(newStatus)))
			}
		}(notification)
	}
}
