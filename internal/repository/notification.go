package repository

import (
	"context"

	"github.com/Amanporwal123/notification-service/internal/model"
	"gorm.io/gorm"
)

// NotificationRepository defines the database operations for a Notification.
type NotificationRepository interface {
	Save(ctx context.Context, notification *model.Notification) error
}

// notificationRepository implements the NotificationRepository interface using GORM.
type notificationRepository struct {
	db *gorm.DB
}

// NewNotificationRepository creates a new NotificationRepository.
func NewNotificationRepository(db *gorm.DB) NotificationRepository {
	return &notificationRepository{db: db}
}

// Save inserts a new notification into the database.
func (r *notificationRepository) Save(ctx context.Context, notification *model.Notification) error {
	return r.db.WithContext(ctx).Create(notification).Error
}
