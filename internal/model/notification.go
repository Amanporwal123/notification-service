package model

import (
	"time"

	"gorm.io/gorm"
)

// Notification represents a message sent to a user in our system.
// We use GORM tags to define the database schema constraints and
// JSON tags to control how it looks when returned via the HTTP API.
type Notification struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	Type        string         `gorm:"type:varchar(20);not null;index" json:"type"` // e.g., EMAIL, SMS, PUSH
	Recipient   string         `gorm:"type:varchar(255);not null;index" json:"recipient"`
	Content     string         `gorm:"type:text;not null" json:"content"`
	Status      string         `gorm:"type:varchar(20);not null;default:'PENDING';index" json:"status"`
	RetryCount  int            `gorm:"default:0" json:"retry_count"`
	ProviderID  string         `gorm:"type:varchar(255)" json:"provider_id,omitempty"` // External ID from SendGrid/Twilio
	
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"` // We hide DeletedAt from JSON responses using "-"
}
