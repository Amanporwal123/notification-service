package constants

// NotificationStatus represents the state of a notification.
// We define it as a custom string type to enforce type safety (Enum pattern in Go).
type NotificationStatus string

const (
	StatusPending NotificationStatus = "PENDING"
	StatusSent    NotificationStatus = "SENT"
	StatusFailed  NotificationStatus = "FAILED"
)

// API Error Messages
const (
	ErrInvalidRequestPayload = "Invalid request payload"
	ErrInternalServer        = "internal server error while saving notification"
)

// API Success Messages
const (
	MsgNotificationQueued = "Notification queued successfully"
)
