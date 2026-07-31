package provider

import "context"

type NotificationProvider interface {
	SendEmail(ctx context.Context, to string, content string) error
	SendSMS(ctx context.Context, to string, content string) error
}
