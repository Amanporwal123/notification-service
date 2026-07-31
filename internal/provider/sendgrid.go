package provider

import (
	"context"
	"fmt"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

type sendGridProvider struct {
	apiKey    string
	fromEmail string
}

func NewSendGridProvider(apiKey, fromEmail string) NotificationProvider {
	return &sendGridProvider{apiKey: apiKey, fromEmail: fromEmail}
}

func (p *sendGridProvider) SendEmail(ctx context.Context, to string, content string) error {
	from := mail.NewEmail("Notification Service", p.fromEmail)
	subject := "New Notification"
	toEmail := mail.NewEmail("User", to)
	
	message := mail.NewSingleEmail(from, subject, toEmail, content, content)
	
	client := sendgrid.NewSendClient(p.apiKey)
	response, err := client.Send(message)
	if err != nil {
		return fmt.Errorf("failed to send email via SendGrid: %w", err)
	}
	
	if response.StatusCode >= 400 {
		return fmt.Errorf("sendgrid returned bad status code: %d", response.StatusCode)
	}
	return nil
}

func (p *sendGridProvider) SendSMS(ctx context.Context, to string, content string) error {
	return fmt.Errorf("SendGrid provider does not support SMS")
}
