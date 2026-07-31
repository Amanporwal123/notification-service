package provider

import (
	"context"
	"fmt"
	"strings"

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
	// Extract a name from the email address (e.g. "amanporwal" from "amanporwal@gmail.com")
	toName := "Customer"
	if atIndex := strings.Index(to, "@"); atIndex > 0 {
		toName = to[:atIndex]
		// Capitalize the first letter
		if len(toName) > 0 {
			toName = strings.ToUpper(toName[:1]) + toName[1:]
		}
	}

	from := mail.NewEmail("System Notifications", p.fromEmail)
	subject := "New System Alert"
	toEmail := mail.NewEmail(toName, to)
	
	plainTextContent := content
	htmlContent := fmt.Sprintf(`
		<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #eaeaec; border-radius: 10px; background-color: #f9f9f9;">
			<h2 style="color: #333333; text-align: center;">Notification Service</h2>
			<div style="background-color: #ffffff; padding: 20px; border-radius: 5px; color: #555555; line-height: 1.6; font-size: 16px;">
				<p>Hello <strong>%s</strong>,</p>
				<p>%s</p>
			</div>
			<div style="margin-top: 20px; text-align: center; color: #999999; font-size: 12px;">
				<p>&copy; 2026 Notification Service Inc. All rights reserved.</p>
				<p>This is an automated message, please do not reply.</p>
			</div>
		</div>
	`, toName, content)
	
	message := mail.NewSingleEmail(from, subject, toEmail, plainTextContent, htmlContent)
	
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
