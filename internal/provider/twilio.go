package provider

import (
	"context"
	"fmt"

	"github.com/twilio/twilio-go"
	openapi "github.com/twilio/twilio-go/rest/api/v2010"
)

type twilioProvider struct {
	client     *twilio.RestClient
	fromNumber string
}

func NewTwilioProvider(username, password, fromNumber string) NotificationProvider {
	client := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: username,
		Password: password,
	})
	return &twilioProvider{client: client, fromNumber: fromNumber}
}

func (p *twilioProvider) SendEmail(ctx context.Context, to string, content string) error {
	return fmt.Errorf("Twilio provider does not support Email")
}

func (p *twilioProvider) SendSMS(ctx context.Context, to string, content string) error {
	params := &openapi.CreateMessageParams{}
	params.SetTo(to)
	params.SetFrom(p.fromNumber)
	params.SetBody(content)

	_, err := p.client.Api.CreateMessage(params)
	if err != nil {
		return fmt.Errorf("failed to send SMS via Twilio: %w", err)
	}
	return nil
}
