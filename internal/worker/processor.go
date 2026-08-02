package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"

	"github.com/Amanporwal123/notification-service/internal/constants"
	"github.com/Amanporwal123/notification-service/internal/model"
	"github.com/Amanporwal123/notification-service/internal/provider"
	"github.com/Amanporwal123/notification-service/pkg/kafka"
	"github.com/Amanporwal123/notification-service/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Processor struct {
	consumer         kafka.Consumer
	producer         kafka.Producer
	email            provider.NotificationProvider
	sms              provider.NotificationProvider
	db               *gorm.DB
	dlqTopic         string
	maxWorkers       int
	maxRetries       uint
	initialBackoffMs uint
}

func NewProcessor(c kafka.Consumer, p kafka.Producer, email provider.NotificationProvider, sms provider.NotificationProvider, db *gorm.DB, dlqTopic string, maxWorkers int, maxRetries uint, initialBackoffMs uint) *Processor {
	// Fallbacks
	if maxWorkers <= 0 {
		maxWorkers = 100
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if initialBackoffMs <= 0 {
		initialBackoffMs = 2000
	}

	return &Processor{
		consumer:         c,
		producer:         p,
		email:            email,
		sms:              sms,
		db:               db,
		dlqTopic:         dlqTopic,
		maxWorkers:       maxWorkers,
		maxRetries:       maxRetries,
		initialBackoffMs: initialBackoffMs,
	}
}

func (p *Processor) Start(ctx context.Context) {
	logger.Log.Info("Starting Kafka Consumer Background Worker...", zap.Int("max_workers", p.maxWorkers), zap.Uint("max_retries", p.maxRetries))

	// Create a worker pool semaphore
	workerPoolLimit := make(chan struct{}, p.maxWorkers)

	for {
		// Read raw message
		msgBytes, err := p.consumer.ReadMessage(ctx)
		if err != nil {
			// Graceful Shutdown Handling:
			// 1. When the user presses CTRL+C (SIGINT) or the OS sends SIGTERM, the main.go file captures it.
			// 2. main.go triggers `workerCancel()`, which immediately cancels the context (`ctx`) passed into this Start() function.
			// 3. The `ReadMessage(ctx)` function inside kafka-go is listening to this context. When canceled, it instantly aborts
			//    its network wait and returns an error: "fetching message: context canceled".
			// 4. We catch that error here. By checking `ctx.Err() != nil`, we confirm the error was caused by a deliberate shutdown,
			//    not a genuine Kafka network failure.
			// 5. We MUST use `break` to exit this infinite for-loop. If we used `continue`, the loop would instantly restart,
			//    try to read from Kafka again (with a dead context), instantly fail, and loop again millions of times a second,
			//    causing severe log spam. Breaking the loop allows the `Start()` goroutine to exit cleanly.
			if ctx.Err() != nil {
				logger.Log.Info("Worker context canceled, stopping consumer loop cleanly")
				break
			}
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

		workerPoolLimit <- struct{}{}

		go func(notif model.Notification) {
			defer func() { <-workerPoolLimit }()

			// Execute retry loop with exponential backoff
			sendErr := retry.Do(
				func() error {
					var err error

					if notif.Type == "EMAIL" {
						err = p.email.SendEmail(ctx, notif.Recipient, notif.Content)
					} else if notif.Type == "SMS" {
						err = p.sms.SendSMS(ctx, notif.Recipient, notif.Content)
					} else {
						return retry.Unrecoverable(fmt.Errorf("unknown notification type: %s", notif.Type))
					}

					// If it's a 401 Unauthorized or 400 Bad Request, retrying will NEVER fix it.
					// We use retry.Unrecoverable to instantly break the loop and send it to the DLQ!
					if err != nil && (strings.Contains(err.Error(), "status code: 401") || strings.Contains(err.Error(), "status code: 400")) {
						return retry.Unrecoverable(err)
					}
					return err
				},
				retry.Attempts(p.maxRetries),
				retry.Delay(time.Duration(p.initialBackoffMs)*time.Millisecond),
				retry.OnRetry(func(n uint, err error) {
					logger.Log.Warn("Retrying notification", zap.Uint("attempt", n+1), zap.Uint("id", notif.ID), zap.Error(err))
				}),
				retry.Context(ctx), // Automatically aborts retries if the server is shutting down!
			)

			if notif.Type != "EMAIL" && notif.Type != "SMS" {
				logger.Log.Warn("Unknown notification type, ignoring", zap.String("type", notif.Type))
				return
			}

			// Determine final status
			newStatus := constants.StatusSent
			if sendErr != nil {
				logger.Log.Error("Failed to send notification via Provider after retries", zap.Error(sendErr), zap.Uint("id", notif.ID))
				newStatus = constants.StatusFailed

				// Push to Dead Letter Queue (DLQ)
				if p.dlqTopic != "" && p.producer != nil {
					logger.Log.Info("Pushing failed notification to DLQ", zap.String("topic", p.dlqTopic), zap.Uint("id", notif.ID))
					if err := p.producer.PublishEvent(ctx, p.dlqTopic, fmt.Sprintf("%d", notif.ID), msgBytes); err != nil {
						logger.Log.Error("Failed to push to DLQ", zap.Error(err), zap.Uint("id", notif.ID))
					}
				}
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
