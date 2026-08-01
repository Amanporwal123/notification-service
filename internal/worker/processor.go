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
	consumer   kafka.Consumer
	email      provider.NotificationProvider
	sms        provider.NotificationProvider
	db         *gorm.DB
	maxWorkers int
}

func NewProcessor(c kafka.Consumer, email provider.NotificationProvider, sms provider.NotificationProvider, db *gorm.DB, maxWorkers int) *Processor {
	// Fallback in case maxWorkers isn't configured
	if maxWorkers <= 0 {
		maxWorkers = 100 
	}
	return &Processor{
		consumer:   c,
		email:      email,
		sms:        sms,
		db:         db,
		maxWorkers: maxWorkers,
	}
}

func (p *Processor) Start(ctx context.Context) {
	logger.Log.Info("Starting Kafka Consumer Background Worker...", zap.Int("max_workers", p.maxWorkers))

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

		// Acquire a token before spinning up the goroutine. 
		// If 100 workers are busy, this line will block and wait for one to finish!
		workerPoolLimit <- struct{}{}

		// Process it in a new goroutine to allow massive concurrency!
		go func(notif model.Notification) {
			// Release token when done
			defer func() { <-workerPoolLimit }()

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
