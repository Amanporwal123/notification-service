package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Amanporwal123/notification-service/pkg/logger"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Producer handles publishing messages to Kafka.
// Using an interface makes it easy to mock during tests!
type Producer interface {
	PublishEvent(ctx context.Context, topic string, key string, value interface{}) error
	Close() error
}

// producer is the concrete implementation of Producer using segmentio/kafka-go
type producer struct {
	writer *kafka.Writer
}

// NewProducer initializes a new Kafka producer and connects to the brokers.
func NewProducer(brokers []string) Producer {
	// A Writer is thread-safe and can be used to write messages to multiple topics
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		// Balancer decides which partition to write to
		Balancer: &kafka.LeastBytes{},
		// By default, Kafka waits 1 second to "batch" multiple messages together before sending.
		// We drop this to 10 milliseconds so the API responds instantly.
		BatchTimeout: 10 * time.Millisecond,
	}

	return &producer{
		writer: w,
	}
}

// PublishEvent serializes the value (struct) to JSON and publishes it to the specified topic.
func (p *producer) PublishEvent(ctx context.Context, topic string, key string, value interface{}) error {
	// 1. Serialize the Go struct into JSON bytes
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value to JSON: %w", err)
	}

	// 2. Prepare the Kafka Message
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key), // The key is used to group related events in the same partition
		Value: bytes,
	}

	// 3. Write to Kafka
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		logger.Log.Error("Failed to write message to Kafka", zap.Error(err), zap.String("topic", topic))
		return err
	}

	logger.Log.Info("Successfully published event to Kafka", zap.String("topic", topic), zap.String("key", key))
	return nil
}

// Close cleanly shuts down the producer connection.
func (p *producer) Close() error {
	return p.writer.Close()
}
