package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	ReadMessage(ctx context.Context) ([]byte, error)
	Close() error
}

type consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string, groupID string) Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	return &consumer{reader: r}
}

func (c *consumer) ReadMessage(ctx context.Context) ([]byte, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	return msg.Value, nil
}

func (c *consumer) Close() error {
	return c.reader.Close()
}
