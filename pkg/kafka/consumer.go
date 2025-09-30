package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

type ConsumerConfig struct {
	Brokers []string
	Topics  []string
	GroupId string
}

func NewConsumer(conf ConsumerConfig) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        conf.Brokers,
		GroupTopics:    conf.Topics,
		GroupID:        conf.GroupId,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024, //	10MB
		StartOffset:    kafka.LastOffset,
		CommitInterval: 0,
	})

	return &Consumer{
		reader: reader,
	}
}

type MessageHandler func(ctx context.Context, message kafka.Message) error

func (c *Consumer) ConsumeMessages(ctx context.Context, handler MessageHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Failed to fetch message: %v", err)
				continue
			}

			if err := handler(ctx, msg); err != nil {
				log.Printf("Failed to handle message: %v", err) //	TODO: implement send to DLQ
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
		}
	}
}

// re-try logic
func (c *Consumer) ConsumeWithRetry(ctx context.Context, handler MessageHandler, maxAttempts int) error {
	return c.ConsumeMessages(ctx, func(ctx context.Context, message kafka.Message) error {
		var lastErr error

		for i := 1; i <= maxAttempts; i++ {
			err := handler(ctx, message)
			if err == nil {
				return nil
			}

			lastErr = err
			log.Printf("Attempt %d/%d failed: %v", i, maxAttempts, err)

			if i < maxAttempts {
				backoff := time.Duration(i) * time.Second
				time.Sleep(backoff)
			}
		}

		log.Printf("Failed to process message after %d attempts: %v", maxAttempts, lastErr)
		return lastErr
	})
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
