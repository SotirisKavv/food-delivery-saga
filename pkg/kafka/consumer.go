package kafka

import (
	"context"
	"encoding/json"
	"errors"
	svcerror "food-delivery-saga/pkg/error"
	"food-delivery-saga/pkg/events"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	relay  DLQPublisher
}

type ConsumerConfig struct {
	Brokers []string
	Topics  []string
	GroupId string
}

func NewConsumer(conf ConsumerConfig, relay DLQPublisher) *Consumer {
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

type KafkaMessage kafka.Message
type MessageHandler func(ctx context.Context, message KafkaMessage) error

func (c *Consumer) ConsumeMessages(ctx context.Context, handler MessageHandler) error {
	partChannels := make(map[int]chan kafka.Message)
	var mu sync.Mutex
	var wg sync.WaitGroup

	defer func() {
		mu.Lock()
		for _, ch := range partChannels {
			close(ch)
		}
		mu.Unlock()
		wg.Wait()
	}()

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

			partition := msg.Partition

			mu.Lock()
			ch, ok := partChannels[partition]
			if !ok {
				ch = make(chan kafka.Message, 1024)
				partChannels[partition] = ch
				wg.Add(1)
				go c.RunWorker(ctx, handler, ch)
			}
			mu.Unlock()

			select {
			case ch <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

}

func (c *Consumer) RunWorker(ctx context.Context, handler MessageHandler, messageChannel <-chan kafka.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messageChannel:
			if !ok {
				return
			}

			if err := handler(ctx, KafkaMessage(msg)); err != nil {
				c.handleMessageError(ctx, err, msg)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
		}
	}
}

func (c *Consumer) handleMessageError(ctx context.Context, err error, msg kafka.Message) {
	var ed *svcerror.ErrorDetails
	if !errors.As(err, &ed) {
		return
	}

	switch ed.Code {
	case svcerror.ErrBusinessError:
		log.Printf("Failed to handle message: %+v", err)
	default:
		var env events.EventEnvelope
		_ = json.Unmarshal(msg.Value, &env)

		dlqError := events.EventDLQ{
			ErrorDetails: err,
			Payload:      msg.Value,
			Metadata: events.Metadata{
				MessageId:     uuid.NewString(),
				Type:          events.EvtTypeDeadLetterQueue,
				OrderId:       env.Metadata.OrderId,
				CorrelationId: env.Metadata.CorrelationId,
				CausationId:   env.Metadata.MessageId,
				Timestamp:     time.Now().UTC(),
			},
		}
		if err := c.relay.PublishToDLQ(ctx, dlqError); err != nil {
			log.Printf("Failed to publish to DLQ: %v", err)
		}
	}
}

// re-try logic
func (c *Consumer) ConsumeWithRetry(ctx context.Context, handler MessageHandler, maxAttempts int) error {
	return c.ConsumeMessages(ctx, func(ctx context.Context, message KafkaMessage) error {
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
