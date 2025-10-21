package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
}

type ProducerConfig struct {
	Brokers []string
}

func NewProducer(conf ProducerConfig) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(conf.Brokers...),
		Balancer:               &kafka.Hash{},
		BatchSize:              1,
		BatchTimeout:           10 * time.Millisecond,
		Async:                  false,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireOne,
	}

	return &Producer{
		Writer: writer,
	}
}

func (p *Producer) PublishEvent(ctx context.Context, evtMessage EventMessage) error {
	value, err := json.Marshal(evtMessage.Event)
	if err != nil {
		return fmt.Errorf("Failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Topic: evtMessage.Topic,
		Key:   []byte(evtMessage.Key),
		Value: value,
		Time:  time.Now(),
	}

	err = p.Writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("Failed to publish event: %w", err)
	}

	return nil
}
func (p *Producer) PublishMultipleEvents(ctx context.Context, events []EventMessage) error {
	messages := make([]kafka.Message, len(events))

	for i, event := range events {
		// value, err := json.Marshal(event.Event)
		// if err != nil {
		// 	return fmt.Errorf("Failed to marshal event %d: %w", i, err)
		// }

		messages[i] = kafka.Message{
			Topic: event.Topic,
			Key:   []byte(event.Key),
			Value: event.Event,
			Time:  time.Now(),
		}
	}

	err := p.Writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("Failed to publish events: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.Writer.Close()
}
