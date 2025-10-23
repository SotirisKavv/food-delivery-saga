package kafka

import (
	"context"
	"encoding/json"
	svcerror "food-delivery-saga/pkg/error"
	"food-delivery-saga/pkg/events"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Writer *kafka.Writer
}

type ProducerConfig struct {
	Brokers []string
}

type DLQPublisher interface {
	PublishToDLQ(ctx context.Context, event events.EventDLQ)
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
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Producer.PublishEvent"),
			svcerror.WithMsg("marshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	msg := kafka.Message{
		Topic: evtMessage.Topic,
		Key:   []byte(evtMessage.Key),
		Value: value,
		Time:  time.Now(),
	}

	err = p.Writer.WriteMessages(ctx, msg)
	if err != nil {
		return svcerror.New(
			svcerror.ErrPublishError,
			svcerror.WithOp("Producer.PublishEvent"),
			svcerror.WithMsg("failed to publish event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return nil
}
func (p *Producer) PublishMultipleEvents(ctx context.Context, events []EventMessage) error {
	messages := make([]kafka.Message, len(events))

	for i, event := range events {
		messages[i] = kafka.Message{
			Topic: event.Topic,
			Key:   []byte(event.Key),
			Value: event.Event,
			Time:  time.Now(),
		}
	}

	err := p.Writer.WriteMessages(ctx, messages...)
	if err != nil {
		return svcerror.New(
			svcerror.ErrPublishError,
			svcerror.WithOp("Producer.PublishMultipleEvents"),
			svcerror.WithMsg("failed to publish events"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.Writer.Close()
}
