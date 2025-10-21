package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"food-delivery-saga/pkg/database"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/utils"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type Relay struct {
	Producer *kafka.Producer
	Database *database.Database
	Every    time.Duration
	Batch    int
	Topic    string
}

func NewRelay(producer *kafka.Producer, database *database.Database, topic string) *Relay {
	durStr := utils.GetEnv("OUTBOX_INTERVAL", "500ms")
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		dur = 500 * time.Millisecond
	}

	batchStr := utils.GetEnv("OUTBOX_BATCH", "200")
	batch, err := strconv.Atoi(batchStr)
	if err != nil {
		batch = 200
	}

	return &Relay{
		Producer: producer,
		Database: database,
		Every:    dur,
		Batch:    batch,
		Topic:    topic,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.Every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.FlushMessages(ctx); err != nil {
				log.Printf("Failed to flush outbox: %+v", err)
			}
		}
	}
}

func (r *Relay) FlushMessages(ctx context.Context) error {
	batch, err := r.Database.GetUnpublishedOutbox(ctx, r.Batch, r.Topic)
	if err != nil {
		return fmt.Errorf("Failed to retrieve outbox: %w", err)
	}

	if len(batch) == 0 {
		return nil
	}

	err = r.PublishMessages(ctx, batch)
	if err != nil {
		return fmt.Errorf("Failed to publish events: %w", err)
	}

	ids := make([]string, 0, len(batch))
	for _, outbox := range batch {
		ids = append(ids, outbox.Id)
	}

	if err := r.Database.UpdateOutboxPublished(ctx, ids); err != nil {
		return fmt.Errorf("Failed to update outbox: %w", err)
	}
	return nil
}

func (r *Relay) PublishMessages(ctx context.Context, batch []models.Outbox) error {
	msgs := make([]kafka.EventMessage, 0, len(batch))
	for _, outbox := range batch {
		msgs = append(msgs, kafka.EventMessage{
			Topic: r.Topic,
			Key:   outbox.Key,
			Event: outbox.Payload,
		})
	}

	return r.Producer.PublishMultipleEvents(ctx, msgs)
}

func (r *Relay) PublishToDLQ(ctx context.Context, event events.EventDLQ) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("Failed to marshal event: %w", err)
	}

	return r.Producer.PublishEvent(ctx, kafka.EventMessage{
		Topic: string(events.EvtTypeDeadLetterQueue),
		Key:   event.Metadata.OrderId,
		Event: payload,
	})
}

func (r *Relay) SaveOutboxEvent(ctx context.Context, raw []byte) error {
	var env events.EventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("Failed to unmarshal payload: %w", err)
	}

	outbox := models.Outbox{
		Id:        uuid.NewString(),
		Key:       env.Metadata.OrderId,
		EventType: string(env.Metadata.Type),
		Topic:     r.Topic,
		Payload:   raw,
	}

	if err := r.Database.SaveOutbox(ctx, outbox); err != nil {
		return fmt.Errorf("Failed to save outbox: %w", err)
	}

	return nil
}
