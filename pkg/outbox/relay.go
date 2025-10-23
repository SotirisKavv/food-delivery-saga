package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"food-delivery-saga/pkg/database"
	svcerror "food-delivery-saga/pkg/error"
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
				switch {
				case errors.Is(err, svcerror.ErrBusinessError) || errors.Is(err, svcerror.ErrInternalError):
					if ed := new(svcerror.ErrorDetails); errors.As(err, &ed) {
						log.Printf("[ERROR] msg=%s trace=%s cause=%v at=%s",
							ed.Msg, ed.TraceString(), ed.Cause, ed.OccuredAt)
					}
				default:
					return svcerror.AddOp(err, "Outbox.Run")
				}
			}
		}
	}
}

func (r *Relay) FlushMessages(ctx context.Context) error {
	batch, err := r.Database.GetUnpublishedOutbox(ctx, r.Batch, r.Topic)
	if err != nil {
		return svcerror.AddOp(err, "Outbox.FlushMessages")
	}

	if len(batch) == 0 {
		return nil
	}

	err = r.PublishMessages(ctx, batch)
	if err != nil {
		return svcerror.AddOp(err, "Outbox.FlushMessages")
	}

	ids := make([]string, 0, len(batch))
	for _, outbox := range batch {
		ids = append(ids, outbox.Id)
	}

	if err := r.Database.UpdateOutboxPublished(ctx, ids); err != nil {
		return svcerror.AddOp(err, "Outbox.FlushMessages")
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
	if err := r.Producer.PublishMultipleEvents(ctx, msgs); err != nil {
		return svcerror.New(
			svcerror.ErrPublishError,
			svcerror.WithOp("Outbox.PublishMessages"),
			svcerror.WithMsg("failed to publish multiple events"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}
	return nil
}

func (r *Relay) PublishToDLQ(ctx context.Context, event events.EventDLQ) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Outbox.PublishToDLQ"),
			svcerror.WithMsg("failed to marshal dlq event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}
	if err := r.Producer.PublishEvent(ctx, kafka.EventMessage{
		Topic: kafka.TopicDeadLetterQueue,
		Key:   event.Metadata.OrderId,
		Event: payload,
	}); err != nil {
		return svcerror.New(
			svcerror.ErrPublishError,
			svcerror.WithOp("Outbox.PublishToDLQ"),
			svcerror.WithMsg("failed to publish dlq event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}
	return nil
}

func (r *Relay) SaveOutboxEvent(ctx context.Context, raw []byte) error {
	var env events.EventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Outbox.SaveOutboxEvent"),
			svcerror.WithMsg("unmarshal payload"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	outbox := models.Outbox{
		Id:        uuid.NewString(),
		Key:       env.Metadata.OrderId,
		EventType: string(env.Metadata.Type),
		Topic:     r.Topic,
		Payload:   raw,
	}

	if err := r.Database.SaveOutbox(ctx, outbox); err != nil {
		return svcerror.AddOp(err, "Outbox.SaveOutboxEvent")
	}

	return nil
}
