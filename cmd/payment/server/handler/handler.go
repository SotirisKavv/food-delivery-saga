package handler

import (
	"context"
	"fmt"
	paymentprocessor "food-delivery-saga/cmd/payment/server/payment-processor"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/repository"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Producer   *kafka.Producer
	Repository repository.Repository[models.PaymentDetails]
	Dispatcher *events.Dispatcher
	Processor  paymentprocessor.Processor
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	paymentProcessor, _ := paymentprocessor.NewProcessor(paymentprocessor.ProcessorMock)
	repo, _ := repository.NewRepository(context.Background(), repository.RepositoryRedis, func(pd models.PaymentDetails) string {
		return pd.OrderId
	})

	h := &Handler{
		Producer:   producer,
		Repository: repo,
		Dispatcher: dispatcher,
		Processor:  paymentProcessor,
	}

	events.Register(h.Dispatcher, events.EvtTypeOrderPlaced, h.OnOrderPlaced)
	events.Register(h.Dispatcher, events.EvtTypeItemsReserved, h.OnItemsReserved)

	return h
}

func (h *Handler) HandleMessage(ctx context.Context, message kafka.KafkaMessage) error {
	return h.Dispatcher.Dispatch(message.Value)
}

func (h *Handler) OnOrderPlaced(evt events.EventOrderPlaced) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	details := models.PaymentDetails{
		OrderId:         evt.Metadata.OrderId,
		CustomerId:      evt.CustomerId,
		Amount:          evt.AmountCents,
		Currency:        evt.Currency,
		PaymentMethodId: evt.PaymentMethodId,
	}

	if err := h.Repository.Save(ctx, details); err != nil {
		return fmt.Errorf("Failed to save payment %s: %w", details.OrderId, err)
	}

	return nil
}

func (h *Handler) OnItemsReserved(evt events.EventItemsProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	details, err := h.Repository.Load(ctx, evt.Metadata.OrderId)
	if err != nil {
		return h.PublishPaymentFailed(ctx, evt, fmt.Sprintf("Failed to retrieve payment details: %+v", err))
	}

	result, err := h.Processor.ProcessPayment(ctx, details)
	if err != nil || !result.Success {
		return h.PublishPaymentFailed(ctx, evt, result.FailureReason)
	}

	return h.PublishPaymentAuthorized(ctx, result, evt)
}

func (h *Handler) PublishPaymentFailed(ctx context.Context, evt events.EventItemsProcessed, reason string) error {
	paymentFailed := events.EventPaymentProcessed{
		Metadata: events.Metadata{
			OrderId:       evt.Metadata.OrderId,
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypePaymentVoided,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerPaymentSvc,
		},
		Reason:  reason,
		Success: false,
	}

	message := kafka.EventMessage{
		Key:   paymentFailed.Metadata.OrderId,
		Topic: kafka.TopicInventory,
		Event: paymentFailed,
	}

	return h.Producer.PublishEvent(ctx, message)
}

func (h *Handler) PublishPaymentAuthorized(ctx context.Context, result models.PaymentResult, evt events.EventItemsProcessed) error {
	paymentAuthorized := events.EventPaymentProcessed{
		Metadata: events.Metadata{
			OrderId:       evt.Metadata.OrderId,
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypePaymentAuthorized,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerPaymentSvc,
		},
		TransactionID: result.TransactionID,
		AmountCents:   result.Amount,
		Currency:      result.Currency,
		Success:       true,
	}

	message := kafka.EventMessage{
		Key:   paymentAuthorized.Metadata.OrderId,
		Topic: kafka.TopicPayment,
		Event: paymentAuthorized,
	}

	return h.Producer.PublishEvent(ctx, message)
}
