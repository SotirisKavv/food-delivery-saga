package handler

import (
	"context"
	"encoding/json"
	paymentprocessor "food-delivery-saga/cmd/payment/server/payment-processor"
	"food-delivery-saga/pkg/database"
	svcerror "food-delivery-saga/pkg/error"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/outbox"
	"food-delivery-saga/pkg/repository"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Relay      *outbox.Relay
	Database   *database.Database
	Repository repository.Repository[models.PaymentDetails]
	Dispatcher *events.Dispatcher
	Processor  paymentprocessor.Processor
}

const paymentKeyPrefix = "payment:"

func NewHandler(database *database.Database, relay *outbox.Relay) *Handler {
	dispatcher := events.NewDispatcher()
	paymentProcessor, _ := paymentprocessor.NewProcessor(paymentprocessor.ProcessorMock)
	repo, _ := repository.NewRepository(context.Background(), repository.RepositoryRedis, func(pd models.PaymentDetails) string {
		return paymentKeyPrefix + pd.OrderId
	})

	h := &Handler{
		Relay:      relay,
		Database:   database,
		Repository: repo,
		Dispatcher: dispatcher,
		Processor:  paymentProcessor,
	}

	events.Register(h.Dispatcher, events.EvtTypeOrderPlaced, h.OnOrderPlaced)
	events.Register(h.Dispatcher, events.EvtTypeItemsReserved, h.OnItemsReserved)
	events.Register(h.Dispatcher, events.EvtTypeRestaurantRejected, h.OnRestaurantRejected)

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
		return svcerror.AddOp(err, "Payment.OnOrderPlaced")
	}

	return nil
}

func (h *Handler) OnItemsReserved(evt events.EventItemsProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	details, err := h.Repository.Load(ctx, paymentKeyPrefix+evt.Metadata.OrderId)
	if err != nil {
		ed := svcerror.AddOp(err, "Payment.OnItemsReserved")
		return h.PublishPaymentFailed(ctx, evt, ed.Error())
	}

	result, err := h.Processor.ProcessPayment(ctx, details)
	if err != nil || !result.Success {
		ed := svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithMsg(result.FailureReason),
		)
		return h.PublishPaymentFailed(ctx, evt, ed.Error())
	}

	if err := h.Database.SavePayment(ctx, details, result.TransactionID); err != nil {
		return svcerror.AddOp(err, "Payment.OnItemsReserved")
	}

	return h.PublishPaymentAuthorized(ctx, result, evt)
}

func (h *Handler) OnRestaurantRejected(evt events.EventRestaurantProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	details, err := h.Database.GetPayment(ctx, evt.Metadata.OrderId)
	if err != nil {
		return svcerror.AddOp(err, "Payment.OnRestaurantRejected")
	}

	result, err := h.Processor.RevertPayment(ctx, details)
	if err != nil || !result.Success {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithMsg(result.FailureReason),
			svcerror.WithCause(err),
		)
	}

	return nil
}

func (h *Handler) PublishPaymentFailed(ctx context.Context, evt events.EventItemsProcessed, reason string) error {
	paymentFailed := events.EventPaymentProcessed{
		Metadata: events.Metadata{
			OrderId:       evt.Metadata.OrderId,
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypePaymentFailed,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerPaymentSvc,
		},
		Reason:  reason,
		Success: false,
	}

	payload, err := json.Marshal(paymentFailed)
	if err != nil {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Payment.PublishPaymentFailed"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
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

	payload, err := json.Marshal(paymentAuthorized)
	if err != nil {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Payment.PublishPaymentAuthorized"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}
