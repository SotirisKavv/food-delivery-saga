package handler

import (
	"context"
	paymentprocessor "food-delivery-saga/cmd/payment/server/payment-processor"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/repository"
)

type Handler struct {
	Producer   *kafka.Producer
	Repository *repository.Repository[models.PaymentDetails]
	Dispatcher *events.Dispatcher
	Processor  *paymentprocessor.Processor
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	paymentProcessor, _ := paymentprocessor.NewProcessor(paymentprocessor.ProcessorMock)
	repo, _ := repository.NewRepository(repository.RepositoryRedis, func(pd models.PaymentDetails) string {
		return pd.OrderId
	})

	h := &Handler{
		Producer:   producer,
		Repository: &repo,
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
	return nil
}

func (h *Handler) OnItemsReserved(evt events.EventOrderPlaced) error {
	return nil
}
