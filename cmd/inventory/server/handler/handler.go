package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/repository"
	"log"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Dispatcher *events.Dispatcher
	Producer   *kafka.Producer
	ReservRepo repository.Repository[models.ItemReservation]
	RestoRepo  repository.Repository[models.Restaurant]
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	itemResRepo, _ := repository.NewRepository(ctx, repository.RepositoryMemory, func(ir models.ItemReservation) string {
		return ir.OrderId
	})

	restoRepo, _ := repository.NewRepository(ctx, repository.RepositoryMemory, func(r models.Restaurant) string {
		return r.RestaurantId
	})

	_ = seedInventory(ctx, restoRepo)
	h := &Handler{
		Producer:   producer,
		Dispatcher: dispatcher,
		RestoRepo:  restoRepo,
		ReservRepo: itemResRepo,
	}
	events.Register(dispatcher, events.EvtTypeOrderPlaced, h.OnOrderPlaced)
	events.Register(dispatcher, events.EvtTypePaymentFailed, h.OnPaymentFailed)
	events.Register(dispatcher, events.EvtTypeRestaurantRejected, h.OnRestaurantRejected)
	return h
}

func (h *Handler) HandleMessage(ctx context.Context, message kafka.KafkaMessage) error {
	return h.Dispatcher.Dispatch(message.Value)
}

func (h *Handler) OnOrderPlaced(evt events.EventOrderPlaced) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	inventory, err := h.RestoRepo.Load(ctx, evt.RestaurantId)
	if err != nil {
		return fmt.Errorf("Failed to load Restaurant with id %s: %+v", evt.RestaurantId, err)
	}

	for id, item := range evt.Items {
		dish, ok := inventory.Items[id]
		if !ok {
			failureReason := fmt.Sprintf("Item %s not available in restaurant %s", id, evt.RestaurantId)
			log.Printf(failureReason)
			return h.PublishItemsReservationFailed(evt, failureReason)
		}

		if dish.Quantity < item.Quantity {
			failureReason := fmt.Sprintf("Restaurant %s has less quantity of %s (needed: %d, has: %d)",
				evt.RestaurantId, id, item.Quantity, dish.Quantity,
			)
			log.Printf(failureReason)
			return h.PublishItemsReservationFailed(evt, failureReason)
		}
	}

	for id, item := range evt.Items {
		dish := inventory.Items[id]
		dish.Quantity -= item.Quantity
		inventory.Items[id] = dish
	}

	if err := h.RestoRepo.Update(ctx, inventory); err != nil {
		failureReason := fmt.Sprintf("Failed to update inventory: %+v", err)
		return h.PublishItemsReservationFailed(evt, failureReason)
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	reservation := models.ItemReservation{
		OrderId:       evt.Metadata.OrderId,
		RestaurantId:  evt.RestaurantId,
		CustomerId:    evt.CustomerId,
		ReservedItems: evt.Items,
	}

	if err := h.ReservRepo.Save(ctx, reservation); err != nil {
		failureReason := fmt.Sprintf("Failed to save reservation: %+v", err)
		return h.PublishItemsReservationFailed(evt, failureReason)
	}

	return h.PublishItemsReserved(evt)

}

func (h *Handler) OnPaymentFailed(evt events.EventPaymentProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	if failureReason := h.HandleServiceFailure(ctx, evt.Metadata.OrderId); failureReason != "" {
		payload, _ := json.Marshal(evt)
		return h.PublishToDLQ(ctx, evt.Metadata, payload, failureReason)
	}
	return nil
}

func (h *Handler) OnRestaurantRejected(evt events.EventRestaurantProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	if failureReason := h.HandleServiceFailure(ctx, evt.Metadata.OrderId); failureReason != "" {
		payload, _ := json.Marshal(evt)
		return h.PublishToDLQ(ctx, evt.Metadata, payload, failureReason)
	}
	return nil
}

func (h *Handler) HandleServiceFailure(ctx context.Context, orderId string) string {
	reservation, err := h.ReservRepo.Load(ctx, orderId)
	if err != nil {
		return fmt.Sprintf("Failed to save reservation: %+v", err)
	}

	inventory, err := h.RestoRepo.Load(ctx, reservation.RestaurantId)
	if err != nil {
		return fmt.Sprintf("Failed to load Restaurant with id %s: %+v", reservation.RestaurantId, err)
	}

	for id, item := range reservation.ReservedItems {
		dish, ok := inventory.Items[id]
		if !ok {
			failureReason := fmt.Sprintf("Item %s not available in Restaurant %s", id, reservation.RestaurantId)
			log.Printf(failureReason)
			return failureReason
		}
		dish.Quantity += item.Quantity
		inventory.Items[id] = dish
	}

	if err := h.RestoRepo.Update(ctx, inventory); err != nil {
		return fmt.Sprintf("Failed to update Inventory: %+v", err)
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	return ""

}

func (h *Handler) PublishItemsReservationFailed(evt events.EventOrderPlaced, reason string) error {
	reservationFailed := &events.EventItemsProcessed{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeItemsReservationFailed,
			OrderId:       evt.Metadata.OrderId,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerInventorySvc,
		},
		RestaurantId: evt.RestaurantId,
		Reason:       reason,
		Success:      false,
	}

	kafkaMessage := kafka.EventMessage{
		Key:   reservationFailed.Metadata.OrderId,
		Topic: kafka.TopicInventory,
		Event: reservationFailed,
	}

	return h.Producer.PublishEvent(context.Background(), kafkaMessage)
}

func (h *Handler) PublishItemsReserved(evt events.EventOrderPlaced) error {
	itemsReserved := &events.EventItemsProcessed{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeItemsReserved,
			OrderId:       evt.Metadata.OrderId,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerInventorySvc,
		},
		RestaurantId:  evt.RestaurantId,
		ItemsReserved: evt.Items,
		Success:       true,
	}

	kafkaMessage := kafka.EventMessage{
		Key:   itemsReserved.Metadata.OrderId,
		Topic: kafka.TopicInventory,
		Event: itemsReserved,
	}

	return h.Producer.PublishEvent(context.Background(), kafkaMessage)
}

func (h *Handler) PublishToDLQ(ctx context.Context, mtdt events.Metadata, payload []byte, reason string) error {
	dlqError := events.EventDLQ{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			CausationId:   mtdt.MessageId,
			CorrelationId: mtdt.CorrelationId,
			Type:          events.EvtTypeDeadLetterQueue,
			OrderId:       mtdt.OrderId,
			Timestamp:     time.Now(),
			Producer:      events.ProducerPaymentSvc,
		},
		ErrorDetails: events.ErrorDetails{
			Message:   reason,
			Service:   events.ProducerPaymentSvc,
			OccuredAt: time.Now(),
		},
		Payload: payload,
	}

	message := kafka.EventMessage{
		Key:   dlqError.Metadata.OrderId,
		Topic: kafka.TopicDeadLetterQueue,
		Event: dlqError,
	}

	return h.Producer.PublishEvent(ctx, message)
}

func seedInventory(ctx context.Context, repo repository.Repository[models.Restaurant]) error {
	restaurants := []models.Restaurant{
		{
			RestaurantId: "resto-1",
			Items: map[string]models.Item{
				"burger": {SKU: "burger", Quantity: 50},
				"fries":  {SKU: "fries", Quantity: 120},
				"cola":   {SKU: "cola", Quantity: 75},
			},
		},
		{
			RestaurantId: "resto-2",
			Items: map[string]models.Item{
				"pizza": {SKU: "pizza", Quantity: 40},
				"salad": {SKU: "salad", Quantity: 25},
				"water": {SKU: "water", Quantity: 200},
			},
		},
	}

	for _, r := range restaurants {
		if err := repo.Save(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
