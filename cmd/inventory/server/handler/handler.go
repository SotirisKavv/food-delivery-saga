package handler

import (
	"context"
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
	Repository repository.Repository[models.Restaurant]
}

var acceptedEvents []events.EventType = []events.EventType{
	events.EvtTypeOrderPlaced,
	events.EvtTypePaymentVoided,
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	repo, _ := repository.NewRepository(repository.RepositoryMemory, func(r models.Restaurant) string {
		return r.RestaurantId
	})

	_ = seedInventory(repo)
	h := &Handler{
		Producer:   producer,
		Dispatcher: dispatcher,
		Repository: repo,
	}
	events.Register(dispatcher, events.EvtTypeOrderPlaced, h.OnOrderPlaced)
	return h
}

func (h *Handler) HandleMessage(ctx context.Context, message kafka.KafkaMessage) error {

	return h.Dispatcher.Dispatch(message.Value)
}

func (h *Handler) OnOrderPlaced(evt events.EventOrderPlaced) error {
	inventory, err := h.Repository.Load(evt.RestaurantId)
	if err != nil {
		return fmt.Errorf("Failed to Load resource with id %s: %w", evt.RestaurantId, err)
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

	if err := h.Repository.Update(inventory); err != nil {
		return fmt.Errorf("Failed to update inventory: %w", err)
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	return h.PublishItemsReserved(evt)

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
			Producer:      "inventory-svc",
		},
		RestaurantId: evt.RestaurantId,
		Reason:       reason,
		Success:      false,
	}

	kafkaMessage := kafka.EventMessage{
		Key:   reservationFailed.Metadata.OrderId,
		Topic: kafka.TopicOrder,
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
			Producer:      "inventory-svc",
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

func seedInventory(repo repository.Repository[models.Restaurant]) error {
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
		if err := repo.Save(r); err != nil {
			return err
		}
	}
	return nil
}
