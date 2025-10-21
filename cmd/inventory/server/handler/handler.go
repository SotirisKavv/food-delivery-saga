package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"food-delivery-saga/pkg/database"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/outbox"
	"log"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Dispatcher *events.Dispatcher
	Relay      *outbox.Relay
	Database   *database.Database
}

func NewHandler(database *database.Database, relay *outbox.Relay) *Handler {
	dispatcher := events.NewDispatcher()

	h := &Handler{
		Relay:      relay,
		Dispatcher: dispatcher,
		Database:   database,
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

	inventory, err := h.Database.GetRestaurantStock(ctx, evt.RestaurantId)
	if err != nil {
		return fmt.Errorf("Failed to load Restaurant with id %s: %+v", evt.RestaurantId, err)
	}

	for id, item := range evt.Items {
		dish, ok := inventory.Items[id]
		if !ok {
			failureReason := fmt.Sprintf("Item %s not available in restaurant %s", id, evt.RestaurantId)
			log.Printf(failureReason)
			return h.PublishItemsReservationFailed(ctx, evt, failureReason)
		}

		if dish.Quantity < item.Quantity {
			failureReason := fmt.Sprintf("Restaurant %s has less quantity of %s (needed: %d, has: %d)",
				evt.RestaurantId, id, item.Quantity, dish.Quantity,
			)
			log.Printf(failureReason)
			return h.PublishItemsReservationFailed(ctx, evt, failureReason)
		}
	}

	for id, item := range evt.Items {
		dish := inventory.Items[id]
		dish.Quantity -= item.Quantity
		inventory.Items[id] = dish
	}

	if err := h.Database.UpdateRestaurantStock(ctx, inventory); err != nil {
		failureReason := fmt.Sprintf("Failed to update inventory: %+v", err)
		return h.PublishItemsReservationFailed(ctx, evt, failureReason)
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	reservation := models.ItemReservation{
		OrderId:       evt.Metadata.OrderId,
		RestaurantId:  evt.RestaurantId,
		CustomerId:    evt.CustomerId,
		ReservedItems: evt.Items,
	}

	if err := h.Database.SaveReservation(ctx, reservation); err != nil {
		failureReason := fmt.Sprintf("Failed to save reservation: %+v", err)
		return h.PublishItemsReservationFailed(ctx, evt, failureReason)
	}

	return h.PublishItemsReserved(ctx, evt)

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
	reservation, err := h.Database.GetReservedItems(ctx, orderId)
	if err != nil {
		return fmt.Sprintf("Failed to save reservation: %+v", err)
	}

	inventory, err := h.Database.GetRestaurantStock(ctx, reservation.RestaurantId)
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

	if err := h.Database.UpdateRestaurantStock(ctx, inventory); err != nil {
		return fmt.Sprintf("Failed to update Inventory: %+v", err)
	}
	if err := h.Database.UpdateReservationReleased(ctx, reservation.OrderId); err != nil {
		return fmt.Sprintf("Failed to update Inventory: %+v", err)
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	return ""
}

func (h *Handler) PublishItemsReservationFailed(ctx context.Context, evt events.EventOrderPlaced, reason string) error {
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

	payload, err := json.Marshal(reservationFailed)
	if err != nil {
		return fmt.Errorf("Failed to marshal event: %w", err)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}

func (h *Handler) PublishItemsReserved(ctx context.Context, evt events.EventOrderPlaced) error {
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

	payload, err := json.Marshal(itemsReserved)
	if err != nil {
		return fmt.Errorf("Failed to marshal event: %w", err)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
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

	return h.Relay.PublishToDLQ(ctx, dlqError)
}
