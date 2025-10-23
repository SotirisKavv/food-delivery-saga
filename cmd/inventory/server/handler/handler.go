package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"food-delivery-saga/pkg/database"
	svcerror "food-delivery-saga/pkg/error"
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
		return svcerror.AddOp(err, "Inventory.OnOrderPlaced.LoadStock")
	}

	for id, item := range evt.Items {
		dish, ok := inventory.Items[id]
		if !ok {
			ed := svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithMsg(fmt.Sprintf("[HANDLER] item %s not available in restaurant %s",
					id, evt.RestaurantId)),
			)
			return h.PublishItemsReservationFailed(ctx, evt, ed.Error())
		}

		if dish.Quantity < item.Quantity {

			ed := svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithMsg(fmt.Sprintf("restaurant %s has less quantity of %s (needed: %d, has: %d)",
					evt.RestaurantId, id, item.Quantity, dish.Quantity,
				)),
			)
			return h.PublishItemsReservationFailed(ctx, evt, ed.Error())
		}
	}

	for id, item := range evt.Items {
		dish := inventory.Items[id]
		dish.Quantity -= item.Quantity
		inventory.Items[id] = dish
	}

	if err := h.Database.UpdateRestaurantStock(ctx, inventory); err != nil {
		ed := svcerror.AddOp(err, "Inventory.OnOrderPlaced")
		return h.PublishItemsReservationFailed(ctx, evt, ed.Error())
	}
	log.Printf("Current Inventory after update: %+v", inventory)

	reservation := models.ItemReservation{
		OrderId:       evt.Metadata.OrderId,
		RestaurantId:  evt.RestaurantId,
		CustomerId:    evt.CustomerId,
		ReservedItems: evt.Items,
	}

	if err := h.Database.SaveReservation(ctx, reservation); err != nil {
		ed := svcerror.AddOp(err, "Inventory.OnOrderPlaced")
		return h.PublishItemsReservationFailed(ctx, evt, ed.Error())
	}

	return h.PublishItemsReserved(ctx, evt)

}

func (h *Handler) OnPaymentFailed(evt events.EventPaymentProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	if err := h.HandleServiceFailure(ctx, evt.Metadata.OrderId); err != nil {
		return svcerror.AddOp(err, "Inventory.OnPaymentFailed")
	}
	return nil
}

func (h *Handler) OnRestaurantRejected(evt events.EventRestaurantProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	if err := h.HandleServiceFailure(ctx, evt.Metadata.OrderId); err != nil {
		return svcerror.AddOp(err, "Inventory.OnRestaurantRejected")
	}
	return nil
}

func (h *Handler) HandleServiceFailure(ctx context.Context, orderId string) error {
	reservation, err := h.Database.GetReservedItems(ctx, orderId)
	if err != nil {
		return svcerror.AddOp(err, "Inventory.HandleServiceFailure")
	}

	inventory, err := h.Database.GetRestaurantStock(ctx, reservation.RestaurantId)
	if err != nil {
		return svcerror.AddOp(err, "Inventory.HandleServiceFailure")
	}

	for id, item := range reservation.ReservedItems {
		dish, ok := inventory.Items[id]
		if !ok {
			return svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithMsg(fmt.Sprintf("item %s not available in restaurant %s", id, reservation.RestaurantId)),
				svcerror.WithOp("Inventory.HandleServiceFailure"),
			)
		}
		dish.Quantity += item.Quantity
		inventory.Items[id] = dish
	}

	if err := h.Database.UpdateRestaurantStock(ctx, inventory); err != nil {
		return svcerror.AddOp(err, "Inventory.HandleServiceFailure")
	}
	if err := h.Database.UpdateReservationReleased(ctx, reservation.OrderId); err != nil {
		return svcerror.AddOp(err, "Inventory.HandleServiceFailure")
	}
	log.Printf("[HANDLER] Current Inventory after update: %+v", inventory)

	return nil
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
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Inventory.PublishItemsReservationFailed"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
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
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Inventory.PublishItemsReserved"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}
