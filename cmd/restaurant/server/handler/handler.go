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
	"food-delivery-saga/pkg/repository"
	"food-delivery-saga/pkg/scheduler"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Relay           *outbox.Relay
	TicketRepo      repository.Repository[models.Ticket]
	Database        *database.Database
	TicketScheduler *scheduler.DelayQueue[models.Ticket]
	Dispatcher      *events.Dispatcher
}

func NewHandler(database *database.Database, relay *outbox.Relay) *Handler {
	dispatcher := events.NewDispatcher()
	ticketRepo, _ := repository.NewRepository(context.Background(), repository.RepositoryRedis, func(r models.Ticket) string {
		return ticketKeyPrefix + r.OrderId
	})
	sched := scheduler.NewQueue[models.Ticket](0)

	h := &Handler{
		Relay:           relay,
		TicketRepo:      ticketRepo,
		Database:        database,
		TicketScheduler: sched,
		Dispatcher:      dispatcher,
	}

	events.Register(h.Dispatcher, events.EvtTypeItemsReserved, h.OnItemsReserved)
	events.Register(h.Dispatcher, events.EvtTypePaymentAuthorized, h.OnPaymentAuthorized)

	return h
}

const ticketKeyPrefix = "ticket:"

func (h *Handler) HandleMessage(ctx context.Context, message kafka.KafkaMessage) error {
	return h.Dispatcher.Dispatch(message.Value)
}

func (h *Handler) OnItemsReserved(evt events.EventItemsProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	ticket := models.Ticket{
		OrderId:      evt.Metadata.OrderId,
		RestaurantId: evt.RestaurantId,
		Items:        evt.ItemsReserved,
	}

	if err := h.TicketRepo.Save(ctx, ticket); err != nil {
		return svcerror.AddOp(err, "Restaurant.OnItemsReserved")
	}

	return nil
}

func (h *Handler) OnPaymentAuthorized(evt events.EventPaymentProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	ticket, err := h.TicketRepo.Load(ctx, ticketKeyPrefix+evt.Metadata.OrderId)
	if err != nil {
		ed := svcerror.AddOp(err, "Restaurant.OnPaymentAuthorized")
		return h.PublishRestaurantRejected(ctx, evt, ed.Error())
	}

	restaurant, err := h.Database.GetRestaurantAndPreparationInfo(ctx, ticket.RestaurantId)
	if err != nil {
		ed := svcerror.AddOp(err, "Restaurant.OnPaymentAuthorized")
		return h.PublishRestaurantRejected(ctx, evt, ed.Error())
	}

	var totalPrepTime int64
	for sku, ticketItem := range ticket.Items {
		inventoryItem := restaurant.Items[sku]
		totalPrepTime += inventoryItem.PrepTime * ticketItem.Quantity
	}
	totalPrepTime /= restaurant.ParallelizationFactor
	load := restaurant.CurrentLoad + totalPrepTime

	if load > restaurant.CapacityMax {
		ed := svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithMsg(fmt.Sprintf("The restaurant's %s load capacity is reached", restaurant.RestaurantId)),
		)
		return h.PublishRestaurantRejected(ctx, evt, ed.Error())
	}

	restaurant.CurrentLoad = load
	err = h.Database.UpdateRestaurantLoad(ctx, restaurant)
	if err != nil {
		ed := svcerror.AddOp(err, "Restaurant.OnPaymentAuthorized")
		return h.PublishRestaurantRejected(ctx, evt, ed.Error())
	}
	ticket.ETAminutes = time.Duration(load) * time.Second
	ticket.AcceptedAt = time.Now()

	if err := h.Database.SaveTicket(ctx, ticket); err != nil {
		return svcerror.AddOp(err, "Restaurant.OnPaymentAuthorized")
	}

	if err := h.TicketScheduler.Push(scheduler.Entry[models.Ticket]{
		ID:      ticket.OrderId,
		Value:   ticket,
		ReadyAt: ticket.AcceptedAt.Add(ticket.ETAminutes),
	}); err != nil {
		return svcerror.AddOp(err, "Restaurant.OnPaymentAuthorized")
	}
	return h.PublishRestaurantAccepted(ctx, ticket, evt)
}

func (h *Handler) CheckForReadyTickets(ctx context.Context) error {
	for item := range h.TicketScheduler.Out {
		ticket := item.Value
		restaurant, err := h.Database.GetRestaurantAndPreparationInfo(ctx, ticket.RestaurantId)
		if err != nil {
			return svcerror.AddOp(err, "Restaurant.CheckForReadyTickets")
		}

		restaurant.CurrentLoad -= int64(ticket.ETAminutes.Seconds())
		if err := h.Database.UpdateRestaurantLoad(ctx, restaurant); err != nil {
			return svcerror.AddOp(err, "Restaurant.CheckForReadyTickets")
		}
		if err := h.Database.UpdateTicketCompleted(ctx, ticket.OrderId); err != nil {
			return svcerror.AddOp(err, "Restaurant.CheckForReadyTickets")
		}
		if err := h.PublishRestaurantReady(ctx, ticket); err != nil {
			return svcerror.AddOp(err, "Restaurant.CheckForReadyTickets")
		}
	}
	return nil
}

func (h *Handler) PublishRestaurantRejected(ctx context.Context, evt events.EventPaymentProcessed, reason string) error {
	rejectionEvt := &events.EventRestaurantProcessed{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeRestaurantRejected,
			OrderId:       evt.Metadata.OrderId,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerRestaurantSvc,
		},
		TransactionID: evt.TransactionID,
		Reason:        reason,
		Success:       false,
	}

	payload, err := json.Marshal(rejectionEvt)
	if err != nil {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Restaurant.PublishRestaurantRejected"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}

func (h *Handler) PublishRestaurantAccepted(ctx context.Context, ticket models.Ticket, evt events.EventPaymentProcessed) error {

	acceptanceEvt := &events.EventRestaurantProcessed{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeRestaurantAccepted,
			OrderId:       evt.Metadata.OrderId,
			CorrelationId: evt.Metadata.CorrelationId,
			CausationId:   evt.Metadata.MessageId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerRestaurantSvc,
		},
		ETAMinutes: int64(ticket.ETAminutes.Seconds()),
		Success:    true,
	}

	payload, err := json.Marshal(acceptanceEvt)
	if err != nil {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Restaurant.PublishRestaurantRejected"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}

func (h *Handler) PublishRestaurantReady(ctx context.Context, ticket models.Ticket) error {

	readyEvt := &events.EventRestaurantProcessed{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeRestaurantReady,
			OrderId:       ticket.OrderId,
			CorrelationId: ticket.OrderId,
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerRestaurantSvc,
		},
		Success: true,
	}

	payload, err := json.Marshal(readyEvt)
	if err != nil {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Restaurant.PublishRestaurantRejected"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
		)
	}

	return h.Relay.SaveOutboxEvent(ctx, payload)
}
