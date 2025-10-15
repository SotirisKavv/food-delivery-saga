package handler

import (
	"context"
	"fmt"
	"food-delivery-saga/pkg/database"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/repository"
	"food-delivery-saga/pkg/scheduler"
	"log"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	Producer        *kafka.Producer
	TicketRepo      repository.Repository[models.Ticket]
	Database        *database.Database
	TicketScheduler *scheduler.DelayQueue[models.Ticket]
	Dispatcher      *events.Dispatcher
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	ticketRepo, _ := repository.NewRepository(context.Background(), repository.RepositoryRedis, func(r models.Ticket) string {
		return ticketKeyPrefix + r.OrderId
	})
	db := database.NewPGDatabase()
	sched := scheduler.NewQueue[models.Ticket](0)

	h := &Handler{
		Producer:        producer,
		TicketRepo:      ticketRepo,
		Database:        db,
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
		return fmt.Errorf("Failed to save ticket %s: %w", ticket.OrderId, err)
	}

	return nil
}

func (h *Handler) OnPaymentAuthorized(evt events.EventPaymentProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	ticket, err := h.TicketRepo.Load(ctx, ticketKeyPrefix+evt.Metadata.OrderId)
	if err != nil {
		return h.PublishRestaurantRejected(ctx, evt, fmt.Sprintf("Failed to retrieve ticket: %+v", err))
	}

	restaurant, err := h.Database.GetRestaurantAndPreparationInfo(ctx, ticket.RestaurantId)
	if err != nil {
		return h.PublishRestaurantRejected(ctx, evt, fmt.Sprintf("Failed to retrieve restaurant: %+v", err))
	}

	var totalPrepTime int64
	for sku, ticketItem := range ticket.Items {
		inventoryItem := restaurant.Items[sku]
		totalPrepTime += inventoryItem.PrepTime * ticketItem.Quantity
	}
	totalPrepTime /= restaurant.ParallelizationFactor
	load := restaurant.CurrentLoad + totalPrepTime

	if load > restaurant.CapacityMax {
		return h.PublishRestaurantRejected(ctx, evt, fmt.Sprintf("The restaurant's %s load capacity is reached", restaurant.RestaurantId))
	}

	restaurant.CurrentLoad = load
	err = h.Database.UpdateRestaurantLoad(ctx, restaurant)
	if err != nil {
		return h.PublishRestaurantRejected(ctx, evt, fmt.Sprintf("Failed to update restaurant: %+v", err))
	}
	ticket.ETAminutes = time.Duration(load) * time.Second
	ticket.AcceptedAt = time.Now()

	if err := h.Database.SaveTicket(ctx, ticket); err != nil {
		return fmt.Errorf("Failed to save ticket (id=%s): %w", ticket.OrderId, err)
	}

	h.TicketScheduler.Push(scheduler.Entry[models.Ticket]{
		ID:      ticket.OrderId,
		Value:   ticket,
		ReadyAt: ticket.AcceptedAt.Add(ticket.ETAminutes),
	})
	return h.PublishRestaurantAccepted(ctx, ticket, evt)
}

func (h *Handler) CheckForReadyTickets(ctx context.Context) error {
	for item := range h.TicketScheduler.Out {
		ticket := item.Value
		restaurant, err := h.Database.GetRestaurantAndPreparationInfo(ctx, ticket.RestaurantId)
		if err != nil {
			return fmt.Errorf("Failed to retrieve restaurant (id=%s): %w", ticket.RestaurantId, err)
		}

		restaurant.CurrentLoad -= int64(ticket.ETAminutes.Seconds())
		if err := h.Database.UpdateRestaurantLoad(ctx, restaurant); err != nil {
			return fmt.Errorf("Failed to update restaurant (id=%s): %w", ticket.RestaurantId, err)
		}
		if err := h.Database.UpdateTicketCompleted(ctx, ticket.OrderId); err != nil {
			return fmt.Errorf("Failed to update ticket (id=%s): %w", ticket.OrderId, err)
		}

		if err := h.PublishRestaurantReady(ctx, ticket); err != nil {
			log.Printf("[HANDLER] Failed to pop ticket out of the scheduler: %v", err)
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

	message := kafka.EventMessage{
		Key:   rejectionEvt.Metadata.OrderId,
		Topic: kafka.TopicRestaurant,
		Event: rejectionEvt,
	}

	return h.Producer.PublishEvent(ctx, message)
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

	message := kafka.EventMessage{
		Key:   acceptanceEvt.Metadata.OrderId,
		Topic: kafka.TopicRestaurant,
		Event: acceptanceEvt,
	}

	return h.Producer.PublishEvent(ctx, message)
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

	message := kafka.EventMessage{
		Key:   readyEvt.Metadata.OrderId,
		Topic: kafka.TopicRestaurant,
		Event: readyEvt,
	}

	return h.Producer.PublishEvent(ctx, message)
}
