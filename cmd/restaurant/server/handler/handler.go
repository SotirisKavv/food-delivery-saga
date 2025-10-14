package handler

import (
	"context"
	"fmt"
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
	RestoRepo       repository.Repository[models.Restaurant]
	TicketScheduler *scheduler.DelayQueue[models.Ticket]
	Dispatcher      *events.Dispatcher
}

func NewHandler(producer *kafka.Producer) *Handler {
	dispatcher := events.NewDispatcher()
	ticketRepo, _ := repository.NewRepository(context.Background(), repository.RepositoryRedis, func(r models.Ticket) string {
		return ticketKeyPrefix + r.OrderId
	})
	restoRepo, _ := repository.NewRepository(context.Background(), repository.RepositoryMemory, func(r models.Restaurant) string {
		return r.RestaurantId
	})
	_ = seedInventory(context.Background(), restoRepo)
	sched := scheduler.NewQueue[models.Ticket](0)

	h := &Handler{
		Producer:        producer,
		TicketRepo:      ticketRepo,
		RestoRepo:       restoRepo,
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

	log.Printf("[HANDLER] %s order=%s, restoID=%s, items=%+v",
		evt.Metadata.Type,
		evt.Metadata.OrderId,
		evt.RestaurantId,
		evt.ItemsReserved,
	)
	ticket := models.Ticket{
		OrderId:      evt.Metadata.OrderId,
		RestaurantId: evt.RestaurantId,
		Items:        evt.ItemsReserved,
	}

	log.Printf("[HANDLER] Ticket to save: %v", ticket)

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
	log.Printf("[HANDLER] Ticket retrieved: orderId=%s, restoId=%s", ticket.OrderId, ticket.RestaurantId)

	if ticket.RestaurantId == "" {
		return h.PublishRestaurantRejected(ctx, evt, "Ticket is missing restaurant_id")
	}

	restaurant, err := h.RestoRepo.Load(ctx, ticket.RestaurantId)
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
	err = h.RestoRepo.Update(ctx, restaurant)
	if err != nil {
		return h.PublishRestaurantRejected(ctx, evt, fmt.Sprintf("Failed to update restaurant: %+v", err))
	}
	ticket.ETAminutes = time.Duration(load) * time.Second
	ticket.AcceptedAt = time.Now()

	h.TicketScheduler.Push(scheduler.Entry[models.Ticket]{
		ID:      ticket.OrderId,
		Value:   ticket,
		ReadyAt: ticket.AcceptedAt.Add(ticket.ETAminutes),
	})
	log.Printf("[HANDLER] Ticket scheduled: order_id=%s ready_at=%s eta=%s", ticket.OrderId, ticket.AcceptedAt.Add(ticket.ETAminutes).Format(time.RFC3339), ticket.ETAminutes)
	return h.PublishRestaurantAccepted(ctx, ticket, evt)
}

func (h *Handler) CheckForReadyTickets(ctx context.Context) error {
	for item := range h.TicketScheduler.Out {
		ticket := item.Value
		restaurant, err := h.RestoRepo.Load(ctx, ticket.RestaurantId)
		if err != nil {
			return fmt.Errorf("Failed to retrieve restaurant (id=%s): %w", ticket.RestaurantId, err)
		}
		restaurant.CurrentLoad -= int64(ticket.ETAminutes.Seconds())
		if err := h.RestoRepo.Update(ctx, restaurant); err != nil {
			return fmt.Errorf("Failed to update restaurant (id=%s): %w", ticket.RestaurantId, err)
		}
		log.Printf("[HANDLER] Ticket ready: order_id=%s", ticket.OrderId)
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

func seedInventory(ctx context.Context, repo repository.Repository[models.Restaurant]) error {
	restaurants := []models.Restaurant{
		{
			RestaurantId:          "resto-1",
			CapacityMax:           120,
			ParallelizationFactor: 2,
			Items: map[string]models.Item{
				"burger": {SKU: "burger", PrepTime: 20},
				"fries":  {SKU: "fries", PrepTime: 5},
				"cola":   {SKU: "cola", PrepTime: 1},
			},
		},
		{
			RestaurantId:          "resto-2",
			CapacityMax:           120,
			ParallelizationFactor: 3,
			Items: map[string]models.Item{
				"pizza": {SKU: "pizza", PrepTime: 30},
				"salad": {SKU: "salad", PrepTime: 5},
				"water": {SKU: "water", PrepTime: 1},
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
