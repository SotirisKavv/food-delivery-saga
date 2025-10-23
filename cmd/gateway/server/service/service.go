package service

import (
	"context"
	"encoding/json"
	"food-delivery-saga/pkg/database"
	svcerror "food-delivery-saga/pkg/error"
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/outbox"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	Dispatcher *events.Dispatcher
	Relay      *outbox.Relay
	Database   *database.Database
}

func NewService(database *database.Database, relay *outbox.Relay) *Service {
	dispatcher := events.NewDispatcher()

	service := &Service{
		Relay:      relay,
		Dispatcher: dispatcher,
		Database:   database,
	}

	events.Register(service.Dispatcher, events.EvtTypeItemsReserved, service.OnItemsProcessed)
	events.Register(service.Dispatcher, events.EvtTypeItemsReservationFailed, service.OnItemsProcessed)
	events.Register(service.Dispatcher, events.EvtTypePaymentAuthorized, service.OnPaymentProcessed)
	events.Register(service.Dispatcher, events.EvtTypePaymentFailed, service.OnPaymentProcessed)
	events.Register(service.Dispatcher, events.EvtTypeRestaurantAccepted, service.OnRestaurantProcessed)
	events.Register(service.Dispatcher, events.EvtTypeRestaurantRejected, service.OnRestaurantProcessed)
	events.Register(service.Dispatcher, events.EvtTypeRestaurantReady, service.OnRestaurantProcessed)

	return service
}

func (s *Service) CreateOrder(ctx context.Context, req *models.OrderRequest) (*models.OrderResponse, error) {
	order, err := s.SaveOrder(ctx, req)
	if err != nil {
		return nil, svcerror.AddOp(err, "OrderGateway.CreateOrder")
	}

	orderEvent, err := s.PublishOrderPlaced(ctx, order.OrderId, req)
	if err != nil {
		return nil, svcerror.AddOp(err, "OrderGateway.CreateOrder")
	}

	return &models.OrderResponse{
		OrderId:       order.OrderId,
		Status:        string(models.ORDER_STATUS_PENDING),
		Message:       "Order received and being processed",
		CorrelationID: orderEvent.Metadata.CorrelationId,
	}, nil

}

func (s *Service) SaveOrder(ctx context.Context, req *models.OrderRequest) (*models.Order, error) {
	order := models.Order{
		OrderId:      uuid.NewString(),
		CustomerId:   req.CustomerId,
		RestaurantId: req.RestaurantId,
		Items:        req.Items,
		Amount:       req.Amount,
		Currency:     req.Currency,
		Status:       models.ORDER_STATUS_PENDING,
	}

	if err := s.Database.SaveOrder(ctx, order); err != nil {
		return nil, svcerror.AddOp(err, "OrderGateway.SaveOrder")
	}

	return &order, nil
}

func (s *Service) PublishOrderPlaced(ctx context.Context, orderId string, req *models.OrderRequest) (*events.EventOrderPlaced, error) {
	orderEvent := events.EventOrderPlaced{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeOrderPlaced,
			OrderId:       orderId,
			CorrelationId: uuid.NewString(),
			Timestamp:     time.Now().UTC(),
			Producer:      events.ProducerOrderSvc,
		},
		CustomerId:      req.CustomerId,
		PaymentMethodId: req.PaymentMethodId,
		AmountCents:     req.Amount,
		Currency:        req.Currency,
		RestaurantId:    req.RestaurantId,
		Items:           req.Items,
	}

	payload, err := json.Marshal(orderEvent)
	if err != nil {
		return nil, svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("OrderGateway.PublishOrderPlaced"),
			svcerror.WithMsg("failed to marshal event"),
			svcerror.WithCause(err),
		)
	}

	if err := s.Relay.SaveOutboxEvent(ctx, payload); err != nil {
		return nil, svcerror.AddOp(err, "OrderGateway.PublishOrderPlaced")
	}

	return &orderEvent, nil
}

func (s *Service) GetOrder(ctx context.Context, id string) (*models.OrderResponse, error) {
	order, err := s.Database.GetOrder(ctx, id)
	if err != nil {
		return nil, svcerror.AddOp(err, "OrderGateway.GetOrder")
	}

	var message string
	if order.CancelationReason != "" {
		message = order.CancelationReason
	} else {
		message = "Order retrieved"
	}

	return &models.OrderResponse{
		OrderId: id,
		Status:  string(order.Status),
		Message: message,
	}, nil
}

func (s *Service) OnItemsProcessed(evt events.EventItemsProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	order, err := s.Database.GetOrder(ctx, evt.Metadata.OrderId)
	if err != nil {
		return svcerror.AddOp(err, "OrderGateway.OnItemsProcessed")
	}

	switch evt.Metadata.Type {
	case events.EvtTypeItemsReleased:
		order.Status = models.ORDER_STATUS_RESERVED
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnItemsProcessed")
		}
	case events.EvtTypeItemsReservationFailed:
		order.Status = models.ORDER_STATUS_CANCELED
		order.CancelationReason = evt.Reason
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnItemsProcessed")
		}
	}

	return nil
}

func (s *Service) OnPaymentProcessed(evt events.EventPaymentProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	order, err := s.Database.GetOrder(ctx, evt.Metadata.OrderId)
	if err != nil {
		return svcerror.AddOp(err, "OrderGateway.OnPaymentProcessed")
	}

	switch evt.Metadata.Type {
	case events.EvtTypePaymentAuthorized:
		order.Status = models.ORDER_STATUS_AUTHORIZED
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnPaymentProcessed")
		}
	case events.EvtTypePaymentFailed:
		order.Status = models.ORDER_STATUS_CANCELED
		order.CancelationReason = evt.Reason
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnPaymentProcessed")
		}
	}

	return nil
}

func (s *Service) OnRestaurantProcessed(evt events.EventRestaurantProcessed) error {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	order, err := s.Database.GetOrder(ctx, evt.Metadata.OrderId)
	if err != nil {
		return svcerror.AddOp(err, "OrderGateway.OnRestaurantProcessed")
	}

	switch evt.Metadata.Type {
	case events.EvtTypeRestaurantAccepted:
		order.Status = models.ORDER_STATUS_ACCEPTED
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnRestaurantProcessed")
		}
	case events.EvtTypeRestaurantRejected:
		order.Status = models.ORDER_STATUS_CANCELED
		order.CancelationReason = evt.Reason
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnRestaurantProcessed")
		}
	case events.EvtTypeRestaurantReady:
		order.Status = models.ORDER_STATUS_COMPLETED
		if err := s.Database.UpdateOrderStatus(ctx, order); err != nil {
			return svcerror.AddOp(err, "OrderGateway.OnRestaurantProcessed")
		}
	}

	return nil
}
