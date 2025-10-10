package events

import (
	"food-delivery-saga/pkg/models"
	"time"
)

type EventType string

const (
	EvtTypeOrderPlaced            EventType = "ORDER_PLACED"
	EvtTypeOrderCompleted         EventType = "ORDER_COMPLETED"
	EvtTypeItemsReserved          EventType = "ITEMS_RESERVED"
	EvtTypeItemsReservationFailed EventType = "ITEMS_RESERVATION_FAILED"
	EvtTypeItemsReleased          EventType = "ITEMS_RELEASED"
	EvtTypePaymentAuthorized      EventType = "PAYMENT_AUTHORIZED"
	EvtTypePaymentVoided          EventType = "PAYMENT_VOIDED"
	EvtTypePaymentCaptured        EventType = "PAYMENT_CAPTURED"
	EvtTypeRestaurantAccepted     EventType = "RESTAURANT_ACCEPTED"
	EvtTypeRestaurantRejected     EventType = "RESTAURANT_REJECTED"
	EvtTypeRestaurantReady        EventType = "RESTAURANT_READY"
)

const (
	ProducerOrderSvc      string = "order-svc"
	ProducerInventorySvc  string = "inventory-svc"
	ProducerPaymentSvc    string = "payment-svc"
	ProducerRestaurantSvc string = "restaurant-svc"
)

type Metadata struct {
	MessageId     string    `json:"message_id"`
	Type          EventType `json:"type"`
	OrderId       string    `json:"order_id"`
	CorrelationId string    `json:"correlation_id"`
	CausationId   string    `json:"causation_id"`
	Timestamp     time.Time `json:"timestamp"`
	Producer      string    `json:"producer"`
}

type DomainEvent interface {
	GetMetadata() Metadata
}

// order-placed
type EventOrderPlaced struct {
	Metadata        Metadata               `json:"mtdt"`
	CustomerId      string                 `json:"customer_id"`
	PaymentMethodId string                 `json:"pm_id"`
	AmountCents     int64                  `json:"amount_cents"`
	Currency        string                 `json:"currency"`
	RestaurantId    string                 `json:"restaurant_id"`
	Items           map[string]models.Item `json:"items"`
}

func (op EventOrderPlaced) GetMetadata() Metadata { return op.Metadata }

// items-reserved/reservation-failed
type EventItemsProcessed struct {
	Metadata      Metadata               `json:"mtdt"`
	RestaurantId  string                 `json:"restaurant_id"`
	ItemsReserved map[string]models.Item `json:"items_reserved"`
	Reason        string                 `json:"reason"`
	Success       bool                   `json:"success"`
}

func (ip EventItemsProcessed) GetMetadata() Metadata { return ip.Metadata }

// payment-authorized/-voided
type EventPaymentProcessed struct {
	Metadata      Metadata `json:"mtdt"`
	ReservationId string   `json:"reservation_id"`
	TransactionID string   `json:"transaction_id"`
	AmountCents   int64    `json:"amount_cents"`
	Currency      string   `json:"currency"`
	Reason        string   `json:"reason"`
	Success       bool     `json:"success"`
}

func (pp EventPaymentProcessed) GetMetadata() Metadata { return pp.Metadata }

// restaurant-accepted/-rejected/-completed
type EventRestaurantProcessed struct {
	Metadata      Metadata `json:"mtdt"`
	ETAMinutes    int64    `json:"eta_min"`
	TransactionID string   `json:"transaction_id"`
	Reason        string   `json:"reason"`
	Success       bool     `json:"success"`
}

func (rp EventRestaurantProcessed) GetMetadata() Metadata { return rp.Metadata }
