package models

import "time"

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
	EvtTypeRestaurantAccepted     EventType = "RESTUARANT_ACCEPTED"
	EvtTypeRestaurantReady        EventType = "RESTUARANT_READY"
	EvtTypeCourierAssigned        EventType = "COURIER_ASSIGNED"
	EvtTypeCourierPickedUp        EventType = "COURIER_PICKED_UP"
	EvtTypeCourierDelivered       EventType = "COURIER_DELIVERED"
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

type EventOrderPlaced struct {
	Metadata        Metadata `json:"mtdt"`
	CustomerId      string   `json:"customer_id"`
	PaymentMethodId string   `json:"pm_id"`
	AmountCents     int64    `json:"amount_cents"`
	Currency        string   `json:"currency"`
	Items           []Item   `json:"items"`
}
