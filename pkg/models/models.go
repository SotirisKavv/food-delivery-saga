package models

import "time"

type OrderStatus string

const (
	ORDER_STATUS_PENDING    OrderStatus = "PENDING"
	ORDER_STATUS_RESERVED   OrderStatus = "RESERVED"
	ORDER_STATUS_AUTHORIZED OrderStatus = "AUTHORIZED"
	ORDER_STATUS_ACCEPTED   OrderStatus = "ACCEPTED"
	ORDER_STATUS_COMPLETED  OrderStatus = "COMPLETED"
	ORDER_STATUS_CANCELED   OrderStatus = "CANCELED"
)

type Item struct {
	SKU      string `json:"sku"` //	Primary Key
	Quantity int64  `json:"quantity"`
	PrepTime int64  `json:"prep_time"`
}

type Order struct {
	OrderId           string          `json:"order_id"` //	Primary Key
	CustomerId        string          `json:"customer_id"`
	RestaurantId      string          `json:"restaurant_id"`
	Items             map[string]Item `json:"items"`
	Amount            int64           `json:"amount_cents"`
	Currency          string          `json:"currency"`
	Status            OrderStatus     `json:"status"`
	CancelationReason string          `json:"cancelation_reason"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type OrderRequest struct {
	CustomerId      string          `json:"customer_id"`
	RestaurantId    string          `json:"restaurant_id"`
	Items           map[string]Item `json:"items"`
	PaymentMethodId string          `json:"pm_id"`
	Amount          int64           `json:"amount_cents"`
	Currency        string          `json:"currency"`
	DeliveryAddress string          `json:"delivery_address"`
}

type OrderResponse struct {
	OrderId       string `json:"order_id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlation_id"`
}

type ItemReservation struct {
	OrderId       string          `json:"order_id"` //	Primary Key
	CustomerId    string          `json:"customer_id"`
	RestaurantId  string          `json:"restaurant_id"`
	ReservedItems map[string]Item `json:"reserved_items"`
	Status        string          `json:"status"`
}

type PaymentDetails struct {
	OrderId         string `json:"order_id"` //	Primary Key
	CustomerId      string `json:"customer_id"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	PaymentMethodId string `json:"pm_id"`
}

type PaymentResult struct {
	Success       bool   `json:"success"`
	OrderId       string `json:"order_id"`
	CustomerId    string `json:"customer_id"`
	TransactionID string `json:"transaction_id"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	FailureReason string `json:"reason"`
}

type Restaurant struct {
	RestaurantId          string          `json:"restaurant_id"` //	Primary Key
	CapacityMax           int64           `json:"max_capacity"`
	CurrentLoad           int64           `json:"curr_load"`
	ParallelizationFactor int64           `json:"parallelization_factor"`
	Items                 map[string]Item `json:"items"`
}

type Ticket struct {
	OrderId      string          `json:"order_id"` //	Primary Key
	RestaurantId string          `json:"restaurant_id"`
	Items        map[string]Item `json:"items"`
	ETAminutes   time.Duration   `json:"eta_minutes"`
	AcceptedAt   time.Time       `json:"accepted_at"`
	Status       string          `json:"status"`
}
