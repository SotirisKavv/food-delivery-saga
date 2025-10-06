package models

type OrderStatus string

const (
	ORDER_STATUS_PENDING    OrderStatus = "PENDING"
	ORDER_STATUS_RESERVED   OrderStatus = "RESERVED"
	ORDER_STATUS_AUTHORIZED OrderStatus = "AUTHORIZED"
	ORDER_STATUS_ACCEPTED   OrderStatus = "ACCEPTED"
	ORDER_STATUS_DISPATCHED OrderStatus = "DISPATCHED"
	ORDER_STATUS_IN_TRANSIT OrderStatus = "IN_TRANSIT"
	ORDER_STATUS_DELIVERED  OrderStatus = "DELIVERED"
	ORDER_STATUS_CAPTURED   OrderStatus = "CAPTURED"
	ORDER_STATUS_COMPLETED  OrderStatus = "COMPLETED"
	ORDER_STATUS_CANCELED   OrderStatus = "CANCELED"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}

type Order struct {
	OrderId         string          `json:"order_id"`
	CustomerId      string          `json:"customer_id"`
	RestaurantId    string          `json:"restaurant_id"`
	Items           map[string]Item `json:"items"`
	Amount          int64           `json:"amount_cents"`
	Currency        string          `json:"currency"`
	DeliveryAddress string          `json:"delivery_address"`
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

type Restaurant struct {
	RestaurantId string          `json:"restaurant_id"`
	Items        map[string]Item `json:"items"`
}

type PaymentDetails struct {
	OrderId         string `json:"order_id"`
	CustomerId      string `json:"customer_id"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	PaymentMethodId string `json:"pm_id"`
}

type PaymentResult struct {
	Success       bool   `json:"success"`
	TransactionID string `json:"transaction_id"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	FailureReason string `json:"reason"`
}
