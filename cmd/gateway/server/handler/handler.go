package handler

import (
	"food-delivery-saga/pkg/events"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/utils"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Handler struct {
	Producer *kafka.Producer
}

func NewHandler(producer *kafka.Producer) *Handler {
	return &Handler{
		Producer: producer,
	}
}

func (h *Handler) CreateOrder(c *gin.Context) {

	var req models.OrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Invalid request format: %v", err)
		utils.SendValidationError(c, err)
		return
	}

	orderEvent := events.EventOrderPlaced{
		Metadata: events.Metadata{
			MessageId:     uuid.NewString(),
			Type:          events.EvtTypeOrderPlaced,
			OrderId:       uuid.NewString(),
			CorrelationId: uuid.NewString(),
			Timestamp:     time.Now().UTC(),
			Producer:      "api-gateway",
		},
		CustomerId:      req.CustomerId,
		PaymentMethodId: req.PaymentMethodId,
		AmountCents:     req.Amount,
		Currency:        req.Currency,
		RestaurantId:    req.RestaurantId,
		Items:           req.Items,
	}

	eventMessages := []kafka.EventMessage{
		{
			Topic: kafka.TopicOrder,
			Key:   orderEvent.Metadata.OrderId,
			Event: orderEvent,
		},
		{
			Topic: kafka.TopicInventory,
			Key:   orderEvent.Metadata.OrderId,
			Event: orderEvent,
		},
	}

	if err := h.Producer.PublishMultipleEvents(c, eventMessages); err != nil {
		log.Printf("Failed to publish message: %v", err)
		utils.SendInternalError(c, "Failed to process order")
		return
	}

	response := models.OrderResponse{
		OrderId:       orderEvent.Metadata.OrderId,
		Status:        string(models.ORDER_STATUS_PENDING),
		Message:       "Order received and being processed",
		CorrelationID: orderEvent.Metadata.CorrelationId,
	}

	log.Printf("Order placed successfully: %s", response.OrderId)
	utils.SendSuccess(c, http.StatusCreated, response.Message, response)
}

func (h *Handler) GetOrder(c *gin.Context) {
	id := c.Param("id")

	response := models.OrderResponse{
		OrderId: id,
		Status:  string(models.ORDER_STATUS_PENDING),
		Message: "Order retrieved",
	}

	utils.SendSuccess(c, http.StatusOK, response.Message, response)
}

func (h *Handler) HealthCheck(c *gin.Context) {
	health := map[string]any{
		"status":  "healthy",
		"service": "api-gateway",
	}
	utils.SendSuccess(c, http.StatusOK, "Service is Healthy", health)
}
