package handler

import (
	"context"
	"fmt"
	"food-delivery-saga/cmd/gateway/server/service"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/utils"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	OrderService *service.Service
}

func NewHandler(service *service.Service) *Handler {
	return &Handler{
		OrderService: service,
	}
}

func (h *Handler) CreateOrder(c *gin.Context) {
	var req models.OrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Invalid request format: %v", err)
		utils.SendValidationError(c, err)
		return
	}

	response, err := h.OrderService.CreateOrder(c, &req)
	if err != nil {
		utils.SendInternalError(c, fmt.Sprintf("Failed to create Order: %+v", err))
		return
	}

	log.Printf("Order placed successfully: %s", response.OrderId)
	utils.SendSuccess(c, http.StatusCreated, response.Message, response)
}

func (h *Handler) GetOrder(c *gin.Context) {
	id := c.Param("id")

	response, err := h.OrderService.GetOrder(c, id)
	if err != nil {
		utils.SendInternalError(c, fmt.Sprintf("Failed to retrieve Order %s: %+v", id, err))
		return
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

func (h *Handler) HandleMessages(ctx context.Context, eventMessage kafka.KafkaMessage) error {
	return h.OrderService.Dispatcher.Dispatch(eventMessage.Value)
}
