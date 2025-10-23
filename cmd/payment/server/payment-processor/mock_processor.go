package paymentprocessor

import (
	"context"
	"fmt"
	"food-delivery-saga/pkg/models"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type MockPaymentProcessor struct {
	FailureRate    float64
	ProcessingTime time.Duration
}

func NewMockPaymentProcessor() *MockPaymentProcessor {
	return &MockPaymentProcessor{
		FailureRate:    0.1,
		ProcessingTime: 5 * time.Second,
	}
}

var reasons = []string{
	"Insufficient funds",
	"Card declined",
	"Invalid card number",
	"Expired card",
	"Transaction limit exceeded",
	"Bank rejected transaction",
}

func (m *MockPaymentProcessor) ProcessPayment(ctx context.Context, details models.PaymentDetails) (models.PaymentResult, error) {
	log.Printf("[MOCK] Processing payment for order %s, amount %d %s",
		details.OrderId,
		details.Amount,
		details.Currency,
	)

	select {
	case <-time.After(m.ProcessingTime):
	case <-ctx.Done():
		return models.PaymentResult{
			Success:       false,
			FailureReason: ctx.Err().Error(),
		}, ctx.Err()
	}

	random := rand.Float32()

	if random < float32(m.FailureRate) {
		reason := reasons[rand.Intn(len(reasons))]
		log.Printf("[MOCK] Payment FAILED for order %s: %s", details.OrderId, reason)

		return models.PaymentResult{
			Success:       false,
			CustomerId:    details.CustomerId,
			OrderId:       details.OrderId,
			FailureReason: reason,
		}, nil
	}

	transactionID := fmt.Sprintf("TXN-%s", uuid.NewString()[:8])
	log.Printf("[MOCK] Payment SUCCESSFUL for order %s, transaction: %s", details.OrderId, transactionID)

	return models.PaymentResult{
		Success:       true,
		OrderId:       details.OrderId,
		CustomerId:    details.CustomerId,
		TransactionID: transactionID,
		Amount:        details.Amount,
		Currency:      details.Currency,
	}, nil
}

func (m *MockPaymentProcessor) RevertPayment(ctx context.Context, details models.PaymentDetails) (models.PaymentResult, error) {
	log.Printf("[MOCK] Compensating customer %s, amount %d %s",
		details.CustomerId,
		details.Amount,
		details.Currency,
	)

	select {
	case <-time.After(m.ProcessingTime):
	case <-ctx.Done():
		return models.PaymentResult{
			Success:       false,
			FailureReason: ctx.Err().Error(),
		}, ctx.Err()
	}

	transactionID := fmt.Sprintf("TXN-%s", uuid.NewString()[:8])
	log.Printf("[MOCK] Compensation SUCCESSFUL for customer %s, transaction: %s", details.CustomerId, transactionID)

	return models.PaymentResult{
		Success:       true,
		OrderId:       details.OrderId,
		CustomerId:    details.CustomerId,
		TransactionID: transactionID,
		Amount:        details.Amount,
		Currency:      details.Currency,
	}, nil
}
