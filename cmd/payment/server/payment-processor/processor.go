package paymentprocessor

import (
	"context"
	"fmt"
	"food-delivery-saga/pkg/models"
)

type Processor interface {
	ProcessPayment(ctx context.Context, details models.PaymentDetails) (models.PaymentResult, error)
	RevertPayment(ctx context.Context, details models.PaymentDetails) (models.PaymentResult, error)
}

type ProcessorType string

const (
	ProcessorMock ProcessorType = "mock"
)

func NewProcessor(processorType ProcessorType) (Processor, error) {
	var processor Processor
	switch processorType {
	case ProcessorMock:
		processor = NewMockPaymentProcessor()
	default:
		return nil, fmt.Errorf("Not available processor type: %s", string(processorType))
	}

	return processor, nil
}
