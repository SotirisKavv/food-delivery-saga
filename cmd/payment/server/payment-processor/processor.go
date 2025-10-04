package paymentprocessor

import "fmt"

type Processor interface{}

type ProcessorType string

const (
	ProcessorMock ProcessorType = "mock"
)

func NewProcessor(processorType ProcessorType) (*Processor, error) {
	var processor *Processor
	switch processorType {
	case ProcessorMock:
		processor = nil
	default:
		return nil, fmt.Errorf("Not available processor type: %s", string(processorType))
	}

	return processor, nil
}
