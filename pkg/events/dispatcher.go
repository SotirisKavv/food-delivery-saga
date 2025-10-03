package events

import (
	"encoding/json"
	"fmt"
)

type TypedHandler func(raw []byte) error

type Dispatcher struct {
	Handlers map[EventType]TypedHandler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{Handlers: make(map[EventType]TypedHandler)}
}

func Register[T DomainEvent](d *Dispatcher, et EventType, handler func(T) error) {
	d.Handlers[et] = func(raw []byte) error {
		var evt T
		if err := json.Unmarshal(raw, &evt); err != nil {
			return fmt.Errorf("Failed to unmarshal %s: %w", et, err)
		}
		return handler(evt)
	}
}

type EventEnvelope struct {
	Metadata Metadata `json:"mtdt"`
}

func (d *Dispatcher) Dispatch(raw []byte) error {
	var env EventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("Failed to unmarshal value: %w", err)
	}

	handler, ok := d.Handlers[env.Metadata.Type]
	if !ok {
		return fmt.Errorf("No handler found for %s", env.Metadata.Type)
	}

	return handler(raw)
}
