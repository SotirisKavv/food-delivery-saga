package events

import (
	"encoding/json"
	svcerror "food-delivery-saga/pkg/error"
	"log"
	"time"
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
			return svcerror.New(
				svcerror.ErrInternalError,
				svcerror.WithOp("Dispatcher.Register"),
				svcerror.WithMsg("failed to unmarshal event"),
				svcerror.WithCause(err),
				svcerror.WithTime(time.Now().UTC()),
			)
		}
		return handler(evt)
	}
	log.Printf("[DISPATCHER] Registered handler for %s", string(et))
}

type EventEnvelope struct {
	Metadata Metadata `json:"mtdt"`
}

func (d *Dispatcher) Dispatch(raw []byte) error {
	var env EventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Dispatcher.Dispatch"),
			svcerror.WithMsg("failed to unmarshal event"),
			svcerror.WithCause(err),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	log.Printf("[DISPATCHER] Handling order=%s type=%s producer=%s", env.Metadata.OrderId, env.Metadata.Type, env.Metadata.Producer)
	handler, ok := d.Handlers[env.Metadata.Type]
	if !ok {
		log.Printf("[DISPATCHER] No handler found for %s", env.Metadata.Type)
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Dispatcher.Register"),
			svcerror.WithMsg("no handler found for event"),
			svcerror.WithTime(time.Now().UTC()),
		)
	}

	return handler(raw)
}
