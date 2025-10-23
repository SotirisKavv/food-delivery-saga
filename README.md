# Food Delivery Saga — Event‑Driven, Resilient Ordering System

Production‑style reference implementation of an event‑driven food ordering platform in Go. It demonstrates the Saga pattern (choreography) across microservices with Kafka, Postgres, Redis, and an Outbox relay for exactly‑once message publication.

- Endpoints: HTTP API Gateway for placing and querying orders
- Services: Inventory, Payment, Restaurant
- Messaging: Kafka topics per bounded context + DLQ
- Persistence: Postgres (system of record) + Redis (fast cache)
- Reliability: Outbox pattern, partition‑ordered consumers, retries, DLQ

Quick links:
- Architecture diagram: `charts/architecture.mmd`
- Flow chart (order lifecycle): `charts/flowchart.mmd`
- Sequence diagram (end‑to‑end): `charts/sequence.mmd`
- ER diagram (database schema): `charts/er.mmd`


## What this project showcases

- Saga pattern (choreography) for a multi‑step business flow without a central orchestrator
- Transactional Outbox to avoid dual‑write issues (DB + Kafka) and ensure at‑least‑once delivery with idempotent consumers
- Per‑partition ordered consumption with worker fan‑out (preserves Kafka partition ordering while scaling consumers)
- Dead‑Letter Queue and retry backoff for non‑business errors
- Clean, typed domain events with a small dispatcher layer


## High‑level architecture

Components:
- API Gateway (Go + Gin): Receives orders, persists Order + Outbox row, updates status from downstream events
- Inventory Service: Reserves and releases items per restaurant stock
- Payment Service: Authorizes/voids payment (mock processor), caches details in Redis
- Restaurant Service: Capacity management, ticket ETA scheduler, final readiness
- Kafka Topics: `order.events`, `inventory.events`, `payment.events`, `restaurant.events`, `dlq.events`
- Data: Postgres for orders/tickets/etc; Redis for ephemeral/cache data

See container diagram: `charts/architecture.mmd`


## How it works (Saga choreography)

1) Client places order (HTTP POST) to the API Gateway
- Gateway saves Order (status=PENDING) and writes an OrderPlaced event to the Outbox table
- Outbox Relay publishes to Kafka topic `order.events`

2) Inventory Service consumes OrderPlaced
- Validates SKUs and quantities against restaurant stock
- On success: reserves stock, saves reservation, emits ItemsReserved
- On failure: emits ItemsReservationFailed

3) Payment Service consumes ItemsReserved
- Loads cached PaymentDetails, calls mock processor
- On success: persists Payment, emits PaymentAuthorized
- On failure: emits PaymentFailed

4) Restaurant Service consumes PaymentAuthorized
- Computes prep load and ETA; accepts or rejects
- On acceptance: saves Ticket, updates restaurant load, emits RestaurantAccepted; scheduler later emits RestaurantReady
- On rejection: emits RestaurantRejected (Payment/Inventory compensate downstream)

5) API Gateway consumes Inventory/Payment/Restaurant events
- Transitions Order status: RESERVED → AUTHORIZED → ACCEPTED → COMPLETED or → CANCELED with reason

6) Reliability & error handling
- Each service writes events to Outbox in the same DB transaction as state changes; a background Relay flushes the Outbox to Kafka in batches
- Consumers retry transient failures with backoff; non‑business errors are forwarded to `dlq.events` with rich metadata

See sequence: `charts/sequence.mmd` and lifecycle flow: `charts/flowchart.mmd`.


## Design patterns and tactics

- Saga (Choreography): Progression driven by domain events, no central coordinator
- Transactional Outbox: `pkg/outbox/relay.go` + `outbox` table for atomic state + event persistence
- Event Dispatcher: Type‑safe handler registration and decoding (`pkg/events/dispatcher.go`)
- Partition‑ordered workers: One worker per Kafka partition to maintain order while parallelizing (`pkg/kafka/consumer.go`)
- Repository Abstractions: Generic Repository with in‑memory or Redis implementations (`pkg/repository/*`)
- Delay Queue / Scheduler: Priority‑heap delay queue for Ticket readiness (`pkg/scheduler/*`)
- Structured errors with trace: `pkg/error/errors.go` adds codes, operation trace, and causes


## Technologies

- Go (Gin, pgx, kafka‑go, go‑redis)
- Apache Kafka (Confluent images) with topics provisioned on startup
- PostgreSQL (system of record) with schema + seed data
- Redis (ephemeral cache)
- Docker Compose for local orchestration


## Run locally

Prereqs: Docker Desktop

1) Start the stack

```powershell
docker compose up -d --build
```

2) Verify
- API Gateway health: http://localhost:8080/health
- Kafka UI: http://localhost:8090/
- Postgres: localhost:5432 (credentials in `.env`)

3) Place an order (example)

```json
POST http://localhost:8080/api/v1/orders
Content-Type: application/json

{
	"customer_id": "cust-123",
	"restaurant_id": "resto-3",
	"items": {
		"BURGER_CLASSIC": { "sku": "BURGER_CLASSIC", "quantity": 2 },
		"FRIES_REGULAR":  { "sku": "FRIES_REGULAR",  "quantity": 1 }
	},
	"pm_id": "pm-111",
	"amount_cents": 2500,
	"currency": "USD",
	"delivery_address": "Main St 1"
}
```

4) Poll order status

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/orders/<order_id>
```


## Services and topics

- order.events: OrderPlaced
- inventory.events: ItemsReserved, ItemsReservationFailed, ItemsReleased
- payment.events: PaymentAuthorized, PaymentFailed
- restaurant.events: RestaurantAccepted, RestaurantRejected, RestaurantReady
- dlq.events: Dead‑lettered envelopes for investigation


## Data model

Postgres schema includes orders, orderItems, reservations, reservedItems, payments, tickets, restaurants, restaurantItems, and outbox. Relations and keys are captured in `charts/er.mmd`.


## Error handling, retries, DLQ

- Business errors stay in the domain (e.g., not enough inventory, capacity reached) and produce failure events
- Infrastructure/unknown errors are retried (exponential backoff) and, on persistent failure, published to `dlq.events` with the original payload and trace


## Notable implementation details

- Exactly‑once publish semantics via Outbox: DB state and event are persisted together; the relay flushes in batches and marks rows as published
- Per‑partition workers: a worker goroutine is lazily created per Kafka partition to preserve event order per key while allowing concurrency across partitions
- Mock payment processor: configurable failure rate and latency simulate real payment systems
- Delay queue scheduler: pushes a ticket into a time‑ordered heap; ready tickets trigger RestaurantReady and capacity release


## Folder map

- `cmd/gateway|inventory|payment|restaurant`: service mains, servers, handlers
- `pkg/events`: event contracts + dispatcher
- `pkg/kafka`: producer/consumer wrappers, topic names
- `pkg/outbox`: relay service to flush outbox table to Kafka
- `pkg/database`: pgx queries for all aggregates and outbox
- `pkg/repository`: generic repository with memory and Redis impls
- `pkg/scheduler`: delay queue for ticket readiness
- `deployments/`: Dockerfiles, compose, Kafka topic setup, Postgres schema/seed
- `charts/`: Mermaid diagrams (architecture, flow, sequence, ER)


## Next steps (ideas)

- Add idempotency keys and dedupe guards around consumers where appropriate
- Add observability: OpenTelemetry traces/spans per event, Prometheus metrics for outbox lag and DLQ rate
- Add authn/z at the gateway, request validation, and richer error mapping
- Introduce CDC‑based outbox or transaction log tailing for higher throughput
- Harden DLQ tooling and re‑processing flows


## License

MIT (see `LICENSE`)
