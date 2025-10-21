-- Database initialization for food-delivery-saga
-- Enums, tables, relations, indexes, and seed data

-- Enums
DO $$ BEGIN
	CREATE TYPE order_status AS ENUM ('PENDING','RESERVED','AUTHORIZED','ACCEPTED','COMPLETED','CANCELED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
	CREATE TYPE reservation_status AS ENUM ('HELD','RELEASED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
	CREATE TYPE ticket_status AS ENUM ('PENDING','COMPLETED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Core tables
CREATE TABLE IF NOT EXISTS restaurants (
	restaurant_id TEXT PRIMARY KEY,
	max_capacity BIGINT NOT NULL,
	curr_load BIGINT NOT NULL DEFAULT 0,
	parallelization_factor BIGINT NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS restaurantitems (
	sku TEXT NOT NULL,
	restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
	quantity BIGINT NOT NULL DEFAULT 0,
	prep_time BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (sku, restaurant_id)
);

CREATE TABLE IF NOT EXISTS orders (
	id TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL,
	restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id) ON DELETE RESTRICT,
	amount_cents BIGINT NOT NULL,
	currency TEXT NOT NULL,
	status order_status NOT NULL DEFAULT 'PENDING',
	cancelation_reason TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orderitems (
	sku TEXT NOT NULL,
	orderid TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
	quantity BIGINT NOT NULL,
	PRIMARY KEY (sku, orderid)
);

CREATE TABLE IF NOT EXISTS reservations (
	order_id TEXT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
	customer_id TEXT NOT NULL,
	restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
	status reservation_status NOT NULL DEFAULT 'HELD',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reserveditems (
	sku TEXT NOT NULL,
	order_id TEXT NOT NULL REFERENCES reservations(order_id) ON DELETE CASCADE,
	quantity BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (sku, order_id)
);

CREATE TABLE IF NOT EXISTS payments (
	order_id TEXT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
	customer_id TEXT NOT NULL,
	amount BIGINT NOT NULL,
	currency TEXT NOT NULL,
	pm_id TEXT NOT NULL,
	transaction_id TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tickets (
	order_id TEXT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
	restaurant_id TEXT NOT NULL REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
	eta_minutes BIGINT NOT NULL DEFAULT 0,
	accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	status ticket_status NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS outbox (
	id TEXT PRIMARY KEY,
	key TEXT NOT NULL,
	event_type TEXT NOT NULL,
	topic TEXT NOT NULL,
	payload JSONB NOT NULL,
	published BOOLEAN NOT NULL DEFAULT FALSE, 
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for frequent lookups
CREATE INDEX IF NOT EXISTS idx_restaurantitems_restaurant ON restaurantitems(restaurant_id);
CREATE INDEX IF NOT EXISTS idx_orderitems_order ON orderitems(orderid);
CREATE INDEX IF NOT EXISTS idx_reserveditems_restaurant ON reserveditems(order_id);

-- Seed data
INSERT INTO restaurants (restaurant_id, max_capacity, curr_load, parallelization_factor)
VALUES
	('resto-1', 50, 0, 2),
	('resto-2', 30, 0, 1),
	('resto-3', 80, 0, 3)
ON CONFLICT (restaurant_id) DO NOTHING;

INSERT INTO restaurantitems (sku, restaurant_id, quantity, prep_time)
VALUES
	('PIZZA_MARGHERITA', 'resto-1', 100, 60),
	('PIZZA_PEPPERONI',  'resto-1',  80, 72),
	('PASTA_CARBONARA',  'resto-1', 120, 54),

	('PIZZA_MARGHERITA', 'resto-2',  60, 65),
	('PIZZA_VEGGIE',     'resto-2',  70, 70),
	('SALAD_GREEK',      'resto-2', 150, 24),

	('BURGER_CLASSIC',   'resto-3', 200, 48),
	('BURGER_CHEESE',    'resto-3', 180, 51),
	('FRIES_REGULAR',    'resto-3', 300, 30)
ON CONFLICT (sku, restaurant_id) DO NOTHING;

