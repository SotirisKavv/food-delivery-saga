package database

import (
	"context"
	"fmt"
	"food-delivery-saga/pkg/models"
	"food-delivery-saga/pkg/utils"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	DB *pgxpool.Pool
}

// Init Database
func NewPGDatabase() *Database {
	dbConn, err := pgxpool.New(context.Background(), utils.GetEnv("PGSQL_URL", ""))
	if err != nil {
		panic(fmt.Errorf("Failed to connect to Postgres DB."))
	}

	return &Database{
		DB: dbConn,
	}
}

// ORDERS
func (d *Database) GetOrder(ctx context.Context, orderId string) (models.Order, error) {
	query := `SELECT id, status, cancelation_reason FROM orders WHERE id = $1 FOR UPDATE;`
	var order models.Order
	row := d.DB.QueryRow(ctx, query, orderId)
	err := row.Scan(&order.OrderId, &order.Status, &order.CancelationReason)
	return order, err
}

func (d *Database) SaveOrder(ctx context.Context, order models.Order) error {
	orderQuery := `INSERT INTO orders(id, customer_id, restaurant_id, amount_cents, currency, status)
			  	   VALUES($1, $2, $3, $4, $5, $6);`
	orderItemQuery := `INSERT INTO orderItems(sku, orderId, quantity)
			  	   VALUES %s;`
	placeholders := []string{}
	values := []any{}

	cnt := 0
	for _, item := range order.Items {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", 1+cnt*3, 2+cnt*3, 3+cnt*3))
		values = append(values, item.SKU, order.OrderId, item.Quantity)
		cnt += 1
	}
	orderItemQuery = fmt.Sprintf(orderItemQuery, strings.Join(placeholders, ","))

	if _, err := d.DB.Exec(ctx, orderQuery,
		order.OrderId, order.CustomerId, order.RestaurantId,
		order.Amount, order.Currency, string(order.Status)); err != nil {
		return err
	}

	if _, err := d.DB.Exec(ctx, orderItemQuery, values...); err != nil {
		return err
	}

	return nil
}

func (d *Database) UpdateOrderStatus(ctx context.Context, order models.Order) error {
	query := `UPDATE orders 
			  SET status = $1, cancelation_reason = $2, updated_at = $3
			  WHERE id = $4;`
	_, err := d.DB.Exec(ctx, query,
		string(order.Status), order.CancelationReason, time.Now().UTC(), order.OrderId)
	return err

}

// RESTAURANTS
func (d *Database) GetRestaurantStock(ctx context.Context, restoId string) (models.Restaurant, error) {
	query := `SELECT r.restaurant_id, ri.sku, ri.quantity 
			  FROM restaurants r 
			  JOIN restaurantItems ri ON r.restaurant_id = ri.restaurant_id 
			  WHERE r.restaurant_id = $1 FOR UPDATE;`
	var restaurant models.Restaurant
	restaurant.Items = make(map[string]models.Item)

	rows, err := d.DB.Query(ctx, query, restoId)
	if err != nil {
		return restaurant, err
	}
	defer rows.Close()

	for rows.Next() {
		var item models.Item
		if err := rows.Scan(&restaurant.RestaurantId, &item.SKU, &item.Quantity); err != nil {
			return restaurant, err
		}
		restaurant.Items[item.SKU] = item
	}
	return restaurant, nil
}

func (d *Database) GetRestaurantAndPreparationInfo(ctx context.Context, restoId string) (models.Restaurant, error) {
	query := `SELECT 
				r.restaurant_id, r.curr_load, r.max_capacity, 
				r.parallelization_factor, ri.sku, ri.prep_time 
			  FROM restaurants r 
			  JOIN restaurantItems ri ON r.restaurant_id = ri.restaurant_id 
			  WHERE r.restaurant_id = $1 FOR UPDATE;`
	var restaurant models.Restaurant
	restaurant.Items = make(map[string]models.Item)

	rows, err := d.DB.Query(ctx, query, restoId)
	if err != nil {
		return restaurant, err
	}
	defer rows.Close()

	for rows.Next() {
		var item models.Item
		err := rows.Scan(
			&restaurant.RestaurantId, &restaurant.CurrentLoad,
			&restaurant.CapacityMax, &restaurant.ParallelizationFactor,
			&item.SKU, &item.PrepTime,
		)
		if err != nil {
			return restaurant, err
		}
		restaurant.Items[item.SKU] = item
	}
	return restaurant, nil
}

func (d *Database) UpdateRestaurantStock(ctx context.Context, restaurant models.Restaurant) error {
	query := `INSERT INTO restaurantItems(sku, restaurant_id, quantity)
			  VALUES %s
			  ON CONFLICT(sku, restaurant_id)
			  DO UPDATE SET
			  quantity = EXCLUDED.quantity;`
	placeholders := []string{}
	values := []any{}

	cnt := 0
	for _, item := range restaurant.Items {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", 1+cnt*3, 2+cnt*3, 3+cnt*3))
		values = append(values, item.SKU, restaurant.RestaurantId, item.Quantity)
		cnt += 1
	}

	query = fmt.Sprintf(query, strings.Join(placeholders, ","))
	_, err := d.DB.Exec(ctx, query, values...)
	return err
}

func (d *Database) UpdateRestaurantLoad(ctx context.Context, restaurant models.Restaurant) error {
	query := `UPDATE restaurants SET curr_load = $1 WHERE restaurant_id = $2;`
	_, err := d.DB.Exec(ctx, query, restaurant.CurrentLoad, restaurant.RestaurantId)
	return err
}

// ITEM RESERVATIONS
func (d *Database) SaveReservation(ctx context.Context, reservation models.ItemReservation) error {
	reservationQuery := `INSERT INTO reservations(order_id, customer_id, restaurant_id)
			  			 VALUES ($1, $2, $3);`
	reservedItemsQuery := `INSERT INTO reservedItems(sku, order_id, quantity)
						   VALUES %s;`
	placeholders := []string{}
	values := []any{}

	cnt := 0
	for _, item := range reservation.ReservedItems {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", 1+cnt*3, 2+cnt*3, 3+cnt*3))
		values = append(values, item.SKU, reservation.OrderId, item.Quantity)
		cnt += 1
	}
	reservedItemsQuery = fmt.Sprintf(reservedItemsQuery, strings.Join(placeholders, ","))

	if _, err := d.DB.Exec(ctx, reservationQuery,
		reservation.OrderId, reservation.CustomerId, reservation.RestaurantId); err != nil {
		return err
	}

	if _, err := d.DB.Exec(ctx, reservedItemsQuery, values...); err != nil {
		return err
	}

	return nil
}

func (d *Database) GetReservedItems(ctx context.Context, reservationId string) (models.ItemReservation, error) {
	query := `SELECT r.restaurant_id, ri.sku, ri.quantity 
			  FROM reservations r 
			  JOIN reservedItems ri ON r.order_id = ri.order_id 
			  WHERE r.order_id = $1 FOR UPDATE;`
	var reservation models.ItemReservation
	reservation.ReservedItems = make(map[string]models.Item)

	rows, err := d.DB.Query(ctx, query, reservationId)
	if err != nil {
		return reservation, err
	}
	defer rows.Close()

	for rows.Next() {
		var item models.Item
		if err := rows.Scan(&reservation.RestaurantId, &item.SKU, &item.Quantity); err != nil {
			return reservation, err
		}
		reservation.ReservedItems[item.SKU] = item
	}
	return reservation, nil
}

func (d *Database) UpdateReservationReleased(ctx context.Context, reservationId string) error {
	query := `UPDATE reservations 
			  SET status = 'RELEASED'
			  WHERE order_id = $1;`
	_, err := d.DB.Exec(ctx, query, reservationId)
	return err
}

// PAYMENTS
func (d *Database) SavePayment(ctx context.Context, details models.PaymentDetails, txnId string) error {
	query := `INSERT INTO payments(order_id, customer_id, amount, currency, pm_id, transaction_id)
			  VALUES ($1, $2, $3, $4, $5, $6);`
	_, err := d.DB.Exec(ctx, query,
		details.OrderId, details.CustomerId, details.Amount,
		details.Currency, details.PaymentMethodId, txnId)

	return err
}

func (d *Database) GetPayment(ctx context.Context, paymentId string) (models.PaymentDetails, error) {
	query := `SELECT order_id, customer_id,	amount,	currency, pm_id
			  FROM payments 
			  WHERE order_id = $1 FOR UPDATE;`
	var details models.PaymentDetails
	row := d.DB.QueryRow(ctx, query, paymentId)
	err := row.Scan(&details.OrderId,
		&details.CustomerId, &details.Amount, &details.Currency,
		&details.PaymentMethodId,
	)
	return details, err
}

// TICKETS
func (d *Database) SaveTicket(ctx context.Context, ticket models.Ticket) error {
	ticketQuery := `INSERT INTO tickets(order_id, restaurant_id, eta_minutes, accepted_at)
			  			 VALUES ($1, $2, $3, $4);`
	_, err := d.DB.Exec(ctx, ticketQuery,
		ticket.OrderId, ticket.RestaurantId,
		ticket.ETAminutes.Seconds(), ticket.AcceptedAt,
	)

	return err
}

func (d *Database) UpdateTicketCompleted(ctx context.Context, ticketId string) error {
	query := `UPDATE tickets 
			  SET status = 'COMPLETED'
			  WHERE order_id = $1;`
	_, err := d.DB.Exec(ctx, query, ticketId)
	return err
}

// OUTBOX
func (d *Database) SaveOutbox(ctx context.Context, outbox models.Outbox) error {
	query := `INSERT INTO outbox(id, key, event_type, payload, topic)
			  VALUES ($1, $2, $3, $4, $5);`
	_, err := d.DB.Exec(ctx, query,
		outbox.Id, outbox.Key, outbox.EventType, outbox.Payload, outbox.Topic,
	)
	return err
}

func (d *Database) GetUnpublishedOutbox(ctx context.Context, limit int, topic string) ([]models.Outbox, error) {
	query := `SELECT id, key, event_type, payload
			  FROM outbox
			  WHERE published = FALSE AND topic = $1
			  LIMIT $2 FOR UPDATE SKIP LOCKED;`
	rows, err := d.DB.Query(ctx, query, topic, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var batch []models.Outbox
	for rows.Next() {
		var outbox models.Outbox
		if err := rows.Scan(&outbox.Id, &outbox.Key, &outbox.EventType, &outbox.Payload); err != nil {
			return nil, err
		}
		batch = append(batch, outbox)
	}

	return batch, nil
}

func (d *Database) UpdateOutboxPublished(ctx context.Context, ids []string) error {
	query := `UPDATE outbox SET published = TRUE WHERE id = ANY($1::text[]);`
	if _, err := d.DB.Exec(ctx, query, ids); err != nil {
		return err
	}
	return nil
}
