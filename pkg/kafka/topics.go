package kafka

type EventMessage struct {
	Topic string
	Key   string
	Event any
}

const (
	TopicOrder           string = "order.events"
	TopicInventory       string = "inventory.events"
	TopicPayment         string = "payment.events"
	TopicRestaurant      string = "restaurant.events"
	TopicNotifications   string = "notifications.events"
	TopicDeadLetterQueue string = "dlq.events"
)
