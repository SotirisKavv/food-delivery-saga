package main

import (
	"food-delivery-saga/cmd/inventory/server"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/utils"
	"log"
	"strings"
)

func main() {
	kafkaBrokers := utils.GetEnv("KAFKA_BROKERS", "kafka:9092")
	brokers := strings.Split(kafkaBrokers, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	prodConf := kafka.ProducerConfig{
		Brokers: brokers,
	}

	consConf := kafka.ConsumerConfig{
		Brokers: brokers,
		Topics: []string{
			kafka.TopicOrder,      //OrderPlaced
			kafka.TopicPayment,    //PaymentFailed
			kafka.TopicRestaurant, //RestaurantRejected
		},
		GroupId: "inventory-svc",
	}

	server := server.NewServer(prodConf, consConf)

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
