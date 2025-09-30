package main

import (
	"food-delivery-saga/cmd/gateway/server"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/utils"
	"log"
	"strings"
	"time"
)

func main() {
	port := utils.GetEnv("API_GATEWAY_PORT", "8080")
	kafkaBrokers := utils.GetEnv("KAFKA_BROKERS", "kafka:9092")

	sConf := server.ServerConfig{
		Port:         port,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	brokers := strings.Split(kafkaBrokers, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	prodConf := kafka.ProducerConfig{
		Brokers: brokers,
	}

	server := server.NewServer(sConf, prodConf)

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
