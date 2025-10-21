package server

import (
	"context"
	"errors"
	"food-delivery-saga/cmd/inventory/server/handler"
	"food-delivery-saga/pkg/database"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/outbox"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

type Server struct {
	Producer *kafka.Producer
	Consumer *kafka.Consumer
	Relay    *outbox.Relay
	Handler  *handler.Handler
}

func NewServer(prodConf kafka.ProducerConfig, consConf kafka.ConsumerConfig) *Server {
	producer := kafka.NewProducer(prodConf)
	database := database.NewPGDatabase()

	relay := outbox.NewRelay(producer, database, kafka.TopicInventory)

	inventoryHandler := handler.NewHandler(database, relay)

	consumer := kafka.NewConsumer(consConf)

	return &Server{
		Producer: producer,
		Consumer: consumer,
		Relay:    relay,
		Handler:  inventoryHandler,
	}
}

func (s *Server) Start() error {
	log.Println("Starting Inventory Service...")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := s.Consumer.ConsumeWithRetry(ctx, s.Handler.HandleMessage, 3)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		if err := s.Relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	return s.HandleShutdown(ctx, g)
}

func (s *Server) HandleShutdown(ctx context.Context, g *errgroup.Group) error {
	<-ctx.Done()
	log.Println("Shutdown signal received, commencing graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.Producer.Close(); err != nil {
		log.Printf("Error closing producer: %v", err)
	}
	if err := s.Consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	if err := g.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	select {
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			log.Println("Graceful shutdown timed out")
		}
	default:
	}
	log.Println("Inventory Service stopped cleanly")
	return nil
}
