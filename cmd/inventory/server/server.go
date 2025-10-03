package server

import (
	"context"
	"errors"
	"food-delivery-saga/cmd/inventory/server/handler"
	"food-delivery-saga/pkg/kafka"
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
	Handler  *handler.Handler
}

func NewServer(prodConf kafka.ProducerConfig, consConf kafka.ConsumerConfig) *Server {
	producer := kafka.NewProducer(prodConf)
	consumer := kafka.NewConsumer(consConf)
	inventoryHandler := handler.NewHandler(producer)

	return &Server{
		Producer: producer,
		Consumer: consumer,
		Handler:  inventoryHandler,
	}
}

func (s *Server) Start() error {
	log.Println("Starting Inventory Service...")
	// Root context canceled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	// Consumer goroutine
	g.Go(func() error {
		err := s.Consumer.ConsumeWithRetry(ctx, s.Handler.HandleMessage, 3)
		if err != nil && !errors.Is(err, context.Canceled) {
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
