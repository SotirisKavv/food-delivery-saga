package server

import (
	"context"
	"errors"
	"food-delivery-saga/cmd/restaurant/server/handler"
	"food-delivery-saga/pkg/kafka"
	"log"
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
	handler := handler.NewHandler(producer)

	return &Server{
		Producer: producer,
		Consumer: consumer,
		Handler:  handler,
	}
}

func (s *Server) Start() error {
	log.Println("Starting Restaurant Service...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	// Run Kafka consumer
	g.Go(func() error {
		if err := s.Consumer.ConsumeWithRetry(ctx, s.Handler.HandleMessage, 3); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	// Run scheduler readiness loop concurrently
	g.Go(func() error {
		if err := s.Handler.CheckForReadyTickets(ctx); err != nil && !errors.Is(err, context.Canceled) {
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
	s.Handler.TicketScheduler.Close()

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
	log.Println("Restaurant Service stopped cleanly")
	return nil
}
