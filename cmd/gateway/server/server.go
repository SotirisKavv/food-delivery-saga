package server

import (
	"context"
	"fmt"
	"food-delivery-saga/cmd/gateway/server/handler"
	"food-delivery-saga/cmd/gateway/server/service"
	"food-delivery-saga/pkg/database"
	"food-delivery-saga/pkg/kafka"
	"food-delivery-saga/pkg/outbox"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type Server struct {
	Config   ServerConfig
	Producer *kafka.Producer
	Consumer *kafka.Consumer
	Handler  *handler.Handler
	Relay    *outbox.Relay
	Router   *gin.Engine
}

type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func NewServer(conf ServerConfig, prodConf kafka.ProducerConfig, consConf kafka.ConsumerConfig) *Server {
	producer := kafka.NewProducer(prodConf)
	database := database.NewPGDatabase()

	relay := outbox.NewRelay(producer, database, kafka.TopicOrder)

	orderService := service.NewService(database, relay)
	orderHandler := handler.NewHandler(orderService)

	consumer := kafka.NewConsumer(consConf, relay)

	server := &Server{
		Config:   conf,
		Producer: producer,
		Consumer: consumer,
		Handler:  orderHandler,
		Relay:    relay,
	}

	server.SetupRouter()

	return server
}

func (s *Server) SetupRouter() {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	//	middleware
	router.Use(gin.Recovery())
	//	TODO: add authentication

	// routes
	api := router.Group("/api/v1")
	{
		orders := api.Group("/orders")
		{
			orders.POST("", s.Handler.CreateOrder)
			orders.GET("/:id", s.Handler.GetOrder)
		}

	}
	router.GET("/health", s.Handler.HealthCheck)

	s.Router = router
}

func (s *Server) Start() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", s.Config.Port),
		Handler:      s.Router,
		ReadTimeout:  s.Config.ReadTimeout,
		WriteTimeout: s.Config.WriteTimeout,
		IdleTimeout:  s.Config.IdleTimeout,
	}

	go func() {
		log.Printf("API Gateway starting on %s", s.Config.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	go func() {
		if err := s.Consumer.ConsumeWithRetry(ctx, s.Handler.HandleMessages, 3); err != nil {
			log.Printf("Failed to consume message: %+v", err)
		}
	}()

	go func() {
		if err := s.Relay.Run(ctx); err != nil {
			log.Printf("Relay failure: %+v", err)
		}
	}()

	return s.HandleShutdown(srv)
}

func (s *Server) HandleShutdown(srv *http.Server) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("Shutting down API Gateway...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Failed to shutdown server: %v", err)
		return err
	}

	if err := s.Producer.Close(); err != nil {
		log.Printf("Failed to close kafka Producer: %v", err)
		return err
	}
	if err := s.Consumer.Close(); err != nil {
		log.Printf("Failed to close kafka Producer: %v", err)
		return err
	}

	log.Printf("API Gateway stopped")
	return nil
}
