package server

import (
	"context"
	"fmt"
	"food-delivery-saga/cmd/gateway/server/handler"
	"food-delivery-saga/pkg/kafka"
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
	Handler  *handler.Handler
	Router   *gin.Engine
}

type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func NewServer(conf ServerConfig, prodConf kafka.ProducerConfig) *Server {
	producer := kafka.NewProducer(prodConf)
	orderHandler := handler.NewHandler(producer)

	server := &Server{
		Config:   conf,
		Producer: producer,
		Handler:  orderHandler,
	}

	server.SetupRouter()

	return server
}

func (s *Server) SetupRouter() {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()

	//	middleware
	router.Use(gin.Recovery())
	//	TODO: add authentication, logging, error-handling

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

	log.Printf("API Gateway stopped")
	return nil
}
