package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache[T any] struct {
	Client *redis.Client
	IDFn   IDExtractor[T]
	TTL    time.Duration
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
}

func NewRedisCache[T any](ctx context.Context, redisConf RedisConfig, ttl time.Duration, idFn IDExtractor[T]) (RedisCache[T], error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisConf.Address,
		Password: redisConf.Password,
		DB:       redisConf.DB,
	})

	if _, err := client.Ping(ctx).Result(); err != nil {
		return RedisCache[T]{}, fmt.Errorf("Error connecting to Redis Client: %w", err)
	}

	return RedisCache[T]{
		Client: client,
		TTL:    ttl,
		IDFn:   idFn,
	}, nil
}

func (r RedisCache[T]) Load(ctx context.Context, id string) (T, error) {
	var zero, value T
	err := r.Client.Get(ctx, id).Scan(&value)
	if err != nil {
		return zero, fmt.Errorf("Error loading resource: %w", err)
	}
	return value, nil
}

func (r RedisCache[T]) Save(ctx context.Context, entity T) error {
	id := r.IDFn(entity)
	if err := r.Client.Set(ctx, id, entity, r.TTL).Err(); err != nil {
		return fmt.Errorf("Error saving resource: %w", err)
	}
	return nil
}

func (r RedisCache[T]) Update(ctx context.Context, entity T) error {
	id := r.IDFn(entity)
	if _, err := r.Client.Get(ctx, id).Result(); err != nil {
		return fmt.Errorf("Failed to find resource with key %s: %w", id, err)
	}

	if err := r.Client.Set(ctx, id, entity, r.TTL).Err(); err != nil {
		return fmt.Errorf("Error saving resource: %w", err)
	}
	return nil
}

func (r RedisCache[T]) Delete(ctx context.Context, id string) error {
	if _, err := r.Client.Del(ctx, id).Result(); err != nil {
		return fmt.Errorf("Failed to find resource with key %s: %w", id, err)
	}
	return nil
}

func (r RedisCache[T]) List(ctx context.Context, filter any) ([]T, error) {
	return nil, fmt.Errorf("/'List/' function is not supported on cache-only repository.")
}
