package repository

import (
	"context"
	"fmt"
	"food-delivery-saga/pkg/utils"
	"time"
)

type IDExtractor[T any] func(T) string

type Repository[T any] interface {
	Load(ctx context.Context, id string) (T, error)
	Save(ctx context.Context, entity T) error
	Update(ctx context.Context, entity T) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, filter any) ([]T, error)
}

type RepositoryType string

const (
	RepositoryMemory RepositoryType = "memory"
	RepositoryRedis  RepositoryType = "cache"
)

func NewRepository[T any](ctx context.Context, repoType RepositoryType, idExtractor IDExtractor[T], opts ...any) (Repository[T], error) {
	var repo Repository[T]

	switch repoType {
	case RepositoryMemory:
		repo = NewMemoryRepo(idExtractor)
	case RepositoryRedis:
		redisConf := RedisConfig{
			Address:  utils.GetEnv("REDIS_CLIENT_ADDRESS", "redis:6379"),
			Password: utils.GetEnv("REDIS_CLIENT_PASSWORD", ""),
			DB:       0,
		}
		ttl, _ := time.ParseDuration(utils.GetEnv("REDIS_CLIENT_TTL", "0"))
		redisRepo, err := NewRedisCache(ctx, redisConf, ttl, idExtractor)
		if err != nil {
			return nil, err
		}
		repo = redisRepo
	default:
		return nil, fmt.Errorf("Failed to create Repository Handler: Unsupported Repository Type")
	}

	return repo, nil
}
