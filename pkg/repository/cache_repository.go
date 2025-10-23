package repository

import (
	"context"
	"encoding/json"
	"fmt"
	svcerror "food-delivery-saga/pkg/error"
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
		return RedisCache[T]{}, svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.NewRedisCache"),
			svcerror.WithMsg("connect to redis"),
			svcerror.WithCause(err),
		)
	}

	return RedisCache[T]{
		Client: client,
		TTL:    ttl,
		IDFn:   idFn,
	}, nil
}

func (r RedisCache[T]) Load(ctx context.Context, id string) (T, error) {
	var zero, value T
	data, err := r.Client.Get(ctx, id).Bytes()
	if err != nil {
		if err == redis.Nil {
			return zero, svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithOp("Repository.Redis.Load"),
				svcerror.WithMsg(fmt.Sprintf("resource with id %s not found", id)),
			)
		}
		return zero, svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.Redis.Load"),
			svcerror.WithMsg("load resource"),
			svcerror.WithCause(err),
		)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return zero, svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Repository.Redis.Load"),
			svcerror.WithMsg("decode resource"),
			svcerror.WithCause(err),
		)
	}
	return value, nil
}

func (r RedisCache[T]) Save(ctx context.Context, entity T) error {
	id := r.IDFn(entity)
	data, err := json.Marshal(entity)
	if err != nil {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Repository.Redis.Save"),
			svcerror.WithMsg("encode resource"),
			svcerror.WithCause(err),
		)
	}
	if err := r.Client.Set(ctx, id, data, r.TTL).Err(); err != nil {
		return svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.Redis.Save"),
			svcerror.WithMsg("save resource"),
			svcerror.WithCause(err),
		)
	}
	return nil
}

func (r RedisCache[T]) Update(ctx context.Context, entity T) error {
	id := r.IDFn(entity)
	if _, err := r.Client.Get(ctx, id).Result(); err != nil {
		if err == redis.Nil {
			return svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithOp("Repository.Redis.Update"),
				svcerror.WithMsg(fmt.Sprintf("resource with id %s not found", id)),
			)
		}
		return svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.Redis.Update"),
			svcerror.WithMsg("load resource for update"),
			svcerror.WithCause(err),
		)
	}

	data, err := json.Marshal(entity)
	if err != nil {
		return svcerror.New(
			svcerror.ErrInternalError,
			svcerror.WithOp("Repository.Redis.Update"),
			svcerror.WithMsg("encode resource"),
			svcerror.WithCause(err),
		)
	}
	if err := r.Client.Set(ctx, id, data, r.TTL).Err(); err != nil {
		return svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.Redis.Update"),
			svcerror.WithMsg("save resource"),
			svcerror.WithCause(err),
		)
	}
	return nil
}

func (r RedisCache[T]) Delete(ctx context.Context, id string) error {
	if _, err := r.Client.Del(ctx, id).Result(); err != nil {
		if err == redis.Nil {
			return svcerror.New(
				svcerror.ErrBusinessError,
				svcerror.WithOp("Repository.Redis.Delete"),
				svcerror.WithMsg(fmt.Sprintf("resource with id %s not found", id)),
			)
		}
		return svcerror.New(
			svcerror.ErrRepositoryError,
			svcerror.WithOp("Repository.Redis.Delete"),
			svcerror.WithMsg("delete resource"),
			svcerror.WithCause(err),
		)
	}
	return nil
}

func (r RedisCache[T]) List(ctx context.Context, filter any) ([]T, error) {
	return nil, svcerror.New(
		svcerror.ErrRepositoryError,
		svcerror.WithOp("Repository.Redis.List"),
		svcerror.WithMsg("List function is not supported on cache-only repository"),
	)
}
