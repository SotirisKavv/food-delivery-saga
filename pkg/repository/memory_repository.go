package repository

import (
	"context"
	"fmt"
	svcerror "food-delivery-saga/pkg/error"
	"sync"
)

const errFmtNotFound = "resource with id %s not found"

type MemoryRepository[T any] struct {
	MU    sync.RWMutex
	Items map[string]T
	IdFn  IDExtractor[T]
}

func NewMemoryRepo[T any](idFn IDExtractor[T]) *MemoryRepository[T] {
	return &MemoryRepository[T]{
		Items: make(map[string]T),
		IdFn:  idFn,
	}
}

func (r *MemoryRepository[T]) Load(ctx context.Context, id string) (T, error) {
	var zero T
	r.MU.RLock()
	defer r.MU.RUnlock()
	v, ok := r.Items[id]
	if !ok {
		return zero, svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Repository.Memory.Load"),
			svcerror.WithMsg(fmt.Sprintf(errFmtNotFound, id)),
		)
	}
	return v, nil
}

func (r *MemoryRepository[T]) Save(ctx context.Context, entity T) error {
	r.MU.Lock()
	defer r.MU.Unlock()
	r.Items[r.IdFn(entity)] = entity
	return nil
}

func (r *MemoryRepository[T]) Update(ctx context.Context, entity T) error {
	r.MU.Lock()
	defer r.MU.Unlock()
	id := r.IdFn(entity)
	if _, ok := r.Items[id]; !ok {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Repository.Memory.Update"),
			svcerror.WithMsg(fmt.Sprintf(errFmtNotFound, id)),
		)
	}
	r.Items[id] = entity
	return nil
}

func (r *MemoryRepository[T]) Delete(ctx context.Context, id string) error {
	r.MU.Lock()
	defer r.MU.Unlock()
	if _, ok := r.Items[id]; !ok {
		return svcerror.New(
			svcerror.ErrBusinessError,
			svcerror.WithOp("Repository.Memory.Delete"),
			svcerror.WithMsg(fmt.Sprintf(errFmtNotFound, id)),
		)
	}
	delete(r.Items, id)
	return nil
}

func (r *MemoryRepository[T]) List(ctx context.Context, filter any) ([]T, error) {
	r.MU.RLock()
	defer r.MU.RUnlock()
	itemsList := make([]T, 0, len(r.Items))
	for _, item := range r.Items {
		itemsList = append(itemsList, item)
	}
	return itemsList, nil
}
