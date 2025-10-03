package repository

import (
	"fmt"
)

type IDExtractor[T any] func(T) string

type Repository[T any] interface {
	Load(id string) (T, error)
	Save(entity T) error
	Update(entity T) error
	Delete(id string) error
	List(filter any) ([]T, error)
}

type RepositoryType string

const (
	RepositoryMemory RepositoryType = "memory"
)

func NewRepository[T any](repoType RepositoryType, idExtractor IDExtractor[T], opts ...any) (Repository[T], error) {
	var repo Repository[T]

	switch repoType {
	case RepositoryMemory:
		repo = NewMemoryRepo(idExtractor)
	default:
		return nil, fmt.Errorf("Failed to create Repository Handler: Unsupported Repository Type")
	}

	return repo, nil
}
