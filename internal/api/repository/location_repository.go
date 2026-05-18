package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/mehmet-ozkan/go-distributed-geofencing/internal/api/model"
)

type ILocationRepository interface {
	Create(ctx context.Context, loc *model.Location) error
}

type locationRepository struct {
	db *gorm.DB
}

func NewLocationRepository(db *gorm.DB) ILocationRepository {
	return &locationRepository{db: db}
}

func (r *locationRepository) Create(ctx context.Context, loc *model.Location) error {
	if err := r.db.WithContext(ctx).Create(loc).Error; err != nil {
		return fmt.Errorf("locationRepository.Create: %w", err)
	}
	return nil
}
