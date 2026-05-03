package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Article struct {
	ID        int64
	Title     string
	Author    string
	Content   string
	AuthorID  uuid.UUID
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ArticleRepository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	CreateWithinTx(ctx context.Context, tx pgx.Tx, article *Article) (*Article, error)
	Create(ctx context.Context, article *Article) (*Article, error)
	GetAll(ctx context.Context) ([]Article, error)
	GetByID(ctx context.Context, id int64) (*Article, error)
	Update(ctx context.Context, article *Article) (*Article, error)
	Delete(ctx context.Context, id int64) error
}

type ArticleUsecase interface {
	Create(ctx context.Context, title, content string, authorID uuid.UUID) (*Article, error)
	GetByID(ctx context.Context, id int64) (*Article, error)
}
