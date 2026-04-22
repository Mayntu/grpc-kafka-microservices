package domain

import (
	"auth/internal/domain/dto"
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

type UserRepository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	GetByID(ctx context.Context, id uuid.UUID) (*User, error)
	Create(ctx context.Context, user *User) (*User, error)
	// ?
	CreateWithinTx(ctx context.Context, tx pgx.Tx, user *User) (*User, error)
	GetByEmail(ctx context.Context, email string) (*User, error)
}

type UserUsecase interface {
	Register(ctx context.Context, email, password string) (uuid.UUID, error)
	Login(ctx context.Context, email, password string) (string, error)
	ValidateToken(ctx context.Context, tokenStr string) (*dto.ValidateTokenResponse, error)
}
