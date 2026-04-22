package postgres

import (
	"auth/internal/domain"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type userRepository struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) domain.UserRepository {
	return &userRepository{
		db: db,
	}
}

func (repo *userRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := repo.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("user repo: begin transaction: %w", err)
	}
	return tx, nil
}

func (repo *userRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	query := "SELECT id, email, password_hash, created_at FROM users WHERE id = $1"

	var u = domain.User{}
	err := repo.db.QueryRow(ctx, query, id).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

func (repo *userRepository) Create(ctx context.Context, user *domain.User) (*domain.User, error) {
	return repo.create(ctx, repo.db, user)
}

func (repo *userRepository) CreateWithinTx(ctx context.Context, tx pgx.Tx, user *domain.User) (*domain.User, error) {
	return repo.create(ctx, tx, user)
}

func (repo *userRepository) create(ctx context.Context, q pgxQuerier, user *domain.User) (*domain.User, error) {
	query := "INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3) RETURNING created_at"
	err := q.QueryRow(ctx, query, user.ID, user.Email, user.PasswordHash).Scan(&user.CreatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "unique constraint") {
			return nil, domain.ErrUserAlreadyExists
		}
		return nil, fmt.Errorf("repo: failed to create user: %w", err)
	}

	return user, nil
}

func (repo *userRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	query := "SELECT id, email, password_hash, created_at FROM users WHERE email = $1"

	var user = domain.User{}
	err := repo.db.QueryRow(ctx, query, email).Scan(&user.ID, &user.Email, &user.PasswordHash, &user.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("repo: failed to get user by email: %w", err)
	}
	return &user, nil
}

type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
