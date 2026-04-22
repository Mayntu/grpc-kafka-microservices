package usecase

import (
	"auth/internal/domain"
	"auth/internal/domain/dto"
	"auth/internal/outbox"
	"context"
	"fmt"
	"go-proj/pkg/events"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type userUsecase struct {
	repo         domain.UserRepository
	outboxRepo   domain.OutboxRepository
	tokenManager domain.TokenManager
}

func NewUserUsecase(repo domain.UserRepository, outboxRepo domain.OutboxRepository, tm domain.TokenManager) domain.UserUsecase {
	return &userUsecase{
		repo:         repo,
		outboxRepo:   outboxRepo,
		tokenManager: tm,
	}
}

func (u *userUsecase) Register(ctx context.Context, email, password string) (uuid.UUID, error) {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	if err != nil {
		return uuid.Nil, fmt.Errorf("usecase: failed to hash password: %w", err)
	}

	user := &domain.User{
		ID:           uuid.New(),
		Email:        email,
		PasswordHash: string(passwordHash),
	}

	tx, err := u.repo.BeginTx(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("register: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	user, err = u.repo.CreateWithinTx(ctx, tx, user)
	if err != nil {
		return uuid.Nil, fmt.Errorf("register: create user: %w", err)
	}

	payload, err := outbox.BuildUserCreatedPayload(user.ID.String(), user.Email)
	if err != nil {
		return uuid.Nil, fmt.Errorf("register: build payload: %w", err)
	}

	outboxEvent := &domain.OutboxEvent{
		EventType: string(events.EventTypeUserCreated),
		Payload:   payload,
		Status:    domain.OutboxStatusPending,
	}
	err = u.outboxRepo.CreateWithinTx(ctx, tx, outboxEvent)
	if err != nil {
		return uuid.Nil, fmt.Errorf("register: create outbox event: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("register: transaction commit: %w", err)
	}
	return user.ID, nil
}

func (u *userUsecase) Login(ctx context.Context, email, password string) (string, error) {
	user, err := u.repo.GetByEmail(ctx, email)
	if err != nil {
		return "", err
	}

	if !checkPassword(password, user.PasswordHash) {
		return "", domain.ErrInvalidCredentials
	}
	token, err := u.tokenManager.GenerateToken(user.ID, email)
	if err != nil {
		return "", fmt.Errorf("usecase: failed to generate token: %w", err)
	}
	return token, nil
}

func (u *userUsecase) ValidateToken(ctx context.Context, tokenStr string) (*dto.ValidateTokenResponse, error) {
	userClaims, err := u.tokenManager.ValidateToken(tokenStr)
	if err != nil {
		return nil, err
	}
	return &dto.ValidateTokenResponse{
		IsValid: true,
		Id:      userClaims.UserID,
		Email:   userClaims.Email,
	}, nil
}

func checkPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}
