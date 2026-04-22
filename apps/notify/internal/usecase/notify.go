package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"notify/internal/domain"
)

type notifyUsecase struct {
	logger *slog.Logger
}

func NewNotifyUsecase(logger *slog.Logger) domain.EventHandler {
	return &notifyUsecase{
		logger: logger,
	}
}

func (u *notifyUsecase) HandleUserCreated(ctx context.Context, email string) error {
	if email == "" {
		return fmt.Errorf("notify usecase: email empty")
	}

	u.logger.Info("Welcome email sent to user", "email", email)
	return nil
}
