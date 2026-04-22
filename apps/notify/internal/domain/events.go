package domain

import "context"

type EventHandler interface {
	HandleUserCreated(ctx context.Context, email string) error
}
