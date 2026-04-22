package domain

import (
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type UserClaims struct {
	UserID uuid.UUID
	Email  string
	jwt.RegisteredClaims
}

type TokenManager interface {
	GenerateToken(userID uuid.UUID, email string) (string, error)
	ValidateToken(tokenStr string) (*UserClaims, error)
}
