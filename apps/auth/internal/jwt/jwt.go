package jwt

import (
	"auth/internal/domain"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type tokenManager struct {
	secretKey string
	tokenTTL  time.Duration
}

func NewTokenManager(secretKey string, ttl time.Duration) domain.TokenManager {
	return &tokenManager{
		secretKey: secretKey,
		tokenTTL:  ttl,
	}
}

func (m *tokenManager) GenerateToken(userID uuid.UUID, email string) (string, error) {
	claims := domain.UserClaims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.tokenTTL)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		claims,
	)
	return token.SignedString([]byte(m.secretKey))
}

func (m *tokenManager) ValidateToken(tokenStr string) (*domain.UserClaims, error) {
	token, err := jwt.ParseWithClaims(
		tokenStr,
		&domain.UserClaims{},
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(m.secretKey), nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if claims, ok := token.Claims.(*domain.UserClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}
