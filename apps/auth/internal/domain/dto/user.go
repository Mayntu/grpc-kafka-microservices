package dto

import "github.com/google/uuid"

type HTTPAuthRequest struct {
	Email    string
	Password string
}

type ValidateTokenResponse struct {
	IsValid bool
	Id      uuid.UUID
	Email   string
}
