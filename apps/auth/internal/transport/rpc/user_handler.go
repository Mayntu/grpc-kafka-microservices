package rpc

import (
	"auth/internal/domain"
	"context"
	"go-proj/pkg/gen/auth"
)

type UserRPCHandler struct {
	usecase domain.UserUsecase
	auth.UnimplementedAuthServiceServer
}

func NewUserRPCHandler(usecase domain.UserUsecase) *UserRPCHandler {
	return &UserRPCHandler{
		usecase: usecase,
	}
}

func (h *UserRPCHandler) ValidateJwt(ctx context.Context, req *auth.ValidateJWTRequest) (*auth.ValidateJWTResponse, error) {
	res, err := h.usecase.ValidateToken(ctx, req.GetJwt())
	if err != nil {
		return nil, err
	}
	return &auth.ValidateJWTResponse{
		IsValid: res.IsValid,
		UserId:  res.Id.String(),
		Email:   res.Email,
	}, nil
}
