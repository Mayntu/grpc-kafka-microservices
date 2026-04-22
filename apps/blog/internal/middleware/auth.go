package middleware

import (
	"context"
	"go-proj/pkg/gen/auth"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type contextKey string

const UserIdKey contextKey = "user_id"
const UserEmailKey contextKey = "user_email"

func EnsureAuthAvailable(conn *grpc.ClientConn) func(http.Handler) http.Handler {
	return func(nextHandler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			st := conn.GetState()

			if st == connectivity.TransientFailure || st == connectivity.Connecting || st == connectivity.Shutdown {
				w.Header().Set("Retry-After", "30")
				http.Error(w, "Auth service is temporaly unavailable. Please try again later.", http.StatusServiceUnavailable)
				return
			}
			nextHandler.ServeHTTP(w, r)
		})
	}
}

func AuthMiddleware(client auth.AuthServiceClient) func(http.Handler) http.Handler {
	return func(nextHandler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "Authorization header is not presented", http.StatusUnauthorized)
				return
			}

			parts := strings.Split(authHeader, " ")
			if len(parts) != 2 || parts[0] != "Bearer" {
				http.Error(w, "not valid auth header Bearer", http.StatusUnauthorized)
				return
			}

			resp, err := client.ValidateJwt(r.Context(), &auth.ValidateJWTRequest{Jwt: parts[1]})
			if err != nil || !resp.IsValid {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), UserIdKey, resp.UserId)
			nextHandler.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
