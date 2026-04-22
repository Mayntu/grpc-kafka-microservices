package http

import (
	"auth/internal/domain"
	"auth/internal/domain/dto"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type UserHTTPHandler struct {
	usecase domain.UserUsecase
}

func NewHandler(usecase domain.UserUsecase) *UserHTTPHandler {
	return &UserHTTPHandler{
		usecase: usecase,
	}
}

func (h *UserHTTPHandler) InitRoutes() *chi.Mux {
	r := chi.NewRouter()

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/register", h.Register)
		r.Post("/login", h.Login)
	})

	return r
}

func (h *UserHTTPHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req dto.HTTPAuthRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "not valid request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	id, err := h.usecase.Register(r.Context(), req.Email, req.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]uuid.UUID{"id": id})
}

func (h *UserHTTPHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req dto.HTTPAuthRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "not valid request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	jwt, err := h.usecase.Login(r.Context(), req.Email, req.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"jwt": jwt})
}
