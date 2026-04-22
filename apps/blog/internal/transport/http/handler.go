package http

import (
	"blog/internal/domain"
	"blog/internal/middleware"
	"blog/internal/transport/http/dto"
	"encoding/json"
	"errors"
	"fmt"
	"go-proj/pkg/gen/auth"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
)

type Handler struct {
	usecase    domain.ArticleUsecase
	authClient auth.AuthServiceClient
}

func NewHandler(usecase domain.ArticleUsecase, authClient auth.AuthServiceClient) *Handler {
	return &Handler{
		usecase:    usecase,
		authClient: authClient,
	}
}

func (h *Handler) InitRoutes(conn *grpc.ClientConn) *chi.Mux {
	r := chi.NewRouter()

	r.Route("/api/v1", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(middleware.EnsureAuthAvailable(conn))
			r.Use(middleware.AuthMiddleware(h.authClient))
			r.Post("/articles", h.CreateArticle)
			r.Get("/articles/{id}", h.GetArticle)
		})
	})

	return r
}

func (h *Handler) CreateArticle(w http.ResponseWriter, r *http.Request) {
	var req dto.CreateRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	article, err := h.usecase.Create(r.Context(), req.Title, req.Content, req.AuthorID)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(article)
}

func (h *Handler) GetArticle(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	article, err := h.usecase.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			http.Error(w, fmt.Sprintf("%d not found", id), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(article)
}
