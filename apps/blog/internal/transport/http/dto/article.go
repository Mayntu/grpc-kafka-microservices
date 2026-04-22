package dto

import "github.com/google/uuid"

type CreateRequest struct {
	Title    string `json:"title" validate:"required"`
	Content  string `json:"content" validate:"required"`
	AuthorID uuid.UUID  `json:"author_id" validate:"required"`
}
