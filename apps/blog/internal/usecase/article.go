package usecase

import (
	"blog/internal/domain"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type articleUsecase struct {
	repo domain.ArticleRepository
}

func NewArticleUsecase(repo domain.ArticleRepository) domain.ArticleUsecase {
	return &articleUsecase{
		repo: repo,
	}
}

func (u *articleUsecase) Create(ctx context.Context, title, content string, authorID uuid.UUID) (*domain.Article, error) {
	if title == "" {
		return nil, errors.New("title is required")
	}
	if len(title) < 3 {
		return nil, errors.New("title too short")
	}

	article := domain.Article{
		Title:    title,
		Content:  content,
		AuthorID: authorID,
		Author:   "System",
	}
	createdArticle, err := u.repo.Create(ctx, &article)
	if err != nil {
		return nil, fmt.Errorf("usecase: create article: %w", err)
	}
	return createdArticle, nil
}

func (u *articleUsecase) GetByID(ctx context.Context, id int64) (*domain.Article, error) {
	article, err := u.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("usecase: get article: %w", err)
	}

	return article, nil
}
