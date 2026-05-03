package usecase

import (
	"blog/internal/domain"
	"blog/internal/worker"
	"context"
	"errors"
	"fmt"
	"go-proj/pkg/events"

	"github.com/google/uuid"
)

type articleUsecase struct {
	repo       domain.ArticleRepository
	outboxRepo domain.OutboxRepo
}

func NewArticleUsecase(repo domain.ArticleRepository, outboxRepo domain.OutboxRepo) domain.ArticleUsecase {
	return &articleUsecase{
		repo:       repo,
		outboxRepo: outboxRepo,
	}
}

func (u *articleUsecase) Create(ctx context.Context, title, content string, authorID uuid.UUID) (*domain.Article, error) {
	if title == "" {
		return nil, errors.New("title is required")
	}
	if len(title) < 3 {
		return nil, errors.New("title too short")
	}

	tx, err := u.repo.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	article := domain.Article{
		Title:    title,
		Content:  content,
		AuthorID: authorID,
		Author:   "System",
	}
	createdArticle, err := u.repo.CreateWithinTx(ctx, tx, &article)
	if err != nil {
		return nil, fmt.Errorf("usecase: create article: %w", err)
	}

	outboxEventID := uuid.NewString()

	payload, err := worker.BuildArticleCreatedPayload(outboxEventID, createdArticle.AuthorID.String(), createdArticle.Title, createdArticle.ID, createdArticle.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("article usecase: build outbox payload: %w", err)
	}

	outboxEvent := &domain.OutboxEvent{
		ID:        outboxEventID,
		EventType: events.EventTypeArticleCreated,
		Payload:   payload,
		Status:    domain.Pending,
	}
	_, err = u.outboxRepo.CreateWithinTx(ctx, tx, outboxEvent)
	if err != nil {
		return nil, fmt.Errorf("article usecase: failed to create outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("article usecase: failed to commit transaction: %w", err)
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
