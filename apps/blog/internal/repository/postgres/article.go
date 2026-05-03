package postgres

import (
	"blog/internal/domain"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type articleRepository struct {
	db *pgxpool.Pool
}

func NewArticleRepository(db *pgxpool.Pool) domain.ArticleRepository {
	return &articleRepository{
		db: db,
	}
}

type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func (r *articleRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("article repo: failed to begin transaction: %w", err)
	}
	return tx, nil
}

func (r *articleRepository) CreateWithinTx(ctx context.Context, tx pgx.Tx, article *domain.Article) (*domain.Article, error) {
	return r.create(ctx, tx, article)
}

func (r *articleRepository) Create(ctx context.Context, article *domain.Article) (*domain.Article, error) {
	return r.create(ctx, r.db, article)
}

func (r *articleRepository) create(ctx context.Context, q pgxQuerier, article *domain.Article) (*domain.Article, error) {
	query := `
		INSERT INTO articles (title, author, content, author_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`

	now := time.Now()

	err := q.QueryRow(ctx, query, article.Title, article.Author, article.Content, article.AuthorID, now, now).Scan(&article.ID, &article.CreatedAt, &article.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("article repo: failed to create article: %w", err)
	}

	return article, nil
}

func (r *articleRepository) GetAll(ctx context.Context) ([]domain.Article, error) {
	query := "SELECT id, title, author, content, author_id, created_at, updated_at FROM articles"

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("repo: get all articles failed %w", err)
	}

	defer rows.Close()

	articles := make([]domain.Article, 0)
	for rows.Next() {
		var a domain.Article
		err := rows.Scan(&a.ID, &a.Title, &a.Author, &a.Content, &a.AuthorID, &a.CreatedAt, &a.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("repo: scan article: %w", err)
		}
		articles = append(articles, a)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("repo: rows error: %w", err)
	}
	return articles, nil
}

func (r *articleRepository) GetByID(ctx context.Context, id int64) (*domain.Article, error) {
	query := "SELECT id, title, author, content, author_id, created_at, updated_at FROM articles WHERE id = $1"

	var a domain.Article
	err := r.db.QueryRow(ctx, query, id).Scan(&a.ID, &a.Title, &a.Author, &a.Content, &a.AuthorID, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("repo: get article by id: %w", err)
	}
	return &a, nil
}

func (r *articleRepository) Update(ctx context.Context, article *domain.Article) (*domain.Article, error) {
	query := "UPDATE articles SET title = $1, content = $2, updated_at = $3 WHERE id = $4"

	now := time.Now()
	tag, err := r.db.Exec(ctx, query, article.Title, article.Content, now, article.ID)
	if err != nil {
		return nil, fmt.Errorf("repo: update article: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return nil, domain.ErrNotFound
	}

	article.UpdatedAt = now
	return article, nil
}

func (r *articleRepository) Delete(ctx context.Context, id int64) error {
	query := "DELETE FROM articles WHERE id = $1"

	tag, err := r.db.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("repo: delete article: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}

	return nil
}
