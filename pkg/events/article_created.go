package events

import "time"

const TopicBlogEvents = "topic.blog.events"
const TopicBlogEventsDLQ = "topic.blog.events.dlq"

const (
	EventTypeArticleCreated EventType = "article.created"
)

type ArticleCreatedEvent struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	UserID    string    `json:"user_id"`
	ArticleID int64     `json:"article_id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
}
