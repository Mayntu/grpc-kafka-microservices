package events

import "time"

const TopicAuthEvents = "topic.auth.events"
const TopicAuthEventsDLQ = "topic.auth.events.dlq"

type EventType string

const (
	EventTypeUserCreated EventType = "user.created"
)

type UserCreatedEvent struct {
	EventID   string    `json:"event_id"`
	EventType EventType `json:"event_type"`
	UserID    string    `json:"user_id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}
