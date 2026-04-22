-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (
    id varchar(64) PRIMARY KEY,
    email varchar(128) NOT NULL,
    password_hash varchar(512) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS users;
-- +goose StatementEnd
