# Запуск инфраструктуры для разработки
infra-up:
	docker compose -f docker-compose.infra.yml up -d

infra-down:
	docker compose -f docker-compose.infra.yml down

# Запуск auth сервиса локально
run:
	go run ./cmd

# Миграции
migrate:
	goose -dir migrations postgres "$(DATABASE_URL)" up