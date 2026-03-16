# CareerOS — Developer Makefile
#
# Usage:
#   make up        — start everything (first run: builds images, runs migrations)
#   make down      — stop everything
#   make logs      — tail all logs
#   make shell     — bash inside the API container
#   make test      — run the test suite
#   make migrate   — run pending migrations
#
# All docker compose commands use the docker/docker-compose.yml file.

COMPOSE = docker compose -f docker/docker-compose.yml
COMPOSE_PROD = $(COMPOSE) -f docker/docker-compose.prod.yml

.PHONY: help up down restart logs shell test migrate makemigration \
        clean psql redis-cli flower build lint

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "CareerOS — available targets:"
	@echo ""
	@echo "  make up              Start full dev stack (builds if needed)"
	@echo "  make down            Stop all containers"
	@echo "  make restart         Restart all containers"
	@echo "  make build           Rebuild all images"
	@echo "  make logs            Tail all container logs"
	@echo "  make logs s=api      Tail logs for specific service"
	@echo ""
	@echo "  make migrate         Run pending Alembic migrations"
	@echo "  make makemigration m='description'  Auto-generate migration"
	@echo ""
	@echo "  make test            Run full test suite"
	@echo "  make lint            Run ruff linter"
	@echo ""
	@echo "  make shell           Bash inside API container"
	@echo "  make psql            psql session in postgres container"
	@echo "  make redis-cli       redis-cli in redis container"
	@echo "  make flower          Open Flower UI (localhost:5555)"
	@echo ""
	@echo "  make clean           Remove all containers + volumes (destructive!)"
	@echo ""


# ── Core lifecycle ────────────────────────────────────────────────────────────

up:
	@echo "→ Copying .env.example to .env if .env doesn't exist..."
	@test -f .env || (cp .env.example .env && echo "  Created .env — fill in your API keys before continuing." && exit 1)
	@echo "→ Starting CareerOS dev stack..."
	$(COMPOSE) up --build -d
	@echo ""
	@echo "✓ CareerOS is starting up."
	@echo "  API:      http://localhost:8000"
	@echo "  Docs:     http://localhost:8000/docs"
	@echo "  MinIO:    http://localhost:9001  (admin / minioadmin)"
	@echo "  Flower:   http://localhost:5555  (admin / careeros)"
	@echo ""
	@echo "  Watch logs:  make logs"
	@echo "  API shell:   make shell"

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart $(s)

build:
	$(COMPOSE) build --no-cache

logs:
	$(COMPOSE) logs -f --tail=100 $(s)

stop:
	$(COMPOSE) stop $(s)


# ── Database ──────────────────────────────────────────────────────────────────

migrate:
	@echo "→ Running Alembic migrations..."
	$(COMPOSE) run --rm migrator alembic upgrade head
	@echo "✓ Migrations complete."

makemigration:
	@test -n "$(m)" || (echo "Usage: make makemigration m='your description'" && exit 1)
	$(COMPOSE) run --rm api alembic revision --autogenerate -m "$(m)"
	@echo "✓ Migration created in app/db/migrations/versions/"

rollback:
	$(COMPOSE) run --rm migrator alembic downgrade -1

psql:
	$(COMPOSE) exec postgres psql -U careeros -d careeros


# ── Debugging ─────────────────────────────────────────────────────────────────

shell:
	$(COMPOSE) exec api bash

redis-cli:
	$(COMPOSE) exec redis redis-cli

flower:
	@echo "→ Opening Flower at http://localhost:5555 (admin / careeros)"
	@open http://localhost:5555 || xdg-open http://localhost:5555 || echo "Open http://localhost:5555 in your browser"


# ── Testing ───────────────────────────────────────────────────────────────────

test:
	$(COMPOSE) run --rm api pytest tests/ -v --tb=short

test-fast:
	$(COMPOSE) run --rm api pytest tests/ -v --tb=short -x

lint:
	$(COMPOSE) run --rm api ruff check app/ tests/

lint-fix:
	$(COMPOSE) run --rm api ruff check --fix app/ tests/


# ── Production ────────────────────────────────────────────────────────────────

prod-up:
	$(COMPOSE_PROD) up -d

prod-down:
	$(COMPOSE_PROD) down

prod-logs:
	$(COMPOSE_PROD) logs -f --tail=100 $(s)


# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	@echo "⚠️  This will delete ALL containers, volumes, and data."
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ] || exit 1
	$(COMPOSE) down -v --remove-orphans
	docker image prune -f
	@echo "✓ Clean complete."
