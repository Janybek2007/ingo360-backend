MODE ?= dev
ENV_FILE ?= .env

COMPOSE_FILE = docker/compose.$(MODE).yml
COMPOSE_FILE_DEV = docker/compose.dev.yml
COMPOSE_FILE_PROD = docker/compose.prod.yml

.PHONY: build_backend up_dev down_dev up_prod down_prod

build_backend:
	docker build -f docker/Dockerfile.$(MODE) -t ingo360-backend:$(MODE) .

d_restart:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) restart

d_start:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) start

d_stop:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) stop

up_dev:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) up -d

down_dev:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) down

up_prod:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) up -d

down_prod:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) down
