ENV_FILE ?= .env

COMPOSE_FILE_DEV = docker/compose.dev.yml
COMPOSE_FILE_PROD = docker/compose.prod.yml

.PHONY: prod_build_backend dev_build_backend prod_restart prod_start prod_stop dev_restart dev_start dev_stop up_dev down_dev up_prod down_prod

prod_build_backend:
	docker build -f docker/Dockerfile.prod -t ingo360-backend:prod .

dev_build_backend:
	docker build -f docker/Dockerfile.dev -t ingo360-backend:dev .

prod_restart:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) restart

prod_start:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) start

prod_stop:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) stop

dev_restart:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) restart

dev_start:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) start

dev_stop:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) stop


up_dev:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) up -d

down_dev:
	docker compose -f $(COMPOSE_FILE_DEV) --env-file $(ENV_FILE) down

up_prod:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) up -d

down_prod:
	docker compose -f $(COMPOSE_FILE_PROD) --env-file $(ENV_FILE) down
