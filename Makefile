COMPOSE_FILE := docker/docker-compose.yml

.PHONY: infra-up infra-down infra-status infra-logs infra-clean \
        kafka-topics kafka-describe psql \
        test test-unit test-integration install

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

infra-up:  ## Start Kafka + PostgreSQL (detached)
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for services to become healthy..."
	@docker compose -f $(COMPOSE_FILE) ps

infra-down:  ## Stop services (preserve data volumes)
	docker compose -f $(COMPOSE_FILE) down

infra-status:  ## Show current service health
	docker compose -f $(COMPOSE_FILE) ps

infra-logs:  ## Tail all service logs (Ctrl-C to exit)
	docker compose -f $(COMPOSE_FILE) logs -f

infra-clean:  ## Stop services AND delete all data volumes (irreversible)
	docker compose -f $(COMPOSE_FILE) down -v

# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

kafka-topics:  ## List all Kafka topics
	docker compose -f $(COMPOSE_FILE) exec kafka-1 \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list

kafka-describe:  ## Describe all topics (partitions, replicas, ISR)
	docker compose -f $(COMPOSE_FILE) exec kafka-1 \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe

# ---------------------------------------------------------------------------
# PostgreSQL helper
# ---------------------------------------------------------------------------

psql:  ## Open an interactive psql shell
	docker compose -f $(COMPOSE_FILE) exec postgres \
		psql -U featurestore -d feature_store

# ---------------------------------------------------------------------------
# Python / tests
# ---------------------------------------------------------------------------

install:  ## Install project + test dependencies via uv
	uv pip install -e ".[test]"

test-unit:  ## Run unit tests only (no Docker required)
	uv run pytest tests/unit/ -v --cov=src

test-integration:  ## Run integration tests (requires running infra)
	uv run pytest tests/integration/ -v -m integration -p no:xdist

test:  ## Run all tests
	uv run pytest tests/ -v --cov=streaming_feature_store --cov-report=term-missing
