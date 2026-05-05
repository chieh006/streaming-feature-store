COMPOSE_FILE := docker/docker-compose.yml

.PHONY: infra-up infra-down infra-status infra-logs infra-clean \
        kafka-topics kafka-describe psql \
        schema-subjects schema-compat \
        register-schemas register-schemas-dry produce-sample \
        schema-evolution schema-evolution-snapshot schema-evolution-clean \
        schema-evolution-report \
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
# Schema Registry helpers
# ---------------------------------------------------------------------------

schema-subjects:  ## List registered Schema Registry subjects
	@curl -fsS http://localhost:8081/subjects | jq .

schema-compat:  ## Show Schema Registry default compatibility level
	@curl -fsS http://localhost:8081/config | jq .

register-schemas:  ## Register all .avsc files under schemas/ with the Registry
	uv run python scripts/register_schemas.py

register-schemas-dry:  ## Show what would be registered without writing
	uv run python scripts/register_schemas.py --dry-run

produce-sample:  ## Send a handful of sample events end-to-end
	uv run python -m streaming_feature_store.producer.avro_producer --sample 5

# ---------------------------------------------------------------------------
# Schema-evolution drills (Week 1 — BACKWARD compatibility)
# ---------------------------------------------------------------------------

schema-evolution:  ## Run all 3 schema-evolution drills end-to-end (requires infra)
	uv run python scripts/run_schema_evolution.py --drill all

schema-evolution-snapshot:  ## Generate v1.x/ on disk without contacting the Registry
	uv run python scripts/run_schema_evolution.py --drill all --snapshot-only \
		--report-path /tmp/_schema_evolution_snapshot_only.md

schema-evolution-clean:  ## Soft-delete experiment versions; keep baseline v1
	uv run python scripts/run_schema_evolution.py --drill all \
		--report-path /tmp/_schema_evolution_clean.md

schema-evolution-report:  ## Open the generated report
	@xdg-open docs/results/week1_schema_evolution_results.md 2>/dev/null \
	  || open docs/results/week1_schema_evolution_results.md 2>/dev/null \
	  || echo "Report at docs/results/week1_schema_evolution_results.md"

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
