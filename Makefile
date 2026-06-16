COMPOSE_FILE := docker/docker-compose.yml
PIDS_DIR := .pids

.PHONY: infra-up infra-down infra-status infra-logs infra-clean \
        kafka-topics kafka-describe psql \
        schema-subjects schema-compat \
        register-schemas register-schemas-dry register-schemas-feed produce-sample \
        schema-evolution schema-evolution-snapshot schema-evolution-clean \
        schema-evolution-report \
        topic-ensure topic-describe \
        load-test load-test-quick load-test-report test-benchmark \
        load-test-mp load-test-mp-quick load-test-mp-report load-test-mp-eos \
        consume-test consume-test-mp consume-test-mp-quick consume-test-report \
        sink-run feeder-run pipeline-up pipeline-down sink-report \
        validator-run validator-run-mp validator-up validator-down \
        validator-report validator-run-eos validator-run-mp-eos eos-verify eos-report \
        redis-cli \
        sliding-run sliding-run-group sliding-run-eos sliding-report sliding-redis-show \
        serving-run serving-run-group serving-smoke \
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

register-schemas-feed:  ## Register the composite schema under 'e-commerce-events-feed-value' (required for the feeder / sink / validator)
	uv run python scripts/register_schemas.py --subject e-commerce-events-feed-value

print-schema:  ## Print the full assembled Avro schema JSON to stdout
	uv run python scripts/register_schemas.py --print-schema

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

# ---------------------------------------------------------------------------
# Topic admin & load-test (Week 1 — synthetic event producer)
# ---------------------------------------------------------------------------

topic-ensure:  ## Idempotently create e-commerce-events (12p, RF=3)
	uv run python -m streaming_feature_store.admin.topic_admin ensure

topic-describe:  ## Print partition assignment for the configured topic
	uv run python -m streaming_feature_store.admin.topic_admin describe

load-test:  ## Run a 10s, 60K evt/s load test and write the report
	uv run python scripts/run_event_load.py --duration-s 10 --target-rate 60000 \ 

load-test-quick:  ## Smoke run: 2s, 5K evt/s, no rate floor enforcement
	uv run python scripts/run_event_load.py --duration-s 2 --target-rate 5000 \
	  --report-path /tmp/_load_quick.md --floor-eps 0

load-test-report:  ## Open the generated report
	@xdg-open docs/results/week1_load_test_results.md 2>/dev/null \
	  || open docs/results/week1_load_test_results.md 2>/dev/null \
	  || echo "Report at docs/results/week1_load_test_results.md"

test-benchmark:  ## Run the 10s/50K-floor benchmark integration test explicitly
	uv run pytest tests/integration/test_load_runner_end_to_end.py \
	  -v -m benchmark -p no:xdist

# ---------------------------------------------------------------------------
# Multi-process load test (Week 1 — GIL-escape harness)
# ---------------------------------------------------------------------------

load-test-mp:  ## Run a 10s, 60K evt/s multi-process load test and write the report
	uv run python scripts/run_event_load_mp.py --duration-s 10 --target-rate 60000

load-test-mp-eos:  ## Run the 10s, 60K evt/s load test with the EOS profile (idempotent, acks=all)
	uv run python scripts/run_event_load_mp.py --duration-s 10 --target-rate 60000 \
	  --eos --report-path docs/results/week1_load_test_results_mp_eos.md

load-test-mp-quick:  ## Smoke run: 2s, 5K evt/s aggregate, 2 processes, no floor
	uv run python scripts/run_event_load_mp.py --duration-s 2 --target-rate 5000 \
	  --processes 2 --workers-per-process 2 \
	  --report-path /tmp/_load_mp_quick.md --floor-eps 0

load-test-mp-report:  ## Open the multi-process report
	@xdg-open docs/results/week1_load_test_results_mp.md 2>/dev/null \
	  || open docs/results/week1_load_test_results_mp.md 2>/dev/null \
	  || echo "Report at docs/results/week1_load_test_results_mp.md"

# ---------------------------------------------------------------------------
# Multi-process consumer group (Week 1 — symmetric GIL ceiling, consume side)
# ---------------------------------------------------------------------------

consume-test:  ## 1-member consumer (control: shows the single-process GIL ceiling)
	uv run python scripts/run_event_consume_mp.py --duration-s 10 --members 1

consume-test-mp:  ## N-member consumer group (planned; drains the producer)
	uv run python scripts/run_event_consume_mp.py --duration-s 10

consume-test-mp-quick:  ## Smoke: 2s, 1 member, no verdict
	uv run python scripts/run_event_consume_mp.py --duration-s 2 --members 1 \
	  --report-path /tmp/_consume_quick.md

consume-test-report:  ## Open the generated consume report
	@xdg-open docs/results/week1_consume_results_mp.md 2>/dev/null \
	  || open docs/results/week1_consume_results_mp.md 2>/dev/null \
	  || echo "Report at docs/results/week1_consume_results_mp.md"

# ---------------------------------------------------------------------------
# Continuous pipeline (Week 1 — Kafka-to-Postgres sink + background feeder)
# ---------------------------------------------------------------------------

feeder-run:  ## Start the low-rate continuous feeder (200 evt/s default) in foreground
	uv run python scripts/run_background_feeder.py

sink-run:  ## Start the Kafka-to-Postgres sink consumer in foreground
	uv run python scripts/run_postgres_sink.py

pipeline-up:  ## Daemonize feeder + sink, write PIDs to $(PIDS_DIR)/
	@mkdir -p $(PIDS_DIR)
	@nohup uv run python scripts/run_background_feeder.py \
	  > $(PIDS_DIR)/feeder.log 2>&1 & echo $$! > $(PIDS_DIR)/feeder.pid
	@nohup uv run python scripts/run_postgres_sink.py \
	  > $(PIDS_DIR)/sink.log 2>&1 & echo $$! > $(PIDS_DIR)/sink.pid
	@echo "feeder PID: $$(cat $(PIDS_DIR)/feeder.pid)"
	@echo "sink   PID: $$(cat $(PIDS_DIR)/sink.pid)"
	@echo "logs in $(PIDS_DIR)/"

pipeline-down:  ## SIGTERM feeder + sink and wait for graceful shutdown
	@if [ -f $(PIDS_DIR)/feeder.pid ]; then \
	  kill -TERM $$(cat $(PIDS_DIR)/feeder.pid) 2>/dev/null || true; \
	  rm -f $(PIDS_DIR)/feeder.pid; \
	fi
	@if [ -f $(PIDS_DIR)/sink.pid ]; then \
	  kill -TERM $$(cat $(PIDS_DIR)/sink.pid) 2>/dev/null || true; \
	  rm -f $(PIDS_DIR)/sink.pid; \
	fi
	@echo "Sent SIGTERM to feeder + sink (allow ~10 s for clean shutdown)."

sink-report:  ## Open the latest sink-run report
	@xdg-open docs/results/week1_postgres_sink_results.md 2>/dev/null \
	  || open docs/results/week1_postgres_sink_results.md 2>/dev/null \
	  || echo "Report at docs/results/week1_postgres_sink_results.md"

# ---------------------------------------------------------------------------
# Inline validator + DLQ router (Week 2 — validation stage)
# ---------------------------------------------------------------------------

validator-run:  ## Start the single-process validator daemon in foreground
	uv run python scripts/run_validator.py

validator-run-mp:  ## Multi-process validator group. EOS=1 exactly-once; N=<procs> member count (default: auto-planned); SOURCE=feed|bench source mode (default: bench)
	uv run python scripts/run_validator_mp.py \
		$(if $(filter 1,$(EOS)),--eos,) $(if $(N),--procs $(N),) $(if $(SOURCE),--source $(SOURCE),)

validator-up:  ## Daemonize the validator alongside feeder + sink
	@mkdir -p $(PIDS_DIR)
	@nohup uv run python scripts/run_background_feeder.py \
	  > $(PIDS_DIR)/feeder.log 2>&1 & echo $$! > $(PIDS_DIR)/feeder.pid
	@nohup uv run python scripts/run_postgres_sink.py \
	  > $(PIDS_DIR)/sink.log 2>&1 & echo $$! > $(PIDS_DIR)/sink.pid
	@nohup uv run python scripts/run_validator.py \
	  > $(PIDS_DIR)/validator.log 2>&1 & echo $$! > $(PIDS_DIR)/validator.pid
	@echo "feeder    PID: $$(cat $(PIDS_DIR)/feeder.pid)"
	@echo "sink      PID: $$(cat $(PIDS_DIR)/sink.pid)"
	@echo "validator PID: $$(cat $(PIDS_DIR)/validator.pid)"
	@echo "logs in $(PIDS_DIR)/"

validator-down:  ## SIGTERM the validator + feeder + sink trio
	@if [ -f $(PIDS_DIR)/validator.pid ]; then \
	  kill -TERM $$(cat $(PIDS_DIR)/validator.pid) 2>/dev/null || true; \
	  rm -f $(PIDS_DIR)/validator.pid; \
	fi
	@if [ -f $(PIDS_DIR)/feeder.pid ]; then \
	  kill -TERM $$(cat $(PIDS_DIR)/feeder.pid) 2>/dev/null || true; \
	  rm -f $(PIDS_DIR)/feeder.pid; \
	fi
	@if [ -f $(PIDS_DIR)/sink.pid ]; then \
	  kill -TERM $$(cat $(PIDS_DIR)/sink.pid) 2>/dev/null || true; \
	  rm -f $(PIDS_DIR)/sink.pid; \
	fi
	@echo "Sent SIGTERM to validator + feeder + sink (allow ~10 s for clean shutdown)."

validator-report:  ## Open the latest validator-run report
	@xdg-open docs/results/week2_validator_results.md 2>/dev/null \
	  || open docs/results/week2_validator_results.md 2>/dev/null \
	  || echo "Report at docs/results/week2_validator_results.md"

# ---------------------------------------------------------------------------
# Exactly-once semantics — transactional consume-process-produce (Week 2 PR #3)
# See docs/design/week2_03_exactly_once_transactions.md
# ---------------------------------------------------------------------------

validator-run-eos:  ## Run the validator with transactional EOS (validated+dlq+offsets atomic)
	uv run python scripts/run_validator.py --eos \
		--bootstrap localhost:19092,localhost:19093,localhost:19094 \
		--registry http://localhost:8081 \
		--report-path docs/results/week2_eos_results.md

validator-run-mp-eos:  ## Alias for `validator-run-mp EOS=1` (per-member transactional.id, §2.3); pass N=<procs> to set member count
	@$(MAKE) --no-print-directory validator-run-mp EOS=1 N=$(N)

eos-verify:  ## Read validated-events at read_committed (only committed, non-aborted records)
	docker compose -f $(COMPOSE_FILE) exec kafka-1 \
	  /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka-1:9092 --topic validated-events \
	  --from-beginning --timeout-ms 5000 --isolation-level read_committed \
	  --property print.key=true --property key.separator=' | '

eos-report:  ## Open the latest EOS validator-run report
	@xdg-open docs/results/week2_eos_results.md 2>/dev/null \
	  || open docs/results/week2_eos_results.md 2>/dev/null \
	  || echo "Report at docs/results/week2_eos_results.md"

# ---------------------------------------------------------------------------
# Sliding-window features consumer (Week 2 PR #2)
# Plain Python Kafka consumer with in-memory windowing — Flink was prototyped
# and rejected; see docs/design/week2_02_sliding_window_features_plain_consumer.md
# §2.1.  Redis is started by `make infra-up` along with the rest of the stack.
# ---------------------------------------------------------------------------

redis-cli:  ## Open an interactive redis-cli session against the running Redis
	docker compose -f $(COMPOSE_FILE) exec redis redis-cli

sliding-run:  ## Run the sliding-features consumer (single process, foreground)
	uv run python scripts/run_sliding_features_consumer.py --redis-host localhost \
		--bootstrap localhost:19092,localhost:19093,localhost:19094 \
		--registry http://localhost:8081

sliding-run-group:  ## Run a consumer group of N processes (default N=4): make sliding-run-group N=4
	uv run python scripts/run_sliding_features_consumer.py --redis-host localhost \
		--bootstrap localhost:19092,localhost:19093,localhost:19094 \
		--registry http://localhost:8081 --num-workers $(or $(N),4)

sliding-run-eos:  ## Run the sliding consumer with transactional EOS (features+late+offsets atomic)
	uv run python scripts/run_sliding_features_consumer.py --eos --redis-host localhost \
		--bootstrap localhost:19092,localhost:19093,localhost:19094 \
		--registry http://localhost:8081

sliding-report:  ## Open the latest sliding-features results report
	@xdg-open docs/results/week2_sliding_features_results.md 2>/dev/null \
	  || open docs/results/week2_sliding_features_results.md 2>/dev/null \
	  || echo "Report at docs/results/week2_sliding_features_results.md"

sliding-redis-show:  ## Dump a sample of the Redis feat:user:* hashes (10 keys)
	@docker compose -f $(COMPOSE_FILE) exec redis sh -c \
	  "redis-cli --scan --pattern 'feat:user:*' | head -10 | xargs -I {} sh -c 'echo \"=== {} ===\"; redis-cli HGETALL {}'"

# ---------------------------------------------------------------------------
# Online feature-serving API (Week 3 PR #1)
# Read-only FastAPI service over the Redis online store; scales via uvicorn
# worker processes, not threads (design week3_01_feature_serving_api.md §2.6).
# Redis is started by `make infra-up` along with the rest of the stack.
# ---------------------------------------------------------------------------

serving-run:  ## Run the feature-serving API (single worker, foreground, port 8000)
	uv run python scripts/run_feature_api.py --redis-host localhost

serving-run-group:  ## Run the API with W uvicorn worker processes (default W=4): make serving-run-group W=4
	uv run python scripts/run_feature_api.py --redis-host localhost --workers $(or $(W),4)

serving-smoke:  ## Crude latency sanity: N sequential curls (default N=20): make serving-smoke N=20 USER=u-000042
	@user="$(or $(USER_ID),u-000042)"; n="$(or $(N),20)"; \
	  echo "Hitting /v1/features/users/$$user x$$n on localhost:8000"; \
	  for i in $$(seq 1 $$n); do \
	    curl -s -o /dev/null -w "%{time_total}s\n" \
	      "localhost:8000/v1/features/users/$$user"; \
	  done
