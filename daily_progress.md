# Daily Progress


## 2026-06-06
### Progress
- Removed Flink, not work reliably. 
- Created a new design doc to replace Flink `week2_02_sliding_window_features_plain_consumer.md`.
- Execute `week2_02_sliding_window_features_plain_consumer.md`.
- Verify `week2_02_sliding_window_features_plain_consumer.md`.
- *TODO*: Create Hashnode post `week2_02_sliding_window_features_plain_consumer.md`.

## 2026-05-27
### Progress
- Executed design doc `./streaming-feature-store/docs/design/week2_02_sliding_window_features.md`.
- Read through line 94 in `week2_02_sliding_window_features.md`.


## 2026-05-26
### Progress
- Create design doc `./streaming-feature-store/docs/design/week2_02_sliding_window_features.md`.
- *TODO*: Execute design doc `./streaming-feature-store/docs/design/week2_02_sliding_window_features.md`. 

## 2026-05-25
### Progress
- Create week2 PR#1 `./streaming-feature-store/docs/design/week2_01_validation_layer_and_dlq.md`.
- Execute `./streaming-feature-store/docs/design/week2_01_validation_layer_and_dlq.md`. 
- Finish week2 PR#1. 


## 2026-05-22
### Progress
- Finish week1 06. 

## 2026-05-21
### Progress
- Read through line 53 in `./streaming-feature-store/docs/design/week1_06_postgres_sink_and_continuous_feeder.md`

## 2026-05-20
### Progress
- Create `./streaming-feature-store/docs/design/week1_06_postgres_sink_and_continuous_feeder.md`
- Execute `./streaming-feature-store/docs/design/week1_06_postgres_sink_and_continuous_feeder.md`
- TODO: Read through & understand what were done in `./streaming-feature-store/docs/design/week1_06_postgres_sink_and_continuous_feeder.md`


## 2026-05-18
### Progress
- Understand fully the consumer - what & why.
- TODO: execute the manual integration test ## 7. How to Run of `week1_05_consumer_group_end_to_end_latency.md`. 

## 2026-05-17
### Progress
- Update `week1_load_test_throughput_investigation.md` with EOS (Exactly once semantics) for multiprocessing
- Create `week1_05_consumer_group_end_to_end_latency.md`. 

## 2026-05-15
### Progress
- Published tech post on hashnode `week1_gil_kafka_throughput_blogpost_hashnode.md` on the finding where multiprocess resolve GIL low throughput limitation. 


## 2026-05-14
### Progress
- Conducted performance benchmark on threading for kafka producer.
- Executed multiprocessing for kafka producer.  
- Conducted performance benchmark on multiprocessing for kafka producer.


## 2026-05-13
### Progress
- Conducted performance benchmark on threading for kafka producer.


## 2026-05-12
### Progress
- Finish `week1_04_synthetic_event_producer.md` 
- Document findings and conclusion on `week1_load_test_throughput_investigation.md`. 

## 2026-05-11
### Progress
- Ran `make load-test-quick`.
- Ran `make load-test`.
- Edited `week1_load_test_throughput_investigation.md`. 
- TODO: execute each potential fix/check one at a time (totally 3 fixes + 1 check) `week1_load_test_throughput_investigation.md`
- TODO: remember document each potential fix in `week1_load_test_throughput_investigation.md`.

## 2026-05-10
### Progress
- Read the line 813 script `run_event_load.py` to understanding threading operations. 
- Created `python_gil_vs_other_languages.md`.
- Created `python_condition_variables.md`.

## 2026-05-08
### Progress
- Executed `./docs/design/week1_04_syntheti_event_producer.md`.
- TODO: run line 813 in `./docs/design/week1_04_syntheti_event_producer.md`.

## 2026-05-07
### Progress
- Created `./docs/design/week1_04_syntheti_event_producer.md`.
- Read through line 56 - Adds a deterministic, fast **synthetic event generator** .... 

## 2026-05-04
### Progress
- Executed `./docs/design/week1_03_schema_evolution_experiments.md`.
- Followed through code to fully understand. 


## 2026-04-28
### Progress
- Read src/streaming_feature_store/schemas/loader.py 
- Read src/streaming_feature_store/schemas/registry.py 
- Read src/streaming_feature_store/schemas/models.py 
- Read src/streaming_feature_store/schemas/avro_producer.py 
- Execute integration tests and other integration related commands that register schemas, and actually produce live Kafka topic messages.
- Created `./docs/design/week1_03_schema_evolution_experiments.md`.


## 2026-04-26
### Progress
- Read through schemas/ecommerce/v1/*.avsc.
- Read through scripts/register_schemas.py. 


## 2026-04-24
### Progress 

- Executed the design doc `docs/design/week1_02_avro_schemas_and_producer_serialization.md`.


## 2026-04-22
### Progress 

- Add **Confluent Schema Registry** setup. 
- Add changelog is in `docs/changelog/2026-04-20_schema_registry.md`.
- Draft design doc `docs/design/week1_02_avro_schemas_and_producer_serialization.md` for the second bulletpoint in week 1 plan specified in `docs/design/gap_project_plan.md`. 


## 2026-04-20
### Progress 

- Create the plan for the addition of **Confluent Schema Registry**.
- Update `docs/design/week1_01_docker_compose_infra.md`. 


## 2026-04-19
### Progress 

- Update `docs/gap_project_plan.md` and `docs/week1_01_docker_compose_infra.md` week 1 plan with the addition of **Confluent Schema Registry** for schema enforcement. 


## 2026-04-02

### Reading Progress

- Last read: `docs/design/week1_kafka_postgres_docker_setup.md`, line 697
  ```bash
  # Run integration tests only (sequential, as required)
  make test-integration
  # Equivalent: pytest tests/integration/ -v -m integration -p no:xdist
  ```
