# Gap Project Plan — ML Infrastructure Engineer Preparation

**Goal:** Close skill gaps and build a portfolio of hands-on projects to land a Senior/Staff ML Infrastructure or Performance Engineering role at $300K+ TC by end of 2026.  
**Timeline:** April 2026 – September 2026 (~6 months, part-time / evenings & weekends)  
**Date Created:** March 21, 2026  
**Last Updated:** March 21, 2026 — Removed Arrow/Parquet, MLflow/model registry, and PyTorch DataLoader basics (already gained through daily work at ASML)

---

## Gap Analysis Summary

| What You Already Have | What Top Companies Also Want to See |
|----------------------|--------------------------------------|
| C++ performance optimization | **CUDA / GPU-accelerated computing** |
| ZeroMQ / custom TCP pipelines | **Kafka / Flink / modern streaming** |
| Cassandra / NoSQL at scale | **Feature stores (Feast, Tecton patterns)** |
| Apache Arrow / Parquet / columnar formats ✅ | ~~Already covered~~ |
| ML-specific CI/CD (MLflow, model registries) ✅ | ~~Already covered~~ |
| PyTorch DataLoader (single-node) ✅ | **Distributed data loading, multi-GPU feeding** |
| mmap / low-latency data access | **LLM / GenAI serving infrastructure** |
| Unified data models / semantic layers | **Kubernetes-native ML workloads** |

> ✅ = Recently gained through daily work at ASML (past 6 months). No project needed.

---

## Project Roadmap Overview

| Phase | Project | Duration | Primary Skills |
|-------|---------|----------|----------------|
| 1 | Real-Time Feature Store & Streaming Pipeline | 5 weeks | Kafka, Redis, feature engineering, online/offline serving |
| 2 | GPU-Distributed ML Training Data Loader | 4 weeks | CUDA, distributed data loading, multi-GPU, C++/pybind11 |
| 3 | LLM Inference Serving Engine | 5 weeks | LLM serving, batching, KV-cache, gRPC, latency optimization |
| 4 | ML Serving Platform on Kubernetes | 4 weeks | K8s, pipeline orchestration, canary deployments, monitoring |
| 5 | System Design Portfolio & Interview Prep | 5 weeks | System design, mock interviews, write-ups |

**Total: ~23 weeks** (down from 32 weeks in the original plan)

---

## Phase 1: Real-Time Feature Store & Streaming Pipeline

**Duration:** 5 weeks (April – early May 2026)  
**Why This First:** Feature stores are the bridge between data engineering and ML — every ML platform team builds or integrates one. This project upgrades your streaming skills from ZeroMQ to Kafka, which appears in nearly every ML infra job posting.

### Technologies

- **Apache Kafka** — distributed event streaming
- **Confluent Schema Registry** (or Apicurio) — schema management, compatibility enforcement, and schema evolution
- **Apache Flink** (or Kafka Streams) — stream processing for feature computation
- **Redis** — online feature serving (low-latency key-value lookups)
- **PostgreSQL or DuckDB** — offline feature storage and batch computation
- **Feast** (open source) — feature store framework (study architecture, optionally integrate)
- **Python** — feature definition DSL and integration tests
- **Docker Compose** — local multi-service orchestration

### Project Description

Build a feature store system that ingests raw events via Kafka with schema evolution support, validates and transforms them in-stream, computes features in real-time, serves them at low latency for online inference, and produces versioned, reproducible training snapshots — while maintaining consistency between online (real-time) and offline (batch) feature values.

### Detailed Plan

**Week 1 — Kafka Fundamentals & Event Ingestion**
- Set up a multi-broker Kafka cluster, PostgreSQL, and **Confluent Schema Registry** in Docker Compose
- Define event schemas in Avro (or Protobuf) and register them with the Schema Registry; configure the producer to serialize events against the registered schema
- **Schema evolution:** Configure `BACKWARD` compatibility mode, then simulate real-world schema changes — add a new optional field (e.g., `device_type`), remove a deprecated field, and promote a field type (e.g., `int` → `long`) — verify that consumers on the old schema can still deserialize new events without breaking, and vice versa
- Build a Python producer that generates synthetic e-commerce events (clicks, purchases, page views) at 50K+ events/second
  - **Topic creation (producer-side, for this project only):** The producer uses `AdminClient` on startup to create the `e-commerce-events` topic with 12 partitions and RF=3 if it does not already exist. This is acceptable here because (a) single developer, no review process needed, (b) no operator/Terraform overhead is appropriate at laptop scale, and (c) it teaches the `AdminClient` API, which is also used in integration tests.
  - **Rule of thumb for production (not what this project does):** If more than one service reads or writes a topic, or if it has non-default config (compaction, retention, RF), it should be declaratively managed — not producer-created. Producer-created topics only make sense for truly private, single-owner topics, and even then most organizations forbid it for consistency. In production, topic definitions live in a Git repo and are applied by a dedicated **"infrastructure-as-code pipeline for Kafka"** — parallel to how Terraform manages AWS resources or Helm charts manage Kubernetes services. This pipeline is **not** part of the application's deploy path: it has its own repo, its own reviewers (platform/data-infra team), and cluster-admin credentials that application services never hold. Common implementations: Strimzi `KafkaTopic` CRDs reconciled by an operator, Terraform's Kafka provider, or GitOps tools like `kafka-gitops` / Julie Ops.
- Build a consumer that reads events and measures end-to-end latency
- Set up a Kafka-to-PostgreSQL sink (Kafka Connect JDBC Sink Connector or a simple Python consumer script) that subscribes to the `e-commerce-events` topic, batches messages (e.g., every 1,000 events or every 10 seconds), and inserts them into a `raw_events` table in PostgreSQL — this runs as a background process from Week 1 onward so that by Week 4, you have a substantial historical dataset for offline feature computation and point-in-time joins
- Experiment with partitioning strategies, consumer groups, and exactly-once semantics:
  - **Key-based partitioning:** Hash `user_id` as the message key so all events for a user land in the same partition — guarantees per-user ordering (required for stateful features like "time since last click") and enables independent per-shard scaling
  - **Consumer groups:** Implement the consumer as a coordinated worker that polls raw batches, deserializes events, measures end-to-end latency, and commits offsets (the "success marker" that ensures crash recovery resumes exactly where it left off) — in Week 1 the consumer only reads and benchmarks; the additional steps of computing features and writing to Redis get layered on in Week 2
  - **Exactly-once semantics (EOS):** Enable idempotent producer (broker-side Producer ID + Sequence Number tracking — lightweight RAM checks, feasible at 50K+ evt/sec) as the foundation, then layer on transactions to atomically commit writes to both Redis and PostgreSQL — this eliminates training-serving skew between your offline and online stores
  - **Read-side EOS:** Configure consumers with `read_committed` isolation so they only see data past the broker's Last Stable Offset (LSO), filtering out uncommitted or aborted transaction data
- Deliverable: Running Kafka cluster with producer/consumer benchmarks, and a continuously-populating `raw_events` table in PostgreSQL

**Week 2 — Validation, Transformation & Feature Computation**
- **Validation layer:** Implement an inline validation stage in the stream processor that checks incoming events for: null/missing required fields, out-of-range values (e.g., negative prices, timestamps in the future), malformed records, and schema conformance against the registry — route invalid events to a `dead-letter-queue` topic with error metadata for debugging
- **Transformation layer:** Apply cleaning and normalization before feature computation — e.g., standardize timestamp formats to UTC, normalize currency values, enrich events with derived fields (e.g., `is_weekend`, `hour_of_day`), and deduplicate late-arriving events using event ID + idempotent upserts
- Implement stream processing using Kafka Streams or Flink to compute real-time features from validated, transformed events:
  - Sliding window aggregations (clicks in last 5 minutes, purchase count in last 24 hours)
  - Session-based features (session duration, pages per session)
  - Entity-level running statistics (user lifetime spend, average order value)
- Write computed features to Redis for online serving
- Deliverable: Stream processor computing 10+ features in real-time with <100ms end-to-end latency

**Week 3 — Online Feature Serving Layer**
- Build a gRPC or REST API that serves features for a given entity (user ID) from Redis
- Implement a feature vector assembly endpoint that joins features from multiple feature groups
- Add caching, connection pooling, and batched lookups for throughput
- Benchmark: target <5ms p99 latency for single-entity feature vector retrieval
- Deliverable: Feature serving API with latency benchmarks

**Week 4 — Offline Feature Store & Versioned Training Data Generation**
- Build a batch pipeline that computes the same features from historical data (using DuckDB or PostgreSQL)
- Implement point-in-time-correct joins to prevent data leakage in training sets
- Validate online/offline consistency by comparing real-time computed features against batch-computed features
- **Dataset versioning:** Tag each training snapshot with a version ID that captures: the data time range, the schema version used (from Schema Registry), the feature pipeline code commit hash, and a row count / checksum — store this metadata in a `dataset_versions` table in PostgreSQL so any snapshot can be reproduced exactly
- **Reproducible snapshots:** Parameterize the batch pipeline so that running it with the same version config (time range + schema version + code hash) always produces a bit-identical Parquet output — this is what enables ML teams to debug model regressions by re-training on an exact historical dataset
- Generate training datasets as versioned Parquet files (leveraging your existing Arrow/Parquet expertise), partitioned by date and tagged with the snapshot version
- Deliverable: Batch pipeline with online/offline consistency report and a `dataset_versions` registry that tracks every training snapshot

**Week 5 — Monitoring, Testing & Documentation**
- Implement feature drift detection (statistical tests comparing recent vs historical feature distributions)
- Add data freshness monitoring (alert if a feature hasn't been updated within SLA)
- Write integration tests that validate end-to-end correctness from event to served feature
- Create architecture diagram and write-up explaining online/offline consistency challenges
- Deliverable: GitHub repo with full system, monitoring dashboard, and architecture documentation

### Learning Objectives

After this project, you should be able to:
- Design a feature store that serves features at <5ms for online inference while maintaining offline consistency
- Explain the tradeoffs between Kafka, Flink, and Kafka Streams for real-time feature computation
- Handle schema evolution gracefully in a streaming pipeline using a schema registry with compatibility modes
- Implement in-stream validation and transformation with dead-letter routing for malformed records
- Produce versioned, reproducible training snapshots that capture data range, schema version, and code hash — and articulate why this matters for debugging model regressions
- Articulate why point-in-time correctness matters for ML training and how to implement it
- Discuss feature drift detection and data quality monitoring for ML systems
- Answer system design questions like "Design a real-time feature platform for a recommendation system" or "How do you handle schema changes in a production ML data pipeline?"

---

## Phase 2: GPU-Distributed ML Training Data Loader

**Duration:** 4 weeks (May – early June 2026)  
**Why This Next:** You already know PyTorch DataLoader fundamentals from daily work. This project leaps ahead to the distributed and GPU-specific aspects — how to keep multiple GPUs fed without bottlenecks. This is the differentiating knowledge that separates ML infra engineers from general data engineers at companies like OpenAI, Meta, and NVIDIA.

### Technologies

- **PyTorch DistributedDataParallel (DDP)** — multi-GPU training
- **CUDA** (basics) — GPU memory management, pinned memory, async transfers
- **C++ / pybind11** — custom high-performance data loading operators
- **NVIDIA DALI** (study) — GPU-accelerated data loading pipeline
- **NVIDIA Nsight Systems** — GPU profiling and timeline analysis
- **Ray Data** (optional) — distributed data loading at scale

### Project Description

Build a high-performance distributed data loading library that feeds a multi-GPU training job without becoming the bottleneck. Focus on the gap between your existing single-node DataLoader knowledge and production-grade distributed training: C++ data loading operators, GPU-direct transfers, distributed sharding, and profiling.

### Detailed Plan

**Week 1 — C++ Custom Data Loading Operator & GPU Memory Pipeline**
- Build a C++ extension (via pybind11 or PyTorch C++ extensions) that reads and decodes Parquet data using your Arrow/Parquet knowledge
- Convert Arrow arrays directly to PyTorch tensors without Python-level copies
- Implement pinned memory allocation for host-to-GPU transfers
- Use CUDA streams for async data transfer overlapping with compute
- Benchmark: compare C++ loader throughput vs pure Python PyArrow loader
- Deliverable: C++ extension with benchmark showing throughput improvement over Python path

**Week 2 — Double Buffering & Compute/Transfer Overlap**
- Implement a double-buffering scheme: while GPU trains on batch N, CPU prepares batch N+1 and transfers batch N+2 to pinned memory
- Profile with NVIDIA Nsight Systems to verify compute/transfer overlap
- Identify and eliminate pipeline bubbles where the GPU is idle waiting for data
- Experiment with prefetch depth and batch size to find optimal configurations
- Deliverable: Nsight Systems timeline showing overlapped data loading and training with no GPU idle time

**Week 3 — Distributed Data Loading for Multi-GPU Training**
- Extend the data loader for DistributedDataParallel (DDP) training across multiple GPUs
- Implement distributed sharding: each worker reads a non-overlapping partition of the dataset
- Handle straggler mitigation: dynamic work stealing when one worker finishes its shard early
- Implement elastic data loading that adapts when GPUs are added/removed
- Test with 2-4 GPU simulation (or use cloud spot instances for a few hours)
- Deliverable: Distributed data loader with sharding and straggler mitigation, scaling benchmarks

**Week 4 — Benchmarking, Comparison & Documentation**
- Run an end-to-end training job (ResNet-50 on ImageNet-scale synthetic data) and profile
- Compare your loader against PyTorch's default DataLoader and NVIDIA DALI
- Document the full data path: disk → C++ decode → pinned memory → GPU → training
- Write architecture documentation with profiling results and performance analysis
- Deliverable: GitHub repo with full pipeline, benchmarks vs DALI/default DataLoader, and data path diagram

### Learning Objectives

After this project, you should be able to:
- Implement zero-copy or minimal-copy data loading using C++ extensions with PyTorch
- Explain the full data path from storage to GPU during ML training and identify bottlenecks at each stage
- Use CUDA profiling tools (Nsight Systems) to verify compute/transfer overlap and diagnose pipeline bubbles
- Design a distributed data loading strategy that scales linearly with the number of GPUs
- Answer questions like "How would you design a data loading pipeline that keeps 1,000 GPUs fed during LLM training?"

---

## Phase 3: LLM Inference Serving Engine

**Duration:** 5 weeks (June – early July 2026)  
**Why This Next:** LLM inference is the hottest area in ML infrastructure. Every AI company on your target list (OpenAI, Anthropic, xAI, Together AI, Groq, etc.) needs engineers who understand inference optimization. This project shows you can work at the intersection of C++ performance and modern AI systems.

### Technologies

- **vLLM** (study and extend) — open-source LLM serving engine
- **C++ / CUDA** — custom inference kernels (basics)
- **gRPC** — high-performance serving API
- **KV-Cache management** — PagedAttention concepts
- **Continuous batching** — dynamic batch assembly for throughput
- **Quantization** (GPTQ, AWQ) — model compression for faster inference
- **NVIDIA TensorRT-LLM** (study) — optimized inference runtime
- **Prometheus + Grafana** — inference metrics and monitoring

### Project Description

Build a simplified LLM inference serving system that implements continuous batching, KV-cache management, and request scheduling to maximize throughput while meeting latency SLAs. You will not build a full production server — instead, focus on the core scheduling and memory management algorithms in C++/Python.

### Detailed Plan

**Week 1 — LLM Inference Fundamentals**
- Study the transformer inference loop: prefill vs decode phases, KV-cache growth, memory requirements
- Run a small open model (e.g., Llama-3-8B or Mistral-7B) locally using vLLM and Hugging Face Transformers
- Benchmark: measure tokens/second, time-to-first-token (TTFT), and GPU memory usage
- Profile with Nsight Systems to understand where time is spent during inference
- Deliverable: Benchmark report comparing naive (Hugging Face) vs optimized (vLLM) inference

**Week 2 — Request Scheduler & Continuous Batching**
- Implement a request scheduler in Python that manages a queue of incoming requests
- Implement continuous batching: instead of waiting for a full batch, dynamically add/remove requests from the running batch
- Implement priority scheduling based on SLA (low-latency interactive vs high-throughput batch)
- Simulate workloads with varying request arrival rates and sequence lengths
- Deliverable: Scheduler with throughput/latency metrics under different workload patterns

**Week 3 — KV-Cache Memory Manager**
- Implement a page-based memory manager for KV-cache (inspired by PagedAttention / vLLM)
- Implement memory allocation, deallocation, and defragmentation for variable-length sequences
- Implement preemption: when memory is exhausted, pause low-priority requests and reclaim their KV-cache
- Benchmark memory utilization vs naive pre-allocated KV-cache
- Deliverable: Memory manager with utilization metrics and preemption demonstrations

**Week 4 — gRPC Serving API & Streaming**
- Build a gRPC server that accepts generation requests and streams tokens back to clients
- Implement the OpenAI-compatible API format (chat completions with streaming)
- Add request queuing, timeout handling, and graceful degradation under load
- Load test with multiple concurrent clients and measure TTFT and tokens/second
- Deliverable: Working gRPC server with streaming responses and load test results

**Week 5 — Quantization Experiments & Documentation**
- Experiment with serving quantized models (4-bit GPTQ, AWQ) vs full precision
- Benchmark throughput, latency, and quality trade-offs at different quantization levels
- Write a comprehensive architecture document covering:
  - Why continuous batching matters (throughput improvement)
  - How PagedAttention reduces memory waste
  - Quantization trade-offs for production serving
- Deliverable: GitHub repo with full system, benchmark comparison, and architecture write-up

### Learning Objectives

After this project, you should be able to:
- Explain the prefill/decode phases of LLM inference and why they have different compute profiles
- Design a continuous batching scheduler that maximizes GPU utilization
- Articulate how PagedAttention works and why it improves memory utilization by 2-4x
- Discuss quantization trade-offs (speed vs quality) for production LLM serving
- Answer system design questions like "Design an LLM serving system that handles 10K concurrent users with <200ms TTFT"

---

## Phase 4: ML Serving Platform on Kubernetes

**Duration:** 4 weeks (July – early August 2026)  
**Why This Next:** Nearly every ML infrastructure role requires Kubernetes experience. Since you already have MLflow and model registry experience from daily work, this phase focuses on the operational side — K8s orchestration, pipeline automation, canary deployments, and production monitoring — which are the remaining gaps.

### Technologies

- **Kubernetes** (minikube or kind for local, or a small GKE/EKS cluster) — orchestration
- **Argo Workflows** or **Kubeflow Pipelines** — ML pipeline orchestration
- **Docker** — containerization of all components
- **Prometheus + Grafana** — monitoring and alerting
- **Helm** — Kubernetes package management
- **MinIO** — S3-compatible object storage for artifacts
- **GitHub Actions** — CI/CD for model training and deployment

### Project Description

Build a Kubernetes-native ML serving platform that automates the lifecycle from pipeline orchestration to model deployment to monitoring. Leverage your existing MLflow and model registry knowledge as the experiment tracking layer, and focus your hands-on time on the K8s operations, deployment automation, and observability you haven't done before.

### Detailed Plan

**Week 1 — Kubernetes Fundamentals & Cluster Setup**
- Set up a local Kubernetes cluster (kind or minikube) with GPU support (if available)
- Deploy MinIO (object storage), PostgreSQL (metadata), and Redis (feature serving from Phase 1) as Kubernetes services
- Write Dockerfiles for your Phase 1 feature store and Phase 3 inference server
- Deploy them as Kubernetes Deployments with proper resource requests/limits, liveness/readiness probes
- Practice key K8s operations: scaling, rolling updates, resource quotas, namespace isolation
- Deliverable: Running K8s cluster with core infrastructure and ML services deployed

**Week 2 — ML Pipeline Orchestration & Deployment Automation**
- Set up Argo Workflows (or Kubeflow Pipelines) for ML pipeline orchestration
- Build a DAG pipeline: data ingestion → feature computation → model training → model evaluation → deployment
- Implement parameterized pipelines (different hyperparameters, data versions)
- Add retry logic and failure handling for each pipeline step
- Connect to your existing MLflow knowledge: configure the pipeline to log experiments and register models automatically
- Deliverable: Automated ML pipeline that runs end-to-end on Kubernetes

**Week 3 — Canary Deployments, Rollbacks & Traffic Management**
- Deploy your Phase 3 LLM inference server as a Kubernetes Deployment
- Implement a canary deployment strategy: route 5% of traffic to the new model, monitor metrics, then gradually increase
- Build a rollback mechanism that automatically reverts if error rate or latency exceeds threshold
- Implement A/B testing infrastructure for comparing model versions using Istio or Nginx ingress traffic splitting
- Deliverable: Canary deployment with automated rollback and A/B testing

**Week 4 — Monitoring, Alerting & Documentation**
- Deploy Prometheus and Grafana for infrastructure and ML-specific metrics
- Build dashboards for: inference latency (p50/p95/p99), throughput, GPU utilization, model prediction distribution, data pipeline lag, feature freshness
- Implement alerts for: latency SLA violations, feature freshness SLA, model drift detection, pipeline failures, resource exhaustion
- Write comprehensive platform documentation: architecture, runbooks, and operational procedures
- Deliverable: Monitored platform with dashboards, alerts, runbooks, and architecture documentation

### Learning Objectives

After this project, you should be able to:
- Deploy and operate ML workloads on Kubernetes with proper resource management, probes, and scaling
- Orchestrate multi-step ML pipelines with retry logic and failure handling
- Implement canary deployments and automated rollbacks for ML models in production
- Build monitoring and alerting for ML-specific metrics (drift, freshness, latency)
- Answer questions like "Design an ML platform that supports 50 ML engineers and 200 models in production"

---

## Phase 5: System Design Portfolio & Interview Prep

**Duration:** 5 weeks (August – mid-September 2026)  
**Why Last:** With four concrete projects behind you plus your daily work experience with Arrow/Parquet, MLflow, and PyTorch, this phase crystallizes everything into interview-ready system design skills and a polished public portfolio.

### Technologies

- **GitHub** — portfolio hosting
- **Excalidraw or draw.io** — system design diagrams
- **Personal blog** (GitHub Pages, dev.to, or Medium) — technical write-ups

### Detailed Plan

**Week 1-2 — System Design Practice Problems**

Practice designing these systems (write full documents with diagrams for each):

| Problem | Key Concepts (Projects + Daily Work) |
|---------|--------------------------------------|
| Design a training data pipeline for a 100B-parameter LLM | Daily work (Arrow/Parquet) + Phase 2 (distributed loading) |
| Design a real-time recommendation feature platform | Phase 1 (feature store, Kafka, online/offline) |
| Design an LLM serving system for 100K RPM | Phase 3 (continuous batching, KV-cache, scheduling) |
| Design an ML platform for 50 engineering teams | Daily work (MLflow) + Phase 4 (K8s, pipeline orchestration) |
| Design a high-throughput data ingestion system for 1B events/day | ASML work + Phase 1 (Kafka streaming) |
| Design a model monitoring and rollback system | Phase 4 (canary, drift detection, alerting) |

For each design, structure your answer as:
1. Requirements clarification (functional + non-functional)
2. High-level architecture diagram
3. Detailed component design with data flow
4. Scalability analysis (bottlenecks, horizontal scaling strategy)
5. Trade-offs and alternatives considered
6. Monitoring and operational concerns

**Week 3 — GitHub Portfolio Polish**

Organize your four projects into a cohesive GitHub portfolio:

- Each project gets a polished README with: problem statement, architecture diagram, benchmark results, design decisions, and what you learned
- Create a top-level portfolio README that tells a story: "I built an end-to-end ML infrastructure stack from feature engineering to LLM serving to production monitoring"
- Add clear commit history showing iterative development (not a single massive commit)
- Add CI/CD (GitHub Actions) to each project for automated testing

**Week 4-5 — Mock Interviews & Refinement**

- Do 2-3 mock system design interviews per week (use Pramp, interviewing.io, or find a study partner)
- Focus on ML infrastructure system design — this is where your projects give you a massive advantage
- Practice articulating your ASML work in terms that map to each target company's problems
- Prepare your "pitch": 60-second summary combining daily work expertise (Arrow, MLflow, PyTorch) with project portfolio (Kafka, CUDA, LLM serving, K8s)
- Refine your resume to highlight quantified impact from both your job and your projects

### Learning Objectives

After this phase, you should be able to:
- Whiteboard a complete ML infrastructure system design in 45 minutes with clear trade-offs
- Walk through your projects with depth — explain why you made each design decision
- Seamlessly blend daily work experience (Arrow/Parquet, MLflow, PyTorch) with project experience (Kafka, CUDA, LLM serving, K8s) to show full-stack ML infra competence
- Map your ASML experience to any ML infrastructure problem without requiring the interviewer to understand semiconductor inspection
- Confidently discuss distributed systems, data pipeline design, and ML serving optimization at the Staff engineer level

---

## Timeline Summary

```
April 2026          May               June              July
|--- Phase 1 ----|--- Phase 2 ----|--- Phase 3 ----|
 Feature Store     GPU-Distributed   LLM Inference
 Kafka/Redis       ML Data Loader    Serving Engine

August             September
|--- Phase 4 ----|--- Phase 5 ----|
 ML Serving on     System Design &
 Kubernetes        Interview Prep

                                     October – November 2026
                                     → Buffer / extra prep / early applications

                                     December 2026
                                     → Full job market entry
```

**Freed up ~9 weeks** compared to original 32-week plan by removing Arrow/Parquet, MLflow/registry, and DataLoader basics. This gives you a 2-month buffer (October–November) before December job market entry — use it for extra mock interviews, early applications, or extending any phase that needs more time.

---

## Hardware / Cloud Budget Estimate

| Item | Estimated Cost | Notes |
|------|---------------|-------|
| Local development machine | Already owned | Your existing setup works for Phases 1, 4, 5 |
| Cloud GPU instances (Phase 2, 3) | $200–$500 | Use spot instances on AWS/GCP for GPU work; ~40 hours of A100 spot time |
| Small GKE/EKS cluster (Phase 4) | $100–$200 | Optional — minikube is free for local development |
| Domains / hosting | $0–$20 | GitHub Pages is free |
| **Total** | **$300–$720** | |

---

## Priority Ranking If Time Is Limited

If you cannot complete all five phases, prioritize in this order:

| Priority | Project | Reason |
|----------|---------|--------|
| 1 | **Phase 3: LLM Inference Serving** | Highest signal for AI company interviews in 2026; very few candidates have hands-on experience |
| 2 | **Phase 5: System Design Prep** | No point having projects if you cannot articulate them in interviews |
| 3 | **Phase 1: Feature Store** | Strong signal for ML platform roles at FAANG and unicorns; upgrades ZeroMQ → Kafka |
| 4 | **Phase 2: GPU-Distributed Data Loader** | Differentiating for training infra roles; builds on your existing DataLoader knowledge |
| 5 | **Phase 4: ML Serving on K8s** | Important but can be learned on the job more easily than the others |

---

## What Your Combined Skill Profile Looks Like to Hiring Managers

After completing these projects, combined with your daily work and ASML experience:

| Skill Area | Source | Depth |
|------------|--------|-------|
| C++ performance engineering | ASML + First Orion (production) | Deep — production-proven |
| Arrow / Parquet / columnar data | ASML daily work | Solid — hands-on production |
| MLflow / model registry / CI/CD for ML | ASML daily work | Solid — hands-on production |
| PyTorch DataLoader (single-node) | ASML daily work | Solid — hands-on production |
| High-throughput data pipelines | First Orion 600M+/day (production) | Deep — production-proven |
| Real-time / low-latency systems | First Orion <25ms (production) | Deep — production-proven |
| Kafka / streaming / feature stores | Phase 1 project | Working — portfolio project |
| CUDA / GPU data pipeline / distributed training | Phase 2 project | Working — portfolio project |
| LLM inference / serving / scheduling | Phase 3 project | Working — portfolio project |
| Kubernetes / ML ops / monitoring | Phase 4 project | Working — portfolio project |
| System design communication | Phase 5 preparation | Interview-ready |

This profile covers the full ML infrastructure stack from data ingestion to model serving, which is exactly what Staff-level ML infra roles require.
