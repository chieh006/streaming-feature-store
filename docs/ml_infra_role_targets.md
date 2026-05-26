# AI/ML Infrastructure Role Targets

50 high-fit roles for ML infrastructure positions following completion of the 6-month preparation plan (Feature Store + LLM Inference Engine projects, DDIA + papers, ML system design practice).

## Compensation caveats

TC ranges reflect typical Senior/Staff-level total compensation (base + equity 4yr-avg + bonus) based on public data sources (levels.fyi, Blind). Actual offers vary significantly by team, location, level placement, negotiation, and market conditions. The AI compensation market in particular has been volatile — frontier labs and AI-specialist companies have paid premiums above standard tech bands. All listed roles target locations where TC reliably exceeds $350k for the relevant level; some smaller startups are borderline at Senior and reliably clear at Staff.

## Background summary (used for fit reasoning)

- ~5 years C++ + high-throughput data systems: Senior SWE at ASML (Feb 2023–present, San Jose) and SWE at First Orion (Jun 2021–Feb 2023, Little Rock)
- ASML: architected a unified data model + semantic layer bridging legacy inspection data into modern ML platforms; powered UI analytics over 100M+ defect data points for cross-team consumers
- 8x throughput + 20% memory reduction via production C++ data structures with tuned accuracy/speed trade-offs; 20x ingestion acceleration via a low-latency mmap data access layer feeding ML training/processing
- First Orion: engineered a C++/ZeroMQ interface that drove a 10x increase in Cassandra upsert rates, ingesting 600M+ daily call records (mobile + landline) into ML-ready pipelines
- Resolved bottlenecks in a custom TCP network client to keep telephony ML applications inside a strict <25ms hard processing limit
- CI/CD + Pytest framework for distributed REST services and NoSQL stores, guaranteeing data-flow integrity for mission-critical, time-sensitive applications
- Phase 1 project: Kafka + Flink + Redis feature store with online/offline consistency
- Phase 2 project: vLLM-style inference engine with paged KV-cache and continuous batching in C++/Python

## Tier 1: Frontier AI Labs

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| OpenAI | Member of Technical Staff — Inference | $700k–$1M+ | Inference project mirrors their core serving stack; C++ + CUDA + KV-cache management is exactly what they hire for. |
| OpenAI | MTS — Training Infrastructure | $700k–$1M+ | 600M+ records/day ingestion experience translates directly to training data pipelines at scale. |
| Anthropic | Senior SWE — Inference | $500k–$850k | Continuous batching + paged attention work aligns with serving team scope; C++ low-latency expertise is rare in this market. |
| Anthropic | Senior SWE — Model Serving Infrastructure | $500k–$850k | mmap/zero-copy data access work translates to high-throughput serving infrastructure. |
| Anthropic | Senior SWE — Data Platform | $500k–$800k | Feature store + Kafka project + ASML data modeling experience fits training data infra. |
| xAI | Senior SWE — Inference | $550k–$900k | Aggressive C++/CUDA inference hiring; project portfolio + low-latency background is a near-perfect match. |
| xAI | Senior SWE — Training Cluster Infrastructure | $550k–$900k | High-throughput data systems background applies to multi-thousand-GPU training data flow. |
| Google DeepMind | Senior SWE — Gemini Serving Infrastructure | $400k–$650k | Inference project depth + distributed systems fluency from DDIA prep. |
| Mistral | Senior SWE — Inference | $350k–$500k | C++ inference work is differentiated; Mistral's open-source serving stack aligns with project scope. |
| Reka AI | Senior SWE — ML Infrastructure | $350k–$500k | Smaller team where end-to-end inference + data infra experience is high-leverage. |

## Tier 2: AI Inference & Serving Specialists

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| Together AI | Senior SWE — Inference Engine | $400k–$600k | Their entire business is the project you just built. Direct skill match. |
| Fireworks AI | Senior SWE — Inference Performance | $400k–$600k | C++ kernel/serving work is core to their differentiation; project demonstrates fluency. |
| Groq | Senior SWE — Compiler & Runtime | $400k–$600k | Low-latency C++ background + inference engine project; Groq's LPU runtime work is C++-heavy. |
| Cerebras | Senior SWE — ML Systems Software | $350k–$500k | Wafer-scale data movement is a data systems problem; high-throughput background applies. |
| SambaNova | Senior SWE — Runtime/Inference | $350k–$500k | C++ runtime work + inference scheduling experience is well-aligned. |
| Modal | Senior SWE — Serverless GPU Infrastructure | $350k–$500k | Serving + data infra polymath profile fits Modal's lean infra team. |
| Baseten | Senior SWE — Model Serving Platform | $350k–$500k | Inference engine project translates directly to their core product (Truss/serving). |
| Replicate | Senior SWE — Model Serving | $350k–$500k | Performance + serving background fits their cold-start / GPU scheduling work. |
| Anyscale | Senior SWE — Ray Serve / LLM Inference | $400k–$550k | Ray Serve + inference scheduling work overlaps directly with Phase 2 project. |
| Perplexity | Senior SWE — Inference Infrastructure | $400k–$600k | Inference latency/cost optimization is core to their unit economics; C++ background is rare. |

## Tier 3: Big Tech ML Platform Teams

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| NVIDIA | Senior SWE — TensorRT-LLM | $400k–$650k | C++ + inference engine project + KV-cache work is the exact skill set TRT-LLM team hires for. |
| NVIDIA | Senior SWE — Triton Inference Server | $400k–$650k | C++ serving infrastructure + scheduling project aligns directly with Triton scope. |
| NVIDIA | Staff SWE — DGX Cloud / AI Infrastructure | $500k–$800k | High-throughput data systems + inference depth fits DGX Cloud platform work. |
| Meta | E5 SWE — GenAI Inference | $400k–$650k | Inference project + C++ low-latency directly fits LLaMA serving infrastructure. |
| Meta | E5 SWE — PyTorch Core / AI Infra | $400k–$650k | C++ data structures + Python interop experience translates to PyTorch internals work. |
| Meta | E6 Staff SWE — ML Platform | $550k–$850k | After projects + writeups, Staff is plausible given 5+ YOE and differentiated portfolio. |
| Apple | ICT4 — ML Platform / Foundation Models | $400k–$550k | Performance-focused C++ background fits Apple's on-device + cloud ML platform work. |
| Apple | ICT5 — ML Infrastructure | $500k–$700k | Senior ML infra at Apple values systems depth + production reliability — both demonstrated. |
| Amazon | L6 SDE — SageMaker Inference | $400k–$600k | Inference serving project maps to SageMaker hosting team responsibilities. |
| Amazon | L6 SDE — Bedrock Model Serving | $400k–$600k | C++ + serving project + data infra fits Bedrock's inference scaling team. |
| Microsoft | Principal SWE — Azure ML / AI Platform | $400k–$600k | Distributed systems + ML platform breadth fits Azure ML / AOAI infra hiring. |
| Google | Staff SWE — Vertex AI Inference | $450k–$700k | Serving project + production data infra fits Vertex AI hosting team scope. |

## Tier 4: Data & ML Platform Companies

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| Databricks | Staff SWE — Mosaic AI / Inference | $450k–$650k | Inference engine project + data platform background is a dual-fit for Mosaic team. |
| Databricks | Senior SWE — ML Platform | $400k–$550k | Feature store project + Kafka + Delta Lake data infra alignment. |
| Snowflake | Senior/Staff SWE — Cortex AI | $400k–$600k | Data platform background + new inference work fits Cortex's intersection of data + AI. |
| Confluent | Staff SWE — Streaming Platform | $400k–$600k | Kafka project + ZeroMQ → Kafka transition + low-latency C++ is a strong Confluent profile. |
| Tecton | Senior SWE — Feature Platform | $350k–$500k | Phase 1 project is literally building a Tecton-like system; direct portfolio alignment. |
| Pinecone | Senior SWE — Vector Database | $350k–$500k | C++ + low-latency + high-throughput data structures fits vector index serving work. |
| Weights & Biases | Senior SWE — Platform/Infrastructure | $350k–$500k | Data systems + ML platform background fits W&B's training infrastructure side. |
| MongoDB | Staff SWE — Vector Search / AI | $400k–$550k | C++ database internals experience would map to their vector search investments. |

## Tier 5: ML Product Teams at Big Tech

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| Netflix | Senior SWE — ML Platform | $500k–$900k | Netflix pays high single-band TC; ML platform team values depth + production rigor demonstrated. |
| Uber | Staff SWE — Michelangelo (ML Platform) | $400k–$600k | Feature store project directly mirrors Michelangelo's online/offline architecture. |
| Stripe | L5 SWE — ML Platform | $400k–$600k | Low-latency systems + ML data infrastructure fits Radar/fraud ML platform work. |
| Airbnb | Senior SWE — ML Infrastructure | $400k–$600k | Feature store + ML platform fluency fits Bighead/Chronon team scope. |
| Pinterest | Staff SWE — ML Platform | $400k–$600k | High-throughput data systems + feature store work fits Pinterest's ML serving infra. |
| LinkedIn | Staff SWE — AI Platform | $400k–$600k | Feature store + Kafka background aligns with LinkedIn's ML platform (Pro-ML, Feathr roots). |
| DoorDash | Senior SWE — ML Platform | $400k–$550k | Real-time ML serving for logistics fits low-latency + feature store experience. |

## Tier 6: Autonomous Systems

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| Tesla | Senior SWE — AI Infrastructure (Dojo / Autopilot) | $400k–$600k | C++ + low-latency + high-throughput data pipelines fits Autopilot data infra and Dojo training. |
| Waymo | Senior SWE — ML Infrastructure | $400k–$650k | Sensor data ingestion + ML training data pipelines is a direct background match. |
| Applied Intuition | Senior SWE — Simulation/ML Infrastructure | $400k–$600k | C++ performance work + data systems fits AV simulation and ML training infra. |

## Tier 7: Quant/HFT (C++ + Low-Latency Premium)

These aren't strictly ML infra, but your C++ + sub-25ms pipeline background is highly valued, and many quant firms now have substantial ML infra teams. Including them because TC is typically the highest of any category.

| Company | Role | Est. TC | Why Fit |
|---|---|---|---|
| Citadel / Citadel Securities | Senior SWE — Core Engineering / ML Infra | $500k–$1M+ | C++ + sub-25ms latency expertise is a near-perfect quant SWE profile. |
| Jane Street | SWE — Infrastructure | $600k–$1M+ | Single-band high TC; systems depth + low-latency background fits even without OCaml experience. |
| Hudson River Trading | Senior Core Developer / ML Engineer | $500k–$900k | C++ + performance optimization is HRT's bread and butter; ML infra team is growing. |
| Jump Trading | Senior SWE — Core Infrastructure | $500k–$900k | Low-latency C++ + high-throughput data is the canonical Jump profile. |
| Two Sigma | Senior SWE — ML Platform | $400k–$700k | Two Sigma has substantial ML platform investment; data infra + C++ fits well. |
| DE Shaw | Senior SWE — Systems / ML Infra | $400k–$700k | Performance engineering + data systems background fits DESCO's quant + research infra. |

## Summary by tier

| Tier | Count | TC Range |
|---|---|---|
| Frontier AI Labs | 10 | $350k–$1M+ |
| Inference Specialists | 10 | $350k–$600k |
| Big Tech ML Platforms | 12 | $400k–$850k |
| Data/ML Platforms | 8 | $350k–$650k |
| Big Tech ML Product | 7 | $400k–$900k |
| Autonomous Systems | 3 | $400k–$650k |
| Quant/HFT | 6 | $400k–$1M+ |
| **Total** | **56** | — |

(Listed 56 for buffer — drop the bottom of any tier you're least interested in to land at 50.)

## Strategic notes

**Where you're strongest:** Inference specialists (Tier 2), NVIDIA, frontier labs inference teams, and quant firms. Your C++ + low-latency + post-projects inference depth is a rare combination — most ML platform candidates come from Python data engineering backgrounds and lack the systems performance side.

**Where you'll need to stretch:** Frontier AI labs at MTS/Senior levels are very competitive — strong portfolio writeups and referrals matter heavily. ML system design rounds at these companies often probe distributed training depth (which you're studying but not building).

**Underrated targets for your profile:** NVIDIA TensorRT-LLM/Triton teams, Confluent (Kafka pedigree match), Tecton (feature store project literally builds their product), and Citadel/Jane Street (where the C++ + latency premium pays the most for the same skill set).

**Application strategy:** Apply broadly across tiers rather than concentrating on Tier 1. A Senior offer from Tier 2 or 3 is often higher-leverage than a junior MTS at Tier 1, and competing offers materially improve your negotiating position.
