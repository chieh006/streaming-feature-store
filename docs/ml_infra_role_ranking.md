# ML Infrastructure Role Ranking — Fit × Hiring Probability

Companion to [ml_infra_role_targets.md](ml_infra_role_targets.md). The source doc lists 56 roles grouped by company tier; this doc re-sorts them by **realistic landing probability** for the target candidate profile (see Background summary below).

> **Last re-scored:** 2026-05-24, after the Background summary in the source doc was expanded with LinkedIn details — Senior SWE title held at ASML (3+ yrs), unified data model / semantic layer work, 10x Cassandra upsert rate, and 20% memory reduction. Same-day follow-up added two alternate-level entries (NVIDIA Senior DGX, Google L5 Vertex) and a Level-downgrade options section. The "What changed in this revision" section at the bottom lists the specific row moves.

## Background summary (used for fit reasoning)

Mirrored from [ml_infra_role_targets.md](ml_infra_role_targets.md); keep in sync if either doc is edited.

- ~5 years C++ + high-throughput data systems: Senior SWE at ASML (Feb 2023–present, San Jose) and SWE at First Orion (Jun 2021–Feb 2023, Little Rock)
- ASML: architected a unified data model + semantic layer bridging legacy inspection data into modern ML platforms; powered UI analytics over 100M+ defect data points for cross-team consumers
- 8x throughput + 20% memory reduction via production C++ data structures with tuned accuracy/speed trade-offs; 20x ingestion acceleration via a low-latency mmap data access layer feeding ML training/processing
- First Orion: engineered a C++/ZeroMQ interface that drove a 10x increase in Cassandra upsert rates, ingesting 600M+ daily call records (mobile + landline) into ML-ready pipelines
- Resolved bottlenecks in a custom TCP network client to keep telephony ML applications inside a strict <25ms hard processing limit
- CI/CD + Pytest framework for distributed REST services and NoSQL stores, guaranteeing data-flow integrity for mission-critical, time-sensitive applications
- Phase 1 project: Kafka + Flink + Redis feature store with online/offline consistency
- Phase 2 project: vLLM-style inference engine with paged KV-cache and continuous batching in C++/Python

## Ranking methodology

Each role was scored against four factors:

1. **Direct skill match** — does the day-to-day work overlap with C++ low-latency systems, the Phase 1 feature store project, or the Phase 2 inference engine project?
2. **Portfolio leverage** — how much do the writeups + code carry the interview, vs. needing to lean on credentials not held (PhD, prior ML-infra production tenure, niche language like OCaml)?
3. **Hiring bar realism** — is the target level (Senior / Staff / MTS) plausible for ~5 YOE with the listed background, or a stretch? (Note: Senior SWE title already held at ASML materially de-risks Staff-level bids.)
4. **Pipeline velocity** — does the team hire often and move quickly, or is it a slow, referral-gated funnel?

Tiers below reflect the combined score, not the source doc's company groupings.

## S-Tier — Highest fit, most realistic to land

| Rank | Company | Role | Why this rank |
|---|---|---|---|
| 1 | Tecton | Senior SWE — Feature Platform | Phase 1 project is literally their product. Portfolio walks itself in. |
| 2 | NVIDIA | Senior SWE — TensorRT-LLM | C++ + KV-cache + paged attention is the exact stack. Team hires aggressively. |
| 3 | NVIDIA | Senior SWE — Triton Inference Server | C++ serving + scheduling is core. Same hiring pattern as TRT-LLM. |
| 4 | Together AI | Senior SWE — Inference Engine | Direct product/skill overlap; lean team values end-to-end depth. |
| 5 | Fireworks AI | Senior SWE — Inference Performance | C++ kernel/serving work is their moat. |
| 6 | Baseten | Senior SWE — Model Serving Platform | Truss/serving maps cleanly to the inference project. |
| 7 | Confluent | Staff SWE — Streaming Platform | Kafka project + ZeroMQ→Kafka transition + C++ is a top-decile Confluent profile. |
| 8 | Pinecone | Senior SWE — Vector Database | C++ + low-latency data structures is rare in their pipeline; 10x Cassandra upsert is a direct storage-perf signal. |
| 9 | Uber | Staff SWE — Michelangelo | "Bridging legacy → modern ML platforms" is literally Michelangelo's mandate. Combined with the feature store project, this is now nearly as strong as Tecton. Senior title de-risks the Staff bid. |

## A-Tier — Strong fit, competitive but achievable

| Rank | Company | Role | Why this rank |
|---|---|---|---|
| 10 | Anyscale | Senior SWE — Ray Serve / LLM Inference | Continuous batching + scheduling overlap. |
| 11 | LinkedIn | Staff SWE — AI Platform | Kafka + feature store pedigree (Pro-ML / Feathr lineage); Staff bid de-risked by Senior title already held. |
| 12 | Perplexity | Senior SWE — Inference Infrastructure | Needs C++ inference depth badly for unit economics. |
| 13 | Snowflake | Senior/Staff SWE — Cortex AI | Semantic layer + data-platform-meets-ML is dead center for Cortex. The ASML unified data model work is the differentiated signal here. |
| 14 | Pinterest | Staff SWE — ML Platform | High-throughput serving + feature store; Staff bid de-risked by Senior title already held. |
| 15 | Airbnb | Senior SWE — ML Infrastructure | Bighead/Chronon scope is direct. |
| 16 | DoorDash | Senior SWE — ML Platform | Real-time logistics ML, low-latency premium. |
| 17 | Modal | Senior SWE — Serverless GPU Infra | Polymath profile fits lean infra team. |
| 18 | Replicate | Senior SWE — Model Serving | Cold-start + scheduling fit. |
| 19 | Databricks | Senior SWE — ML Platform | Kafka + feature store + Delta alignment. |
| 20 | Groq | Senior SWE — Compiler & Runtime | C++ runtime work is rare; strong fit. |
| 21 | Cerebras | Senior SWE — ML Systems Software | High-throughput data movement match. |
| 22 | SambaNova | Senior SWE — Runtime/Inference | C++ runtime + scheduling. |
| 23 | Databricks | Staff SWE — Mosaic AI / Inference | Dual fit (data platform + inference); Senior title already held removes most of the Staff-stretch concern. |
| 24 | Mistral | Senior SWE — Inference | Open-source serving stack lines up. |
| 25 | MongoDB | Staff SWE — Vector Search / AI | 10x Cassandra upsert rate is a direct database-internals perf signal that wasn't surfaced before. Senior title de-risks the Staff bid. |
| 26 | Reka AI | Senior SWE — ML Infrastructure | End-to-end profile valued at smaller labs. |
| 27 | Tesla | Senior SWE — AI Infra (Dojo / Autopilot) | C++ + high-throughput data pipelines. |
| 28 | NVIDIA | Senior SWE — DGX Cloud / AI Infra *(alternate-level — see also Staff at rank 56)* | C++ + high-throughput data + training-pipeline experience is a clean L4/L5 NVIDIA bid. Senior bar is well within reach; team is staffing aggressively. |
| 29 | Waymo | Senior SWE — ML Infrastructure | Sensor ingestion + training pipelines. |
| 30 | Applied Intuition | Senior SWE — Simulation/ML Infra | C++ perf + data systems. |
| 31 | Hudson River Trading | Senior Core Dev / ML Eng | C++ perf is their bread and butter. |
| 32 | Jump Trading | Senior SWE — Core Infrastructure | Canonical Jump profile. |
| 33 | Citadel / Citadel Securities | Senior SWE — Core / ML Infra | Sub-25ms C++ is exactly what they pay for. |
| 34 | Two Sigma | Senior SWE — ML Platform | Data infra + C++ fits. |
| 35 | DE Shaw | Senior SWE — Systems / ML Infra | Perf engineering + data systems fits DESCO. |

## B-Tier — Good fit, higher bar or more competition

| Rank | Company | Role | Why this rank |
|---|---|---|---|
| 36 | xAI | Senior SWE — Inference | Hires fast, C++/CUDA premium; bar is high but pipeline moves quickly. |
| 37 | xAI | Senior SWE — Training Cluster Infra | Same as above, slightly less direct match. |
| 38 | Anthropic | Senior SWE — Inference | Direct fit; bar and competition are very high. |
| 39 | Anthropic | Senior SWE — Model Serving Infra | Same shape as above. |
| 40 | Anthropic | Senior SWE — Data Platform | Feature store + ASML data modeling helps. |
| 41 | OpenAI | MTS — Inference | Highest fit at OAI; very competitive, referral-dependent. |
| 42 | OpenAI | MTS — Training Infrastructure | 600M+ records/day translates; competitive. |
| 43 | Google DeepMind | Senior SWE — Gemini Serving | Strong fit, slower process, level placement risk. |
| 44 | Meta | E5 SWE — GenAI Inference | Direct fit at E5; Meta hiring is structured/predictable. |
| 45 | Meta | E5 SWE — PyTorch Core / AI Infra | C++ + Python interop fits PyTorch internals. |
| 46 | Google | L5 SWE — Vertex AI Inference *(alternate-level — see also Staff at rank 51)* | L5 conversion is roughly 1.5× the Staff bid; TC ~$350–500k still clears the doc's floor. Use this as the primary Google Vertex bid; keep Staff as the stretch shot. |
| 47 | Amazon | L6 SDE — Bedrock Model Serving | Direct serving match. |
| 48 | Amazon | L6 SDE — SageMaker Inference | Hosting team match. |
| 49 | Microsoft | Principal SWE — Azure ML / AOAI | Title is Senior-equivalent at MS; fit is solid. |
| 50 | Apple | ICT4 — ML Platform / Foundation Models | Performance C++ fits; process is opaque. |
| 51 | Google | Staff SWE — Vertex AI Inference | Serving + data infra; Senior title de-risks the Staff bid. Use as the stretch shot alongside the L5 entry at rank 46. |
| 52 | Stripe | L5 SWE — ML Platform | Radar/fraud low-latency match; high bar generally. |
| 53 | Meta | E6 Staff SWE — ML Platform | Still ambitious, but no longer a structural stretch now that Senior title is established. |
| 54 | Weights & Biases | Senior SWE — Platform / Infrastructure | Training infra side is the fit. |
| 55 | Netflix | Senior SWE — ML Platform | Pays top of band; very small hiring funnel. |
| 56 | NVIDIA | Staff SWE — DGX Cloud / AI Infra | Staff-level stretch eases with Senior title held; distributed training depth still the missing lever. Use as the stretch shot alongside the Senior entry at rank 28. |
| 57 | Apple | ICT5 — ML Infrastructure | Staff-level stretch eases with Senior title held; Apple process remains opaque. |

## C-Tier — Stretch (level mismatch or skill gap)

| Rank | Company | Role | Why this rank |
|---|---|---|---|
| 58 | Jane Street | SWE — Infrastructure | TC is great, but OCaml-heavy stack and culture-fit bar make conversion rate low even for strong C++ candidates. |

## Level-downgrade options

Where a company's Staff/Principal role is in B-tier or below, it's worth asking whether a Senior bid at the same company would convert better. Some of these create new entries; some collapse into existing rows; one is a deliberate skip.

| Company / role | Decision | Rationale |
|---|---|---|
| NVIDIA — DGX Cloud / AI Infra | **Added as alternate-level (Senior at rank 28; Staff at rank 56)** | Senior bar is well within reach for this profile; TC ~$350–500k still clears the floor. Senior is the primary bid, Staff is the stretch. |
| Google — Vertex AI Inference | **Added as alternate-level (L5 at rank 46; Staff at rank 51)** | L5 conversion is roughly 1.5× Staff; TC still clears the floor. Primary bid is L5, Staff is the stretch. |
| Microsoft — Azure ML / AOAI | **Skip Senior; keep Principal only (rank 49)** | Microsoft Senior — TC drops below the doc's $350k floor and you'd be downleveling from your current ASML title. Skip unless the team is uniquely interesting. |
| Meta — ML Platform | **No new entry needed** | Meta E6 (rank 53) downgraded to E5 collapses into the existing E5 GenAI Inference (44) / E5 PyTorch (45) entries. Adding a third E5 ML Platform row would be duplicative. |
| Apple — ML Infrastructure | **No new entry needed** | Apple ICT5 (rank 57) downgraded to ICT4 collapses into the existing ICT4 ML Platform / Foundation Models entry (rank 50). |

## Funnel strategy

- **Lead wave (apply first):** all 9 S-Tier roles + top 10 of A-Tier (ranks 10–19). High expected conversion; treat as the realistic landing zone.
- **Offer-leverage wave:** B-Tier process-predictable roles — Meta E5 (44–45), Google L5 Vertex (46), Amazon L6 (47–48), Microsoft Principal (49). These generate competing offers on a known timeline and materially improve negotiating position.
- **High-variance wave:** Frontier labs (38–42) and Google DeepMind (43). Treat as lottery tickets; do not pace timeline around them. A referral is roughly worth one full tier of probability lift here.
- **Skip unless a referral materializes:** C-Tier (58).

## When to revisit this ranking

Re-score after each of the following events, since they materially shift the probabilities:

- Phase 1 / Phase 2 project writeups published — moves S-Tier conversion from "likely" to "near-certain" and lifts inference-specialist A-Tier roles into S consideration.
- A referral lands at a Frontier lab — that specific row jumps one full tier.
- Distributed training depth added (multi-node, FSDP, sharded checkpointing) — lifts OpenAI Training Infra (42), xAI Training (37), and NVIDIA DGX Staff (56) by roughly half a tier each. NVIDIA DGX Senior (28) is already at its expected ceiling for this profile.
- Market shift (frontier lab hiring freeze, inference-startup funding round) — re-evaluate Tier 1 / Tier 2 from the source doc wholesale.

## What changed in this revision

Triggered by the Background summary refresh on 2026-05-24 (LinkedIn detail added to the source doc):

| Role | Old rank | New rank | Driver |
|---|---|---|---|
| Uber — Staff SWE, Michelangelo | 9 (A) | 9 (S) | Legacy → modern ML platform bridging is Michelangelo's mandate; Senior title de-risks Staff bid. |
| Snowflake — Senior/Staff SWE, Cortex AI | 19 | 13 | ASML unified data model / semantic layer work is dead center for Cortex. |
| LinkedIn — Staff SWE, AI Platform | 12 | 11 | Staff bid de-risked by Senior title already held. |
| Pinterest — Staff SWE, ML Platform | 14 | 14 (de-risked) | Position unchanged numerically; Staff bid now de-risked. |
| Databricks — Staff SWE, Mosaic AI / Inference | 49 (B) | 23 (A) | Senior title removes most Staff-stretch concern; dual fit. |
| MongoDB — Staff SWE, Vector Search / AI | 50 (B) | 25 (A) | 10x Cassandra upsert is direct database-internals signal. |
| Meta — E6 Staff SWE, ML Platform | 53 (C) | 53 (B) | No longer a structural stretch with Senior title established. |
| NVIDIA — Staff SWE, DGX Cloud / AI Infra | 54 (C) | 56 (B) | Same logic; distributed training depth still the missing piece. |
| Apple — ICT5, ML Infrastructure | 55 (C) | 57 (B) | Same logic. |
| **NVIDIA — Senior SWE, DGX Cloud / AI Infra** | *(new)* | **28 (A)** | Alternate-level entry. Senior bar lifts conversion materially while TC still clears the floor. |
| **Google — L5 SWE, Vertex AI Inference** | *(new)* | **46 (B)** | Alternate-level entry. L5 conversion ~1.5× Staff; TC still clears the floor. |

Unchanged: all S-tier inference specialists (Phase 2 project drives those; new background didn't strengthen them further), Frontier labs (gating factor is referrals + distributed training depth, not title), Jane Street (OCaml/culture-fit problem unchanged).

Deliberately not added as alternate-level entries: Microsoft Senior (TC below floor + downleveling), Meta E5 ML Platform (covered by existing E5 rows), Apple ICT4 ML Infra (covered by existing ICT4 row). See the Level-downgrade options table for the full reasoning.
