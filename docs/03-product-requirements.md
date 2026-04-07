# `distributed-alignment` — Product Requirements Document (PRD)

> **Document ID**: DA-PRD-001  
> **Version**: 2.0  
> **Status**: Approved  
> **Author**: Jonny  
> **Date**: April 2026  
> **Related**: DA-URD-001 (User Requirements), DA-TDD-001 (Technical Design)  

---

## 1. Product Vision

`distributed-alignment` makes large-scale protein sequence alignment reliable, observable, and ML-ready — without requiring users to become distributed systems engineers.

Metagenomic and comparative genomics studies routinely need to align millions of protein sequences against databases of hundreds of millions. DIAMOND provides the alignment algorithm, but the operational challenge — distributing work, recovering from failures, aggregating results, and extracting analytical value — is left entirely to the user. Most researchers solve this with brittle bash scripts, manual job tracking, and post-hoc data wrangling.

`distributed-alignment` fills this gap: a structured, fault-tolerant orchestration layer around DIAMOND that treats alignment as a data engineering problem, not a scripting problem.

---

## 2. Problem Statement

### 2.1 Who has this problem?

**Bioinformatics researchers** conducting large-scale metagenomic surveys, phylogenomic studies, or protein function annotation projects. They need to align query proteins (from sequenced samples) against reference databases (UniRef, NCBI nr) that contain hundreds of millions of sequences.

**ML engineers in biotech** who need alignment-derived features as inputs to protein function prediction, taxonomic classification, or novelty detection models. They need structured, versioned, queryable feature tables — not raw DIAMOND output files.

**Data engineers in life sciences** who build and operate the platforms that serve both groups above. They need systems that are observable, fault-tolerant, and deployable with standard tooling.

### 2.2 How is this currently handled?

Teams approach large-scale alignment with varying levels of sophistication:

**Ad-hoc scripting**: At the simpler end, a researcher splits input FASTA with a bash script, submits Slurm jobs manually, concatenates outputs with `cat`, and wrangles results in pandas. This works for small studies but breaks down at scale — a failed job means manual re-submission, results are difficult to reproduce, and there's no structured handoff to downstream consumers.

**Workflow managers** (NextFlow, Snakemake, CWL): More mature teams use workflow managers that handle job submission, dependency tracking, and basic retry logic. These solve the job orchestration problem well, particularly in HPC environments. However, they're typically focused on the bioinformatics workflow itself rather than the data engineering concerns around the results — schema enforcement, feature extraction, cataloguing, and ML-readiness are usually handled separately or ad-hoc.

**Custom platform code**: Some larger groups build internal tooling around their alignment pipelines — job tracking databases, result parsers, quality checks. These can be effective but tend to be tightly coupled to specific infrastructure (e.g., a particular Slurm cluster) and are rarely designed for portability across environments.

### 2.3 Common gaps across approaches

Even well-implemented solutions tend to leave gaps in specific areas:

- **Fault tolerance beyond retry**: Workflow managers retry failed jobs, but few handle mid-execution worker death gracefully or provide visibility into *why* things failed (OOM vs. transient error vs. data issue).
- **Observability**: "How far along is the alignment?" usually means checking file counts or log files. Real-time dashboards with throughput, error rates, and cost estimates are rare outside production platform teams.
- **Reproducibility across environments**: A NextFlow pipeline tuned for one Slurm cluster may not transfer easily to a cloud K8s environment. Infrastructure portability is typically an afterthought.
- **Schema enforcement and data contracts**: Results are often written as TSV and parsed ad-hoc by downstream consumers. Silent corruption (truncated files, encoding issues, schema drift) can propagate undetected.
- **ML-readiness**: Turning alignment results into structured, versioned, queryable feature tables for model training is almost always a separate manual process.
- **Data lineage**: Tracing a feature table row back to the specific alignment run, parameters, and input data that produced it typically requires manual archaeology.

---

## 3. Success Metrics

| Metric | Target |
|---|---|
| End-to-end pipeline runs on demo data | < 5 minutes on a laptop (docker-compose) |
| Fault recovery works | Killing a worker mid-run does not stall or corrupt the pipeline |
| Results are queryable | SQL queries via DuckDB return correct, ranked alignment results |
| Features are ML-ready | Parquet feature table with documented schema, version, lineage |
| Scaling is demonstrable | Benchmark showing approximately linear throughput scaling with worker count |
| Infrastructure is reproducible | `terraform apply` + `docker-compose up` → working system |
| Code quality passes automated checks | mypy strict, ruff, >80% test coverage, CI green |
| Architecture decisions are documented | ADRs for every major technical choice |

---

## 4. Scope

### 4.1 In scope

- Distributed DIAMOND alignment orchestration with fault tolerance
- FASTA ingestion with validation and deterministic chunking
- Result merging, deduplication, and ranking
- Taxonomic enrichment via NCBI taxonomy
- ML feature extraction (alignment statistics + ESM-2 embeddings)
- Observability: structured logging, Prometheus metrics, Grafana dashboard
- Data catalogue with lineage tracking
- Infrastructure as code (Terraform + K8s + docker-compose)
- Interactive results explorer (FastAPI + HTMX)
- CI/CD pipeline
- Engineering documentation (URD, TDD, PRD, ADRs, data contracts)

### 4.2 Out of scope

- Training ML models on the extracted features (the feature table is the deliverable, not a model)
- Production cloud deployment with real cost (local Kind cluster + docker-compose only)
- Custom DIAMOND modifications (it's a wrapped binary)
- Real-time streaming alignment (batch-only)
- Multi-tenant access control (single-user system)
- GPU acceleration (DIAMOND is CPU-based)

### 4.3 Stretch goals (if time permits)

- Slurm compatibility layer (sbatch script generator for HPC environments)
- S3-backed work stack (conditional PUT implementation)
- Terraform AWS environment (EKS + S3)
- Streaming ingestion from FASTQ sources
- Data lineage visualisation (DAG rendering in the explorer)
- Cost model comparison (spot vs. on-demand vs. HPC)

---

## 5. User Journeys

### Journey 1: Run an alignment pipeline

```
Researcher has: 
  - A FASTA file with 100K metagenomic protein sequences
  - A reference database (UniRef50 subset, 10K sequences)

1. $ distributed-alignment ingest --queries proteins.fasta --reference uniref50.fasta
   → Validates, chunks, produces manifest
   → "Ingested 100,000 queries into 10 chunks, 10,000 references into 2 chunks"

2. $ distributed-alignment run --sensitivity very-sensitive --workers 4
   # --sensitivity maps to DIAMOND's sensitivity modes, controlling the
   # trade-off between alignment speed and detection of distant homologs
   → Generates 20 work packages (10 × 2)
   → Spawns 4 Ray workers
   → Workers claim and process packages
   → Dashboard at http://localhost:3000 shows live progress
   → "Pipeline complete: 20/20 packages, 0 failures, 4m 32s"

3. $ distributed-alignment status
   → "Run run_20260404: COMPLETED, 100K queries, 247K hits, 4m 32s"

4. $ distributed-alignment explore
   → Opens browser to http://localhost:8000
   → Browse results, run SQL queries, explore features
```

### Journey 2: Recover from failure

```
Pipeline is running, 12/20 packages complete.
Worker 3 is killed (OOM, node failure, etc.)

→ Worker 3's in-progress package (wp_q005_r001) stops sending heartbeats
→ After 120 seconds, the reaper detects the stale heartbeat
→ wp_q005_r001 is moved back to PENDING (attempt 2 of 3)
→ Worker 1 (between jobs) claims wp_q005_r001 and processes it
→ Pipeline completes normally

Dashboard shows:
  - Worker 3: OFFLINE (heartbeat stale)
  - wp_q005_r001: retried, now COMPLETED
  - No data loss, no corruption
```

### Journey 3: Explore results and features

```
Researcher opens the explorer at http://localhost:8000

1. Run Browser: sees run_20260404, clicks in
   → Summary: 100K queries, 247K hits, 10 chunks, 20 packages, 4m 32s
   → Cost estimate: 0.3 CPU-hours, ~£0.01

2. SQL Console: runs pre-loaded query
   → "Proteins with hits in >5 phyla"
   → Returns 342 proteins with broad taxonomic reach
   → These might be horizontally transferred genes — interesting!

3. Feature Explorer:
   → Histogram of taxonomic_entropy: bimodal distribution
   → UMAP of feature vectors: clear clusters emerge
   → Coloured by num_phyla: clusters correspond to taxonomic breadth
   → Features look sensible for a downstream classifier

4. Lineage Viewer:
   → Clicks on a feature row for protein_X
   → Sees: protein_X → chunk q003 → work packages wp_q003_r* → merged hits → enriched → features
   → Full provenance from raw sequence to final feature
```

### Journey 4: Re-run with new data (incremental processing)

```
A week later, the researcher has 20K additional query sequences from a new sample.

1. $ distributed-alignment ingest --queries new_sample.fasta --reference uniref50.fasta
   → Chunking uses deterministic hashing, so existing sequences hash to the same chunks
   → Only chunks containing new sequences are flagged as changed
   → "Ingested 20,000 new queries. 3 chunks modified, 7 unchanged."

2. $ distributed-alignment run --sensitivity very-sensitive --workers 4
   → Only generates work packages for modified query chunks × all ref chunks
   → 6 work packages instead of 20 (3 modified query chunks × 2 ref chunks)
   → "Pipeline complete: 6/6 packages, 0 failures, 1m 12s"

3. $ distributed-alignment status
   → Shows both runs with lineage linking them to the shared reference database
```

### Journey 5: Investigate a poisoned work package

```
A pipeline run completes with 19/20 packages succeeded and 1 poisoned.

1. $ distributed-alignment status --run run_20260410
   → "19 COMPLETED, 1 POISONED: wp_q007_r001 (3 attempts exhausted)"

2. $ distributed-alignment logs --package wp_q007_r001
   → Structured JSON logs for all 3 attempts:
     Attempt 1: DIAMOND exit code 137 (OOM) after 12m — worker had 8GB, chunk needed more
     Attempt 2: Same OOM on a different worker (same resource limits)
     Attempt 3: Same

3. Researcher increases worker memory in config and re-runs:
   $ distributed-alignment run --resume run_20260410 --worker-memory 16g
   → Only the poisoned package is re-attempted
   → "Pipeline complete: 1/1 packages, 0 failures, 18m 04s"
   → Merger re-runs for the affected query chunk, producing updated results
```

---

## 6. Design Process

This project follows a structured engineering design process:

```
User Requirements (URD)     — What the system needs to do and why
        │
        ▼
Product Requirements (PRD)  — Scope, success criteria, user journeys
        │
        ▼
Technical Design (TDD)      — Architecture, components, data contracts
        │
        ├── Architecture Decision Records (ADRs) — Rationale for each major choice
        │
        ▼
Implementation Phases       — Ordered delivery with clear milestones
        │
        ▼
Testing Strategy            — How we verify correctness and resilience
        │
        ▼
CI/CD Pipeline              — Continuous quality assurance
        │
        ▼
Documentation & Demo        — Making the work accessible
```

These documents are maintained in the repository alongside the code. Changes to requirements flow through the design documents before reaching the implementation.

---

## 7. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| DIAMOND installation complexity on different platforms | Medium | High | Docker containerisation eliminates platform dependency |
| Demo dataset too small to show meaningful scaling | Low | Medium | Architecture and benchmarks demonstrate the patterns; scale isn't the point of the demo |
| ESM-2 embeddings too computationally expensive for demo | Medium | Low | Pre-compute on small dataset or use cached embeddings. Feature is supplementary, not core |
| NCBI taxonomy dump too large for quick setup | Medium | Low | Provide a pre-processed subset for demo; script for full download |
| Scope creep into ML model training | Medium | High | Feature table is the explicit deliverable; out-of-scope section and feature engineering documentation make the boundary clear |
| K8s/Terraform complexity for a solo project | Medium | Medium | Local Kind cluster keeps it manageable. Cloud deployment is a stretch goal only |

---

## 8. Timeline

| Phase | Duration | Deliverables | Milestone |
|---|---|---|---|
| **Phase 1: Core MVP** | 2 weekends | Ingest → chunk → schedule → align → merge | Pipeline runs end-to-end |
| **Phase 2: Fault tolerance** | 2 weekends | Multi-worker, heartbeats, retry, Ray, Docker, CI/CD | Worker death doesn't break pipeline |
| **Phase 3: Enrichment & features** | 1-2 weekends | Taxonomy, features, ESM-2, catalogue, notebook | ML-ready feature table |
| **Phase 4: Infrastructure & explorer** | 1-2 weekends | Terraform, K8s, explorer UI, cost tracking | Infrastructure as code works |
| **Phase 5: Polish & demo** | 1 weekend | README, ADRs, demo video, contracts doc | Ready for others to evaluate |
| **Total** | ~8-10 weekends | | |

---

## 9. Definition of Done

The project is complete when:

1. **Functional**: Pipeline runs end-to-end on demo data via `docker-compose up` + single CLI command
2. **Fault-tolerant**: Killing a worker during a run doesn't corrupt results or stall the pipeline
3. **Observable**: Grafana dashboard shows real-time pipeline health; structured logs are queryable
4. **ML-ready**: Feature table exists in Parquet with versioned schema, documented features, and lineage
5. **Deployable**: Terraform + K8s manifests provision a working local cluster; docker-compose works standalone
6. **Tested**: CI passes with >80% coverage on core modules; contract tests cover all boundaries
7. **Documented**: README, URD, PRD, TDD, ADRs, data contracts all complete and consistent
8. **Demonstrable**: A demo video or walkthrough shows the complete workflow including fault recovery
9. **Queryable**: Results explorer with SQL console returns meaningful insights from the alignment data
