# `distributed-alignment` — User Requirements Document (URD)

> **Document ID**: DA-URD-001  
> **Version**: 2.0  
> **Status**: Approved  
> **Author**: Jonny  
> **Date**: April 2026  

---

## 1. Purpose & Scope

This document defines the user-facing requirements for `distributed-alignment`, a distributed protein sequence alignment system. It captures *what* the system must do and *why*, without prescribing *how*. Technical design decisions are deferred to the Technical Design Document (DA-TDD-001).

Large-scale protein sequence alignment is a genuine bottleneck in bioinformatics. Metagenomic studies, phylogenomic surveys, and protein function annotation projects routinely need to align millions of query sequences against reference databases containing hundreds of millions of proteins. Tools like DIAMOND provide the alignment algorithm, but leave the operational challenge — distributing work across machines, recovering from failures, aggregating results, and extracting analytical value — entirely to the user. In practice, this means brittle bash scripts, manual job tracking, and ad-hoc post-processing that's difficult to reproduce.

`distributed-alignment` addresses this gap: a structured, fault-tolerant orchestration layer around DIAMOND that treats large-scale alignment as a data engineering problem. It handles chunking, distributed scheduling, fault recovery, result aggregation, taxonomic enrichment, and ML-ready feature extraction — with full observability and infrastructure defined as code.

This project also reflects a personal interest in understanding distributed systems deeply, having encountered the problem of orchestrating DIAMOND at scale in a real interview setting and wanting to build, not just describe, a solution.

---

## 2. Stakeholders

| Stakeholder | Interest |
|---|---|
| **Bioinformatics researcher** | Needs to align millions of protein sequences against large reference databases reliably, without managing infrastructure manually |
| **ML engineer (downstream)** | Needs alignment results and derived features in a structured, versioned, queryable format ready for model training |
| **Platform/ops engineer** | Needs visibility into pipeline health, resource usage, and cost; needs the system to be deployable and operable via standard tooling |

---

## 3. Functional Requirements

### FR-1: Sequence Ingestion

| ID | Requirement | Priority |
|---|---|---|
| FR-1.1 | The system will accept protein sequences in FASTA format as input | Must |
| FR-1.2 | The system will validate input sequences (valid amino acid alphabet, non-empty, reasonable length bounds) | Must |
| FR-1.3 | The system will split input sequences into deterministic chunks of configurable size | Must |
| FR-1.4 | Chunking will be reproducible: identical input always produces identical chunks regardless of file ordering | Must |
| FR-1.5 | The system will produce a manifest cataloguing all chunks with sequence counts and content checksums | Must |
| FR-1.6 | The system should support streaming ingestion (processing sequences without loading entire files into memory) | Should |

### FR-2: Distributed Alignment

| ID | Requirement | Priority |
|---|---|---|
| FR-2.1 | The system will decompose the alignment problem into independent work packages via the Cartesian product of query and reference chunks | Must |
| FR-2.2 | The system will distribute work packages across multiple workers for parallel execution | Must |
| FR-2.3 | Workers will execute DIAMOND BLAST for each work package | Must |
| FR-2.4 | The system should support configurable DIAMOND sensitivity modes (fast, sensitive, very-sensitive, ultra-sensitive) | Should |
| FR-2.5 | Workers will operate independently with no designated primary/coordinator process | Must |
| FR-2.6 | The system will allow workers to join and leave at runtime without disrupting in-progress work | Must |

### FR-3: Fault Tolerance

| ID | Requirement | Priority |
|---|---|---|
| FR-3.1 | If a worker dies, its in-progress work packages will be detected and re-enqueued for another worker | Must |
| FR-3.2 | Work package claims will be atomic — no two workers can claim the same package | Must |
| FR-3.3 | The system will support configurable retry limits per work package | Must |
| FR-3.4 | Workers will emit heartbeats; the system will detect stale heartbeats and reclaim timed-out packages | Must |
| FR-3.5 | All pipeline operations will be idempotent — safe to re-run without producing incorrect or duplicate results | Must |

### FR-4: Result Aggregation

| ID | Requirement | Priority |
|---|---|---|
| FR-4.1 | The system will merge alignment results across all reference chunks for each query chunk | Must |
| FR-4.2 | The merger will deduplicate and globally rank hits (e.g., top-N per query by e-value) | Must |
| FR-4.3 | Merged results will be stored in a columnar format (Parquet) with enforced schema | Must |
| FR-4.4 | Results will be queryable via SQL without additional infrastructure | Must |

### FR-5: Taxonomic Enrichment

| ID | Requirement | Priority |
|---|---|---|
| FR-5.1 | The system will join alignment hits against NCBI taxonomy data to annotate each hit with taxonomic lineage (kingdom, phylum, class, order, family, genus, species) | Must |
| FR-5.2 | The system will compute per-query taxonomic profiles (e.g., distribution of hits across phyla) | Must |
| FR-5.3 | Taxonomic annotations will be stored alongside alignment results in the same queryable format | Must |

### FR-6: Feature Engineering & ML Readiness

| ID | Requirement | Priority |
|---|---|---|
| FR-6.1 | The system will extract alignment-derived features per query sequence (hit statistics, taxonomic diversity, coverage metrics) | Must |
| FR-6.2 | The system should integrate pre-computed protein language model embeddings (e.g., ESM-2) as a complementary feature stream | Should |
| FR-6.3 | Features will be output as a versioned Parquet feature table with documented schema | Must |
| FR-6.4 | The system should produce exploratory analysis of extracted features (distributions, correlations, clustering) | Should |
| FR-6.5 | The feature pipeline will be framed as one input stream to an ML pipeline — the system provides the data engineering, not the model itself | Must |

### FR-7: Observability

| ID | Requirement | Priority |
|---|---|---|
| FR-7.1 | The system will emit structured JSON logs with correlation IDs across all pipeline stages | Must |
| FR-7.2 | The system will expose Prometheus-compatible metrics (work package states, durations, worker health, throughput) | Must |
| FR-7.3 | The system will provide a monitoring dashboard showing pipeline progress, worker health, error rates, and resource utilisation | Must |
| FR-7.4 | The system should estimate remaining time and projected cost during a run | Should |

### FR-8: Data Catalogue & Lineage

| ID | Requirement | Priority |
|---|---|---|
| FR-8.1 | The system will maintain a metadata catalogue recording all datasets, their creation parameters, schemas, and timestamps | Must |
| FR-8.2 | The system should track data lineage: from input sequence → chunk → work package → alignment hit → feature row | Should |
| FR-8.3 | Catalogue entries should be queryable (e.g., "which runs used this reference database?") | Should |

### FR-9: Results Explorer

| ID | Requirement | Priority |
|---|---|---|
| FR-9.1 | The system should provide an interactive interface for browsing pipeline runs, querying results, and exploring features | Should |
| FR-9.2 | The explorer should support SQL queries against the results database | Should |
| FR-9.3 | The explorer could visualise feature distributions, taxonomic profiles, and data lineage | Could |

---

## 4. Non-Functional Requirements

### NFR-1: Performance

| ID | Requirement | Priority |
|---|---|---|
| NFR-1.1 | Pipeline throughput will scale approximately linearly with the number of workers | Must |
| NFR-1.2 | Coordination overhead (claiming packages, heartbeats) will be negligible relative to alignment compute time | Must |
| NFR-1.3 | The system should provide benchmark results demonstrating scaling behaviour | Should |

### NFR-2: Reliability

| ID | Requirement | Priority |
|---|---|---|
| NFR-2.1 | No single point of failure: the system will continue operating if any single worker or non-critical component fails | Must |
| NFR-2.2 | Data integrity: the system will never produce partial, corrupted, or silently incorrect results | Must |
| NFR-2.3 | All state transitions will be logged for audit purposes | Must |

### NFR-3: Deployability

| ID | Requirement | Priority |
|---|---|---|
| NFR-3.1 | The system will be deployable via containers (Docker) | Must |
| NFR-3.2 | The system will be deployable on Kubernetes with Ray as the compute layer | Must |
| NFR-3.3 | All infrastructure will be defined as code (Terraform) | Must |
| NFR-3.4 | The system will include CI/CD configuration (GitHub Actions) for linting, type-checking, testing, and image building | Must |
| NFR-3.5 | The system will run on a single laptop (docker-compose) for development and demonstration | Must |

### NFR-4: Maintainability

| ID | Requirement | Priority |
|---|---|---|
| NFR-4.1 | Code will pass mypy strict mode type checking | Must |
| NFR-4.2 | Code will pass ruff linting with no exceptions | Must |
| NFR-4.3 | Test coverage should exceed 80% for core modules (scheduler, merger, feature extractor) | Should |
| NFR-4.4 | The project will be packaged as an installable Python library with pyproject.toml | Must |
| NFR-4.5 | Configuration will follow a layered precedence: defaults → config file → environment variables → CLI arguments | Must |

### NFR-5: Documentation

| ID | Requirement | Priority |
|---|---|---|
| NFR-5.1 | The repository will include a README with architecture overview, quickstart, and design rationale | Must |
| NFR-5.2 | Key architectural decisions will be documented as Architecture Decision Records (ADRs) | Must |
| NFR-5.3 | Data contracts (Parquet schemas at each stage boundary) will be documented | Must |
| NFR-5.4 | The project should include a recorded demo video showing the pipeline running with fault recovery | Should |

---

## 5. Constraints

| Constraint | Description |
|---|---|
| **Demo dataset** | The system will be demonstrated with small datasets (~10K reference sequences, ~1K queries) that run in minutes on a laptop. The architecture supports arbitrary scale, but the demo prioritises showing the patterns, not the volume. |
| **DIAMOND dependency** | DIAMOND is an external binary. The system wraps it as a subprocess, does not modify its internals. |
| **Cost** | All infrastructure must run locally (docker-compose) or on free-tier cloud resources. No production cloud spend. |
| **Timeline** | Implementation target: 4-6 weekends of focused work. |

---

## 6. Acceptance Criteria

The project is considered complete when:

1. The pipeline runs end-to-end on the demo dataset, producing queryable alignment results with taxonomic annotations and ML feature tables
2. Fault tolerance is demonstrated: killing a worker mid-run does not corrupt results or stall the pipeline
3. Observability is live: a monitoring dashboard shows real-time pipeline progress and worker health
4. The results explorer allows interactive SQL queries and feature visualisation
5. CI/CD passes: lint, type-check, test, Docker build all green
6. Infrastructure is codified: Terraform configs + Helm chart + docker-compose all functional
7. Documentation is complete: README, ADRs, data contracts, architecture diagram
8. A demo video or recording demonstrates all of the above
