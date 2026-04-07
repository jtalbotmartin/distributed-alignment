# `distributed-alignment`

A distributed, fault-tolerant protein sequence alignment system built around DIAMOND BLAST.

Decomposes large-scale alignment problems into independent work packages, distributes them across elastic workers via Ray, and produces ML-ready feature tables — with full observability and infrastructure as code.

## Status

🚧 Under active development. See [`docs/task-tracker.md`](docs/task-tracker.md) for progress.

## Documentation

- [User Requirements](docs/01-user-requirements.md)
- [Technical Design](docs/02-technical-design.md)
- [Product Requirements](docs/03-product-requirements.md)
- [Architecture Decision Records](docs/adr/)
