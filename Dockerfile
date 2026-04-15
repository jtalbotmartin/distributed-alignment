# Production multi-stage Dockerfile for distributed-alignment.
#
# Usage:
#   docker build -t distributed-alignment .
#   docker run distributed-alignment --help
#   docker run -v ./data:/data distributed-alignment ingest \
#       --queries /data/queries.fasta --reference /data/reference.fasta --output-dir /data/work
#
# See docker-compose.yml for the full stack (workers + monitoring).

# --- Stage 1: Build ---
FROM python:3.11-slim AS build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml ./
COPY src/distributed_alignment/__init__.py src/distributed_alignment/__init__.py

# Install runtime dependencies only (no dev deps)
RUN uv sync --no-dev --no-install-project 2>/dev/null || true

# Copy source
COPY src/ src/

# --- Stage 2: Runtime ---
FROM python:3.11-slim AS runtime

LABEL maintainer="Jonny"
LABEL version="0.1.0"
LABEL description="Distributed, fault-tolerant protein sequence alignment"

# Install DIAMOND via miniforge + bioconda (works on x86_64 and arm64)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ARG DIAMOND_VERSION=2.1.10
RUN ARCH=$(uname -m) && \
    curl -fsSL "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-${ARCH}.sh" \
        -o /tmp/miniforge.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && \
    rm /tmp/miniforge.sh && \
    /opt/conda/bin/conda install -y -c conda-forge -c bioconda "diamond=${DIAMOND_VERSION}" && \
    /opt/conda/bin/conda clean -afy && \
    ln -s /opt/conda/bin/diamond /usr/local/bin/diamond && \
    diamond version

# Create non-root user
RUN useradd --create-home --shell /bin/bash da && \
    mkdir -p /data && chown da:da /data

# Copy venv and source from build stage
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/src /app/src

WORKDIR /app
ENV PYTHONPATH=src
ENV PATH="/app/.venv/bin:$PATH"

USER da

# Health check — verify the package is importable
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import distributed_alignment" || exit 1

EXPOSE 9090

ENTRYPOINT ["python", "-m", "distributed_alignment"]
CMD ["--help"]
