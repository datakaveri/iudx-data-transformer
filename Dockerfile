# =============================================================================
# Multi-stage build for the IUDX Data Transformer cron job
# =============================================================================

# ---- Stage 1: dependency builder -------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools needed by some packages (pyarrow wheels are pre-built,
# but keep gcc for any source builds)
RUN apt-get update && apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---- Stage 2: runtime image ------------------------------------------------
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="iudx-data-transformer"
LABEL org.opencontainers.image.description="Incremental ES → Parquet → Object Store transformer"

# Non-root user for security
RUN useradd --create-home --shell /bin/bash transformer

# Copy installed packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Application source
COPY src/ ./

# Runtime directories (checkpoint volume is mounted here)
RUN mkdir -p /data/checkpoints && chown transformer /data/checkpoints

USER transformer

# CONFIG_PATH can be overridden at runtime (e.g. via docker run -e CONFIG_PATH=...)
ENV CONFIG_PATH=/app/config.yaml \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Mount config.yaml at /app/config.yaml via docker-compose volume
CMD ["python", "main.py"]
