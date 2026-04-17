# syntax=docker/dockerfile:1.7
#
# bot_btc_1hr_kalshi — Cloud Run container.
# Minimal Python 3.12 slim base with numerics stack. No privileged binaries,
# no root at runtime.
#
# Build: gcloud builds submit --tag <region>-docker.pkg.dev/<project>/bot-btc-1hr-kalshi/bot-btc-1hr-kalshi:latest .

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps for numba (llvmlite), pyarrow, and ntpdate (clock validation at boot)
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      ntpdate \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- Python deps layer (cached unless pyproject changes) --------------------
COPY pyproject.toml uv.lock* requirements.txt* ./
RUN pip install --upgrade pip && \
    if [ -f requirements.txt ]; then \
        pip install -r requirements.txt; \
    elif [ -f pyproject.toml ]; then \
        pip install . ; \
    else \
        echo "No dependency manifest found — failing build" && exit 1; \
    fi

# ---- Application code -------------------------------------------------------
COPY src/ ./src/
COPY config/ ./config/

# ---- Drop to non-root user --------------------------------------------------
RUN useradd --system --uid 10001 --home /app bot-btc-1hr-kalshi && \
    chown -R bot-btc-1hr-kalshi:bot-btc-1hr-kalshi /app
USER bot-btc-1hr-kalshi

# Cloud Run sets PORT; our admin HTTP binds to it.
ENV PORT=8080
EXPOSE 8080

# Healthcheck is handled by Cloud Run /readyz probe, not Dockerfile HEALTHCHECK.
ENTRYPOINT ["python", "-m", "bot_btc_1hr_kalshi"]
