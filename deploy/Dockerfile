# syntax=docker/dockerfile:1.7
#
# bot_btc_1hr_kalshi — Cloud Run container.
# Minimal Python 3.12 slim base with the numerics stack. Non-root at runtime.
#
# Build: gcloud builds submit --tag <region>-docker.pkg.dev/<project>/bot-btc-1hr-kalshi/bot-btc-1hr-kalshi:latest .

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps for numba (llvmlite), cryptography wheels, and ntpdate for boot clock-drift validation.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      ntpdate \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- Application code (must be present before `pip install .` so hatchling can
# build the wheel against src/bot_btc_1hr_kalshi). We copy pyproject + src + config
# together; a src-only change still invalidates the install layer, which is fine
# for a single-service container. ---------------------------------------------
COPY pyproject.toml ./
COPY src/ ./src/
COPY config/ ./config/

RUN pip install --upgrade pip && pip install .

# ---- Drop to non-root user --------------------------------------------------
RUN useradd --system --uid 10001 --home /app bot-btc-1hr-kalshi && \
    chown -R bot-btc-1hr-kalshi:bot-btc-1hr-kalshi /app
USER bot-btc-1hr-kalshi

# Cloud Run sets PORT; admin HTTP binds to it. Default to 8080 for local runs.
ENV PORT=8080
EXPOSE 8080

# Healthcheck is handled by Cloud Run /readyz probe, not Dockerfile HEALTHCHECK.
ENTRYPOINT ["python", "-m", "bot_btc_1hr_kalshi"]
CMD ["--mode", "paper", "--host", "0.0.0.0", "--port", "8080"]
