# bot_btc_1hr_kalshi — developer workflow targets.
# Run `make help` for a description of each target.

SHELL := /usr/bin/env bash
PY ?= python3.12
VENV ?= .venv
VENV_PY := $(VENV)/bin/python
VENV_PIP := $(VENV)/bin/pip

.DEFAULT_GOAL := help
.PHONY: help install venv fmt lint typecheck test test-fast replay backtest paper live \
        reconcile shadow clean bq-query docker-build

help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

$(VENV)/bin/activate:
	$(PY) -m venv $(VENV)
	$(VENV_PIP) install --upgrade pip

venv: $(VENV)/bin/activate  ## Create local virtualenv in .venv/

install: venv  ## Install package + dev extras into .venv
	$(VENV_PIP) install -e '.[dev]'

fmt:  ## Format code with ruff
	$(VENV)/bin/ruff format src tests

lint:  ## Lint with ruff
	$(VENV)/bin/ruff check src tests

typecheck:  ## Strict mypy over src/
	$(VENV)/bin/mypy src

test: lint typecheck  ## Full suite (lint + typecheck + unit + integration, excludes slow)
	$(VENV)/bin/pytest -m "not slow"

test-fast:  ## Unit tests only, skip lint/typecheck
	$(VENV)/bin/pytest tests/unit

replay:  ## Replay captured tick data (requires ./data/ticks/ — see scripts/fetch_ticks.sh)
	$(VENV_PY) -m bot_btc_1hr_kalshi.research.replay --data ./data/ticks

backtest:  ## Walk-forward backtest — prints Sharpe / maxDD / hit rate
	$(VENV_PY) -m bot_btc_1hr_kalshi.research.backtest --data ./data/ticks

paper:  ## Live market data, simulated fills, no real orders
	BOT_BTC_1HR_KALSHI_MODE=paper $(VENV_PY) -m bot_btc_1hr_kalshi

shadow:  ## Live feeds + full decision pipeline, orders routed to /dev/null
	BOT_BTC_1HR_KALSHI_MODE=shadow $(VENV_PY) -m bot_btc_1hr_kalshi

live:  ## PRODUCTION — requires RISK_COMMITTEE_SIGNED=yes in env
	@test "$$RISK_COMMITTEE_SIGNED" = "yes" || { echo "refusing live without RISK_COMMITTEE_SIGNED=yes"; exit 1; }
	BOT_BTC_1HR_KALSHI_MODE=live $(VENV_PY) -m bot_btc_1hr_kalshi

reconcile:  ## Reconcile local OMS state vs Kalshi broker state
	$(VENV_PY) -m bot_btc_1hr_kalshi.execution.reconcile

bq-query:  ## Run canned BigQuery tuning queries (default q1; pass Q=q3 etc.)
	./scripts/query_bets.sh $${Q:-q1}

docker-build:  ## Build the Cloud Run container image locally
	docker build -f deploy/Dockerfile -t bot-btc-1hr-kalshi:local .

clean:  ## Remove build artifacts and caches
	rm -rf build dist .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
