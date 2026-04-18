# bot_btc_1hr_kalshi — developer workflow targets.
# Run `make help` for a description of each target.

SHELL := /usr/bin/env bash
PY ?= python3.12
VENV ?= .venv
VENV_PY := $(VENV)/bin/python
VENV_PIP := $(VENV)/bin/pip

.DEFAULT_GOAL := help
.PHONY: help install venv fmt lint typecheck clock-lint test test-fast replay backtest paper live \
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

# Hard rule #5: trading logic must use the injected clock, never the wall
# clock directly. Ruff DTZ catches datetime.now() / datetime.utcnow() with
# no tz — but does not catch time.time(), time.time_ns(), time.monotonic(),
# or time.monotonic_ns(), which would sneak past the lint. scripts/
# check_clock_usage.py walks src/ as AST (no docstring false positives)
# and allow-lists the one file permitted to call time.time_ns() — obs/
# clock.py, which IS the SystemClock implementation.
clock-lint:  ## AST-check src/ for banned wall-clock calls (hard rule #5)
	$(VENV_PY) scripts/check_clock_usage.py

test: lint typecheck clock-lint  ## Full suite (lint + typecheck + clock-lint + unit + integration, excludes slow)
	$(VENV)/bin/pytest -m "not slow"

test-fast:  ## Unit tests only, skip lint/typecheck
	$(VENV)/bin/pytest tests/unit

replay:  ## Replay captured tick data (requires ./data/ticks/ — see scripts/fetch_ticks.sh)
	$(VENV_PY) -m bot_btc_1hr_kalshi.research.replay --data ./data/ticks

backtest:  ## Replay tick archive -> Sharpe / maxDD / hit rate. Requires MARKET, STRIKE_USD.
	$(VENV_PY) -m bot_btc_1hr_kalshi.research.backtest_cli \
	  --archive-dir $${ARCHIVE_DIR:-./archive} \
	  --market $${MARKET:?set MARKET=KBTC-...} \
	  --strike-usd $${STRIKE_USD:?set STRIKE_USD=60000} \
	  --bankroll $${BANKROLL:-1000} \
	  $${FROM:+--from $$FROM} $${TO:+--to $$TO}

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
