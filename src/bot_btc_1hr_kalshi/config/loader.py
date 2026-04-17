"""YAML config loader with env-var substitution.

Supports `${VAR}` and `${VAR:-default}` patterns anywhere in string values.
Secrets (API keys, tokens) stay in env vars only (hard rule #4) — YAML files
reference them by name via `${...}`.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from bot_btc_1hr_kalshi.config.settings import Mode, Settings

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _substitute(value: str, env: dict[str, str]) -> str:
    def repl(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in env:
            return env[name]
        if default is not None:
            return default
        raise KeyError(f"env var {name} is required but not set")

    return _ENV_VAR_PATTERN.sub(repl, value)


def _walk(node: Any, env: dict[str, str]) -> Any:
    if isinstance(node, str):
        return _substitute(node, env)
    if isinstance(node, dict):
        return {k: _walk(v, env) for k, v in node.items()}
    if isinstance(node, list):
        return [_walk(v, env) for v in node]
    return node


def load_settings(
    mode: Mode,
    *,
    config_dir: Path | None = None,
    env: dict[str, str] | None = None,
) -> Settings:
    """Load config/{mode}.yaml, substitute env vars, validate into `Settings`.

    `config_dir` defaults to `./config/`. `env` defaults to os.environ.
    """
    cfg_dir = config_dir if config_dir is not None else Path.cwd() / "config"
    env_map = dict(env) if env is not None else dict(os.environ)

    path = cfg_dir / f"{mode}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"config file not found: {path}")

    with path.open("r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a YAML mapping at the top level")

    substituted = _walk(raw, env_map)
    return Settings.model_validate(substituted)
