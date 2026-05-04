"""Configuration loader.

Reads ``config/settings.yaml`` and resolves all relative paths against the
project root, so the pipeline behaves the same regardless of the working
directory it is launched from.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


# Project root = two levels up from this file (src/utils/config.py -> project/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load YAML config and inject absolute paths.

    Returns a dict with the same structure as ``settings.yaml``, but with
    ``paths.*`` keys converted to absolute :class:`pathlib.Path` objects.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Resolve paths relative to project root
    cfg["paths"] = {
        key: (PROJECT_ROOT / value).resolve()
        for key, value in cfg["paths"].items()
    }
    cfg["project_root"] = PROJECT_ROOT
    return cfg
