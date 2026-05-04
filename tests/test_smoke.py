"""Smoke tests.

Run with:
    python -m pytest tests/

These tests deliberately avoid spinning up Spark; they cover pure-Python
helpers so that they run quickly and without a JVM.
"""
from __future__ import annotations

from src.utils.config import load_config


def test_config_loads_with_absolute_paths():
    cfg = load_config()
    assert cfg["paths"]["data_dir"].is_absolute()
    assert cfg["paths"]["output_dir"].is_absolute()
    assert "steamspy" in cfg
    assert cfg["validation"]["min_expected_records"] > 0


def test_config_has_required_keys():
    cfg = load_config()
    for key in ("paths", "steamspy", "input_files", "spark", "validation"):
        assert key in cfg, f"Missing top-level config key: {key}"
    for f in ("games", "metadata", "recommendations", "steamspy", "genre_categories"):
        assert f in cfg["input_files"]
