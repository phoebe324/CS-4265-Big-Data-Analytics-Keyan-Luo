"""SteamSpy API ingestion.

Fetches the configured ``request_type`` (default: ``top100owned``) from the
SteamSpy public API and writes a flat CSV to ``<data_dir>/steamspy_data.csv``.

Compared to the M3 version, this module:
  * Reads URL / paths / retry policy from ``config/settings.yaml``
    (no hardcoded absolute paths).
  * Retries on network errors, timeouts, and HTTP 429 / 5xx with backoff.
  * Logs through the shared logger instead of ``print``.
"""
from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

import requests

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Columns we extract from each SteamSpy game record. Stored as a module
# constant so the data dictionary stays in sync with the code.
STEAMSPY_FIELDS = [
    "appid",
    "name",
    "developer",
    "publisher",
    "owners",
    "average_forever",
    "median_forever",
    "ccu",
    "price",
]


def fetch_steamspy(cfg: dict[str, Any]) -> dict[str, dict]:
    """Call the SteamSpy API with retry/backoff.

    Raises:
        requests.HTTPError: if all retries are exhausted.
    """
    spy_cfg = cfg["steamspy"]
    url = spy_cfg["base_url"]
    params = {"request": spy_cfg["request_type"]}
    timeout = spy_cfg["timeout_seconds"]
    max_retries = spy_cfg["max_retries"]
    backoff = spy_cfg["backoff_seconds"]

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("SteamSpy request attempt %d/%d", attempt, max_retries)
            response = requests.get(url, params=params, timeout=timeout)
            # SteamSpy uses 429 for rate limiting; treat 5xx as retryable too.
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"Retryable HTTP {response.status_code}", response=response
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            wait = backoff * attempt
            logger.warning(
                "SteamSpy attempt %d failed (%s). Retrying in %ds.",
                attempt, exc, wait,
            )
            time.sleep(wait)

    logger.error("All %d SteamSpy attempts failed.", max_retries)
    raise RuntimeError("Failed to fetch SteamSpy data") from last_exc


def write_steamspy_csv(data: dict[str, dict], output_path: Path) -> int:
    """Write API payload to a CSV. Returns the number of rows written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(STEAMSPY_FIELDS)
        for _, game in data.items():
            writer.writerow([game.get(field, "") for field in STEAMSPY_FIELDS])
            rows_written += 1
    return rows_written


def download_steamspy(cfg: dict[str, Any] | None = None) -> Path:
    """Top-level entry point. Returns the path to the written CSV."""
    cfg = cfg or load_config()
    data = fetch_steamspy(cfg)
    logger.info("Received %d games from SteamSpy", len(data))

    output_path = cfg["paths"]["data_dir"] / cfg["input_files"]["steamspy"]
    n = write_steamspy_csv(data, output_path)
    logger.info("Wrote %d rows to %s", n, output_path)
    return output_path


if __name__ == "__main__":
    download_steamspy()
