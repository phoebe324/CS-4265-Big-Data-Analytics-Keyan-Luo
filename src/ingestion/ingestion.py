"""Spark data ingestion.

Loads all input sources from disk into Spark DataFrames and verifies that
each required file exists before invoking Spark. Missing inputs raise a
clear ``FileNotFoundError`` instead of cryptic Spark exceptions.

The Wikidata file is optional: if it is absent, the pipeline still runs but
the Wikidata join is skipped. This lets reviewers run the pipeline
end-to-end without first hitting the SPARQL endpoint, while still
exercising the full join when the file is present.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _require(path: Path) -> Path:
    """Raise FileNotFoundError if ``path`` is missing or empty."""
    if not path.exists():
        raise FileNotFoundError(f"Required input file is missing: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Required input file is empty: {path}")
    return path


def load_data(
    spark: SparkSession,
    cfg: dict[str, Any],
) -> dict[str, DataFrame]:
    """Load all input data sources.

    Returns a dict keyed by logical source name. The ``wikidata`` key is
    only populated when the corresponding file is present; otherwise it
    maps to ``None``.
    """
    data_dir: Path = cfg["paths"]["data_dir"]
    files = cfg["input_files"]

    logger.info("Loading data from %s", data_dir)

    games_path = _require(data_dir / files["games"])
    metadata_path = _require(data_dir / files["metadata"])
    recs_path = _require(data_dir / files["recommendations"])
    steamspy_path = _require(data_dir / files["steamspy"])
    genre_path = _require(data_dir / files["genre_categories"])

    games = spark.read.csv(str(games_path), header=True, inferSchema=True)
    metadata = spark.read.json(str(metadata_path))
    recs = spark.read.csv(str(recs_path), header=True, inferSchema=True)
    steamspy = spark.read.csv(str(steamspy_path), header=True, inferSchema=True)
    genres = spark.read.csv(str(genre_path), header=True, inferSchema=True)

    sources: dict[str, DataFrame | None] = {
        "games": games,
        "metadata": metadata,
        "recs": recs,
        "steamspy": steamspy,
        "genre_categories": genres,
    }

    # Wikidata is optional — loaded only if the file exists.
    wd_path = data_dir / files["wikidata"]
    if wd_path.exists() and wd_path.stat().st_size > 0:
        wikidata = spark.read.json(str(wd_path))
        sources["wikidata"] = wikidata
        logger.info("Wikidata loaded from %s", wd_path)
    else:
        sources["wikidata"] = None
        logger.warning(
            "Wikidata file not found at %s — Wikidata join will be skipped. "
            "Run `python -m src.ingestion.download_wikidata` to fetch it.",
            wd_path,
        )

    for name, df in sources.items():
        if df is not None:
            logger.info("  %-18s -> %d columns", name, len(df.columns))

    return sources
