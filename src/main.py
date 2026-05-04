"""Pipeline entry point.

Wires the four stages together:
    ingestion -> processing -> aggregation -> storage
and runs validation on the output before exiting.

Usage:
    python -m src.main                      # default config
    python -m src.main --config path.yaml   # custom config

Exit codes:
    0  pipeline succeeded (warnings allowed)
    1  pipeline failed
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from src.ingestion.ingestion import load_data
from src.processing.aggregation import aggregate_data
from src.processing.processing import process_data
from src.processing.validation import run_validation
from src.storage.storage import save_data
from src.utils.config import load_config
from src.utils.logger import get_logger


def build_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg["spark"]
    spark = (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
        .config("spark.driver.maxResultSize", spark_cfg.get("driver_max_result_size", "2g"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(spark_cfg["log_level"])
    return spark


def main(config_path: Path | None = None) -> int:
    cfg = load_config(config_path)
    log_file = cfg["project_root"] / "data" / "processed" / "pipeline.log"
    logger = get_logger("pipeline", log_file=log_file)

    logger.info("=" * 60)
    logger.info("Starting Steam Big Data Pipeline")
    logger.info("=" * 60)

    spark = None
    start = time.time()
    try:
        spark = build_spark(cfg)

        sources = load_data(spark, cfg)
        processed = process_data(sources)

        aggregated = aggregate_data(processed)

        save_data(aggregated, cfg["paths"]["output_dir"])

        runtime = time.time() - start
        run_validation(sources, processed, aggregated, cfg, runtime_seconds=runtime)

        logger.info("Pipeline complete in %.1fs", runtime)
        return 0

    except FileNotFoundError as e:
        logger.error("Input file missing: %s", e)
        return 1
    except Exception:
        logger.exception("Pipeline failed with an unhandled exception.")
        return 1
    finally:
        if spark is not None:
            spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Steam pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to settings.yaml (defaults to config/settings.yaml).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(main(args.config))
