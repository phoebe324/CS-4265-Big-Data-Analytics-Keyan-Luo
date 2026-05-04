"""Output writers.

Writes the aggregated DataFrame to Parquet (the canonical, columnar output
consumed by downstream tools) and a small CSV preview useful for spot checks
and the validation report.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from src.utils.logger import get_logger

logger = get_logger(__name__)


def save_data(df: DataFrame, output_dir: Path) -> None:
    """Persist the aggregated DataFrame.

    Produces:
      * ``<output_dir>/result_parquet/`` — full Spark-native Parquet output.
      * ``<output_dir>/result_preview.csv`` — first 100 rows as a single CSV
        (handy for README screenshots and quick sanity checks).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / "result_parquet"
    df.write.mode("overwrite").parquet(str(parquet_path))
    logger.info("Wrote Parquet output to %s", parquet_path)

    preview_path = output_dir / "result_preview.csv"
    (
        df.limit(100)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(preview_path))
    )
    logger.info("Wrote CSV preview to %s", preview_path)
