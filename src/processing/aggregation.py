"""Aggregation logic.

Produces a per-game summary table used as the pipeline's main output.
When Wikidata is present, the output also includes cross-source columns
that the original Steam-only dataset cannot produce — most notably the
publisher and country-of-origin fields, which let downstream analyses ask
"do games published in country X have higher recommend rates on Steam?"
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, first, sum as spark_sum

from src.utils.logger import get_logger

logger = get_logger(__name__)


def aggregate_data(df: DataFrame) -> DataFrame:
    """One row per ``app_id`` summarising recommendations and metadata."""
    logger.info("Aggregating data...")

    agg_exprs = [
        avg("is_recommended_num").alias("steam_recommend_rate"),
        count("*").alias("num_reviews"),
        spark_sum("is_recommended_num").alias("num_positive_reviews"),
    ]

    # Carry forward useful descriptive columns when present.
    descriptors = [
        "title",
        "primary_tag",
        "genre_category",
        "price",
        # Wikidata-sourced columns (only present when the join ran)
        "wikidata_qid",
        "wd_release_date",
        "wd_metacritic",
        "wd_publisher",
        "wd_country",
    ]
    for descriptor in descriptors:
        if descriptor in df.columns:
            agg_exprs.append(first(descriptor, ignorenulls=True).alias(descriptor))

    return df.groupBy("app_id").agg(*agg_exprs)
