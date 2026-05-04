"""Transformation logic.

Joins all input sources into one enriched DataFrame.

The technical centerpiece is the reconciliation between Steam's ``app_id``
and Wikidata's ``Q-id``. We use a two-stage strategy: an exact equi-join on
``app_id <-> steam_app_id`` first, then a normalized-title fallback for any
games the primary key did not match.

All ``app_id`` values are explicitly cast to integer in this module so that
join key types always agree, regardless of how Spark's ``inferSchema``
happened to interpret them on disk.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce, col, lower, regexp_replace, size, trim, when,
)
from pyspark.sql.types import IntegerType

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_title(c):
    """lowercase + strip punct + collapse whitespace."""
    cleaned = regexp_replace(lower(c), r"[^a-z0-9\s]", "")
    cleaned = regexp_replace(cleaned, r"\s+", " ")
    return trim(cleaned)


def _cast_app_id(df: DataFrame, name: str = "app_id") -> DataFrame:
    if name in df.columns:
        return df.withColumn(name, col(name).cast(IntegerType()))
    return df


def process_data(sources: dict[str, DataFrame]) -> DataFrame:
    games = sources["games"]
    metadata = sources["metadata"]
    recs = sources["recs"]
    steamspy = sources["steamspy"]
    genres_map = sources["genre_categories"]
    wikidata = sources.get("wikidata")

    logger.info("Processing data...")

    # ---- Force consistent integer join keys -------------------------------
    games = _cast_app_id(games)
    metadata = _cast_app_id(metadata)
    recs = _cast_app_id(recs)
    steamspy = steamspy.withColumnRenamed("appid", "app_id")
    steamspy = _cast_app_id(steamspy)

    # ---- Clean each source -------------------------------------------------
    games = games.dropna(subset=["app_id", "title"])
    metadata = metadata.dropna(subset=["app_id"])
    recs = recs.dropna(subset=["app_id", "is_recommended"])

    # ---- Dedup recs (no count() to avoid extra full scans) -----------------
    if "user_id" in recs.columns:
        recs = recs.dropDuplicates(["app_id", "user_id"])
        logger.info("  applied dedup on (app_id, user_id)")

    # ---- Join games + metadata (left, so all games survive) ---------------
    df = games.join(metadata, "app_id", "left")
    df = df.withColumn(
        "tag_count",
        when(col("tags").isNull(), 0).otherwise(size(col("tags"))),
    )

    # ---- Join recs (inner: only games that have at least one review) ------
    df = df.join(recs, "app_id")

    # ---- Convert is_recommended to 0/1 -----------------------------------
    # is_recommended in this dataset is already boolean-like; we coerce it
    # robustly via a string check that handles "true"/"True"/True alike.
    df = df.withColumn(
        "is_recommended_num",
        when(lower(col("is_recommended").cast("string")) == "true", 1).otherwise(0),
    )

    # ---- Join SteamSpy popularity (left) ----------------------------------
    df = df.join(steamspy, "app_id", "left")

    # ---- Tag the primary genre with its external category -----------------
    if "tags" in df.columns and genres_map is not None:
        gm = genres_map.select(
            lower(col("genre")).alias("genre_key"),
            col("category").alias("genre_category"),
        )
        df = df.withColumn(
            "primary_tag",
            when(col("tag_count") > 0, col("tags").getItem(0)).otherwise(None),
        )
        df = df.join(
            gm, lower(col("primary_tag")) == col("genre_key"), "left"
        ).drop("genre_key")

    # ---- Join Wikidata in two stages --------------------------------------
    if wikidata is not None:
        wd = wikidata.withColumn(
            "steam_app_id", col("steam_app_id").cast(IntegerType())
        )

        # Stage 1: exact app_id match
        wd_by_id = (
            wd.filter(col("steam_app_id").isNotNull())
              .select(
                  col("steam_app_id").alias("app_id"),
                  col("wikidata_qid"),
                  col("release_date").alias("wd_release_date"),
                  col("metacritic").alias("wd_metacritic"),
                  col("publisher").alias("wd_publisher"),
                  col("country").alias("wd_country"),
              )
              .dropDuplicates(["app_id"])
        )
        df = df.join(wd_by_id, "app_id", "left")

        # Stage 2: normalized-title fallback for games stage 1 missed
        wd_by_title = (
            wd.withColumn("title_key", _normalize_title(col("name")))
              .select(
                  col("title_key"),
                  col("wikidata_qid").alias("wd_qid_t"),
                  col("release_date").alias("wd_release_date_t"),
                  col("metacritic").alias("wd_metacritic_t"),
                  col("publisher").alias("wd_publisher_t"),
                  col("country").alias("wd_country_t"),
              )
              .dropDuplicates(["title_key"])
        )

        df = df.withColumn("title_key", _normalize_title(col("title")))
        df = df.join(wd_by_title, "title_key", "left").drop("title_key")

        # Coalesce primary-key matches with title-fallback matches
        df = (
            df
            .withColumn("wikidata_qid", coalesce("wikidata_qid", "wd_qid_t"))
            .withColumn("wd_release_date", coalesce("wd_release_date", "wd_release_date_t"))
            .withColumn("wd_metacritic", coalesce("wd_metacritic", "wd_metacritic_t"))
            .withColumn("wd_publisher", coalesce("wd_publisher", "wd_publisher_t"))
            .withColumn("wd_country", coalesce("wd_country", "wd_country_t"))
            .drop("wd_qid_t", "wd_release_date_t", "wd_metacritic_t",
                  "wd_publisher_t", "wd_country_t")
        )
        logger.info("Wikidata join complete (two-stage)")
    else:
        logger.info("Skipping Wikidata join (data not available).")

    return df
