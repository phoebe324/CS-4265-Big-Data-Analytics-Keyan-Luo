"""Data quality validation.

Computes record counts, null rates, schema conformance, and basic
reasonableness checks at each pipeline stage. Writes a Markdown report
to ``docs/validation.md`` so the evidence is committed alongside the code.

This module is the M4 centerpiece: it is the difference between "the
pipeline runs" (M3) and "the pipeline produces verified, meaningful output"
(M4).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when

from src.utils.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
#  Metric helpers
# --------------------------------------------------------------------------- #

def record_counts(sources: dict[str, DataFrame]) -> dict[str, int]:
    """Row count for each source DataFrame. Skips sources that are None."""
    return {name: df.count() for name, df in sources.items() if df is not None}


def wikidata_match_rate(processed: DataFrame) -> tuple[int, int, float] | None:
    """How many distinct games successfully resolved against Wikidata.

    Returns ``(matched, total, percentage)`` or ``None`` when Wikidata data
    was not joined on this run.
    """
    if "wikidata_qid" not in processed.columns:
        return None
    total = processed.select("app_id").distinct().count()
    if total == 0:
        return (0, 0, 0.0)
    from pyspark.sql.functions import col
    matched = (
        processed.filter(col("wikidata_qid").isNotNull())
        .select("app_id")
        .distinct()
        .count()
    )
    return (matched, total, matched / total * 100)


def null_rates(df: DataFrame, columns: list[str]) -> dict[str, float]:
    """Fraction of nulls per requested column. Missing columns map to NaN-like.

    Uses ``isnan`` only on numeric columns (it raises on strings) and falls
    back to a plain null check otherwise.
    """
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in columns}

    rates: dict[str, float] = {}
    numeric_types = {"double", "float", "int", "bigint", "smallint", "decimal"}

    for c in columns:
        if c not in df.columns:
            rates[c] = float("nan")
            continue

        dtype = dict(df.dtypes)[c]
        if any(dtype.startswith(t) for t in numeric_types):
            null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        else:
            null_count = df.filter(col(c).isNull()).count()
        rates[c] = null_count / total
    return rates


def summary_stats(df: DataFrame, numeric_cols: list[str]) -> dict[str, dict[str, float]]:
    """Min / max / mean for the requested numeric columns."""
    available = [c for c in numeric_cols if c in df.columns]
    if not available:
        return {}
    desc = df.select(available).summary("min", "max", "mean").collect()
    stats: dict[str, dict[str, float]] = {c: {} for c in available}
    for row in desc:
        label = row["summary"]
        for c in available:
            try:
                stats[c][label] = float(row[c]) if row[c] is not None else None
            except (TypeError, ValueError):
                stats[c][label] = None
    return stats


def sample_rows(df: DataFrame, n: int = 5) -> list[dict[str, Any]]:
    """Return ``n`` rows as plain dicts (safe for Markdown rendering)."""
    return [row.asDict(recursive=True) for row in df.limit(n).collect()]


# --------------------------------------------------------------------------- #
#  Threshold checks
# --------------------------------------------------------------------------- #

def check_thresholds(
    counts: dict[str, int],
    rates: dict[str, float],
    cfg: dict[str, Any],
) -> list[str]:
    """Return a list of human-readable warnings for any threshold violations."""
    warnings: list[str] = []
    v = cfg["validation"]

    min_records = v["min_expected_records"]
    for name, n in counts.items():
        if n < min_records:
            warnings.append(
                f"Source '{name}' has only {n} records "
                f"(below threshold {min_records})."
            )

    max_null = v["max_null_rate_critical_cols"]
    for c in v["critical_columns"]:
        rate = rates.get(c)
        if rate is None or (isinstance(rate, float) and rate != rate):  # NaN
            continue
        if rate > max_null:
            warnings.append(
                f"Critical column '{c}' has null rate {rate:.2%} "
                f"(above threshold {max_null:.0%})."
            )
    return warnings


# --------------------------------------------------------------------------- #
#  Report writer
# --------------------------------------------------------------------------- #

def write_markdown_report(
    report_path: Path,
    counts_raw: dict[str, int],
    rows_processed: int,
    rows_aggregated: int,
    rates: dict[str, float],
    stats: dict[str, dict[str, float]],
    samples: list[dict[str, Any]],
    warnings: list[str],
    runtime_seconds: float,
    wd_match: tuple[int, int, float] | None = None,
) -> None:
    """Render the validation report as Markdown."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = []
    lines.append("# Data Quality & Validation Report")
    lines.append("")
    lines.append(f"_Generated: {timestamp}_")
    lines.append("")

    # 1. Record counts
    lines.append("## 1. Record Counts at Each Stage")
    lines.append("")
    lines.append("| Stage | Rows |")
    lines.append("| --- | ---: |")
    for name, n in counts_raw.items():
        lines.append(f"| raw — {name} | {n:,} |")
    lines.append(f"| after processing (joined) | {rows_processed:,} |")
    lines.append(f"| final aggregated output | {rows_aggregated:,} |")
    lines.append("")

    # 2. Null rates
    lines.append("## 2. Null Rates on Critical Columns (post-processing)")
    lines.append("")
    lines.append("| Column | Null Rate |")
    lines.append("| --- | ---: |")
    for c, r in rates.items():
        if r != r:  # NaN -> column missing
            lines.append(f"| {c} | _column not present_ |")
        else:
            lines.append(f"| {c} | {r:.2%} |")
    lines.append("")

    # 3. Summary statistics
    lines.append("## 3. Summary Statistics on Aggregated Output")
    lines.append("")
    if stats:
        lines.append("| Column | Min | Mean | Max |")
        lines.append("| --- | ---: | ---: | ---: |")
        for c, s in stats.items():
            def fmt(v):
                return "—" if v is None else f"{v:,.4f}"
            lines.append(f"| {c} | {fmt(s.get('min'))} | {fmt(s.get('mean'))} | {fmt(s.get('max'))} |")
    else:
        lines.append("_No numeric columns available for summary._")
    lines.append("")

    # 4. Sample rows
    lines.append("## 4. Sample Aggregated Rows")
    lines.append("")
    if samples:
        keys = list(samples[0].keys())
        lines.append("| " + " | ".join(keys) + " |")
        lines.append("| " + " | ".join("---" for _ in keys) + " |")
        for row in samples:
            cells = [str(row.get(k, "")) for k in keys]
            lines.append("| " + " | ".join(cells) + " |")
    else:
        lines.append("_No rows returned._")
    lines.append("")

    # 4b. Cross-source entity-resolution rate
    lines.append("## 4b. Cross-Source Entity Resolution (Steam ↔ Wikidata)")
    lines.append("")
    if wd_match is None:
        lines.append("_Wikidata was not joined on this run (file absent)._")
    else:
        matched, total, pct = wd_match
        lines.append(
            f"Matched **{matched:,} / {total:,}** distinct Steam games to Wikidata "
            f"entities via normalized-title join (**{pct:.1f}%** match rate)."
        )
    lines.append("")

    # 5. Threshold warnings
    lines.append("## 5. Threshold Checks")
    lines.append("")
    if warnings:
        for w in warnings:
            lines.append(f"- ⚠️  {w}")
    else:
        lines.append("✅ All configured thresholds satisfied.")
    lines.append("")

    # 6. Performance
    lines.append("## 6. Performance")
    lines.append("")
    lines.append(f"- End-to-end runtime: **{runtime_seconds:.1f} s**")
    lines.append("")

    # 7. Edge case behavior (static reference)
    lines.append("## 7. Edge-Case Behavior (Documented)")
    lines.append("")
    lines.append(
        "| Scenario | Behavior |\n"
        "| --- | --- |\n"
        "| Missing input file | `FileNotFoundError` raised in `ingestion.load_data` before Spark is invoked. |\n"
        "| Empty input file | `ValueError` raised by `_require()`. |\n"
        "| SteamSpy API timeout / 5xx / 429 | Retried up to `steamspy.max_retries` with linear backoff. |\n"
        "| Malformed JSON / CSV row | Spark's permissive mode coerces to nulls; `dropna` removes rows missing critical keys. |\n"
        "| Duplicate `(app_id, user_id)` reviews | Removed via `dropDuplicates` in `process_data`. |\n"
        "| Unexpected schema fields | Ignored — pipeline projects only the columns it needs. |\n"
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Validation report written to %s", report_path)


# --------------------------------------------------------------------------- #
#  Public entry point
# --------------------------------------------------------------------------- #

def run_validation(
    sources: dict[str, DataFrame],
    processed: DataFrame,
    aggregated: DataFrame,
    cfg: dict[str, Any],
    runtime_seconds: float,
) -> dict[str, Any]:
    """Compute all metrics, write the report, and return the metric dict."""
    logger.info("Running validation...")

    counts_raw = record_counts(sources)
    rows_processed = processed.count()
    rows_aggregated = aggregated.count()

    rates = null_rates(processed, cfg["validation"]["critical_columns"])
    stats = summary_stats(
        aggregated,
        ["steam_recommend_rate", "num_reviews", "num_positive_reviews"],
    )
    samples = sample_rows(aggregated, n=5)
    warnings = check_thresholds(counts_raw, rates, cfg)
    wd_match = wikidata_match_rate(processed)

    report_path = cfg["project_root"] / "doc" / "validation.md"
    write_markdown_report(
        report_path,
        counts_raw,
        rows_processed,
        rows_aggregated,
        rates,
        stats,
        samples,
        warnings,
        runtime_seconds,
        wd_match=wd_match,
    )

    if warnings:
        for w in warnings:
            logger.warning(w)
    else:
        logger.info("All validation thresholds satisfied.")

    return {
        "counts_raw": counts_raw,
        "rows_processed": rows_processed,
        "rows_aggregated": rows_aggregated,
        "null_rates": rates,
        "summary_stats": stats,
        "warnings": warnings,
    }
