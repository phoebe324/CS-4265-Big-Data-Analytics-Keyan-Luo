"""Wikidata SPARQL ingestion.

Wikidata is a free, collaboratively-edited knowledge base maintained by the
Wikimedia Foundation. It is **genuinely independent** of Steam: different
operator (Wikimedia, not Valve), different primary key (``Q``-numbered
entity IDs, e.g. ``Q4290`` for Half-Life), different schema (RDF triples
exposed via SPARQL rather than tabular CSV), broader scope (every notable
video game), and crowd-curated metadata (publisher, country of origin,
Metacritic score, release date).

Query strategy:
  We restrict the query to Wikidata entities that have a Steam application
  ID (property ``P1733``). This has three benefits:

    1. The result set is bounded (~10k entities, not millions), so the
       query reliably completes within the public endpoint's timeout.
    2. Every returned record is, by construction, joinable to the Steam
       Kaggle data — yielding a high entity-resolution rate.
    3. We can join on the precise Steam app ID *and* on the normalized
       title, demonstrating two different reconciliation strategies in
       the same pipeline.

  Note: this does **not** make Wikidata a "Steam-derived" source. Wikidata
  is curated by an entirely separate community on entirely separate
  infrastructure. We are merely filtering its independent dataset down to
  the subset that overlaps with our Steam-side records — which is exactly
  what entity resolution is for.

Etiquette:
  Wikimedia's User-Agent policy
  (https://meta.wikimedia.org/wiki/User-Agent_policy) asks consumers to
  identify themselves. We comply.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

USER_AGENT = "CS4265-BigDataPipeline/1.0 (student project; contact via course staff)"

# Restrict to entities that have a Steam app ID (P1733). We deliberately
# avoid ORDER BY because sorting on the public endpoint is expensive;
# duplicate-elimination happens on our side.
SPARQL_TEMPLATE = """
SELECT ?game ?gameLabel ?steamAppId ?releaseDate ?metacritic ?publisherLabel ?countryLabel
WHERE {{
  ?game wdt:P31    wd:Q7889 .          # instance of: video game
  ?game wdt:P1733  ?steamAppId .       # Steam application ID
  OPTIONAL {{ ?game wdt:P577  ?releaseDate . }}
  OPTIONAL {{ ?game wdt:P1712 ?metacritic . }}
  OPTIONAL {{ ?game wdt:P123  ?publisher . }}
  OPTIONAL {{ ?game wdt:P495  ?country . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT {limit}
OFFSET {offset}
"""


def _request_sparql(
    query: str,
    endpoint: str,
    timeout: int,
    max_retries: int,
    backoff: int,
) -> dict[str, Any]:
    """GET a SPARQL query with retries on transient failures."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                endpoint,
                params={"query": query, "format": "json"},
                headers=headers,
                timeout=timeout,
            )
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
                "Wikidata attempt %d/%d failed (%s). Retrying in %ds.",
                attempt, max_retries, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError("Wikidata SPARQL query failed after retries") from last_exc


def fetch_wikidata_games(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Page through the SPARQL endpoint and collect game records."""
    wd_cfg = cfg["wikidata"]
    endpoint = wd_cfg["endpoint"]
    page_size = wd_cfg["page_size"]
    max_games = wd_cfg["max_games"]
    timeout = wd_cfg["timeout_seconds"]
    max_retries = wd_cfg["max_retries"]
    backoff = wd_cfg["backoff_seconds"]
    sleep_between = wd_cfg.get("sleep_between_pages_seconds", 2)

    games: list[dict[str, Any]] = []
    offset = 0

    while len(games) < max_games:
        logger.info(
            "Wikidata SPARQL — collected %d/%d (offset=%d, page_size=%d)",
            len(games), max_games, offset, page_size,
        )
        query = SPARQL_TEMPLATE.format(limit=page_size, offset=offset)
        payload = _request_sparql(query, endpoint, timeout, max_retries, backoff)
        bindings = payload.get("results", {}).get("bindings", [])
        if not bindings:
            logger.info("Wikidata returned 0 bindings — stopping pagination.")
            break
        games.extend(bindings)
        offset += page_size
        # Stop early if a partial page came back (we've consumed the source).
        if len(bindings) < page_size:
            break
        time.sleep(sleep_between)

    return games[:max_games]


def normalize_wikidata_binding(binding: dict[str, Any]) -> dict[str, Any]:
    """Flatten a single SPARQL binding into a plain dict."""
    def v(key: str) -> str | None:
        node = binding.get(key)
        return node.get("value") if node else None

    entity_uri = v("game") or ""
    qid = entity_uri.rsplit("/", 1)[-1] if entity_uri else None

    # Steam app IDs in Wikidata are stored as strings — coerce to int when
    # possible so the downstream join key is numeric (matching Steam's
    # own integer ``app_id``).
    raw_app_id = v("steamAppId")
    try:
        steam_app_id = int(raw_app_id) if raw_app_id is not None else None
    except (TypeError, ValueError):
        steam_app_id = None

    return {
        "wikidata_qid": qid,
        "wikidata_uri": entity_uri,
        "steam_app_id": steam_app_id,
        "name": v("gameLabel"),
        "release_date": v("releaseDate"),
        "metacritic": v("metacritic"),
        "publisher": v("publisherLabel"),
        "country": v("countryLabel"),
    }


def write_wikidata_json(rows: list[dict[str, Any]], output_path: Path) -> int:
    """Write normalized rows as newline-delimited JSON.

    Wikidata's OPTIONAL clauses can produce duplicate Q-ids (one row per
    publisher, etc.). We deduplicate by Q-id, keeping the first occurrence.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    seen_qids: set[str] = set()
    written = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            qid = row.get("wikidata_qid")
            if qid in seen_qids:
                continue
            if qid:
                seen_qids.add(qid)
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            written += 1
    return written


def download_wikidata(cfg: dict[str, Any] | None = None) -> Path:
    """Top-level entry point. Returns the path to the JSON file."""
    cfg = cfg or load_config()
    raw = fetch_wikidata_games(cfg)
    logger.info("Fetched %d raw bindings from Wikidata", len(raw))

    rows = [normalize_wikidata_binding(b) for b in raw]
    output_path = cfg["paths"]["data_dir"] / cfg["input_files"]["wikidata"]
    n = write_wikidata_json(rows, output_path)
    logger.info("Wrote %d unique Wikidata rows to %s", n, output_path)
    return output_path


if __name__ == "__main__":
    download_wikidata()
