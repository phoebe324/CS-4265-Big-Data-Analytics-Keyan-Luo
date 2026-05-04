# Steam Cross-Source Analytics Pipeline

A Spark-based Big Data pipeline that ingests Steam game review data from two
genuinely independent sources, reconciles their heterogeneous schemas through
two-stage entity resolution, and produces a verified per-game analytics table
in columnar Parquet format.

> **CS 4265 Big Data Analytics — Milestone 4 (Final Deliverable)**
> Author: Keyan Luo · Spring 2026

---

## What this pipeline does

* Loads four files from the **Steam Kaggle dataset** (50,872 games and
  **41,154,794 reviews**) into Spark DataFrames.
* Fetches **9,255 Wikidata entities** live from Wikimedia's public SPARQL
  endpoint, restricted to entities that have a Steam application ID (P1733).
* Joins all sources together using a **two-stage entity resolution** strategy
  (exact `app_id` match, then normalized-title fallback).
* Aggregates to **one row per game** (37,610 distinct titles) with Steam
  recommendation metrics plus Wikidata-sourced publisher / country /
  Metacritic / release-date columns.
* Writes Spark-native Parquet output and a small CSV preview.
* Generates a Markdown data-quality report at every run.

End-to-end runtime: **≈ 121 seconds** on a single laptop (Windows 11,
16 GB RAM, single-node local Spark).

---

## Architecture

![Pipeline architecture](docs/architecture.png)

Three color-coded source lanes feed independent ingestion modules; processing
performs two-stage entity resolution before aggregating to one row per game;
validation observes the processed frame and emits a Markdown report.

See [`docs/architecture.md`](docs/architecture.md) for a deeper technical
description.

---

## Technology stack

| Component | Why |
| --- | --- |
| **Apache Spark 3.5 (PySpark)** | Distributed processing; native Parquet writer; SQL-like joins on tens of millions of rows |
| **Wikidata SPARQL endpoint** | Independent, open knowledge base. No API key, different operator (Wikimedia, not Valve), different schema (RDF triples) |
| **SteamSpy public API** | Supplementary popularity statistics |
| **PyYAML** | Centralized configuration in `config/settings.yaml` (no hardcoded paths) |
| **Python `logging`, `requests`, `pytest`** | Stdlib + minimal external deps, all pinned in `requirements.txt` |

---

## Repository layout

```
project/
├── README.md                   # this file
├── LICENSE                     # MIT
├── requirements.txt            # pinned dependencies
├── .env.example                # template for any future credentials
├── .gitignore                  # excludes data, credentials, IDE artifacts
├── config/
│   └── settings.yaml           # all tunables
├── src/
│   ├── ingestion/              # data acquisition (Steam files + Wikidata SPARQL)
│   ├── processing/             # cleaning, joins, entity resolution, validation
│   ├── storage/                # Parquet + CSV writers
│   ├── utils/                  # logger, config loader
│   └── main.py                 # entry point
├── data/
│   ├── raw/                    # (gitignored) place input files here
│   ├── processed/              # (gitignored) Parquet + CSV preview output
│   └── sample/                 # small example data committed for testing
├── docs/
│   ├── architecture.md         # technical design walkthrough
│   ├── data_dictionary.md      # column-by-column schema reference
│   └── validation.md           # auto-generated data-quality report
└── tests/
    └── test_smoke.py           # config + import sanity checks
```

---

## Setup

### Prerequisites

* **Python 3.10+** (tested on 3.12)
* **Java 17** (required by Spark; install Eclipse Temurin from
  <https://adoptium.net/>)
* **Windows users**: install `winutils.exe` and set `HADOOP_HOME` so Spark
  can write Parquet natively. See <https://github.com/cdarlint/winutils>.

### Installation

```bash
# Clone the repo
git clone https://github.com/<your-username>/steam-pipeline.git
cd steam-pipeline

# Create and activate a virtual environment
python -m venv .venv
# macOS / Linux:
source .venv/bin/activate
# Windows PowerShell:
.venv\Scripts\activate

# Install pinned dependencies
pip install -r requirements.txt

# Verify the install
python -m pytest tests/ -v
```

Expected output: `2 passed`.

### Data acquisition

The pipeline expects six files in `data/raw/`:

| File | Source | Size | How to get it |
| --- | --- | --- | --- |
| `games.csv` | Kaggle | ~5 MB | <https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam> |
| `games_metadata.json` | Kaggle | ~18 MB | same Kaggle dataset |
| `recommendations.csv` | Kaggle | ~2 GB | same Kaggle dataset |
| `steamspy_data.csv` | SteamSpy API | ~10 KB | `python -m src.ingestion.download_steamspy` |
| `external_genres.csv` | curated | <1 KB | committed to `data/sample/` — copy to `data/raw/` |
| `wikidata_games.json` | Wikidata SPARQL | ~2 MB | `python -m src.ingestion.download_wikidata` |

### Run the pipeline

```bash
python -m src.main
```

This will:
1. Load all input sources
2. Run cleaning, dedup, and the two-stage Wikidata join
3. Write Parquet to `data/processed/result_parquet/`
4. Generate `docs/validation.md` with all data-quality metrics

---

## Usage examples

### Read the Parquet output back into Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/processed/result_parquet")
print(f"Total games: {df.count()}")
df.show(10)
```

### Find the most-recommended Steam games published in a specific country

```python
from pyspark.sql.functions import col

(df.filter(col("wd_country") == "Japan")
   .filter(col("num_reviews") > 1000)
   .orderBy(col("steam_recommend_rate").desc())
   .select("title", "steam_recommend_rate", "num_reviews", "wd_publisher")
   .show(10, truncate=False))
```

This is the kind of question the original Steam-only Kaggle dataset cannot
answer; it requires the cross-source join.

### Refresh the Wikidata cache

```bash
python -m src.ingestion.download_wikidata
```

The cached file (`data/raw/wikidata_games.json`) is reused on subsequent
pipeline runs, so the SPARQL endpoint is only hit when refreshed manually.

---

## Output description

The aggregated output (`data/processed/result_parquet/`) contains one row
per game with these columns:

* **Steam metrics**: `app_id`, `steam_recommend_rate`, `num_reviews`,
  `num_positive_reviews`
* **Descriptive**: `title`, `primary_tag`, `genre_category`, `price`
* **Wikidata-sourced**: `wikidata_qid`, `wd_release_date`, `wd_metacritic`,
  `wd_publisher`, `wd_country`

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for full
column-by-column documentation.

---

## Validation

Every pipeline run regenerates `docs/validation.md` with:

* Record counts at each stage (raw → joined → aggregated)
* Null rates on critical columns
* Summary statistics on the aggregated output
* Steam ↔ Wikidata entity-resolution match rate
* Sample aggregated rows
* Threshold checks
* End-to-end runtime

The most recent run reports:

* 41,154,794 → 41,154,773 → **37,610** rows through the three stages
* 0.00% null on `app_id`, `title`, `is_recommended`
* Mean `steam_recommend_rate` = 0.7674
* **10.1% match rate** to Wikidata (3,799 / 37,610)
* Runtime: 120.9 s

---

## Project status

**Complete.** All M4 deliverables are in place. Known limitations:

* Single-node Spark only — has not been tested on a real cluster.
* The 10.1% Wikidata match rate is bounded by Wikidata's coverage of
  Steam, not by the join logic itself; long-tail / indie titles do not
  have Wikidata entries.
* Wikidata ingestion runs as a separate manual step
  (`python -m src.ingestion.download_wikidata`) rather than via a
  scheduler. Future work would orchestrate this through Airflow or
  Prefect for automatic refresh.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Author and acknowledgments

**Keyan Luo** (kluo2@students.kennesaw.edu) — Kennesaw State University,
CS 4265 Big Data Analytics, Spring 2026.

Built on top of the Steam reviews dataset published on Kaggle by Anton
Kozyriev, the SteamSpy public API maintained by Sergey Galyonkin, and the
Wikidata knowledge base maintained by the Wikimedia Foundation.
