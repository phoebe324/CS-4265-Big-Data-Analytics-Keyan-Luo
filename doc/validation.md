# Data Quality & Validation Report

_Generated: 2026-05-04 03:11:18 UTC_

## 1. Record Counts at Each Stage

| Stage | Rows |
| --- | ---: |
| raw — games | 50,872 |
| raw — metadata | 50,872 |
| raw — recs | 41,154,794 |
| raw — steamspy | 100 |
| raw — genre_categories | 3 |
| raw — wikidata | 9,255 |
| after processing (joined) | 41,154,773 |
| final aggregated output | 37,610 |

## 2. Null Rates on Critical Columns (post-processing)

| Column | Null Rate |
| --- | ---: |
| app_id | 0.00% |
| title | 0.00% |
| is_recommended | 0.00% |

## 3. Summary Statistics on Aggregated Output

| Column | Min | Mean | Max |
| --- | ---: | ---: | ---: |
| steam_recommend_rate | 0.0000 | 0.7674 | 1.0000 |
| num_reviews | 1.0000 | 1,094.2508 | 319,491.0000 |
| num_positive_reviews | 0.0000 | 938.6966 | 294,878.0000 |

## 4. Sample Aggregated Rows

| app_id | steam_recommend_rate | num_reviews | num_positive_reviews | title | primary_tag | genre_category | price | wikidata_qid | wd_release_date | wd_metacritic | wd_publisher | wd_country |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 70 | 0.9641318793934445 | 62451 | 60211 | Half-Life | FPS | None | 999 | Q279744 | 1998-11-19T00:00:00Z | None | Valve Corporation | United States |
| 300 | 0.9047005795235029 | 9318 | 8430 | Day of Defeat: Source | FPS | None | None | Q1045397 | 2005-09-26T00:00:00Z | None | Valve Corporation | United States |
| 340 | 0.9002338269680437 | 6415 | 5775 | Half-Life 2: Lost Coast | FPS | None | 1999 | Q223629 | 2005-10-27T00:00:00Z | None | Valve Corporation | United States |
| 1250 | 0.9494886505362934 | 32072 | 30452 | Killing Floor | FPS | None | None | None | None | None | None | None |
| 2840 | 0.7586206896551724 | 145 | 110 | X: Beyond the Frontier | Simulation | None | None | Q1190522 | 1999-07-01T00:00:00Z | None | THQ | Germany |

## 4b. Cross-Source Entity Resolution (Steam ↔ Wikidata)

Matched **3,799 / 37,610** distinct Steam games to Wikidata entities via normalized-title join (**10.1%** match rate).

## 5. Threshold Checks

- ⚠️  Source 'genre_categories' has only 3 records (below threshold 100).

## 6. Performance

- End-to-end runtime: **126.4 s**

## 7. Edge-Case Behavior (Documented)

| Scenario | Behavior |
| --- | --- |
| Missing input file | `FileNotFoundError` raised in `ingestion.load_data` before Spark is invoked. |
| Empty input file | `ValueError` raised by `_require()`. |
| SteamSpy API timeout / 5xx / 429 | Retried up to `steamspy.max_retries` with linear backoff. |
| Malformed JSON / CSV row | Spark's permissive mode coerces to nulls; `dropna` removes rows missing critical keys. |
| Duplicate `(app_id, user_id)` reviews | Removed via `dropDuplicates` in `process_data`. |
| Unexpected schema fields | Ignored — pipeline projects only the columns it needs. |
