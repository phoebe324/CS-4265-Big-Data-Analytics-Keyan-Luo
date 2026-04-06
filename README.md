
# CS4265 Big Data Analytics Project

This repository contains the implementation of a comprehensive Big Data pipeline developed across multiple milestones (M1, M2, M3).

## Milestone 3 (M3) - Complete Pipeline

In Milestone 3, a full end-to-end pipeline is implemented using **Apache Spark**. The system is designed to process a large-scale Steam dataset with over **41 million records**, covering data ingestion, cleaning, transformation, and aggregation.

---

## Project Structure

```text
src/
├── run_pipeline.py    # Main entry point to trigger the full pipeline
├── ingestion.py       # Handles data loading from CSV and JSON sources
├── processing.py      # Performs data cleaning, joining, and feature engineering
├── aggregation.py     # Executes distributed grouping and statistical analysis
└── storage.py         # Manages final data output and persistence
```

---

## How to Run (M3)

### 1. Prerequisites
* **Python 3.x**
* **Apache Spark** (proper environment setup required)
* **Dependencies**: Install via pip.
  ```bash
  pip install -r requirements.txt
  ```

### 2. Data Preparation
## Data Source

The dataset used in this project is the *Game Recommendations on Steam* dataset from Kaggle:

https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam

This dataset contains over 41 million cleaned and preprocessed user recommendations from the Steam platform, along with game and user information. :contentReference[oaicite:1]{index=1}

The dataset includes:

- games.csv (game information such as price, release date, ratings)
- users.csv (user profile information)
- recommendations.csv (user-game interactions and recommendations)
- games_metadata.json (additional game details such as tags and descriptions)

Due to the large size of the dataset (~600MB+), it is not included in this repository.

After downloading, place all files into the `data/` folder before running the pipeline.

Place the following dataset files in the `data/` folder at the project root:
* `games.csv`
* `users.csv`
* `recommendations.csv`
* `games_metadata.json`

### 3. Execution
Run the pipeline from the **project root** directory:
```bash
python src/run_pipeline.py
```

---

## Output & Architecture

The pipeline generates the following analytical output:
* **Location**: `output/result.csv`

### Technical Design Note:
* **Distributed Processing**: Spark is utilized for heavy-duty distributed joins and aggregations on the full 41M record dataset.
* **Storage Strategy**: After aggregation, the result size becomes much smaller, so it is written locally for simplicity and stability.

---

## Previous Milestones

* **M1**: Initial project proposal, design, and dataset selection.
* **M2**: Initial pipeline framework, POC of data processing, and environment setup.

---
