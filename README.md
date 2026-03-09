# CS-4265 Big Data Analytics – Keyan Luo

## Project Overview

This project implements a proof-of-concept Big Data analytics pipeline using Apache Spark.  
The goal is to demonstrate how a distributed data processing framework can handle large datasets and perform analytical operations such as joins and schema analysis.

The project originally started as a stock market analytics idea, but it was later changed to use a much larger dataset from the Steam platform. The Steam dataset contains over 41 million recommendation records, which makes it more suitable for demonstrating Big Data processing.

---

## Dataset

The project uses a Steam dataset containing the following files:

- `games.csv`
- `recommendations.csv`
- `users.csv` (planned for later stages)

The current prototype successfully processed:

- 50,872 game records  
- 41,154,794 recommendation records

Due to the dataset size, the raw data files are not fully included in the repository.

---
## Project Structure

```
CS-4265-Big-Data-Analytics-Keyan-Luo
│
├── src/
│   └── pipeline_poc.py
│
├── docs/
│   ├── 1.png
│   ├── 2.png
│   ├── 3.png
│   ├── 4.png
│   └── 5.png
│
├── data/
│   └── README.md
│
├── output/
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Environment Setup

Requirements:

- Python 3.9+
- Java JDK (required for Apache Spark)
- Apache Spark (running in local mode)

Install dependencies:

pip install -r requirements.txt
## How to Run

Run the Spark pipeline using:

python src/pipeline_poc.py

The pipeline performs the following steps:

1. Initialize the Spark environment
2. Load the Steam datasets
3. Perform a distributed join between the datasets
4. Display a preview of the joined dataset
5. Print the dataset schema

## Pipeline Execution Evidence

Screenshots of the pipeline execution are available in the `docs/` folder.
These screenshots demonstrate:

- Spark environment initialization
- Dataset loading
- Distributed join execution
- Dataset schema inspection
- Output stage error related to Hadoop configuration
