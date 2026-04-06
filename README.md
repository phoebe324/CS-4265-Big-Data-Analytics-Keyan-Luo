# CS4265 Big Data Analytics Project

This repository contains the implementation of a big data pipeline developed across multiple milestones (M1, M2, M3).

## Milestone 3 (M3) - Complete Pipeline

In M3, a full end-to-end pipeline is implemented using Apache Spark.  
The system processes over 41 million records and performs data cleaning, transformation, and aggregation.

## Project Structure


src/
run_pipeline.py
ingestion.py
processing.py
aggregation.py
storage.py


## How to Run (M3)

1. Install dependencies:


pip install -r requirements.txt


2. Place dataset files in the `data/` folder:

- games.csv  
- users.csv  
- recommendations.csv  
- games_metadata.json  

3. Run the pipeline from project root:


python src/run_pipeline.py


## Output

The pipeline generates:


output/result.csv


## Notes

Spark is used for distributed processing of large-scale data (41M records).  
After aggregation, the result size is reduced and written locally for stability.

## Previous Milestones

- M1: Initial project idea and design  
- M2: Initial pipeline setup and data processing  
