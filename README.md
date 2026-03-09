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
