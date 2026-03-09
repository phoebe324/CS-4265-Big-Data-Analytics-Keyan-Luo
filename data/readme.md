# Dataset Description

The dataset used in this project is the **Steam Store Games and Recommendations Dataset** from Kaggle.

This dataset contains user recommendation data and game metadata from the Steam platform. It is significantly larger than the dataset originally proposed for the project and better represents a Big Data workload.

---

## Data Source

Kaggle Dataset:

https://www.kaggle.com/datasets/antonkozyriev/steam-games

---

## Dataset Files

The pipeline uses the following files:

- `games.csv` – metadata for Steam games  
- `recommendations.csv` – user recommendation records  
- `users.csv` – user information (planned for later processing)

---

## Dataset Size

The dataset used in the project contains approximately:

- **50,872 game records**
- **41,154,794 recommendation records**

This large dataset allows the project to demonstrate distributed data processing using Apache Spark.

---

## Data Schema

Key fields in the dataset include:

- **app_id** – unique identifier for each game  
- **title** – game title  
- **date_release** – game release date  
- **rating** – overall user rating category  
- **positive_ratio** – percentage of positive reviews  
- **price_final** – final price of the game  

Recommendation dataset fields include:

- **user_id** – anonymized user identifier  
- **app_id** – game identifier  
- **is_recommended** – whether the user recommended the game  
- **hours** – playtime hours  
- **review_id** – review identifier  

---

## Data Availability

Due to the large size of the dataset, the raw data files are **not stored in this repository**.

To run the pipeline locally, download the dataset from Kaggle and place the files in this folder:
