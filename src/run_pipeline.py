from pyspark.sql import SparkSession
from ingestion import load_data
from processing import process_data
from aggregation import aggregate_data
from storage import save_data

def main():
    print("[INFO] Starting pipeline...")

    spark = SparkSession.builder \
        .appName("Steam M3 Pipeline") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    games, users, metadata, recs = load_data(spark)

    df = process_data(games, users, metadata, recs)

    result = aggregate_data(df)

    save_data(result)

    print("[INFO] Pipeline complete")

if __name__ == "__main__":
    main()