from pyspark.sql import SparkSession
from ingestion import load_data
from processing import process_data
from aggregation import aggregate_data
from storage import save_data

def main():
    try:
        print("[INFO] Starting pipeline...")

        spark = SparkSession.builder \
            .appName("Steam M3 Pipeline") \
            .getOrCreate()

        data_dir = "data"
        output_dir = "output"

        games, users, metadata, recs = load_data(spark, data_dir)
        df = process_data(games, users, metadata, recs)
        result = aggregate_data(df)
        save_data(result, output_dir)

        print("[INFO] Pipeline complete")
        spark.stop()

    except Exception as e:
        print("[ERROR]", e)

if __name__ == "__main__":
    main()