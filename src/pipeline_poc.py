import os
import sys
from pyspark.sql import SparkSession

# Set Java environment
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-19'
os.environ['PATH'] = os.path.join(os.environ['JAVA_HOME'], 'bin') + ';' + os.environ['PATH']


def run_pipeline():

    print("======================================")
    print(" Steam Big Data Pipeline - Proof of Concept")
    print("======================================")

    print("Initializing Spark distributed processing environment...")

    spark = SparkSession.builder \
        .appName("SteamDataPipelinePOC") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    base_path = "data"

    games_csv = os.path.join(base_path, 'games.csv')
    recs_csv = os.path.join(base_path, 'recommendations.csv')   
    print(f"Loading datasets from: {games_csv} and {recs_csv}")

    try:
        # Load datasets
        games_df = spark.read.csv(games_csv, header=True, inferSchema=True)
        recs_df = spark.read.csv(recs_csv, header=True, inferSchema=True)

        print(f"Games dataset row count: {games_df.count()}")
        print(f"Recommendations dataset row count: {recs_df.count()}")

        print("Performing distributed join operation...")

        joined_df = recs_df.join(games_df, "app_id")

        print("Preview of joined dataset:")
        joined_df.show(5)

        print("Schema preview:")
        joined_df.printSchema()

        output_path = r'D:\Steam-Big-Data-Analysis\output\joined_data'

        print("Writing result to Parquet storage...")

        joined_df.write.mode("overwrite").parquet(output_path)

        print(f"Data successfully converted to Parquet and stored at: {output_path}")

    except Exception as e:
        print(f"Pipeline execution failed with error: {e}")

    finally:
        spark.stop()
        print("Spark job finished safely.")


if __name__ == "__main__":
    run_pipeline()