from pyspark.sql.functions import avg, count

def aggregate_data(df):
    print("[INFO] Aggregating data...")

    result = df.groupBy("app_id").agg(
        avg("is_recommended_num").alias("avg_recommendation"),
        count("*").alias("num_reviews")
    )

    return result