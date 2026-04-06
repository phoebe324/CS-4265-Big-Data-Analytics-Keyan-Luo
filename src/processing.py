from pyspark.sql.functions import col, size, when

def process_data(games, users, metadata, recs):
    print("[INFO] Processing data...")

    games = games.dropna(subset=["app_id", "title"])
    metadata = metadata.dropna(subset=["app_id"])
    recs = recs.dropna(subset=["app_id", "is_recommended"])

    df = games.join(metadata, "app_id", "left")
    df = df.withColumn("tag_count", size(col("tags")))

    df = df.join(recs, "app_id")

    df = df.withColumn(
        "is_recommended_num",
        when(col("is_recommended") == True, 1).otherwise(0)
    )

    return df