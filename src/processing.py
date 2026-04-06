from pyspark.sql.functions import col, size, when

def process_data(games, users, metadata, recs):
    print("[INFO] Processing data...")

    # clean
    games = games.dropna()
    metadata = metadata.dropna()
    recs = recs.dropna()

    # join metadata
    df = games.join(metadata, "app_id", "left")

    # feature: tag count
    df = df.withColumn("tag_count", size(col("tags")))

    # join recommendations
    df = df.join(recs, "app_id")
    df = df.withColumn(
        "is_recommended_num",
        when(col("is_recommended") == True, 1).otherwise(0)
    )

    return df