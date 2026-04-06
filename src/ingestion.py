import os

def load_data(spark, data_dir):
    print("[INFO] Loading data...")

    base_dir = os.path.abspath(data_dir)

    games = spark.read.csv(
        os.path.join(base_dir, "games.csv"),
        header=True,
        inferSchema=True
    )

    users = spark.read.csv(
        os.path.join(base_dir, "users.csv"),
        header=True,
        inferSchema=True
    )

    metadata = spark.read.json(
        os.path.join(base_dir, "games_metadata.json")
    )

    recs = spark.read.csv(
        os.path.join(base_dir, "recommendations.csv"),
        header=True,
        inferSchema=True
    )

    print("[INFO] Data loaded successfully")

    return games, users, metadata, recs