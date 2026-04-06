def load_data(spark):
    print("[INFO] Loading data...")

    games = spark.read.csv("data/games.csv", header=True, inferSchema=True)
    users = spark.read.csv("data/users.csv", header=True, inferSchema=True)
    metadata = spark.read.json("data/games_metadata.json")
    recs = spark.read.csv("data/recommendations.csv", header=True, inferSchema=True)

    print(f"[INFO] Games: {games.count()}")
    print(f"[INFO] Users: {users.count()}")
    print(f"[INFO] Metadata: {metadata.count()}")
    print(f"[INFO] Recommendations: {recs.count()}")

    return games, users, metadata, recs