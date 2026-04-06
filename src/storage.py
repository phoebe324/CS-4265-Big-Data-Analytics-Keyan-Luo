import os

def save_data(df):
    print("[INFO] Saving data...")

    os.makedirs("output", exist_ok=True)
    with open("output/result.csv", "w", encoding="utf-8") as f:
        # 写 header
        f.write(",".join(df.columns) + "\n")

        count = 0
        for row in df.toLocalIterator():
            line = ",".join([str(x) for x in row])
            f.write(line + "\n")

            count += 1
            if count % 100000 == 0:
                print(f"[INFO] Written {count} rows...")

    print("[INFO] Data saved to output/result.csv")