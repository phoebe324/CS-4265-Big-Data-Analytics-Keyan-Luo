import os
import csv

def save_data(df, output_path):
    print("[INFO] Saving data...")

    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, "result.csv")

    rows = df.collect()

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for row in rows:
            writer.writerow(list(row))

    print(f"[INFO] Data saved to {output_file}")