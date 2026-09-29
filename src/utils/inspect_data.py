import sys
import os
import argparse
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def inspect_layer(layer: str, columns: list = None, limit: int = 5):
    spark = get_spark_session("DataInspection")
    
    # Map the medallion layer to its file path
    path_map = {
        "raw": "data/raw/complaints.csv",
        "bronze": "data/bronze/complaints.parquet",
        "silver": "data/silver/complaints.parquet",
        "gold": "data/gold/complaints.parquet"
    }

    if layer not in path_map:
        logging.error(f"Invalid layer: {layer}. Must be one of {list(path_map.keys())}")
        sys.exit(1)

    file_path = path_map[layer]
    logging.info(f"Loading data from {layer.upper()} layer at {file_path}")

    try:
        # Handle the fact that raw is CSV, but the rest are Parquet
        if layer == "raw":
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        else:
            df = spark.read.parquet(file_path)

        print(f"\n=== Schema for {layer.upper()} Layer ===")
        df.printSchema()

        print(f"\n=== Data Preview ({limit} rows) ===")
        if columns:
            df.select(*columns).show(n=limit, truncate=False, vertical=True)
        else:
            df.show(n=limit, truncate=False, vertical=True)

    except Exception as e:
        logging.error(f"Failed to load data from {file_path}: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Medallion Architecture Layers")
    parser.add_argument("--layer", type=str, required=True, choices=["raw", "bronze", "silver", "gold"], help="The medallion layer to inspect")
    parser.add_argument("--columns", type=str, nargs="+", help="Specific columns to display (space-separated)")
    parser.add_argument("--limit", type=int, default=3, help="Number of rows to display")
    args = parser.parse_args()
    inspect_layer(args.layer, args.columns, args.limit)