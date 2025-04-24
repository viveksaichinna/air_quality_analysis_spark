# merge_and_sort.py
from pyspark.sql import SparkSession
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("BatchMergingAndSorting") \
        .getOrCreate()

    try:
        # 1. Define path and verify
        base_path = "/workspaces/air_quality_analysis_spark/ingestion/data/processed"
        input_path = f"{base_path}/batch_*/*.csv"  # Updated pattern
        
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Base directory not found: {base_path}")
        
        # 2. Read all batch files
        merged_df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )
        logger.info(f"Read {merged_df.count()} records")

        # 3. Sort and save
        output_path = f"/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task1"
        merged_df.sort("timestamp").coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path)
        
        logger.info(f"Saved sorted data to {output_path}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()