from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json
import os
from pyspark.sql.functions import hour, sin, cos, rand, round
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("AirQualityIngestion") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("location_id", StringType()),
        StructField("sensors_id", StringType()),
        StructField("location", StringType()),
        StructField("datetime", StringType()),
        StructField("lat", StringType()),
        StructField("lon", StringType()),
        StructField("parameter", StringType()),
        StructField("units", StringType()),
        StructField("value", DoubleType())
    ])

def parse_json_array(json_str):
    try:
        return json.loads(json_str.replace("'", '"'))
    except json.JSONDecodeError:
        logger.error(f"Failed to parse: {json_str}")
        return None

def read_from_tcp_stream(spark):
    logger.info("Creating TCP stream reader")
    parse_json_udf = udf(parse_json_array, ArrayType(StringType()))

    return spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load() \
        .withColumn("parsed", parse_json_udf(col("value"))) \
        .filter(size(col("parsed")) == 9) \
        .select(
            col("parsed")[0].alias("location_id"),
            col("parsed")[1].alias("sensors_id"),
            col("parsed")[2].alias("location"),
            col("parsed")[3].alias("datetime"),
            col("parsed")[4].alias("lat"),
            col("parsed")[5].alias("lon"),
            col("parsed")[6].alias("parameter"),
            col("parsed")[7].alias("units"),
            col("parsed")[8].cast("double").alias("value")
        )

def transform_with_watermark(stream_df):
    logger.info("Applying timestamp conversion and watermark")
    return stream_df \
        .withColumn("timestamp", to_timestamp("datetime", "yyyy-MM-dd'T'HH:mm:ssXXX")) \
        .withWatermark("timestamp", "10 minutes") \
        .select(
            col("location_id").cast("integer"),
            "timestamp",
            "location",
            col("lat").cast("double"),
            col("lon").cast("double"),
            "parameter",
            "value"
        )

def pivot_metrics(df):
    logger.info("Pivoting metrics and generating realistic weather data")
    pivoted_df = df.groupBy("location_id", "timestamp", "location", "lat", "lon") \
        .pivot("parameter", ["pm25"]) \
        .agg(expr("first(value, true)")) \
        .withColumnRenamed("pm25", "pm2_5")
    
    # Generate realistic temperature (based on time of day)
    return pivoted_df.withColumn(
        "hour_of_day", hour("timestamp")
    ).withColumn(
        "temperature", round(
            25.0 + 10.0 * sin((col("hour_of_day") - 6) * (math.pi / 12)) + (rand() * 6 - 3), 
            1
        )
    ).withColumn(
        "humidity", round(
            70.0 - 0.5 * col("temperature") + (rand() * 20 - 10), 
            1
        )
    ).drop("hour_of_day")

def handle_outliers(df):
    """Cap outliers instead of filtering them out"""
    logger.info("Handling outliers by capping extreme values")
    return df.withColumn(
        "pm2_5", 
        when(col("pm2_5") < 0, 0.0)          # Replace negative values with 0
        .when(col("pm2_5") > 500, 500.0)     # Cap values above 500
        .otherwise(col("pm2_5"))
    ).withColumn(
        "temperature",
        when(col("temperature") < -40, -40.0)  # Extreme cold cap
        .when(col("temperature") > 50, 50.0)   # Extreme heat cap
        .otherwise(col("temperature"))
    ).withColumn(
        "humidity",
        when(col("humidity") < 0, 0.0)        # Minimum 0%
        .when(col("humidity") > 100, 100.0)   # Maximum 100%
        .otherwise(col("humidity"))
    ).withColumn("is_valid", lit(True))       # Mark all rows as valid

def save_to_csv(batch_df, batch_id):
    """Save each micro-batch as CSV"""
    output_path = f"/workspaces/air_quality_analysis_spark/ingestion/data/processed/batch_{batch_id}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    batch_df.write.mode("append").option("header", "true").csv(output_path)

def write_stream(df):
    logger.info("Starting stream writes")
    return df.writeStream \
        .foreachBatch(save_to_csv) \
        .outputMode("append") \
        .option("checkpointLocation", "/workspaces/air_quality_analysis_spark/data/checkpoints_csv") \
        .start()

def main():
    logger.info("=== STARTING STREAM PROCESSING ===")
    spark = create_spark_session()

    try:
        raw_stream = read_from_tcp_stream(spark)
        transformed = transform_with_watermark(raw_stream)
        pivoted = pivot_metrics(transformed)
        processed = handle_outliers(pivoted)  # Replaces validate_data

        csv_query = write_stream(processed)
        csv_query.awaitTermination()

    except Exception as e:
        logger.error(f"Stream processing failed: {str(e)}")
        for query in spark.streams.active:
            query.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()