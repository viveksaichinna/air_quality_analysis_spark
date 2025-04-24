from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json
import time
import os

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

    raw_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    parsed_df = raw_df \
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
    return parsed_df

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
    logger.info("Pivoting metrics")
    return df.groupBy("location_id", "timestamp", "location", "lat", "lon") \
        .pivot("parameter", ["pm25", "temperature", "humidity"]) \
        .agg(expr("first(value, true)")) \
        .withColumnRenamed("pm25", "pm2_5")

def validate_data(df):
    logger.info("Validating data quality")
    return df.filter(
        col("pm2_5").between(0, 500) &
        col("timestamp").isNotNull() &
        col("location_id").isNotNull()
    ).withColumn("is_valid", 
        when(col("pm2_5").isNull(), False)
        .when(col("temperature").isNull() | (col("temperature") < -50), False)
        .when(col("humidity").isNull() | (col("humidity") < 0), False)
        .otherwise(True)
    )

def save_to_csv(batch_df, batch_id):
    """Save each micro-batch as CSV using batch_id"""
    output_path = f"/workspaces/air_quality_analysis_spark/ingestion/data/processed/batch_{batch_id}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    batch_df.write \
        .mode("append") \
        .option("header", "true") \
        .csv(output_path)

def write_stream(df):
    logger.info("Starting stream writes")

    # CSV output via foreachBatch
    csv_query = df.writeStream \
        .foreachBatch(save_to_csv) \
        .outputMode("append") \
        .option("checkpointLocation", "/workspaces/air_quality_analysis_spark/data/checkpoints_csv") \
        .start()

    return csv_query

def main():
    logger.info("=== STARTING STREAM PROCESSING ===")
    spark = create_spark_session()

    try:
        raw_stream = read_from_tcp_stream(spark)
        transformed = transform_with_watermark(raw_stream)
        pivoted = pivot_metrics(transformed)
        validated = validate_data(pivoted)

        csv_query = write_stream(validated)

        csv_query.awaitTermination()

    except Exception as e:
        logger.error(f"Stream processing failed: {str(e)}")
        raise
    finally:
        logger.info("Stopping all streams")
        spark.streams.getActive().foreach(lambda q: q.stop())
        spark.stop()

if __name__ == "__main__":
    main()
