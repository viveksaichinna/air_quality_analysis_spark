from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("AirQualityIngestion") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("location_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("parameter", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType())
    ])

def read_from_tcp_stream(spark, schema):
    """Read streaming data from TCP socket with watermarking"""
    return spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load() \
        .select(from_csv(col("value"), schema.simpleString()).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "10 minutes")

def transform_stream(stream_df):
    """Clean and transform the stream"""
    processed_df = stream_df.withColumn("timestamp", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .select(
            col("location_id").cast("integer"),
            "timestamp",
            "parameter",
            "value",
            "city",
            "country"
        )
    
    return processed_df.groupBy("location_id", "timestamp", "city", "country") \
        .pivot("parameter", ["pm25", "temperature", "humidity"]) \
        .agg(first("value")) \
        .withColumnRenamed("pm25", "pm2_5")

def validate_data(df):
    """Run data quality checks"""
    df = df.filter(
        (col("pm2_5").between(0, 500)) & 
        (col("timestamp").isNotNull()) &
        (col("location_id").isNotNull())
    )
    
    return df.withColumn("is_valid", 
        when(col("pm2_5").isNull(), False)
        .when(col("temperature").isNull() | (col("temperature") < -50), False)
        .when(col("humidity").isNull() | (col("humidity") < 0), False)
        .otherwise(True)
    )

def write_stream(stream_df):
    """Write the stream to console and parquet"""
    # Console output (for debugging) - using update mode
    console_query = stream_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # For Parquet, we need to use complete mode with aggregations
    # First write to memory sink, then batch write to Parquet
    memory_query = stream_df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("air_quality_temp") \
        .start()
    
    # Periodically write to Parquet
    def write_to_parquet(batch_id):
        spark.sql("SELECT * FROM air_quality_temp") \
            .write \
            .mode("append") \
            .parquet("/workspaces/air_quality_analysis_spark/data/processed")
    
    parquet_query = stream_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_parquet) \
        .option("checkpointLocation", "/workspaces/air_quality_analysis_spark/data/checkpoints") \
        .start()
    
    return console_query

def main():
    spark = create_spark_session()
    schema = define_schema()
    
    raw_stream = read_from_tcp_stream(spark, schema)
    processed_stream = transform_stream(raw_stream)
    validated_stream = validate_data(processed_stream)
    
    query = write_stream(validated_stream)
    query.awaitTermination()

if __name__ == "__main__":
    main()