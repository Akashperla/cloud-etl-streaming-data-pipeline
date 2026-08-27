import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),
])


def transform(df):
    """Validate and transform raw transaction records."""
    return (
        df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .filter(F.col("amount") >= 0)
        .dropDuplicates(["transaction_id"])
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("event_time")
    )


def run(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("cloud-etl-streaming-data-pipeline").getOrCreate()

    try:
        raw_df = (
            spark.read
            .option("header", True)
            .schema(SCHEMA)
            .csv(input_path)
        )

        curated_df = transform(raw_df)

        (
            curated_df
            .withColumn("event_date", F.to_date("event_timestamp"))
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(output_path)
        )
    finally:
        spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run the Cloud ETL PySpark pipeline")
    parser.add_argument("--input", required=True, help="Input CSV path or S3 URI")
    parser.add_argument("--output", required=True, help="Output directory or S3 URI")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output)
