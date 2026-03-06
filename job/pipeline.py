import argparse
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, DateType
)
from pyspark.sql.functions import (
    col, lower, to_date, when,
    datediff, count, count_distinct, sum as spark_sum
)


# ============================================================
# Config
# ============================================================
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# Spark Session
# ============================================================
def get_spark(app_name: str = "de-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ============================================================
# Schemas
# ============================================================
def get_event_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("event_ts", TimestampType(), False),
    ])


def get_user_schema() -> StructType:
    return StructType([
        StructField("user_id", StringType(), False),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
    ])


# ============================================================
# Bronze Layer
# ============================================================
def bronze_layer(spark: SparkSession, config: dict) -> tuple[DataFrame, DataFrame]:
    """
    - Read raw JSONL files with explicit schema
    - Validate records
    - Deduplicate
    - Normalize
    - Fill missing values
    - Return (clean_df, rejected_df)
    """

    event_schema = get_event_schema()

    df_raw = (
        spark.read
        .schema(event_schema)
        .json(config["paths"]["raw_events"])
    )

    # Validation rule
    df_flagged = df_raw.withColumn(
        "is_valid",
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isNotNull() &
        col("event_ts").isNotNull() &
        ~col("event_id").eqNullSafe("") &
        ~col("user_id").eqNullSafe("") &
        ~col("event_type").eqNullSafe("")
    )

    df_valid = df_flagged.filter("is_valid = true").drop("is_valid")
    df_rejected = df_flagged.filter("is_valid = false")

    # Deduplicate by event_id (idempotency guarantee)
    df_valid = df_valid.dropDuplicates(["event_id"])

    # Normalize
    df_valid = df_valid.withColumn(
        "event_type",
        lower(col("event_type"))
    )

    # Fill missing numeric values
    df_valid = df_valid.fillna({
        "value": 0.0
    })

    # Add event_date (partition column early)
    df_valid = df_valid.withColumn(
        "event_date",
        to_date("event_ts")
    )

    # Write bronze (clean)
    (
        df_valid.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{config['paths']['output']}/bronze")
    )

    # Write quarantined records
    (
        df_rejected.write
        .mode("overwrite")
        .parquet(f"{config['paths']['output']}/quarantine")
    )

    return df_valid, df_rejected


# ============================================================
# Silver Layer
# ============================================================
def silver_layer(spark: SparkSession, df_bronze: DataFrame, config: dict) -> DataFrame:
    """
    - Join with user reference data
    - Handle missing dimension records
    - Add derived fields
    """

    user_schema = get_user_schema()

    users_df = (
        spark.read
        .schema(user_schema)
        .option("header", True)
        .csv(config["paths"]["users"])
    )

    # Left join (never lose fact records)
    df_silver = df_bronze.join(
        users_df,
        on="user_id",
        how="left"
    )

    # Handle missing dimension
    df_silver = df_silver.fillna({
        "country": "unknown"
    })

    # Derived fields
    df_silver = (
        df_silver
        .withColumn(
            "is_purchase",
            when(col("event_type") == "purchase", 1).otherwise(0)
        )
        .withColumn(
            "days_since_signup",
            datediff(col("event_ts"), col("signup_date"))
        )
    )

    # Write silver
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{config['paths']['output']}/silver")
    )

    return df_silver


# ============================================================
# Gold Layer
# ============================================================
def gold_layer(df_silver: DataFrame, config: dict) -> DataFrame:
    """
    - Produce daily country-level aggregates
    - Idempotent via dynamic partition overwrite
    """

    df_gold = (
        df_silver
        .groupBy("event_date", "country")
        .agg(
            count("*").alias("total_events"),
            spark_sum("value").alias("total_value"),
            spark_sum("is_purchase").alias("total_purchases"),
            count_distinct("user_id").alias("unique_users"),
        )
    )

    (
        df_gold.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{config['paths']['output']}/gold")
    )

    return df_gold


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    spark = get_spark()

    try:
        df_bronze, df_rejected = bronze_layer(spark, config)
        df_silver = silver_layer(spark, df_bronze, config)
        df_gold = gold_layer(df_silver, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()