import os
from datetime import datetime
import tempfile
import pandas as pd
import yaml
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    TimestampType,
    DateType,
)

from job.pipeline import (
    load_config,
    get_spark,
    get_event_schema,
    get_user_schema,
    bronze_layer,
    silver_layer,
    gold_layer
)

def create_test_config(tmpdir, raw_dir, users_file):
    return {
        "paths": {
            "raw_events": raw_dir,
            "users": users_file,
            "output": tmpdir
        }
    }


def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(r + "\n")


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()

# ============================================================
# load_config
# ============================================================

def test_load_config(tmp_path):
    config_data = {
        "kafka": {"topic": "events"},
        "database": {"host": "localhost"}
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)

    assert config["kafka"]["topic"] == "events"
    assert config["database"]["host"] == "localhost"


# ============================================================
# Spark Session
# ============================================================

def test_get_spark():
    spark = get_spark("test-app")

    assert spark is not None
    assert spark.sparkContext.appName == "test-app"

    spark.stop()


# ============================================================
# Event Schema
# ============================================================

def test_event_schema_structure():
    schema = get_event_schema()

    assert isinstance(schema, StructType)

    fields = {f.name: f.dataType for f in schema.fields}

    assert fields["event_id"] == StringType()
    assert fields["user_id"] == StringType()
    assert fields["event_type"] == StringType()
    assert fields["value"] == DoubleType()
    assert fields["event_ts"] == TimestampType()


def test_event_schema_nullability():
    schema = get_event_schema()
    fields = {f.name: f.nullable for f in schema.fields}

    assert fields["event_id"] is False
    assert fields["user_id"] is False
    assert fields["event_ts"] is False


# ============================================================
# User Schema
# ============================================================

def test_user_schema_structure():
    schema = get_user_schema()

    assert isinstance(schema, StructType)

    fields = {f.name: f.dataType for f in schema.fields}

    assert fields["user_id"] == StringType()
    assert fields["country"] == StringType()
    assert fields["signup_date"] == DateType()


# ============================================================
# Schema compatibility with Spark
# ============================================================

def test_event_schema_with_spark(spark):
    schema = get_event_schema()

    data = [
        ("e1", "u1", "purchase", 10.5, datetime(2024, 1, 1, 10, 0, 0)),
    ]

    df = spark.createDataFrame(data, schema=schema)

    assert df.count() == 1
    assert "event_id" in df.columns


def test_user_schema_with_spark(spark):
    schema = get_user_schema()

    data = [
        ("u1", "ID", datetime(2024, 1, 1)),
    ]

    df = spark.createDataFrame(data, schema=schema)

    assert df.count() == 1
    assert "user_id" in df.columns

# --------------------------------------------------
# Bronze Layer Test
# --------------------------------------------------

def test_bronze_layer_cleaning(spark):

    with tempfile.TemporaryDirectory() as tmpdir:

        raw_dir = os.path.join(tmpdir, "events")
        os.makedirs(raw_dir)

        file_path = os.path.join(raw_dir, "data.jsonl")

        rows = [
            '{"event_id":"1","user_id":"u1","event_type":"PURCHASE","value":10.0,"event_ts":"2025-01-01T10:00:00"}',
            '{"event_id":"1","user_id":"u1","event_type":"PURCHASE","value":10.0,"event_ts":"2025-01-01T10:00:00"}',
            '{"event_id":"","user_id":"u2","event_type":"CLICK","value":5,"event_ts":"2025-01-01T11:00:00"}'
        ]

        write_jsonl(file_path, rows)

        config = create_test_config(tmpdir, raw_dir, "")

        df_valid, df_rejected = bronze_layer(spark, config)

        valid_rows = df_valid.collect()
        rejected_rows = df_rejected.collect()

        assert len(valid_rows) == 1
        assert len(rejected_rows) == 1

        assert valid_rows[0]["event_type"] == "purchase"


# --------------------------------------------------
# Silver Layer Test
# --------------------------------------------------

def test_silver_layer_enrichment(spark):

    with tempfile.TemporaryDirectory() as tmpdir:

        users_path = os.path.join(tmpdir, "users.csv")

        users = pd.DataFrame({
            "user_id": ["u1"],
            "country": ["US"],
            "signup_date": ["2024-12-01"]
        })

        users.to_csv(users_path, index=False)

        bronze_df = spark.createDataFrame([
            ("1", "u1", "purchase", 10.0, datetime(2025,1,1,10,0,0), "2025-01-01")
        ], [
            "event_id",
            "user_id",
            "event_type",
            "value",
            "event_ts",
            "event_date"
        ])

        config = {
            "paths": {
                "users": users_path,
                "output": tmpdir
            }
        }

        df_silver = silver_layer(spark, bronze_df, config)

        row = df_silver.collect()[0]

        assert row["country"] == "US"
        assert row["is_purchase"] == 1
        assert row["days_since_signup"] > 0


# --------------------------------------------------
# Gold Layer Test
# --------------------------------------------------

def test_gold_layer_aggregation(spark):

    with tempfile.TemporaryDirectory() as tmpdir:

        df_silver = spark.createDataFrame([
            ("u1", "purchase", 10.0, "US", 1, "2025-01-01"),
            ("u2", "click", 5.0, "US", 0, "2025-01-01")
        ], [
            "user_id",
            "event_type",
            "value",
            "country",
            "is_purchase",
            "event_date"
        ])

        config = {
            "paths": {
                "output": tmpdir
            }
        }

        df_gold = gold_layer(df_silver, config)

        row = df_gold.collect()[0]

        assert row["total_events"] == 2
        assert row["total_value"] == 15.0
        assert row["total_purchases"] == 1
        assert row["unique_users"] == 2