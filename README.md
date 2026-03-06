# Data Engineering Pipeline – Bronze / Silver / Gold Architecture

## Overview

This project implements a **batch data pipeline using PySpark** following a **Medallion Architecture**:

* **Bronze Layer** – Raw ingestion + data validation
* **Silver Layer** – Enrichment and transformation
* **Gold Layer** – Aggregated analytical dataset

The pipeline processes **event data** and joins it with **user reference data** to produce **daily country-level metrics**.

Key goals implemented:

* Explicit schema enforcement
* Data quality validation
* Rejected record quarantine
* Deduplication and normalization
* Enrichment with dimension data
* Idempotent incremental processing
* Partitioned Parquet outputs
* Unit tests for pipeline components

---

# Project Structure

```
├── README.md
├── config
│   └── pipeline.yaml
├── data
│   ├── raw
│   │   └── events
│   │       ├── day_2025-01-01.jsonl
│   │       └── day_2025-01-02.jsonl
│   └── reference
│       └── users.csv
├── job
│   └── pipeline.py
├── tests
│   └── test_pipeline.py
└── pyproject.toml
```

### Key Components

| Component              | Description                             |
| ---------------------- | --------------------------------------- |
| `pipeline.py`          | Main Spark pipeline implementation      |
| `config/pipeline.yaml` | Pipeline configuration                  |
| `data/raw`             | Raw event data                          |
| `data/reference`       | Dimension data                          |
| `tests/`               | Unit tests                              |
| `pyproject.toml`       | Project dependencies and CLI entrypoint |

---

# Pipeline Design

The pipeline follows a **3-layer Medallion Architecture** commonly used in modern data platforms.

## Bronze Layer

Purpose:

* Ingest raw event data
* Apply schema enforcement
* Validate records
* Separate rejected records

Input:

```
data/raw/events/*.jsonl
```

Schema enforced:

| Column     | Type      |
| ---------- | --------- |
| event_id   | string    |
| user_id    | string    |
| event_type | string    |
| value      | double    |
| event_ts   | timestamp |

Processing steps:

1. Read JSONL files with explicit schema
2. Validate mandatory fields
3. Deduplicate events by `event_id`
4. Normalize `event_type`
5. Fill missing numeric values
6. Derive `event_date`

Outputs:

```
output/bronze/
output/quarantine/
```

---

## Silver Layer

Purpose:

* Enrich events with user reference data
* Add derived business fields

Input:

```
bronze layer
data/reference/users.csv
```

User reference schema:

| Column      | Type   |
| ----------- | ------ |
| user_id     | string |
| country     | string |
| signup_date | date   |

Processing steps:

1. Left join events with user dimension
2. Handle missing user records
3. Add derived fields

Derived fields:

| Field             | Description                                 |
| ----------------- | ------------------------------------------- |
| event_date        | Date extracted from event timestamp         |
| is_purchase       | Flag for purchase events                    |
| days_since_signup | Difference between event_ts and signup_date |

Output:

```
output/silver/
```

---

## Gold Layer

Purpose:
Produce analytics-ready aggregated dataset.

Aggregation level:

```
event_date, country
```

Metrics produced:

| Metric          | Description               |
| --------------- | ------------------------- |
| total_events    | Number of events          |
| total_value     | Sum of event value        |
| total_purchases | Number of purchase events |
| unique_users    | Distinct users            |

Output:

```
output/gold/
```

---

# Data Quality Rules

The following validations are applied during ingestion.

## Mandatory Fields

Records are rejected if any of the following are missing:

* `event_id`
* `user_id`
* `event_type`
* `event_ts`

Invalid records are written to:

```
output/quarantine/
```

---

## Deduplication

Events are deduplicated using:

```
event_id
```

This ensures **idempotency** when reprocessing the same data.

---

## Normalization

The pipeline normalizes:

```
event_type -> lowercase
```

Example:

```
Purchase → purchase
PURCHASE → purchase
```

---

## Missing Value Handling

| Column  | Strategy              |
| ------- | --------------------- |
| value   | filled with `0.0`     |
| country | filled with `unknown` |

---

# Incremental & Late Data Strategy

The pipeline is designed to be **idempotent and safe for reprocessing**.

## Partition Strategy

Data is partitioned by:

```
event_date
```

This allows efficient incremental processing.

---

## Idempotency

Deduplication using `event_id` guarantees:

* Running the pipeline multiple times **will not duplicate records**

---

## Late Data Handling

Late events are handled through:

```
dynamic partition overwrite
```

Spark configuration:

```
spark.sql.sources.partitionOverwriteMode = dynamic
```

When reprocessing:

* Only affected partitions are overwritten
* Late events automatically update the correct aggregates

Example:

```
late event from 2025-01-01
→ partition 2025-01-01 is recalculated
```

---

# Storage Format

All outputs are written as:

```
Parquet
```

Reasons:

* Columnar format
* Efficient compression
* Fast analytics queries
* Industry standard for data lakes

---

# Other Improvements / Initiatives

Additional improvements included in this implementation:

### Explicit Schemas

Avoids schema inference issues and improves pipeline stability.

---

### Layered Architecture

Clear separation of concerns:

```
Bronze → ingestion
Silver → transformation
Gold → analytics
```

---

### Partitioned Output

All layers partitioned by:

```
event_date
```

Benefits:

* faster queries
* efficient incremental processing

---

### Unit Testing

Core pipeline functions are tested using **pytest**.

Tests cover:

* schema validation
* transformation logic
* aggregation logic

---

# Configuration

Pipeline configuration is stored in:

```
config/pipeline.yaml
```

Example:

```
paths:
  raw_events: data/raw/events
  users: data/reference/users.csv
  output: output
```

---

# Running the Pipeline

## 1. Install dependencies

Requires **Python 3.12+**

Install project dependencies:

```
pip install -e .
```

---

## 2. Run the pipeline

```
run-pipeline --config config/pipeline.yaml
```

This will execute:

```
Bronze → Silver → Gold
```

Output directories will be created automatically.

---

# Running Tests

Install dev dependencies:

```
pip install -e .[dev]
```

Run tests:

```
pytest
```

---

# Example Output

Gold dataset example:

| event_date | country | total_events | total_value | total_purchases | unique_users |
| ---------- | ------- | ------------ | ----------- | --------------- | ------------ |
| 2025-01-01 | US      | 120          | 5400        | 25              | 80           |
| 2025-01-01 | ID      | 90           | 3200        | 18              | 60           |

---

# Assumptions

Some assumptions were made during implementation:

1. `event_id` uniquely identifies events
2. `event_ts` represents event occurrence time
3. user reference data is relatively small and suitable for broadcast joins
4. partitioning by `event_date` is sufficient for incremental updates

---

# Possible Future Improvements

Several enhancements could be added in a production environment:

### Streaming Ingestion

Using Spark Structured Streaming for near real-time pipelines.

---

### Data Quality Framework

Integrate with tools such as:

* Great Expectations
* Deequ

---

### Data Catalog Integration

Register datasets in:

* Hive Metastore
* AWS Glue Catalog

---

### Workflow Orchestration

Use orchestration tools such as:

* Airflow
* Dagster
* Prefect
