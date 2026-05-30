# Distributed Data Processing Project

## Overview

This project demonstrates key distributed data engineering concepts using Databricks, Delta Lake, and Apache Spark:

* Incremental data ingestion using Auto Loader
* Schema evolution handling
* Partitioning strategies
* Delta Lake optimization
* Bronze → Silver → Gold Medallion Architecture
* Incremental processing using MERGE
* Unity Catalog and Volumes

The project simulates a retail analytics pipeline where new sales files arrive daily and schema changes can occur over time.

---

# Architecture

```text
                +------------------+
                |   Raw CSV Files  |
                +------------------+
                         |
                         v
                +------------------+
                | Bronze Layer     |
                | Auto Loader      |
                +------------------+
                         |
                         v
                +------------------+
                | Silver Layer     |
                | Clean & Dedupe   |
                +------------------+
                         |
                         v
                +------------------+
                | Gold Layer       |
                | Aggregations     |
                +------------------+
```

---

# Technology Stack

* Apache Spark (PySpark)
* Delta Lake
* Databricks
* Unity Catalog
* Auto Loader
* Delta MERGE
* Delta OPTIMIZE & ZORDER

---

# Project Structure

```text
distributed-data-processing/
│
├── data/
│   ├── sales_day1.csv
│   ├── sales_day2.csv
│   └── sales_day3_schema_changed.csv
│
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformation
│   ├── 03_gold_aggregation
│   └── 04_optimization
│
└── README.md
```

---

# Step 1: Create Catalog

```sql
CREATE CATALOG IF NOT EXISTS distributed_data_processing;
```

---

# Step 2: Create Schema

```sql
CREATE SCHEMA IF NOT EXISTS distributed_data_processing.distributed_project;
```

---

# Step 3: Create Volume

```sql
CREATE VOLUME distributed_data_processing.distributed_project.retail_volume;
```

---

# Step 4: Create Folder Structure

Create the following folders inside the volume:

```text
retail_volume/
│
├── raw/
├── schema/
├── checkpoints/
│   └── bronze/
├── bronze/
├── silver/
└── gold/
```

---

# Sample Data

## Day 1

sales_day1.csv

```csv
order_id,customer_id,product_id,amount,order_date
1,101,P100,500,2026-05-01
2,102,P200,700,2026-05-01
3,103,P300,900,2026-05-01
```

## Day 2

sales_day2.csv

```csv
order_id,customer_id,product_id,amount,order_date
4,104,P100,1000,2026-05-02
5,105,P200,1200,2026-05-02
```

## Day 3 (Schema Evolution)

sales_day3_schema_changed.csv

```csv
order_id,customer_id,product_id,amount,order_date,payment_mode
6,106,P500,1500,2026-05-03,CARD
7,107,P600,1800,2026-05-03,UPI
```

---

# Bronze Layer - Incremental Ingestion

## Enable Schema Evolution

```python
spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)
```

## Define Paths

```python
raw_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/raw/"

schema_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/schema/"

checkpoint_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/checkpoints/bronze/"

bronze_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/bronze/sales/"
```

## Read Incrementally Using Auto Loader

```python
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(raw_path)
)
```

## Write Bronze Layer

```python
(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .start(bronze_path)
)
```

### Incremental Processing

Day 1:

* Upload sales_day1.csv
* Run Bronze notebook

Day 2:

* Upload sales_day2.csv
* Run Bronze notebook
* Auto Loader processes only the new file

Day 3:

* Upload sales_day3_schema_changed.csv
* Run Bronze notebook
* Auto Loader detects new column payment_mode

---

# Silver Layer - Data Cleaning

## Read Bronze Data

```python
bronze_df = spark.read.format("delta").load(bronze_path)
```

## Clean and Deduplicate

```python
from pyspark.sql.functions import col

silver_df = (
    bronze_df
    .dropDuplicates(["order_id"])
    .filter(col("amount") > 0)
)
```

---

# Production Approach for Schema Evolution

Enable auto schema evolution:

```python
spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)
```

---

# Initial Silver Load

```python
silver_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/silver/sales/"

(
    silver_df.write
    .format("delta")
    .partitionBy("order_date")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)
```

---

# Incremental Silver Processing (Production Pattern)

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    silver_path
)

(
    delta_table.alias("t")
    .merge(
        silver_df.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

Benefits:

* Incremental processing
* Handles late arriving data
* Supports schema evolution
* No full table rewrite

---

# Gold Layer

## Read Silver Data

```python
silver_df = spark.read.format("delta").load(silver_path)
```

## Create Daily Sales Aggregation

```python
from pyspark.sql.functions import sum

gold_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        sum("amount").alias("total_sales")
    )
)
```

## Write Gold Layer

```python
gold_path = "/Volumes/distributed_data_processing/distributed_project/retail_volume/gold/daily_sales/"

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)
```

---

# Delta Optimization

## Optimize Table

```sql
OPTIMIZE distributed_data_processing.distributed_project.silver_sales;
```

## ZORDER

```sql
OPTIMIZE distributed_data_processing.distributed_project.silver_sales
ZORDER BY (customer_id);
```

---

# Key Concepts Demonstrated

## Incremental Processing

* Auto Loader processes only new files.
* Checkpointing prevents duplicate ingestion.

## Schema Evolution

* New columns are automatically added.
* Existing records retain NULL values for new columns.

## Partitioning Strategy

Silver layer is partitioned by:

```text
order_date
```

Benefits:

* Reduced data scanning
* Improved query performance
* Better distributed execution

## Distributed Processing

* Parallel execution using Spark executors
* Repartitioning support
* Delta Lake optimization

---

# Interview Talking Points

### How was incremental ingestion implemented?

Using Databricks Auto Loader with checkpointing and schema tracking.

### How was schema evolution handled?

Using:

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

and

```python
spark.databricks.delta.schema.autoMerge.enabled=true
```

### Why partition by order_date?

Most analytical queries filter by date, reducing file scans and improving performance.

### Why use MERGE instead of overwrite?

MERGE supports:

* Incremental updates
* Late arriving data
* Efficient upserts
* Reduced compute cost

---

# Resume Description

Built a distributed retail analytics pipeline using Apache Spark and Delta Lake implementing incremental ingestion, schema evolution handling, and partition optimization. Designed Bronze-Silver-Gold architecture using Databricks Auto Loader and Delta MERGE operations, enabling scalable processing of evolving datasets while improving query performance through partitioning and Delta optimization techniques.
