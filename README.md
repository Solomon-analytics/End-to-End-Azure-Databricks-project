# BrewTrack Analytics: End-to-End Data Engineering Pipeline on Microsoft Fabric

**A production-grade, cloud-native data engineering portfolio project built on Microsoft Fabric and Azure Data Lake Storage Gen2**

---

## Project Overview

BrewTrack Analytics is a fully automated, end-to-end data engineering pipeline built for a fictional US-based coffee and baked goods retail chain operating across multiple store locations. The project demonstrates real-world data engineering practices: ingesting raw operational data from a remote HTTP source, processing it through a layered Medallion Architecture (Raw > Landing > Bronze > Silver > Gold), and serving analytical-ready semantic models in Power BI.

This project was built entirely on **Microsoft Fabric** and **Azure Data Lake Storage Gen2**, with all orchestration managed through a parameterised Fabric Data Pipeline.

---

## Table of Contents

- [Business Problem](#business-problem)
- [Solution Architecture](#solution-architecture)
- [Technology Stack](#technology-stack)
- [Pipeline Layers Explained](#pipeline-layers-explained)
- [Data Model](#data-model)
- [Orchestration](#orchestration)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Semantic Models](#semantic-models)

---

## Business Problem

Retail chains in the food and beverage sector face a common set of operational data challenges that, if left unresolved, directly hurt profitability and the ability to make informed decisions. BrewTrack Analytics was designed to address the following:

### 1. Fragmented and Siloed Operational Data
Sales, inventory, customer, employee, product, and store data are generated daily across multiple store locations but stored in separate flat files with no unified view. Analysts cannot answer cross-functional questions such as: *Which stores are selling out of top products before closing time?*

### 2. No Incremental Data Loading Strategy
Without a reliable mechanism for tracking which data has already been processed, every pipeline run risks either reprocessing all historical data (costly and slow) or missing new records entirely. This leads to stale dashboards and duplicated analytical workloads.

### 3. Poor Data Quality Upstream
Source files contain inconsistent date formats, duplicate records, invalid email addresses, missing primary keys, and non-standardised categorical values (e.g. gender encoded as `m`, `M`, `Male`, `MALE`). These issues compound across millions of rows and produce misleading reports.

### 4. No Single Source of Truth for Inventory
Store managers lack a consolidated view of daily inventory levels, stockout events, waste quantities, and sell-through rates. Decisions about restocking are made reactively rather than from data.

### 5. No Customer or Employee Analytics Capability
The business holds valuable data on customer loyalty, purchase behaviour, and employee tenure but has no analytical layer to surface insights such as age-group purchasing patterns, loyalty card engagement, or staff-to-sales performance.

---

## Solution Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION LAYER                              │
│              Microsoft Fabric Data Pipeline (Parameterised)              │
│    ForEach Loop > Copy Data > Notebook (x4) - Fully Automated Daily Run  │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │         INGESTION LAYER              │
          │  HTTP Source (GitHub) → Raw ADLS     │
          │  6 CSV files per batch_month         │
          │  Partitioned by batch_month          │
          └──────────────────┬──────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │         LANDING LAYER                │
          │  Raw ADLS → Landing ADLS             │
          │  PySpark Notebook                    │
          │  Adds processing_date column         │
          │  Writes Parquet partitioned by       │
          │  processing_date (incremental)       │
          └──────────────────┬──────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │         BRONZE LAYER                 │
          │  Landing ADLS → Bronze Lakehouse     │
          │  Microsoft Fabric Lakehouse          │
          │  Picks up latest processing_date     │
          │  Delta format, schema applied        │
          └──────────────────┬──────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │         SILVER LAYER                 │
          │  Bronze → Silver Lakehouse           │
          │  5-point data quality framework:     │
          │  - Deduplication                     │
          │  - Null checks on primary keys       │
          │  - Data type casting & validation    │
          │  - Standardisation (trim, case,      │
          │    date formats, categoricals)       │
          │  - Referential integrity checks      │
          │  UPSERT (MERGE) logic via Delta Lake │
          └──────────────────┬──────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │         GOLD LAYER                   │
          │  Silver → Gold Lakehouse             │
          │  8 tables: 2 fact, 4 dim, 2 calendar │
          │  Business enrichment (age, category, │
          │  surrogate keys, date IDs)           │
          │  Star schema - ready for BI          │
          └──────────────────┬──────────────────┘
                             │
          ┌──────────────────▼──────────────────┐
          │       SEMANTIC MODEL LAYER           │
          │  Power BI Semantic Models            │
          │  Sales Model: fact_sales + 4 dims    │
          │  Inventory Model: fact_inventory     │
          │  + 3 dims                            │
          └─────────────────────────────────────┘
```

---

## Technology Stack

| Component | Tool / Service |
|---|---|
| Cloud Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute & Processing | Microsoft Fabric (Spark Notebooks) |
| Orchestration | Microsoft Fabric Data Pipeline |
| Data Format (Landing) | Apache Parquet |
| Data Format (Lakehouse) | Delta Lake |
| Transformation Language | PySpark (Python) |
| Data Modelling | Microsoft Fabric Lakehouse (OneLake) |
| Semantic Layer | Power BI Semantic Models |
| Source Data | HTTP / GitHub (CSV files) |
| Pipeline Parameters | Fabric Pipeline Parameters (batch_month, processed_date, today_date, file array, storage accounts, workspace, lakehouse IDs) |

---

## Pipeline Layers Explained

### Raw ADLS
Six CSV files are ingested from an HTTP source (GitHub) per `batch_month` using the Fabric Data Pipeline ForEach loop with a `Copy data` activity. Files ingested:
- `customer_lookup`
- `employee_lookup`
- `food_inventory`
- `product_lookup`
- `sales_by_store`
- `store_lookup`

Files land in ADLS under month-partitioned folders (e.g. `2017-01/`, `2017-02/`).

### Landing ADLS (PySpark Notebook: raw_ADLS_to_landing_ADLS)
- Authenticates to ADLS using an access key
- Reads each CSV file from the raw container
- Validates the file contains data before processing
- Appends a `processing_date` column (passed as a pipeline parameter) to enable incremental loading
- Writes output as Parquet, partitioned by `processing_date`, to the landing ADLS container

### Bronze Lakehouse (PySpark Notebook: landing_to_bronze_lh)
- Reads Parquet files from the landing ADLS container, filtered to the latest `processing_date`
- Writes to Delta tables in the Bronze Fabric Lakehouse
- Provides a schema-applied, queryable layer before transformation begins

### Silver Lakehouse (PySpark Notebook: bronze_to_silver)
Applies a structured **5-point data quality framework** across all 6 datasets:

1. **Duplicate detection and removal** using meaningful key combinations (e.g. `customer_id + customer_since + birthdate`)
2. **Null checks** on primary key columns; rows failing null checks are dropped and logged
3. **Data type casting** to appropriate types (INT, DATE, DECIMAL, STRING)
4. **Standardisation**: whitespace trimming, lowercase column headers, title-case string values, multi-format date normalisation (`yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`, etc.), categorical value standardisation (e.g. gender)
5. **Business rule validation**: email format checks (invalid `@` and `.` patterns blanked out), date boundary checks (no future dates, no pre-1900 dates), Boolean enforcement (`Y`/`N` flags)

All tables are loaded using **Delta Lake MERGE (UPSERT)** logic:
- `WHEN MATCHED`: update existing records
- `WHEN NOT MATCHED`: insert new records

Tables created in Silver: `customer_silver`, `employee_silver`, `product_silver`, `sales_silver`, `store_silver`, `food_inventory_silver`

### Gold Lakehouse (PySpark Notebook: silver_to_gold)
Business transformation and enrichment layer. Produces 8 tables in a star schema:

**Dimension Tables**
- `dim_customer`: includes derived `age` and `age_category` fields
- `dim_employee`: includes `duration_category` tenure banding
- `dim_product`: product catalogue with pricing and margin fields
- `dim_store`: store geolocation and neighbourhood data

**Fact Tables**
- `fact_sales`: transactional sales data with SHA-256 surrogate `sales_key` and `transaction_date_id`
- `fact_inventory`: daily inventory records with `stockout_flag`, `sell_through_rate`, and `waste_quantity`

**Calendar Tables**
- `transaction_calendar`: date spine for sales analysis (fiscal month, day of week, etc.)
- `baked_calendar`: date spine for inventory analysis

---

## Data Model

### Sales Semantic Model
```
transaction_calendar (1) ─── (*) fact_sales (*) ─── (1) dim_product
                                      (*)
                                      │
                              (1) dim_customer
                                      │
                              (1) dim_employee
```

### Inventory Semantic Model
```
baked_calendar (1) ─── (*) fact_inventory (*) ─── (1) dim_product
                                  (*)
                                  │
                          (1) dim_store
```

---

## Orchestration

The pipeline is driven by a single **Microsoft Fabric Data Pipeline** with the following parameterised inputs:

| Parameter | Description |
|---|---|
| `batch_month` | The monthly batch to process (e.g. `2017-01`) |
| `processed_date` | Date value appended to landing files for partitioning |
| `today_date` | Used in silver and gold notebooks to filter the latest batch |
| `p_file_name` | Array of 6 file names to loop through during HTTP ingestion |
| `source_account` | Source ADLS storage account name |
| `source_container` | Source ADLS container name (`raw`) |
| `destination_account` | Destination ADLS storage account name |
| `destination_container` | Destination ADLS container name (`landing`) |
| `workspace` | Microsoft Fabric workspace ID |
| `lakehouse` | Target Fabric Lakehouse ID |

**Pipeline flow:**
1. ForEach loop iterates over `p_file_name` array, runs a Copy data activity per file (HTTP to raw ADLS)
2. Notebook: `raw ADLS to landing ADLS` (reads raw, adds `processing_date`, writes Parquet to landing)
3. Notebook: `ADLS landing to bronze lh` (reads landing Parquet, writes Delta to bronze lakehouse)
4. Notebook: `process from bronze to silver` (applies DQ framework, UPSERT to silver lakehouse)
5. Notebook: `process from silver to gold` (enrichment, writes 8 gold tables)

All 5 activities completed successfully in under 20 minutes in testing (full run logged on 20/05/2026).

---

## Key Engineering Decisions

**Incremental loading via `processing_date` partitioning**
Rather than full reloads, each notebook filters on `processing_date == today_date`, ensuring only that day's batch is processed. This pattern supports daily scheduled runs without reprocessing historical data.

**UPSERT (MERGE) over append or overwrite**
Using Delta Lake MERGE logic allows the pipeline to handle both new records and corrections to existing records without duplicating data. This is critical for slowly changing dimensions like customer and product data.

**Try/Except table creation pattern**
Silver and gold notebooks attempt to read an existing Delta table; if it does not exist, they create it with an explicit schema before running the MERGE. This makes the pipeline idempotent and safe to run on first execution without manual setup.

**SHA-256 surrogate key for sales**
Rather than relying on a potentially non-unique `transaction_id`, a composite surrogate key (`sales_key`) is generated using SHA-256 hashing across `transaction_id + transaction_date + transaction_time + order + line_item_id`. This ensures row-level uniqueness for MERGE operations.

**ForEach loop in Fabric Pipeline**
HTTP ingestion is driven by a `p_file_name` array parameter passed to a ForEach loop, making it straightforward to add new source files without restructuring the pipeline.

---

## Project Structure

```
brewtrack-analytics/
│
├── notebooks/
│   ├── raw_ADLS_to_landing_ADLS.py       # HTTP raw → Landing ADLS (Parquet)
│   ├── landing_to_bronze_lh.py           # Landing ADLS → Bronze Lakehouse (Delta)
│   ├── bronze_to_silver.py               # Bronze → Silver (DQ + UPSERT)
│   └── silver_to_gold.py                 # Silver → Gold (Enrichment + Star Schema)
│
├── gold_tables/
│   ├── dim_customer.csv
│   ├── dim_employee.csv
│   ├── dim_product.csv
│   ├── dim_store.csv
│   ├── fact_sales.csv
│   ├── fact_inventory.csv
│   ├── transaction_calendar.csv
│   └── baked_calendar.csv
│
├── pipeline/
│   └── ORCHESTRATION_PIPELINE_PARAMETERS.txt
│
├── docs/
│   ├── all_data_ingested_from_http_to_adls_raw.png
│   ├── data_processed_from_raw_to_landing_adls_using_fabric_notebook.png
│   ├── orchestration_pipeline.png
│   ├── sales_semantic_model.png
│   └── inventory_semantic_model.png
│
└── README.md
```

---

## How to Run

1. **Provision infrastructure**: Create an ADLS Gen2 storage account with `raw` and `landing` containers. Create a Microsoft Fabric workspace with three Lakehouses (Bronze, Silver, Gold).

2. **Upload notebooks**: Import all four notebooks into your Fabric workspace.

3. **Create the pipeline**: Build a Fabric Data Pipeline matching the architecture above. Define all parameters listed in the Orchestration section.

4. **Trigger the pipeline**: Set `batch_month` to the target month (e.g. `2017-01`), `processed_date` and `today_date` to the current run date (e.g. `2026-05-20`), and provide your storage account credentials.

5. **Verify outputs**: Check the Gold Lakehouse for all 8 tables, then validate the Power BI semantic models.

---

## Semantic Models

Two Power BI semantic models are served from the Gold Lakehouse:

**Sales Semantic Model**
Connects `fact_sales` to `dim_product`, `dim_customer`, `dim_employee`, and `transaction_calendar`. Supports analysis of revenue, transaction volume, product mix, customer demographics, and time-of-day patterns.

**Inventory Semantic Model**
Connects `fact_inventory` to `dim_product`, `dim_store`, and `baked_calendar`. Supports analysis of daily stock levels, sell-through rates, waste quantities, and stockout events by store and product.

---

## Author

Built as a portfolio project demonstrating end-to-end data engineering skills on Microsoft Fabric and Azure, including pipeline orchestration, incremental data loading, PySpark transformation, Delta Lake UPSERT patterns, data quality frameworks, and dimensional modelling.

