# Databricks Medallion Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end ELT pipeline using Databricks
and Delta Lake following the Medallion Architecture.

**Architecture**

    Raw CSV Files (Volumes)
            │
            ▼
    Bronze (Raw Delta Tables)
            │
            ▼
    Silver (Cleaned & Standardized Tables)
            │
            ▼
    Gold (Business-ready Fact & Dimension Tables)

## Tech Stack

-   Databricks
-   Apache Spark (PySpark)
-   Delta Lake
-   Databricks Workflows (Jobs)
-   Databricks SQL
-   Unity Catalog
-   GitHub

## Bronze Layer

### Source

-   CSV files stored in Databricks Volumes

### Processing

-   Read CSV files
-   Infer/provide schema
-   Write as Delta tables

### Output

-   `datawarehouse.bronze.*`

## Silver Layer

One notebook per source table.

Typical transformations include:

-   Remove duplicate records
-   Keep latest record using Window functions
-   Remove invalid IDs
-   Trim whitespace
-   Standardize text (`initcap`)
-   Map gender (`M/F -> Male/Female`)
-   Standardize marital status
-   Replace invalid values with NULL/defaults
-   Convert date columns
-   Extract Year / Month / Day
-   Rename columns
-   Reorder columns
-   Drop unnecessary columns
-   Write cleaned Delta tables

Output:

-   `datawarehouse.silver.*`

## Gold Layer (Planned)

Dimension tables:

-   dim_customers
-   dim_products
-   dim_locations

Fact tables:

-   fact_sales

Built using SQL with joins and aggregations.

Example pattern:

``` sql
CREATE OR REPLACE TABLE datawarehouse.gold.dim_customers AS
SELECT ...
FROM datawarehouse.silver.customers_info_cleaned;
```

## Workflow

Databricks Job orchestrates the pipeline.

    Bronze Ingestion
            │
            ▼
    Silver Customers
    Silver Products
    Silver Sales
    Silver Location
    ...
            │
            ▼
    Gold Dimensions
            │
            ▼
    Gold Facts

During development notebooks were validated individually before adding
them to the workflow.

## Design Decisions

-   Delta tables used for every layer
-   One Silver notebook per table for modularity
-   Overwrite mode during development
-   SQL reserved primarily for Gold layer
-   PySpark used for ingestion and cleansing

## Repository Structure

    project/
    │
    ├── notebooks/
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    │
    ├── sql/
    ├── images/
    └── README.md

## Future Improvements

-   Incremental loading
-   Parameterized notebooks
-   Data quality checks
-   Logging and monitoring
-   Retry policies
-   CI/CD deployment

## Learning Outcomes

-   Databricks Workflows
-   Medallion Architecture
-   PySpark transformations
-   Delta Lake
-   SQL data warehousing
-   Star schema design
-   ELT orchestration
