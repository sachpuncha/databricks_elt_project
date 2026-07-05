# Databricks Medallion Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end ELT pipeline using Databricks
and Delta Lake following the Medallion Architecture.

**Architecture**

<img width="881" height="551" alt="architecture_databricks" src="https://github.com/user-attachments/assets/d31dcfd8-36dd-4dd6-a32b-1bc4efae6387" />


## Tech Stack

-   Databricks
-   Apache Spark (PySpark)
-   Delta Lake
-   Databricks Workflows (Jobs)
-   Databricks SQL
-   Unity Catalog
-   GitHub

## Data Flow

<img width="831" height="551" alt="dataflow" src="https://github.com/user-attachments/assets/d9c7e592-379c-4601-935d-ff342529d9aa" />

## Bronze Layer

### Source

-   CSV files stored in Databricks Volumes

### Processing

-   Read CSV files
-   Schema
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
-   Standardize text
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

<img width="459" height="543" alt="table relationships" src="https://github.com/user-attachments/assets/4f24b5c9-d186-455a-8641-a86fa6711f39" />


## Gold Layer (Planned)

Dimension tables:

-   dim_customers
-   dim_products

Fact tables:

-   fact_sales

Built using SQL with joins and aggregations.

Example pattern:

``` sql
CREATE OR REPLACE TABLE datawarehouse.gold.dim_customers AS
SELECT ...
FROM datawarehouse.silver.customers_info_cleaned;
```

<img width="751" height="412" alt="star schema relatio" src="https://github.com/user-attachments/assets/4cee346c-94bb-4976-b4a9-b8226ae886a3" />


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
<img width="690" height="412" alt="image" src="https://github.com/user-attachments/assets/20d443c0-4a89-4fc8-8e4f-f5bc853d40bb" />

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
    |   |__ xplore/
    │
    ├── sql_queries/
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
