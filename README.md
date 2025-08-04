# End-to-End Data Lakehouse with Spark, Iceberg, and Snowflake

A complete, end-to-end data engineering project that demonstrates how to build a transactional Data Lakehouse on AWS S3 using a modern data stack. The pipeline ingests raw data from the public arXiv API, processes it through a Bronze, Silver, and Gold Medallion Architecture using Apache Spark and Apache Iceberg, and makes it available for high-performance analytics in Snowflake.

## Table of Contents

- [Highlights](#highlights)
- [Project Architecture](#project-architecture)
- [Technology Stack](#technology-stack)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [How to Run the Project](#how-to-run-the-project)
- [Snowflake Integration](#snowflake-integration)
- [Architectural Decisions](#architectural-decisions)
- [Future Evolution & Production Readiness](#future-evolution--production-readiness)
- [File Structure](#file-structure)

## Highlights

This project serves as a practical implementation of several key modern data engineering concepts:

-   **Modern Data Stack:** Utilizes Docker, Python, Apache Spark, and Apache Iceberg for a robust and scalable solution.
-   **Medallion Architecture:** Organizes data into Bronze (raw), Silver (cleaned, transactional), and Gold (aggregated, business-ready) layers for reliability and governance.
-   **Transactional Data Lake:** Leverages Apache Iceberg to provide ACID transactions, schema evolution, and time-travel capabilities directly on S3, solving common Data Lake challenges.
-   **Decoupled Storage and Compute:** AWS S3 acts as the central, persistent storage layer, while Apache Spark (running locally via Docker) provides the flexible compute power for data processing.
-   **Seamless Cloud DW Integration:** Demonstrates how the final Gold layer tables in the Lakehouse can be transparently queried by Snowflake for advanced BI and analytics.

## Project Architecture

The project follows the Medallion Architecture, ensuring data flows from raw ingestion to business-ready tables in a reliable and governed manner.

1.  **Bronze Layer:** Raw XML/JSON files are ingested from the arXiv API and stored untouched in a bronze layer in S3.
2.  **Silver Layer:** The raw data is cleaned, structured with a proper schema, and loaded into a transactional Apache Iceberg table located in a silver layer in S3.
3.  **Gold Layer:** The clean Silver data is aggregated to create business-focused tables (e.g., yearly publication stats, author summaries) and stored as new Iceberg tables in a gold layer in S3.

## Technology Stack

-   **Orchestration:** Docker & Makefile
-   **Language:** Python
-   **Processing Framework:** Apache Spark
-   **Table Format:** Apache Iceberg
-   **Storage:** AWS S3
-   **Data Warehouse:** Snowflake
-   **Core Libraries:** PySpark, PyIceberg, Boto3, Requests, Ruff

## Data Flow

The data processing is entirely decoupled, with a local machine acting as the compute unit and AWS S3 as the persistent storage layer.

1.  **Ingestion (Bronze):** The `ingest_to_bronze.py` script runs locally, fetches data from the arXiv API, and writes the raw files directly to the `bronze/` path in the S3 bucket.
2.  **Processing (Silver):** The `process_to_silver.py` Spark job reads the raw JSON files from the `bronze/` path, cleans and transforms the data in memory, and writes it back to S3 as a partitioned Iceberg table in the `silver/` path.
3.  **Aggregation (Gold):** The `build_gold_layer.py` Spark job reads the trusted Silver table, performs aggregations (e.g., `groupBy`, `count`), and writes the results as new, optimized Iceberg tables to the `gold/` path in S3.
4.  **Consumption (Snowflake):** Snowflake queries the Gold layer tables directly. Through a configured Catalog Integration, it reads the Iceberg metadata from S3 to locate the necessary data files (Parquet) and execute queries performantly within its own cloud environment.

## Prerequisites

-   AWS Account
-   IAM User with programmatic access and permissions for the S3 bucket.
-   A unique S3 Bucket to act as the Data Lake.
-   Snowflake Account
-   Docker
-   Python 3.13

## How to Run the Project

This project is orchestrated using a `Makefile` for simplicity and reproducibility.

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/BrunoChiconato/lakehouse-from-scratch.git
    cd lakehouse-from-scratch
    ```

2.  **Configure environment variables:**
    Copy the `.env.example` to `.env` and fill in your AWS credentials and S3 bucket name.
    ```bash
    cp .env.example .env
    # Edit the .env file with your credentials
    ```

3.  **Build the environment:**
    This command builds the Docker image with all necessary dependencies (Python, Java, Spark).
    ```bash
    make build
    ```

### Running the ETL Pipeline

You can run each step of the Medallion Architecture individually or the full pipeline at once.

-   **Run the full pipeline (Bronze -> Silver -> Gold):**
    ```bash
    make run_full_pipeline
    ```

-   **Run individual steps:**
    ```bash
    make bronze  # Step 1: Ingest data to Bronze
    make silver  # Step 2: Process data to Silver
    make gold    # Step 3: Build aggregated Gold tables
    ```

## Snowflake Integration

The final Gold layer tables can be queried directly from Snowflake. This integration is achieved by creating key objects in Snowflake that securely point to your Data Lakehouse in S3.

### Setup Steps

1.  **Configure AWS IAM:**
    -   Create an IAM Policy that grants read/list permissions to your S3 bucket.
    -   Create an IAM Role (`SnowflakeRole`) and attach the policy to it.
    -   Configure the Role's Trust Policy to allow Snowflake's IAM user to assume it, using the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` provided by Snowflake.

2.  **Configure Snowflake:**
    -   Create a `STORAGE INTEGRATION` that references the ARN of the IAM Role you created.
    -   Create a `CATALOG INTEGRATION` to inform Snowflake that the table catalog is externally managed in S3.
    -   Create `ICEBERG TABLE`s pointing to the metadata files of your Gold tables in S3.

### Example SQL in Snowflake

After setting up the integration, you can query your data with standard SQL:

```sql
-- Create the necessary objects in Snowflake
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE ACADEMIC_LAKEHOUSE;
CREATE OR REPLACE SCHEMA GOLD_LAYER;

CREATE OR REPLACE EXTERNAL VOLUME your_external_volume
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'your-s3-storage-location'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://<your-s3-bucket-name>/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-aws-account-id>:role/<your-snowflake-iam-role>'
      )
    );
 
CREATE OR REPLACE CATALOG INTEGRATION your_iceberg_catalog_integration
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

CREATE OR REPLACE ICEBERG TABLE your_gold_table
  EXTERNAL_VOLUME = 'your_external_volume'
  CATALOG = 'your_iceberg_catalog_integration'
  METADATA_FILE_PATH = 'path/to/table/metadata/vX.metadata.json';

-- Refresh and query the table
ALTER ICEBERG TABLE your_gold_table REFRESH 'path/to/table/metadata/vX.metadata.json';

SELECT * FROM your_gold_table LIMIT 10;
```

## Architectural Decisions

  - **Why Iceberg over Delta Lake or Hudi?** Iceberg was chosen for its open, platform-independent metadata model, which avoids the vendor lock-in that can be associated with other solutions.
  - **Why Spark for this data volume?** While the initial data volume from the arXiv API could be handled by Pandas/Polars, using Spark from the outset builds a scalable foundation. The architecture is ready to handle future growth in data volume without a complete redesign.
  - **What business problem do ACID transactions solve here?** The ability to perform `UPDATE` and `DELETE` operations on the Data Lake simulates real-world scenarios where metadata must be corrected post-publication (e.g., correcting author names or categories). This is a critical capability impossible in a traditional Parquet-based Data Lake.

## Future Evolution & Production Readiness

This project uses a local Docker environment for development simplicity. The next logical step is to evolve the architecture for a production-ready, cloud-native environment.

  - **Managed AWS Services:** Replace local Spark execution with **AWS Glue** or **Amazon EMR Serverless** for scalable, on-demand compute. The `Makefile` orchestration can be upgraded to **AWS Step Functions** or **Managed Workflows for Apache Airflow (MWAA)**.
  - **Kubernetes (k8s):** For container-centric environments, the Spark jobs can be containerized and deployed on **Amazon EKS (Elastic Kubernetes Service)**, using the **Spark on k8s Operator** to manage the application lifecycle, providing resilience and standardized management.

## File Structure

```
.
├── docker-compose.yml
├── Dockerfile
├── .env
├── .env.example
├── .gitignore
├── Makefile
├── pyproject.toml
├── .python-version
├── README.md
├── requirements.txt
├── src
│   ├── build_gold_layer.py
│   ├── ingest_to_bronze.py
│   └── process_to_silver.py
└── uv.lock
```