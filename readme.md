PySpark + Airflow Batch ETL Pipeline (API → MySQL)

**Overview**

This project demonstrates a small-scale batch ETL pipeline that ingests user data from a public API, processes it using PySpark, and loads analytics-ready data into a relational database. **Apache Airflow** is used to orchestrate the end-to-end workflow, manage task dependencies, retries, and ensure reliable data loading.

The project serves as a proof-of-concept for integrating PySpark-based transformations within an Airflow-managed ETL pipeline before scaling to a larger producer–consumer architecture on AWS.

**Business Use Case**

- In data-driven systems (SaaS platforms, consumer applications, analytics teams), user data is generated daily and must be ingested, processed, and stored reliably for reporting and analysis.
- This pipeline simulates a daily user ingestion process where raw API data is transformed into a structured, query-ready format and loaded into a database with idempotent behavior to support retries and reprocessing.

**Architecture Overview**

The pipeline follows a batch ETL design with the following components:

- **Data Source**
	- Public API (RandomUser API) providing simulated user profile data.

- **Orchestration**
	- Apache Airflow manages task execution, dependencies, and retries.

- **Processing**
	- PySpark parses nested JSON, applies schema selection, and performs data cleaning.

- **Storage**
	- Processed data is loaded into a MySQL database using Airflow-managed connections.

- **Load Strategy**
	- Idempotent loading using delete-and-insert logic based on ingestion date.

An architecture and data flow diagram is included in the repository for clarity.

**Data Flow (high level)**

1. Fetch user data from the RandomUser API.
2. Temporarily store raw API response in Airflow XComs (POC-scale).
3. Process and flatten nested JSON fields using PySpark.
4. Add ingestion metadata (ingestion date, source system).
5. Load cleaned data into MySQL using Airflow hooks.
6. Support safe retries and re-runs without duplicate records.

**Key Features**

- Batch ETL pipeline orchestrated with Apache Airflow.
- PySpark-based transformation of nested JSON data.
- Clear separation between data processing and data loading.
- Idempotent database loads to handle retries and backfills.
- Designed as a foundation for scaling to AWS S3 and larger data volumes.

**Tech Stack**

- Apache Airflow
- PySpark
- Python
- MySQL
- REST API (RandomUser API)

**Future Enhancements**

- Replace XCom-based data passing with S3-backed raw and processed data layers.
- Scale transformations using larger datasets and partitioned Parquet files.
- Integrate AWS services such as S3 and managed Airflow.
- Extend analytics storage to Redshift or similar data warehouse solutions.

**Notes**

This project is intentionally kept small in scale to validate the core ETL design and technology integration. It acts as a stepping stone toward a full producer–consumer data platform with cloud-based storage and processing.