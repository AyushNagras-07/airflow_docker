# PySpark + Airflow Producer-Consumer ETL Pipeline (API → S3 → PostgreSQL)

## Overview

This project demonstrates a production-ready batch ETL pipeline implementing the **producer-consumer architecture** pattern with Apache Airflow. The pipeline ingests user data from a public API, stages it on AWS S3, processes it using PySpark with data quality checks, and loads analytics-ready data into PostgreSQL. 

The solution showcases best practices for distributed data pipelines including external task dependencies, sensor-based orchestration, scalable processing with PySpark, and idempotent database operations to ensure reliable retries and backfills.

---

## Architecture Diagram

![Architecture Diagram](https://raw.githubusercontent.com/AyushNagras-07/airflow-producer-consumer-etl/main/Architecture_Diagram.png)

---

## Business Use Case

In data-driven systems (SaaS platforms, analytics teams, real-time dashboards), user data is continuously generated and must be ingested, processed, and stored reliably for downstream analysis and reporting. This pipeline simulates a production daily batch ingestion workflow addressing:

- **Decoupled producers and consumers**: Independent teams can iterate on data generation and consumption separately
- **Data reliability**: Idempotent loads ensure safe retries without duplicates
- **Scalability**: S3-based staging layer enables handling larger datasets with distributed processing
- **Observability**: Task dependencies and sensors provide clear visibility into data pipeline execution

---

## Architecture Overview

The pipeline implements a **two-DAG producer-consumer pattern** with clear separation of concerns:

### Producer DAG (`producer_users_daily`)
- **Data Source**: JSONPlaceholder public API (https://jsonplaceholder.typicode.com/users)
- **Task**: Fetch user data and stage raw JSON to **AWS S3**
- **Schedule**: Daily (@daily)
- **Output**: Raw JSON files in `s3://ayush-consumer-producer/data/sample/{execution_date}.json`

### Consumer DAG (`consumer_users_daily`)
- **Dependencies**: Waits for producer completion and S3 file availability using sensors
- **Processing**: 
  - PySpark-based transformation of JSON data
  - Schema validation and null-value data quality checks
  - Flatten and select specific fields (id, name, email)
- **Storage**: Processed data written to **PostgreSQL** using idempotent delete-insert logic
- **Output**: Partitioned Parquet files in `s3://ayush-consumer-producer/data/processed/users/`

### Infrastructure Components
- **Orchestration**: Apache Airflow (DAG Dependencies, Task Sensors, Context Management)
- **Processing**: PySpark with S3A filesystem support
- **Data Lake**: AWS S3 (raw and processed data layers)
- **Data Warehouse**: PostgreSQL
- **Cloud Connection**: AWS credentials via Airflow connections

---

## Data Flow (Detailed)

### Producer Pipeline
```
Start (BashOperator) 
  ↓
Fetch Data (Python) → Call JSONPlaceholder API
  ↓
Upload to S3 (S3Hook) → Store raw JSON with execution date as partition
  ↓
End (BashOperator)
```

### Consumer Pipeline
```
Start (BashOperator)
  ↓
Wait for Producer (ExternalTaskSensor) → Polls producer_users_daily.producer task
  ↓
Wait for S3 File (S3KeySensor) → Ensures data/sample/{ds}.json exists in S3
  ↓
Transform with Spark (PythonOperator)
  ├─ Read JSON from S3 with predefined schema
  ├─ Validate null values (quality check)
  ├─ Select columns: id, name, email
  ├─ Write processed data as Parquet to ds={execution_date} partition
  └─ Return row counts for monitoring
  ↓
Load to PostgreSQL (PythonOperator)
  ├─ Read Parquet from S3
  ├─ Delete existing records for execution date (idempotent)
  ├─ Insert transformed rows into users_daily table
  └─ Commit transaction
  ↓
End (BashOperator)
```

---

## Key Features

✅ **Producer-Consumer Architecture**: Decoupled data generation and consumption for scalability  
✅ **External Task Dependencies**: Producer DAG completion triggers consumer using sensors  
✅ **S3-Based Data Lake**: Scalable staging layer for raw and processed data  
✅ **PySpark Transformations**: Distributed processing with schema validation  
✅ **Data Quality Checks**: Null-value validation before database load  
✅ **Idempotent Database Loads**: Delete-and-insert pattern prevents duplicates on retries  
✅ **Sensor-Based Orchestration**: S3KeySensor waits for file availability with configurable retries  
✅ **Execution Date Tracking**: All operations partitioned by execution date for easy backfilling  

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | Apache Airflow 2.x |
| **Processing** | PySpark 3.x with Hadoop AWS (S3A) |
| **Data Lake** | AWS S3 |
| **Data Warehouse** | PostgreSQL |
| **API Source** | JSONPlaceholder REST API |
| **Languages** | Python 3.x |
| **Deployment** | Docker & Docker Compose |

---

## Setup & Configuration

### Prerequisites
- Apache Airflow 2.x running with AWS provider package
- AWS credentials configured in Airflow connections (`aws_conn`)
- PostgreSQL connection configured in Airflow (`postgres_default`)
- AWS S3 bucket: `ayush-consumer-producer`
- PySpark and Hadoop libraries with S3A support

### Database Schema
The consumer DAG expects the following PostgreSQL table:

```sql
CREATE TABLE users_daily (
    id INTEGER,
    name VARCHAR(255),
    email VARCHAR(255),
    dt DATE,
    PRIMARY KEY (id, dt)
);
```

### Airflow Connections
- **AWS Connection** (`aws_conn`): S3 access credentials
- **PostgreSQL Connection** (`postgres_default`): Database access

---

## File Structure

```
├── dags/
│   ├── producer_users_daily.py      # Producer DAG - API → S3
│   ├── consumer_users_daily.py      # Consumer DAG - S3 → Transform → PostgreSQL
│   └── ... (other DAGs)
├── config/                           # Airflow configuration
├── data/                             # Local test data
├── docker-compose.yaml               # Local Airflow environment
├── Dockerfile                        # Airflow image with dependencies
└── requirements.txt                  # Python dependencies
```

---

## Running the Pipeline

### Option 1: Docker Compose (Recommended)
```bash
docker-compose up -d
# Access Airflow UI at http://localhost:8080
```

### Option 2: Manual Execution
1. Place DAG files in `$AIRFLOW_HOME/dags/`
2. Configure Airflow connections for AWS and PostgreSQL
3. Trigger `producer_users_daily` manually or via schedule
4. `consumer_users_daily` auto-triggers based on ExternalTaskSensor

### Monitoring
- **Airflow UI**: View DAG dependencies, task logs, and execution history
- **S3 Console**: Verify raw and processed data layers
- **PostgreSQL**: Query `users_daily` table for loaded records

---

## Idempotent Design Pattern

The consumer DAG ensures safety for retries and backfills:

1. **Delete Before Insert**: Each run deletes records matching the execution date
2. **Execution Date Partitioning**: All data partitioned by `dt` (execution date)
3. **S3 Partitioning**: Raw and processed data organized by execution date
4. **Sensor Retries**: S3KeySensor with configurable `poke_interval` and `timeout`

This pattern allows safe re-execution without manual cleanup.

---

## Future Enhancements

🚀 **Scalability**
- Multi-partition data loads using dynamic PySpark repartitioning
- Incremental loading with watermark-based change data capture (CDC)
- Auto-scaling compute using AWS Glue or EMR instead of local PySpark

🔧 **Monitoring & Observability**
- Data quality framework integration (Great Expectations)
- Alerting on SLA breaches and task failures
- Metrics export to CloudWatch/Prometheus

📊 **Advanced Analytics**
- Integration with Redshift for analytical queries
- dbt models on top of PostgreSQL for dimensional modeling
- Real-time streaming option using Kafka + Spark Streaming

🔐 **Security & Governance**
- Row-level security in PostgreSQL
- Data lineage tracking with Apache Atlas
- Cost optimization with S3 lifecycle policies

---

## Troubleshooting

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| Consumer task waiting indefinitely | Producer DAG not scheduled | Check producer DAG schedule and recent execution |
| S3KeySensor timeout | File not uploaded to S3 | Verify producer task logs, AWS credentials, S3 bucket name |
| PySpark null validation failure | Invalid source data | Check JSONPlaceholder API response format |
| PostgreSQL insert failure | Table not created or connection issue | Verify table schema, connection credentials, network access |

---

## Notes

- **POC Foundation**: This pipeline is designed as a validated foundation for scaling to production data volumes
- **Idempotency First**: All operations support safe retry and backfill operations
- **Cloud Native**: Leverages AWS S3 for scalable storage and future cloud migration
- **Technology Integration**: Demonstrates best practices for Airflow + PySpark + PostgreSQL stacks

---

**Project Link**: https://github.com/AyushNagras-07/airflow-producer-consumer-etl