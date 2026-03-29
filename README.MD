# 🚀 Real-Time Data Pipeline Project

A fully containerized, end-to-end data engineering pipeline that streams, stores, transforms, and visualizes data using industry-standard tools — all orchestrated within Docker.

---

## 🏗️ Architecture Overview

![alt text](<Screenshot 2026-03-19 035653.png>)


## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Containerization | Docker |
| Streaming | Apache Kafka |
| Object Storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow |
| Data Warehouse | Snowflake |
| Transformation | dbt (data build tool) |
| Visualization | Power BI |

---

## 📦 Pipeline Stages

### 1. Data Source
Raw banking data (e.g. transactions, accounts, customers, loans) is ingested from a source database and published to a Kafka topic for real-time streaming.

### 2. Apache Kafka
Kafka acts as the message broker, decoupling banking data producers from consumers and enabling fault-tolerant, high-throughput streaming of financial events such as transactions and account updates.

### 3. MinIO S3 Bucket
Kafka messages are consumed and landed into **MinIO**, an S3-compatible object storage layer that serves as the banking data lake — storing raw transaction files, account records, and more.

---

## 🪣 S3 Bucket Loaded

![alt text](<Screenshot 2026-03-16 002049.png>)


### 4. Snowflake Data Warehouse
Data is loaded from MinIO into Snowflake and processed through three medallion layers:

| Layer | Description |
|---|---|
| 🥉 **Raw** | Banking data landed as-is from MinIO — transactions, accounts, customers, loans |
| 🥈 **Cleaned** | Deduplicated, validated, and standardized financial records (e.g. consistent date formats, nulls handled, correct data types) |
| 🥇 **Business Ready** | Aggregated models ready for analytics — customer 360, transaction summaries, risk scoring, loan performance |

Transformations between layers are handled by **dbt**, which runs SQL models within Snowflake.

![alt text](<Screenshot 2026-03-18 003817.png>) ![alt text](<Screenshot 2026-03-17 175227.png>) ![alt text](<Screenshot 2026-03-17 171940.png>)


### 5. Apache Airflow
Airflow schedules and orchestrates the pipeline from minio s3 bucket to snowflake data warehouse

![alt text](<Screenshot 2026-03-16 212901.png>)

### 6. Power BI
The Business Ready layer in Snowflake is connected to **Power BI** for banking dashboards and reporting — covering metrics like transaction volumes, account balances, fraud indicators, and loan analytics.
![alt text](<Screenshot 2026-03-19 004048.png>)


---
