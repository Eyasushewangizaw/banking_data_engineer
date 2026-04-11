# 🏦 Banking Data Engineering Pipeline

> A real-time, end-to-end data engineering pipeline for banking analytics — streaming raw financial data through a medallion architecture and delivering actionable insights via Power BI dashboards.

## 🏗️ Architecture Overview

![alt text](<Screenshot 2026-03-20 012849.png>)

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Pipeline Stages](#-pipeline-stages)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Medallion Layers](#-medallion-layers)
- [Screenshots](#-screenshots)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🌐 Overview

This project implements a **production-grade, real-time data pipeline** for banking data. It ingests raw financial events (transactions, accounts, customers, loans), streams them through Kafka, lands them in MinIO object storage, loads them into Snowflake, and transforms them using dbt — all orchestrated by Apache Airflow inside a fully containerized Docker environment.

The result: clean, business-ready data models powering rich Power BI dashboards for financial analysis and reporting.





## 🧰 Tech Stack

| Layer             | Tool / Technology          | Purpose                                      |
|-------------------|----------------------------|----------------------------------------------|
| Containerization  | **Docker & Docker Compose** | Full local environment, reproducible builds |
| Streaming         | **Apache Kafka**            | Real-time event ingestion & message brokering |
| Object Storage    | **MinIO (S3-compatible)**   | Data lake landing zone                       |
| Orchestration     | **Apache Airflow**          | Pipeline scheduling & workflow management   |
| Data Warehouse    | **Snowflake**               | Scalable cloud analytics warehouse           |
| Transformation    | **dbt (data build tool)**   | SQL-based layered data transformations       |
| Visualization     | **Power BI**                | Business dashboards & financial reporting    |
| Language          | **Python**                  | Data generation, Kafka producers/consumers  |


## 📦 Pipeline Stages

### 1. 🗄️ Data Source
Raw banking data — including transactions, accounts, customer records, and loan data — is generated and published to a Kafka topic, simulating a real-time operational database feed.

### 2. 📨 Apache Kafka
Kafka acts as the central message broker. It decouples producers (banking systems) from consumers, enabling fault-tolerant, high-throughput streaming of financial events at scale.

### 3. 🪣 MinIO Object Storage
A Kafka consumer reads messages and lands them into **MinIO** — an S3-compatible object storage layer that serves as the raw data lake. Raw files (transactions, accounts, etc.) are partitioned and stored here before warehouse ingestion.

### 4. ❄️ Snowflake Data Warehouse
Airflow DAGs load data from MinIO into Snowflake, where it flows through three medallion layers (see below). **dbt** handles all SQL transformations between layers, ensuring clean, tested, documented data models.

![alt text](<Screenshot 2026-03-17 171940.png>) ![alt text](<Screenshot 2026-03-18 003817.png>) ![alt text](<Screenshot 2026-03-17 175227.png>)



### 5. 🔁 Apache Airflow
Airflow orchestrates the full ingestion and transformation workflow — from pulling data out of MinIO to triggering dbt runs in Snowflake. DAGs are scheduled, monitored, and retried automatically.

![alt text](<Screenshot 2026-03-16 212901.png>)


### 6. 📊 Power BI
Business-ready Snowflake models are connected to Power BI for interactive financial dashboards covering customer analytics, transaction trends, loan performance, and risk scoring.

---





### 1. Clone the repository

```bash
git clone https://github.com/Eyasushewangizaw/banking_data_engineer.git
cd banking_data_engineer
```

### 2. Configure environment variables

Create a `.env` file in the root directory and populate it with your credentials:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=BANKING_DB
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=your_role

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=banking-raw

# Airflow
AIRFLOW_UID=50000
```

### 3. Start the full stack

```bash
docker-compose up --build -d
```

This spins up: Kafka, Zookeeper, MinIO, Airflow (webserver + scheduler), and supporting services.

| Service          | URL                        | Default Credentials   |
|------------------|----------------------------|-----------------------|
| Airflow UI       | http://localhost:8080       | airflow / airflow     |
| MinIO Console    | http://localhost:9001       | minioadmin / minioadmin |
| Kafka (broker)   | localhost:9092             | —                     |

### 4. Generate and stream banking data

```bash
docker exec -it <data_generator_container> python generate_data.py
```

### 5. Run dbt transformations

```bash
cd banking_dbt
dbt run
dbt test
```

---

## 🥇 Medallion Layers

Data in Snowflake is organized into three progressive quality tiers:

| Layer | Schema | Description |
|-------|--------|-------------|
| 🥉 **Raw** | `RAW` | Data landed as-is from MinIO — no transformations applied |
| 🥈 **Cleaned** | `CLEANED` | Deduplicated, null-handled, type-cast, and standardized financial records |
| 🥇 **Business Ready** | `BUSINESS` | Aggregated models: customer 360, transaction summaries, loan performance, risk scoring |

## 📸 Screenshots

### MinIO — S3 Bucket Loaded

![alt text](<Screenshot 2026-03-16 002049.png>)

### Snowflake — Raw & Cleaned Layers

![alt text](<Screenshot 2026-03-17 171940.png>) ![alt text](<Screenshot 2026-03-18 003817.png>) ![alt text](<Screenshot 2026-03-17 175227.png>)


### Apache Airflow — DAG Orchestration

![alt text](<Screenshot 2026-03-16 212901.png>)
















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

![alt text](<Screenshot 2026-03-17 171940.png>) ![alt text](<Screenshot 2026-03-18 003817.png>) ![alt text](<Screenshot 2026-03-17 175227.png>)


### 5. Apache Airflow
Airflow schedules and orchestrates the pipeline from minio s3 bucket to snowflake data warehouse


![alt text](<Screenshot 2026-03-16 212901.png>)