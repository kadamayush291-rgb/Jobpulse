# 🚀 JobPulse — Real-Time & Batch Job Market Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%20Mode-red.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20%2F%204.0-orange.svg)](https://spark.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-OLAP%20Engine-yellow.svg)](https://duckdb.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-teal.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Flask](https://img.shields.io/badge/Dashboard-Flask%20%2B%20Chart.js-brightgreen.svg)](http://flask.palletsprojects.com/)

An end-to-end modern Data Engineering platform combining **event-driven real-time streaming ingestion** with **distributed batch ETL orchestration**. JobPulse continuously ingests, validates, cleans, and analyzes job postings from 16+ sector-specific datasets (Indian unicorns, GCC tech centers, global AI/deep-tech, US fintech, and more) to provide instant labor market intelligence.

---

## 🏗️ Architecture Overview

JobPulse implements a dual-path data architecture (**Lambda-inspired architecture**):

```
                                  ┌───────────────────────────────┐
                                  │   16+ Streaming CSV Datasets  │
                                  │   (Indian & Global Tech Jobs) │
                                  └───────────────┬───────────────┘
                                                  │
                                                  ▼
                                 ┌─────────────────────────────────┐
                                 │   Kafka Streaming Ingestion     │
                                 │   (Topic: jobpulse-job-postings)│
                                 └───────────────┬─────────────────┘
                                                 │
                                                 ▼
                                 ┌─────────────────────────────────┐
                                 │    PySpark Data Quality Engine  │
                                 │   • Schema & Range Validation   │
                                 │   • Deduplication & Cleaning    │
                                 │   • Salary & Location Normalizer│
                                 └───────────────┬─────────────────┘
                                                 │
                                                 ▼
                                 ┌─────────────────────────────────┐
                                 │     DuckDB OLAP Fast Store      │
                                 │  (Vectorized SQL, Sub-ms Stats) │
                                 └───────────────┬─────────────────┘
                                                 │
                                                 ▼
┌───────────────────────────┐    ┌─────────────────────────────────┐
│     Apache Airflow        │───▶│  PostgreSQL / SQLite Storage    │
│  (Batch ETL Orchestrator) │    │  (Relational Staging & Storage) │
└───────────────────────────┘    └───────────────┬─────────────────┘
                                                 │
                                                 ▼
                                 ┌─────────────────────────────────┐
                                 │  Interactive Flask Dashboard    │
                                 │  • Real-Time Kafka Stream KPIs  │
                                 │  • Spark Quality Scorecards     │
                                 │  • DuckDB OLAP Analytics        │
                                 │  • Chart.js Interactive Graphs  │
                                 └─────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technology | Key Role |
|---|---|---|
| **Streaming Ingestion** | **Apache Kafka (KRaft mode)** | High-throughput distributed message broker for raw job events |
| **Data Quality & Transformation** | **Apache Spark (PySpark)** | Distributed batch & stream processing, anomaly detection, deduplication |
| **Fast OLAP Analytics** | **DuckDB** | In-process columnar OLAP storage for ultra-low latency analytics |
| **Workflow Orchestration** | **Apache Airflow 2.7.3** | Automated DAG workflow scheduling, task retries, error handling |
| **Relational Storage** | **PostgreSQL 15 / SQLite** | Structured storage for staging, dimensional tables, and run logs |
| **API & Backend** | **Flask + Python** | REST API endpoints for data retrieval and scheduler management |
| **Frontend & Visualization** | **Chart.js + Modern CSS** | Interactive live dashboards with real-time streaming controls |
| **Containerization** | **Docker & Docker Compose** | Multi-container setup for Kafka, Spark, Airflow, and Postgres |

---

## 📂 Project Structure

```
JobPulse/
├── docker-compose.yml                  # Multi-container setup (Kafka, Spark, Airflow, Postgres)
├── requirements.txt                    # Root environment dependencies
├── pyrightconfig.json                  # IDE & language server configuration
├── .env.example                        # Template for environment variables
├── README.md                           # Project documentation
│
├── dags/
│   └── jobpulse_etl_dag.py             # Airflow DAG (6-task batch ETL pipeline)
│
├── scripts/
│   ├── kafka_pipeline.py               # Kafka producer & consumer streaming client
│   ├── spark_quality_engine.py         # PySpark data quality checks & cleaning
│   ├── duckdb_storage.py               # DuckDB OLAP engine & materialized views
│   ├── kafka_stream_scheduler.py       # Automated 1-minute streaming scheduler
│   ├── run_streaming_pipeline.py       # Standalone runner for continuous streaming
│   ├── run_kafka_spark_duckdb_pipeline.py # End-to-end Kafka + Spark + DuckDB pipeline
│   ├── generate_streaming_datasets.py  # Generator for 16 specialized industry datasets
│   ├── test_all_streams.py             # Validation script for stream workflows
│   ├── extract.py                      # Task 1: Staging extraction
│   ├── transform.py                    # Task 2: Data cleaning & standardization
│   ├── validate.py                     # Task 3: Quality validation
│   ├── load.py                         # Task 4: Load to database & skill dimensions
│   ├── analytics.py                    # Task 5: Compute summary metrics
│   └── db_utils.py                     # Database connection helper
│
├── dashboard/
│   ├── app.py                          # Flask backend with 15+ REST endpoints
│   ├── requirements.txt                # Dashboard runtime dependencies
│   └── templates/
│       └── index.html                  # Interactive live telemetry dashboard
│
├── data/
│   ├── jobs_dataset.csv                # Baseline dataset
│   └── streaming_datasets/             # 16 domain-specific CSV datasets:
│       ├── dataset_1_india_unicorns.csv
│       ├── dataset_2_india_mnc_gcc.csv
│       ├── dataset_3_india_it_services_consulting.csv
│       ├── dataset_4_india_ai_deeptech.csv
│       ├── dataset_5_us_fintech_banking.csv
│       ├── dataset_6_europe_cloud_platform.csv
│       ├── dataset_7_india_gcc_deeptech.csv
│       ├── dataset_8_us_healthcare_lifesciences.csv
│       ├── dataset_9_apac_ecommerce_logistics.csv
│       ├── dataset_10_us_edtech_learning.csv
│       ├── dataset_11_india_startup_product.csv
│       ├── dataset_12_us_defense_govt.csv
│       ├── dataset_13_climate_tech_esg.csv
│       ├── dataset_14_india_remote_global.csv
│       └── dataset_15_global_ai_llm.csv
│
└── sql/
    └── init.sql                        # PostgreSQL schema definition (5 tables)
```

---

## ⚡ Core Pipeline Capabilities

### 1. Automated Real-Time Streaming (Kafka)
- Feeds 16 unique datasets sequentially on an automated **1-minute schedule**.
- Handles schema negotiation, message batching, and network retries with automatic fallback buffers (`kafka_stream_buffer.jsonl`).
- Simulates realistic enterprise events across global and Indian tech markets.

### 2. PySpark Data Quality Engine
- **Duplicate Detection & Removal**: Identifies fuzzy duplicate postings across title, company, and location.
- **Completeness & Null Verification**: Validates critical fields with custom quality scorecards.
- **Outlier & Range Clamping**: Identifies unrealistic compensation ranges and sanitizes values.
- **Standardization**: Cleans and normalizes skills, experience levels, and city hierarchies.

### 3. DuckDB Columnar OLAP Analytics
- Aggregates millions of rows in sub-millisecond query latency.
- Dynamically computes:
  - Top in-demand programming languages and frameworks
  - Salary distributions across experience tiers
  - Remote vs. on-site trends
  - City-wise hiring volumes (Bengaluru, Hyderabad, Pune, SF, London, Singapore)

### 4. Interactive Live Dashboard
- **Streaming Telemetry**: Live Kafka message rates, latency, and ingestion counters.
- **Quality Scorecard**: Instant counters for records processed, valid records, duplicates purged, and rejection reasons.
- **Interactive Controls**: Trigger on-demand stream bursts or trigger the batch Airflow ETL directly from the browser.

---

## 🚀 Getting Started

### 1. Clone & Setup

```bash
git clone https://github.com/kadamayush291-rgb/Jobpulse.git
cd Jobpulse

# Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\activate   # On Windows
# source .venv/bin/activate # On Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

---

### 2. Run Modes

#### 🌟 Mode A: Standalone Real-Time Streaming & Dashboard (Recommended for quick testing)

Run the real-time streaming pipeline and web dashboard locally without needing external containers:

```bash
# Terminal 1: Start the Dashboard with Kafka & DuckDB background scheduler
python dashboard/app.py
```
Open **[http://localhost:5000](http://localhost:5000)** to view live streaming telemetry.

To trigger an immediate streaming batch test:
```bash
# Terminal 2: Run the streaming pipeline
python scripts/run_streaming_pipeline.py
```

---

#### 🌟 Mode B: Complete Local Runner with Scheduler & Retries

```bash
python run_local.py
```
Options:
- `--interval 5`: Change scheduler interval in minutes.
- `--retries 3`: Set maximum retry attempts on failure.
- `--run-once`: Execute single run and exit.

---

#### 🌟 Mode C: Enterprise Containerized Orchestration (Docker Compose)

Starts Kafka (KRaft), Spark Master + Worker, Airflow 2.7.3, and PostgreSQL 15:

```bash
docker-compose up -d
```

| Service | Port | Description |
|---|---|---|
| **Airflow Webserver** | `http://localhost:8080` (admin / admin) | Orchestration DAG management |
| **Spark Master UI** | `http://localhost:8081` | Spark cluster monitoring |
| **Flask Dashboard** | `http://localhost:5000` | Real-time analytics UI |
| **Apache Kafka** | `localhost:9092` | KRaft message broker |
| **PostgreSQL** | `localhost:5432` | Relational storage layer |

---

## 📊 Analytics & Insights

The pipeline automatically derives:
- **Skill Demand Indices**: Frequency analysis of modern tools (Python, SQL, Spark, Kafka, AWS, Docker, React, PyTorch).
- **Salary Benchmarking**: Mean, median, and percentile salary distributions by experience tier.
- **Regional Tech Hub Dynamics**: Distribution of job opportunities across Tier-1 hubs.
- **Data Quality Health**: Real-time pass/fail rates for every incoming stream.

---

## 📄 License
This project is open-source and available under the [MIT License](LICENSE).
