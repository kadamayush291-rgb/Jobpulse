# 🚀 JobPulse — Automated Job Market Data Engineering Pipeline

## 📖 Introduction

**JobPulse** is a data engineering project that automatically collects job-posting data from online sources/datasets and converts raw job data into structured and useful information. The entire workflow is orchestrated by **Apache Airflow**, making the pipeline scalable, repeatable, reliable and easier to monitor.

## 🏗️ Architecture

```
Data Source → Ingestion → Raw Storage → Airflow → Transformation → Data Quality → PostgreSQL → Dashboard
```

```
┌──────────┐    ┌───────────┐    ┌─────────────┐    ┌─────────────────┐
│ CSV Data │───▶│ Ingestion │───▶│ Raw Storage  │───▶│  Apache Airflow │
│ (Source) │    │ (Python)  │    │  (Staging)   │    │ (Orchestrator)  │
└──────────┘    └───────────┘    └─────────────┘    └────────┬────────┘
                                                             │
                    ┌────────────────────────────────────────┘
                    ▼
┌────────────────┐    ┌──────────────┐    ┌────────────┐    ┌───────────┐
│ Transformation │───▶│ Data Quality │───▶│ PostgreSQL │───▶│ Dashboard │
│ (Pandas)       │    │ (Validation) │    │ (Database) │    │ (Flask)   │
└────────────────┘    └──────────────┘    └────────────┘    └───────────┘
```

## 🔄 How It Works

The project follows an automated **ETL (Extract, Transform, Load) pipeline**:

1. **Data Ingestion** — Job data (title, company, location, skills, experience, salary) is collected from CSV datasets or available APIs.

2. **Apache Airflow** — Acts as the **workflow orchestrator**, scheduling and managing pipeline tasks: extraction, cleaning, transformation, validation, and loading.

3. **Data Cleaning** — Python + Pandas handles missing values, incorrect formats, and duplicate job postings.

4. **Data Transformation** — Raw data is converted into structured information with standardized job titles, parsed salaries, normalized locations, and individual skills extraction.

5. **Data Quality** — Validation checks ensure the data is accurate, complete, and consistent. Failed checks can trigger Airflow retries or task failures.

6. **Database** — Processed data is stored in **PostgreSQL**, which serves as the structured storage / data warehouse layer.

7. **Analytics** — Processed data is analyzed to identify the most demanded skills, top job roles, hiring locations, companies, and experience levels.

8. **Dashboard** — A Flask-based dashboard presents these insights using interactive charts, tables, and KPIs.

## 📂 Project Structure

```
JobPulse/
├── docker-compose.yml          # Docker services (Airflow, PostgreSQL, Dashboard)
├── .env                        # Environment variables
├── requirements.txt            # Core Python dependencies
├── README.md                   # Project documentation
│
├── dags/
│   └── jobpulse_etl_dag.py     # Airflow DAG — 6-task pipeline
│
├── scripts/
│   ├── __init__.py
│   ├── db_utils.py             # Database connection utility
│   ├── extract.py              # Task 1: CSV extraction → raw_jobs table
│   ├── transform.py            # Task 2: Data cleaning & transformation
│   ├── validate.py             # Task 3: Data quality validation
│   ├── load.py                 # Task 4: Load to PostgreSQL + skill categorization
│   └── analytics.py            # Task 5: Compute summary metrics
│
├── data/
│   └── jobs_dataset.csv        # Sample job market dataset (~200 rows)
│
├── dashboard/
│   ├── app.py                  # Flask API backend (12+ REST endpoints)
│   ├── requirements.txt        # Dashboard dependencies
│   └── templates/
│       └── index.html          # Interactive analytics dashboard
│
├── sql/
│   └── init.sql                # PostgreSQL schema (5 tables)
│
├── logs/                       # Airflow logs
└── plugins/                    # Airflow plugins
```

## 🛠️ Technology Stack

| Component          | Technology                  |
|--------------------|-----------------------------|
| Programming        | Python 3.11                 |
| Data Processing    | Pandas                      |
| Orchestration      | Apache Airflow 2.7.3        |
| Database           | PostgreSQL 15               |
| API Backend        | Flask                       |
| Visualization      | Chart.js                    |
| Containerization   | Docker & Docker Compose     |
| Version Control    | Git / GitHub                |

## ⚙️ Airflow DAG — Pipeline Tasks

```
extract_csv_data → transform_data → validate_raw_data → load_transformed_data → compute_analytics → log_pipeline_run
```

| Task | Description |
|------|-------------|
| `extract_csv_data` | Reads CSV dataset, loads into `raw_jobs` staging table |
| `transform_data` | Parses salaries, standardizes dates, splits locations, cleans data |
| `validate_raw_data` | Data quality checks — completeness, ranges, duplicates, consistency |
| `load_transformed_data` | Inserts into `transformed_jobs` + creates `job_skills` dimension table |
| `compute_analytics` | Computes 11 categories of summary metrics |
| `log_pipeline_run` | Records pipeline run status in `pipeline_runs` table |

## 📊 Key Data Engineering Concepts Used

- ✅ ETL/ELT Pipeline
- ✅ Data Ingestion
- ✅ Data Cleaning & Transformation
- ✅ Data Quality & Validation
- ✅ Workflow Orchestration (Airflow)
- ✅ Scheduling & Automation
- ✅ PostgreSQL Database
- ✅ Error Handling & Retries
- ✅ Data Visualization (Dashboard)
- ✅ Containerization (Docker)

### Option A: Local Run (No Docker Required — Lightweight Mode)

Run the automated pipeline with built-in task retries, background scheduling, and the dashboard:

```bash
# 1. Start pipeline with default 5-minute scheduler + retries + dashboard:
python run_local.py

# Optional CLI flags:
python run_local.py --interval 10      # Customize schedule interval in minutes
python run_local.py --retries 5        # Set max 5 retries on failure (with exponential backoff)
python run_local.py --run-once         # Run pipeline once with retries and exit (no server)
python run_local.py --no-schedule      # Start dashboard without recurring background schedule
```

- **Dashboard**: Visit `http://localhost:5000`
- Click the **"⚡ Run Pipeline Now"** button in the dashboard to trigger an on-demand run.
- Monitor execution status, retry policies, and historical run logs in real-time under the **Scheduler** section.

---

### Option B: Docker & Apache Airflow Orchestration

If you have Docker Desktop installed and want enterprise-grade Airflow orchestration:

```bash
# 1. Start all container services:
docker-compose up -d

# 2. Wait ~45-60 seconds for Airflow initialization
```

### Access Points

| Service         | URL                      | Credentials         |
|-----------------|--------------------------|----------------------|
| Airflow UI      | http://localhost:8080     | admin / admin        |
| Dashboard       | http://localhost:5000     | —                    |
| PostgreSQL      | localhost:5432            | jobpulse / jobpulse123 |

### Running the Pipeline in Airflow

1. Open Airflow UI at `http://localhost:8080` (admin / admin)
2. Find the `jobpulse_etl_pipeline` DAG
3. Toggle it **ON** (unpause) — the Airflow scheduler will now automatically trigger it on the `@daily` schedule.
4. Click **Trigger DAG** (Play button) to run the pipeline immediately on-demand.
5. Monitor task execution, dependencies, and retry attempts in the Graph or Grid views.
6. Visit the Dashboard at `http://localhost:5000` to see live results.

### Stopping Docker Services

```bash
docker-compose down        # Stop services
docker-compose down -v     # Stop and remove data volumes
```

## 🎯 Expected Outcome

The system provides an automated and reliable pipeline that transforms raw job-market data into meaningful insights, helping users understand current technology and employment trends.

## 💡 Main Advantage

Instead of manually processing job data, the entire workflow is automated through Airflow, making the pipeline **scalable, repeatable, reliable, and easier to monitor**.

---

*JobPulse — Data Engineering Mini Project*
