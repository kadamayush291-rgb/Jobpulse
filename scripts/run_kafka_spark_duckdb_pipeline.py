"""
JobPulse - End-to-End Ingestion, Spark Quality, DuckDB OLAP Pipeline Runner
Coordinates:
1. Apache Kafka Ingestion: Raw CSV -> Kafka Topic (streaming records with partitions & offsets)
2. Apache Kafka Consumer: Topic -> Raw Event Batch
3. Apache Spark Engine: Data Quality Validation (Completeness, Validity, Uniqueness, Consistency) + Cleaning
4. Python DuckDB Storage: Clean Fact/Dimension Columnar Ingestion
5. Python DuckDB OLAP: Advanced Analytical Queries (Rollups, Window functions, Percentiles)
6. Metrics & Audit Logging
"""
import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import Dict, Any

# Ensure script directory is on sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from kafka_pipeline import JobPostingKafkaProducer, JobPostingKafkaConsumer
from spark_quality_engine import SparkQualityEngine, process_kafka_records_with_spark
from duckdb_storage import DuckDBStorage
from db_utils import get_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobPulse.MasterPipeline")


def run_full_pipeline(csv_path: str = None, max_records: int = None) -> Dict[str, Any]:
    """
    Executes the complete Kafka -> Spark -> DuckDB -> OLAP pipeline.
    """
    pipeline_start = time.time()
    run_id = f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logger.info("=" * 70)
    logger.info(f"🚀 JobPulse Master Pipeline Execution Started: {run_id}")
    logger.info("=" * 70)

    # Resolve CSV Path
    if not csv_path:
        project_root = os.path.dirname(CURRENT_DIR)
        csv_path = os.path.join(project_root, "data", "jobs_dataset.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Input CSV not found at: {csv_path}")

    # -------------------------------------------------------------
    # STAGE 1: Apache Kafka Raw Ingestion
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 1] Apache Kafka Raw CSV Ingestion ---")
    t0 = time.time()
    producer = JobPostingKafkaProducer()
    kafka_ingest_result = producer.ingest_csv(csv_path=csv_path, max_rows=max_records, clear_buffer_before_ingest=True)
    stage1_time = round(time.time() - t0, 3)

    # -------------------------------------------------------------
    # STAGE 2: Apache Kafka Stream Consumption
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 2] Apache Kafka Consumer Reading Stream ---")
    t1 = time.time()
    consumer = JobPostingKafkaConsumer()
    consumed_messages = consumer.consume_all(max_messages=max_records)
    stage2_time = round(time.time() - t1, 3)
    logger.info(f"Consumed {len(consumed_messages)} event messages from Kafka topic in {stage2_time}s.")

    # -------------------------------------------------------------
    # STAGE 3: Apache Spark Data Quality & Cleaning Engine
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 3] Apache Spark Data Quality Validation & Cleaning ---")
    t2 = time.time()
    fact_df, skills_df, quality_report = process_kafka_records_with_spark(consumed_messages)
    stage3_time = round(time.time() - t2, 3)
    logger.info(f"Spark Engine finished in {stage3_time}s.")
    logger.info(f"   Quality Score: {quality_report['quality_score']}% ({quality_report['overall_status']})")
    logger.info(f"   Rules Evaluated: {quality_report['total_rules']} | Passed: {quality_report['passed_rules']} | Warnings: {quality_report['warning_rules']} | Failed: {quality_report['failed_rules']}")

    # -------------------------------------------------------------
    # STAGE 4: Python DuckDB Columnar Storage Loading
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 4] Python DuckDB Columnar Storage Loading ---")
    t3 = time.time()
    duckdb_warehouse = DuckDBStorage()
    duckdb_load_res = duckdb_warehouse.load_clean_data(
        fact_df=fact_df,
        skills_df=skills_df,
        quality_report=quality_report,
        kafka_messages=consumed_messages
    )
    stage4_time = round(time.time() - t3, 3)
    logger.info(f"DuckDB Storage Load complete in {stage4_time}s ({duckdb_load_res['load_time_ms']}ms internal).")

    # -------------------------------------------------------------
    # STAGE 5: Python DuckDB Advanced OLAP Analytical Suite
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 5] Python DuckDB OLAP Analytical Suite Execution ---")
    t4 = time.time()
    olap_results = duckdb_warehouse.run_olap_suite()
    stage5_time = round(time.time() - t4, 3)
    logger.info(f"DuckDB executed {olap_results['total_queries_executed']} OLAP queries in {olap_results['cumulative_execution_time_ms']}ms (Avg: {olap_results['average_latency_ms']}ms per query)!")

    # -------------------------------------------------------------
    # STAGE 6: Synchronize to Operational DB (Postgres/SQLite fallback)
    # -------------------------------------------------------------
    logger.info("\n--- [STAGE 6] Syncing Clean Data to Operational DB ---")
    try:
        import sqlite3
        project_root = os.path.dirname(CURRENT_DIR)
        sqlite_path = os.path.join(project_root, "data", "jobpulse.db")
        with sqlite3.connect(sqlite_path) as sq_conn:
            fact_df.to_sql('transformed_jobs', con=sq_conn, if_exists='replace', index=False)
            if not skills_df.empty:
                compat_skills = skills_df.rename(columns={'skill_name': 'skill'})
                compat_skills.to_sql('job_skills', con=sq_conn, if_exists='replace', index=False)
        logger.info("Operational DB successfully synchronized.")
    except Exception as e:
        logger.warning(f"Could not synchronize to operational DB: {e}")

    total_pipeline_time = round(time.time() - pipeline_start, 3)

    summary = {
        "run_id": run_id,
        "status": "SUCCESS",
        "total_duration_seconds": total_pipeline_time,
        "stages": {
            "kafka_ingestion": {
                "records_ingested": kafka_ingest_result.get("total_records", len(consumed_messages)),
                "topic": producer.topic,
                "duration_seconds": stage1_time,
                "throughput_msg_sec": kafka_ingest_result.get("throughput_msg_per_sec", 0),
                "mode": kafka_ingest_result.get("broker_mode", "simulator")
            },
            "kafka_consumption": {
                "records_consumed": len(consumed_messages),
                "duration_seconds": stage2_time
            },
            "spark_processing": {
                "quality_score": quality_report["quality_score"],
                "quality_status": quality_report["overall_status"],
                "facts_produced": len(fact_df),
                "skills_extracted": len(skills_df),
                "duration_seconds": stage3_time,
                "audit_rules_passed": quality_report["passed_rules"],
                "audit_rules_warning": quality_report["warning_rules"],
                "audit_rules_failed": quality_report["failed_rules"]
            },
            "duckdb_storage": {
                "load_time_ms": duckdb_load_res["load_time_ms"],
                "tables_loaded": ["fact_jobs", "dim_skills", "data_quality_audit", "kafka_events_log"],
                "duration_seconds": stage4_time
            },
            "duckdb_olap": {
                "queries_executed": olap_results["total_queries_executed"],
                "total_olap_ms": olap_results["cumulative_execution_time_ms"],
                "avg_query_latency_ms": olap_results["average_latency_ms"]
            }
        },
        "duckdb_stats": duckdb_warehouse.get_stats(),
        "olap_summary": {
            k: {
                "title": v["title"],
                "category": v["category"],
                "execution_time_ms": v["execution_time_ms"],
                "row_count": v["row_count"],
                "sample_row": v["data"][0] if v["data"] else {}
            }
            for k, v in olap_results["queries"].items()
        }
    }

    logger.info("=" * 70)
    logger.info(f"✅ Pipeline Completed Successfully in {total_pipeline_time}s!")
    logger.info(f"   Quality: {quality_report['quality_score']}% | DuckDB OLAP Latency: {olap_results['cumulative_execution_time_ms']}ms")
    logger.info("=" * 70)

    return summary


if __name__ == "__main__":
    result = run_full_pipeline()
    print("\n--- PIPELINE EXECUTION SUMMARY ---")
    print(json.dumps(result, indent=2, default=str))
