"""
JobPulse - Automated 1-Minute Apache Kafka Stream Scheduler
Automatically cycles through multiple CSV datasets (including Indian Tech Hubs, Unicorns,
GCCs, IT Leaders, and AI DeepTech), streaming each dataset into Apache Kafka topic
every 60 seconds, triggering Spark Quality checks, and loading into DuckDB for OLAP.
"""
import os
import sys
import time
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from kafka_pipeline import JobPostingKafkaProducer, JobPostingKafkaConsumer
from spark_quality_engine import process_kafka_records_with_spark
from duckdb_storage import DuckDBStorage
from db_utils import get_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobPulse.KafkaScheduler")

DATA_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "data")
STREAMING_DATASETS_DIR = os.path.join(DATA_DIR, "streaming_datasets")
SCHEDULER_STATE_FILE = os.path.join(DATA_DIR, "kafka_scheduler_state.json")


class KafkaDatasetStreamScheduler:
    """
    Automated background scheduler that ingests datasets into Apache Kafka
    on a strict 1-minute (60s) schedule.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(KafkaDatasetStreamScheduler, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, interval_seconds: int = 60):
        if self._initialized:
            return
        self.interval_seconds = interval_seconds
        self.running = False
        self.paused = False
        self.current_index = 0
        self.last_run_time = None
        self.next_run_time = None
        self.total_streamed = 0
        self.latest_result = None
        self.thread = None
        self.datasets = self._discover_datasets()
        self._initialized = True
        self._load_state()

    def _discover_datasets(self) -> List[str]:
        os.makedirs(STREAMING_DATASETS_DIR, exist_ok=True)
        files = [
            f for f in sorted(os.listdir(STREAMING_DATASETS_DIR))
            if f.endswith('.csv')
        ]
        if not files:
            # Fallback to base dataset
            base_csv = os.path.join(DATA_DIR, "jobs_dataset.csv")
            if os.path.exists(base_csv):
                dest = os.path.join(STREAMING_DATASETS_DIR, "dataset_0_global_sales_tech.csv")
                import shutil
                shutil.copyfile(base_csv, dest)
                files = ["dataset_0_global_sales_tech.csv"]
        return files

    def _save_state(self):
        state = {
            "status": "PAUSED" if self.paused else ("RUNNING" if self.running else "STOPPED"),
            "interval_seconds": self.interval_seconds,
            "current_index": self.current_index,
            "current_dataset": self.datasets[self.current_index] if self.datasets else None,
            "dataset_queue": self.datasets,
            "total_datasets_in_queue": len(self.datasets),
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "next_run_time": self.next_run_time.isoformat() if self.next_run_time else None,
            "total_streamed_count": self.total_streamed,
            "latest_result": self.latest_result
        }
        try:
            with open(SCHEDULER_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save scheduler state: {e}")

    def _load_state(self):
        if os.path.exists(SCHEDULER_STATE_FILE):
            try:
                with open(SCHEDULER_STATE_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    self.current_index = state.get("current_index", 0) % max(1, len(self.datasets))
                    self.total_streamed = state.get("total_streamed_count", 0)
                    self.latest_result = state.get("latest_result")
            except Exception:
                pass

    def stream_dataset(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Executes one full streaming cycle:
        CSV -> Kafka Producer -> Kafka Consumer -> Spark Quality -> DuckDB -> OLAP.
        """
        if not self.datasets:
            self.datasets = self._discover_datasets()
            if not self.datasets:
                return {"status": "ERROR", "message": "No datasets found in queue"}

        if dataset_name is None:
            dataset_name = self.datasets[self.current_index % len(self.datasets)]

        csv_path = os.path.join(STREAMING_DATASETS_DIR, dataset_name)
        if not os.path.exists(csv_path):
            csv_path = os.path.join(DATA_DIR, "jobs_dataset.csv")

        cycle_start = time.time()
        logger.info(f"⚡ [KAFKA 1-MIN STREAM] Ingesting dataset: {dataset_name} ({self.current_index + 1}/{len(self.datasets)})")

        # 1. Produce to Kafka
        producer = JobPostingKafkaProducer()
        kafka_ingest = producer.ingest_csv(csv_path=csv_path, clear_buffer_before_ingest=True)

        # 2. Consume from Kafka
        consumer = JobPostingKafkaConsumer()
        consumed_msgs = consumer.consume_all()

        # 3. Spark Data Quality & Cleaning
        fact_df, skills_df, quality_report = process_kafka_records_with_spark(consumed_msgs)

        # 4. Store in DuckDB
        duckdb_wh = DuckDBStorage()
        duckdb_load = duckdb_wh.load_clean_data(
            fact_df=fact_df,
            skills_df=skills_df,
            quality_report=quality_report,
            kafka_messages=consumed_msgs
        )

        # 5. Execute OLAP suite
        olap_results = duckdb_wh.run_olap_suite()

        # 6. Sync to Operational DB (SQLite / Postgres)
        try:
            import sqlite3
            sqlite_path = os.path.join(DATA_DIR, "jobpulse.db")
            with sqlite3.connect(sqlite_path) as sq_conn:
                try:
                    existing_facts = pd.read_sql("SELECT * FROM transformed_jobs", sq_conn)
                    combined_facts = pd.concat([existing_facts, fact_df], ignore_index=True).drop_duplicates(subset=['job_title', 'company', 'city'])
                except Exception:
                    combined_facts = fact_df

                combined_facts.to_sql('transformed_jobs', con=sq_conn, if_exists='replace', index=False)

                if not skills_df.empty:
                    compat_skills = skills_df.rename(columns={'skill_name': 'skill'})
                    try:
                        existing_skills = pd.read_sql("SELECT * FROM job_skills", sq_conn)
                        combined_skills = pd.concat([existing_skills, compat_skills], ignore_index=True).drop_duplicates(subset=['job_id', 'skill'])
                    except Exception:
                        combined_skills = compat_skills
                    combined_skills.to_sql('job_skills', con=sq_conn, if_exists='replace', index=False)
            logger.info("Operational DB successfully synchronized.")
        except Exception as e:
            logger.warning(f"Operational DB sync note: {e}")

        duration = round(time.time() - cycle_start, 3)
        self.last_run_time = datetime.now()
        self.total_streamed += 1

        # Advance to next dataset in rotation
        self.current_index = (self.current_index + 1) % len(self.datasets)

        result_summary = {
            "dataset": dataset_name,
            "timestamp": self.last_run_time.isoformat(),
            "duration_seconds": duration,
            "records_ingested": kafka_ingest.get("total_records", len(consumed_msgs)),
            "kafka_mode": kafka_ingest.get("broker_mode", "simulator"),
            "quality_score": quality_report.get("quality_score", 100.0),
            "quality_status": quality_report.get("overall_status", "PASSED"),
            "facts_stored": len(fact_df),
            "skills_extracted": len(skills_df),
            "duckdb_olap_latency_ms": olap_results.get("cumulative_execution_time_ms", 0.0),
            "next_dataset": self.datasets[self.current_index % len(self.datasets)]
        }
        self.latest_result = result_summary
        self._save_state()

        logger.info(f"✅ [KAFKA 1-MIN STREAM] Completed {dataset_name} in {duration}s! Quality: {result_summary['quality_score']}% | DuckDB: {result_summary['duckdb_olap_latency_ms']}ms")
        return result_summary

    def _worker(self):
        logger.info(f"🔄 Kafka Dataset 1-Minute Stream Scheduler Worker Started (Interval: {self.interval_seconds}s).")
        while self.running:
            if not self.paused:
                self.next_run_time = datetime.now() + timedelta(seconds=self.interval_seconds)
                self._save_state()
                try:
                    self.stream_dataset()
                except Exception as e:
                    logger.error(f"Error in Kafka scheduler stream execution: {e}", exc_info=True)
                
                # Sleep interval in small slices to allow fast pause/stop
                sleep_end = time.time() + self.interval_seconds
                while time.time() < sleep_end and self.running and not self.paused:
                    time.sleep(1)
            else:
                time.sleep(1)

    def start(self):
        if not self.running:
            self.running = True
            self.paused = False
            self.next_run_time = datetime.now() + timedelta(seconds=self.interval_seconds)
            self._save_state()
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()
            logger.info("Kafka 1-Minute Stream Scheduler started.")

    def pause(self):
        self.paused = True
        self._save_state()
        logger.info("Kafka Stream Scheduler paused.")

    def resume(self):
        self.paused = False
        self.next_run_time = datetime.now() + timedelta(seconds=self.interval_seconds)
        self._save_state()
        logger.info("Kafka Stream Scheduler resumed.")

    def stop(self):
        self.running = False
        self._save_state()
        logger.info("Kafka Stream Scheduler stopped.")

    def get_status(self) -> Dict[str, Any]:
        countdown = 0
        if self.next_run_time and self.running and not self.paused:
            delta = (self.next_run_time - datetime.now()).total_seconds()
            countdown = max(0, int(delta))

        return {
            "status": "PAUSED" if self.paused else ("RUNNING" if self.running else "IDLE"),
            "interval_seconds": self.interval_seconds,
            "seconds_until_next_run": countdown,
            "current_index": self.current_index,
            "current_dataset": self.datasets[self.current_index % len(self.datasets)] if self.datasets else None,
            "dataset_queue": self.datasets,
            "total_datasets_in_queue": len(self.datasets),
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "next_run_time": self.next_run_time.isoformat() if self.next_run_time else None,
            "total_streamed_count": self.total_streamed,
            "latest_result": self.latest_result
        }


_global_scheduler = None

def get_kafka_scheduler(interval_seconds: int = 60) -> KafkaDatasetStreamScheduler:
    global _global_scheduler
    if _global_scheduler is None:
        _global_scheduler = KafkaDatasetStreamScheduler(interval_seconds=interval_seconds)
    return _global_scheduler


if __name__ == "__main__":
    scheduler = get_kafka_scheduler(interval_seconds=60)
    print("Running initial stream cycle...")
    res = scheduler.stream_dataset()
    print(json.dumps(res, indent=2))
