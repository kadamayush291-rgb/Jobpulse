"""
JobPulse - Pipeline Execution Engine with Retries and Scheduling
Provides robust task retry mechanisms, execution tracking, and background scheduling.
Works in both local mode (SQLite/PostgreSQL) and with Apache Airflow.
"""
import os
import sys
import time
import logging
import threading
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JobPulse.PipelineRunner")

# Ensure scripts path is accessible
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from db_utils import get_engine

# Global execution lock to prevent overlapping runs
_pipeline_lock = threading.Lock()
_scheduler_instance = None


def ensure_pipeline_runs_table():
    """Ensure the pipeline_runs table exists in the active database."""
    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Works across both SQLite and PostgreSQL
            create_sql = text("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY,
                    run_id VARCHAR(100),
                    dag_id VARCHAR(100),
                    task_id VARCHAR(100),
                    status VARCHAR(50),
                    records_processed INTEGER DEFAULT 0,
                    records_failed INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
            conn.execute(create_sql)
    except Exception as e:
        logger.warning(f"Could not verify pipeline_runs table: {e}")


def execute_with_retry(task_func, task_name, max_retries=3, delay_seconds=2, backoff=2):
    """
    Executes a task function with automatic retries and exponential backoff.
    
    :param task_func: Python callable for the task
    :param task_name: Human-readable task name
    :param max_retries: Number of retry attempts (default: 3)
    :param delay_seconds: Initial delay in seconds before retry (default: 2)
    :param backoff: Multiplier for exponential backoff (default: 2)
    :return: The result of task_func() if successful
    """
    attempt = 0
    current_delay = delay_seconds
    
    while attempt <= max_retries:
        try:
            if attempt > 0:
                logger.info(f"🔄 [RETRY {attempt}/{max_retries}] Executing: {task_name}...")
            else:
                logger.info(f"▶️ Starting task: {task_name}...")
            
            result = task_func()
            logger.info(f"✅ [SUCCESS] {task_name} finished successfully.")
            return result
            
        except Exception as exc:
            attempt += 1
            if attempt <= max_retries:
                logger.warning(
                    f"⚠️ [FAILURE] {task_name} failed (Attempt {attempt}/{max_retries}): {exc}. "
                    f"Retrying in {current_delay}s..."
                )
                time.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.error(
                    f"❌ [FAILED] {task_name} exhausted all {max_retries} retries. Error: {exc}"
                )
                raise exc


def record_pipeline_run(run_id, status, task_id="pipeline_complete", records_processed=0,
                        records_failed=0, error_message=None, started_at=None, completed_at=None):
    """Logs the pipeline run outcome into the pipeline_runs table."""
    try:
        ensure_pipeline_runs_table()
        engine = get_engine()
        
        record = {
            'run_id': run_id,
            'dag_id': 'jobpulse_pipeline',
            'task_id': task_id,
            'status': status,
            'records_processed': int(records_processed or 0),
            'records_failed': int(records_failed or 0),
            'error_message': str(error_message) if error_message else None,
            'started_at': started_at or datetime.now(),
            'completed_at': completed_at or datetime.now()
        }
        
        df_run = pd.DataFrame([record])
        with engine.begin() as conn:
            df_run.to_sql('pipeline_runs', con=conn, if_exists='append', index=False)
        logger.info(f"📝 Pipeline run {run_id} logged to database with status: {status}")
    except Exception as e:
        logger.warning(f"Failed to record pipeline run in database: {e}")


def run_pipeline(triggered_by="manual", max_retries=3, retry_delay=2):
    """
    Runs the complete 5-step ETL pipeline with retry handling on every step.
    Thread-safe to prevent simultaneous executions.
    """
    if not _pipeline_lock.acquire(blocking=False):
        logger.warning("Pipeline is already currently running. Skipping duplicate execution.")
        return {
            'status': 'ALREADY_RUNNING',
            'message': 'Pipeline is already executing in another thread.'
        }
    
    started_at = datetime.now()
    run_id = f"run_{started_at.strftime('%Y%m%d_%H%M%S')}_{triggered_by}"
    logger.info("=" * 65)
    logger.info(f"🚀 JobPulse ETL Pipeline Execution Started (Run ID: {run_id})")
    logger.info(f"   Triggered by: {triggered_by} | Max Retries: {max_retries} | Retry Delay: {retry_delay}s")
    logger.info("=" * 65)
    
    try:
        from extract import extract_data
        from transform import transform_data
        from validate import validate_data
        from load import load_data
        from analytics import compute_analytics
        
        pipeline_steps = [
            ("Step 1/5: Extract CSV Data", extract_data),
            ("Step 2/5: Transform & Clean Data", transform_data),
            ("Step 3/5: Validate Data Quality", validate_data),
            ("Step 4/5: Load Data & Dimensional Models", load_data),
            ("Step 5/5: Compute Analytics Summaries", compute_analytics),
        ]
        
        step_results = {}
        for step_name, step_callable in pipeline_steps:
            res = execute_with_retry(
                step_callable,
                step_name,
                max_retries=max_retries,
                delay_seconds=retry_delay
            )
            step_results[step_name] = res
        
        # Check record count
        engine = get_engine()
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT COUNT(*) FROM transformed_jobs")).scalar()
                records_processed = res or 0
        except Exception:
            records_processed = 0
            
        completed_at = datetime.now()
        duration_s = (completed_at - started_at).total_seconds()
        
        logger.info("=" * 65)
        logger.info(f"🎉 Pipeline Run Completed Successfully in {duration_s:.2f}s!")
        logger.info(f"   Total Records Processed: {records_processed}")
        logger.info("=" * 65)
        
        record_pipeline_run(
            run_id=run_id,
            status='SUCCESS',
            task_id='pipeline_complete',
            records_processed=records_processed,
            started_at=started_at,
            completed_at=completed_at
        )
        
        return {
            'run_id': run_id,
            'status': 'SUCCESS',
            'duration_seconds': duration_s,
            'records_processed': records_processed,
            'started_at': started_at.isoformat(),
            'completed_at': completed_at.isoformat()
        }
        
    except Exception as exc:
        completed_at = datetime.now()
        duration_s = (completed_at - started_at).total_seconds()
        logger.error(f"❌ Pipeline Run Failed: {exc}", exc_info=True)
        
        record_pipeline_run(
            run_id=run_id,
            status='FAILED',
            task_id='pipeline_step_failed',
            records_processed=0,
            records_failed=1,
            error_message=str(exc),
            started_at=started_at,
            completed_at=completed_at
        )
        
        return {
            'run_id': run_id,
            'status': 'FAILED',
            'error': str(exc),
            'duration_seconds': duration_s,
            'started_at': started_at.isoformat(),
            'completed_at': completed_at.isoformat()
        }
        
    finally:
        _pipeline_lock.release()


class PipelineScheduler:
    """
    Lightweight, background thread-based scheduler for automated recurring pipeline runs.
    """
    def __init__(self, interval_minutes=5, max_retries=3, retry_delay=5):
        self.interval_seconds = max(10, int(interval_minutes * 60))
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.running = False
        self.thread = None
        self.last_run_time = None
        self.next_run_time = None
        self.last_status = None
        self.total_runs = 0
        
    def _worker(self):
        logger.info(f"⏰ Scheduler worker loop started (Interval: {self.interval_seconds // 60}m).")
        while self.running:
            self.next_run_time = datetime.now() + timedelta(seconds=self.interval_seconds)
            logger.info(f"⏳ Next scheduled pipeline run at: {self.next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Sleep in 1-second chunks so scheduler responds quickly to stop requests
            for _ in range(self.interval_seconds):
                if not self.running:
                    break
                time.sleep(1)
                
            if not self.running:
                break
                
            logger.info("🔔 Scheduled trigger fired. Starting pipeline execution...")
            result = run_pipeline(
                triggered_by="scheduler",
                max_retries=self.max_retries,
                retry_delay=self.retry_delay
            )
            
            self.last_run_time = datetime.now()
            self.last_status = result.get('status')
            self.total_runs += 1

    def start(self):
        """Starts the scheduler in a daemon thread."""
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True, name="JobPulse-Scheduler")
        self.thread.start()
        logger.info(f"✅ Pipeline Scheduler started in background (Every {self.interval_seconds // 60} minutes).")

    def stop(self):
        """Stops the scheduler."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        logger.info("🛑 Pipeline Scheduler stopped.")

    def trigger_now(self):
        """Triggers an immediate run in a background thread."""
        threading.Thread(
            target=run_pipeline,
            kwargs={'triggered_by': 'manual_api', 'max_retries': self.max_retries, 'retry_delay': self.retry_delay},
            daemon=True
        ).start()
        return {'status': 'TRIGGERED', 'message': 'Pipeline execution initiated.'}

    def get_status(self):
        """Returns the current status of the scheduler."""
        return {
            'scheduler_active': self.running,
            'interval_minutes': self.interval_seconds // 60,
            'last_run_time': self.last_run_time.strftime('%Y-%m-%d %H:%M:%S') if self.last_run_time else None,
            'next_run_time': self.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if self.next_run_time else None,
            'last_status': self.last_status,
            'total_runs': self.total_runs,
            'max_retries': self.max_retries,
            'retry_delay_seconds': self.retry_delay
        }


def get_global_scheduler(interval_minutes=5, max_retries=3, retry_delay=5):
    """Singleton getter for the global pipeline scheduler."""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PipelineScheduler(
            interval_minutes=interval_minutes,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
    return _scheduler_instance
