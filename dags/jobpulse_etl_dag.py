import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add scripts directory to path to allow importing modules
# In Docker, scripts are mounted at /opt/airflow/scripts
airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
scripts_path = os.path.join(airflow_home, 'scripts')
if not os.path.exists(scripts_path):
    # Fallback for local development
    scripts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
sys.path.insert(0, scripts_path)

from extract import extract_data
from validate import validate_data
from transform import transform_data
from load import load_data
from analytics import compute_analytics

default_args = {
    'owner': 'jobpulse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id='jobpulse_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for JobPulse Data Engineering project',
    schedule_interval='*/5 * * * *',  # Runs automatically every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['jobpulse', 'etl', 'data-engineering'],
) as dag:
    
    dag.doc_md = """
    # JobPulse ETL Pipeline
    This DAG extracts job market data from CSV, validates it, transforms it into clean structural data, 
    loads it into PostgreSQL, and computes basic analytics.
    """

    def log_status(**context):
        """Logs the pipeline run status to the pipeline_runs table."""
        import logging
        from datetime import datetime
        try:
            import sys
            airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
            sp = os.path.join(airflow_home, 'scripts')
            if sp not in sys.path:
                sys.path.insert(0, sp)
            from db_utils import get_engine
            import pandas as pd
            
            run_record = pd.DataFrame([{
                'run_id': context.get('run_id', 'manual'),
                'dag_id': 'jobpulse_etl_pipeline',
                'task_id': 'log_pipeline_run',
                'status': 'SUCCESS',
                'records_processed': context.get('ti', {}).xcom_pull(key='loaded_records_count', task_ids='load_transformed_data') or 0,
                'records_failed': 0,
                'error_message': None,
                'started_at': datetime.now(),
                'completed_at': datetime.now()
            }])
            
            engine = get_engine()
            run_record.to_sql('pipeline_runs', con=engine, if_exists='append', index=False)
            logging.info("Pipeline run logged successfully.")
        except Exception as e:
            logging.warning(f"Could not log pipeline run: {e}")
            logging.info("Pipeline executed successfully (logging to DB failed).")
        
    extract_task = PythonOperator(
        task_id='extract_csv_data',
        python_callable=extract_data,
        provide_context=True,
    )

    validate_task = PythonOperator(
        task_id='validate_raw_data',
        python_callable=validate_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_transformed_data',
        python_callable=load_data,
        provide_context=True,
    )

    analytics_task = PythonOperator(
        task_id='compute_analytics',
        python_callable=compute_analytics,
        provide_context=True,
    )

    log_task = PythonOperator(
        task_id='log_pipeline_run',
        python_callable=log_status,
        provide_context=True,
    )

    extract_task >> transform_task >> validate_task >> load_task >> analytics_task >> log_task
