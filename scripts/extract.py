import pandas as pd
import logging
import os
import sys

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)

from db_utils import get_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(**context):
    """
    Extracts data from the CSV file and loads it into the raw_jobs table.
    """
    try:
        # Define paths - handle Docker vs local
        airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
        data_dir = os.path.join(airflow_home, 'data')
        if not os.path.exists(data_dir):
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        csv_path = os.path.join(data_dir, 'jobs_dataset.csv')
        
        logger.info(f"Extracting data from {csv_path}")
        
        # Read CSV with Pandas
        df = pd.read_csv(csv_path, encoding='utf-8', on_bad_lines='skip')
        
        # Basic checks
        if df.empty:
            logger.error("Extracted dataset is empty.")
            raise ValueError("Empty CSV file")
        
        logger.info(f"Successfully read {len(df)} rows from CSV.")
        
        # Load into raw_jobs table
        engine = get_engine()
        df.to_sql('raw_jobs', con=engine, if_exists='replace', index=False)
        
        logger.info("Successfully loaded data into raw_jobs table.")
        
        if context and 'ti' in context:
            context['ti'].xcom_push(key='raw_row_count', value=len(df))
            
        return "Extraction successful"
        
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise
