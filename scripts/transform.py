import pandas as pd
import numpy as np
import re
import logging
import os
import sys

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)

from db_utils import get_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_salary(salary_str):
    if pd.isna(salary_str):
        return None, None, None
    
    # Clean string
    s = str(salary_str).upper().replace(',', '').replace('$', '').strip()
    
    # Handle 'K'
    def parse_number(num_str):
        num_str = num_str.strip()
        if num_str.endswith('K'):
            try:
                return float(num_str[:-1]) * 1000
            except:
                return None
        try:
            return float(num_str)
        except:
            return None

    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        min_s = parse_number(parts[0])
        max_s = parse_number(parts[1])
        if min_s is not None and max_s is not None:
            return min_s, max_s, (min_s + max_s) / 2
    else:
        val = parse_number(s)
        if val is not None:
            return val, val, val
            
    return None, None, None

def categorize_skills(skill_list):
    if not isinstance(skill_list, list):
        return 'Other'
    
    programming = ['PYTHON', 'JAVA', 'C++', 'GO', 'JAVASCRIPT', 'TYPESCRIPT', 'RUBY', 'C#', 'SCALA']
    cloud = ['AWS', 'GCP', 'AZURE', 'DOCKER', 'KUBERNETES', 'TERRAFORM']
    data = ['SQL', 'SPARK', 'HADOOP', 'PANDAS', 'SNOWFLAKE', 'BIGQUERY', 'KAFKA']
    
    skill_upper = [s.strip().upper() for s in skill_list]
    for s in skill_upper:
        if s in programming: return 'Programming'
        if s in cloud: return 'Cloud'
        if s in data: return 'Data/DB'
    return 'Other'

def transform_data(**context):
    """
    Cleans and transforms data from raw_jobs table.
    """
    try:
        engine = get_engine()
        df = pd.read_sql('SELECT * FROM raw_jobs', con=engine)
        
        logger.info("Starting data transformation.")
        
        # Remove duplicates
        raw_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = raw_count - len(df)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows ({raw_count} raw -> {len(df)} unique).")
        else:
            logger.info("No duplicate rows detected in raw dataset.")
        
        # Handle missing values
        df['job_title'] = df['job_title'].fillna('Unknown')
        df['company'] = df['company'].fillna('Unknown')
        
        # Parse salaries
        salary_parsed = df['salary'].apply(parse_salary)
        df['salary_min'] = [x[0] for x in salary_parsed]
        df['salary_max'] = [x[1] for x in salary_parsed]
        df['salary_avg'] = [x[2] for x in salary_parsed]
        
        # Validate salaries (set invalid to null)
        df.loc[(df['salary_avg'] < 0) | (df['salary_avg'] > 1000000), ['salary_min', 'salary_max', 'salary_avg']] = None
        
        # Standardize dates
        df['posted_date'] = pd.to_datetime(df['posted_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['posted_date'] = df['posted_date'].fillna('1970-01-01') # Default fallback
        
        # Split location
        location_split = df['location'].str.split(',', expand=True)
        df['city'] = location_split[0].str.strip() if 0 in location_split else None
        df['state'] = location_split[1].str.strip() if 1 in location_split else None
        df['country'] = 'US' # Default based on dataset requirement
        
        # Clean and normalize skills
        df['skills_list'] = df['skills'].str.split(',')
        df['skills_list'] = df['skills_list'].apply(lambda x: [s.strip() for s in x] if isinstance(x, list) else [])
        
        # Skill categories mapping can be done here or in load
        
        # Save intermediate state back to DB for loading task
        df.drop(columns=['skills_list'], inplace=True) # Cannot save lists to simple SQL easily
        
        df.to_sql('transformed_jobs', con=engine, if_exists='replace', index=False)
        
        logger.info(f"Transformation complete. {len(df)} rows ready for loading (duplicates removed: {duplicates_removed}).")
        
        if context and 'ti' in context:
            context['ti'].xcom_push(key='transformed_row_count', value=len(df))
            context['ti'].xcom_push(key='duplicates_removed', value=duplicates_removed)
            
        return f"Transformation successful: {duplicates_removed} duplicates removed, {len(df)} records kept"
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise
