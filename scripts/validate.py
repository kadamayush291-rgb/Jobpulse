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

def validate_data(**context):
    """
    Validates data quality on the transformed_jobs table.
    Checks for completeness, accuracy and consistency after cleaning.
    """
    try:
        engine = get_engine()
        df = pd.read_sql('SELECT * FROM transformed_jobs', con=engine)
        
        total_rows = len(df)
        logger.info(f"Validating {total_rows} transformed rows.")
        
        issues = []
        
        # 1. Required fields completeness check
        required_cols = ['job_title', 'company', 'city']
        for col in required_cols:
            null_count = df[col].isnull().sum() + (df[col] == 'Unknown').sum()
            if null_count > 0:
                issues.append(f"{col}: {null_count} missing/unknown values")
                logger.warning(f"Validation: {col} has {null_count} missing/unknown values")
        
        # 2. Salary range validation (should be > 0 and < 1,000,000)
        salary_valid = df['salary_avg'].dropna()
        invalid_salary = salary_valid[(salary_valid <= 0) | (salary_valid > 1000000)]
        if len(invalid_salary) > 0:
            issues.append(f"salary_avg: {len(invalid_salary)} values out of valid range")
            logger.warning(f"Validation: {len(invalid_salary)} salary values out of range")
        
        # 3. Date validation (should be parseable and not default)
        null_dates = df['posted_date'].isnull().sum()
        default_dates = (df['posted_date'].astype(str) == '1970-01-01').sum()
        if null_dates + default_dates > 0:
            issues.append(f"posted_date: {null_dates + default_dates} invalid/default dates")
            logger.warning(f"Validation: {null_dates + default_dates} invalid dates")
        
        # 4. Duplicate check
        duplicate_count = df.duplicated(subset=['job_title', 'company', 'city']).sum()
        if duplicate_count > 0:
            issues.append(f"Duplicates: {duplicate_count} potential duplicate rows")
            logger.warning(f"Validation: {duplicate_count} duplicate rows found")
        else:
            logger.info("Validation: 0 duplicate rows found in transformed data (deduplication confirmed).")
        
        # 5. Experience level validation
        valid_levels = ['Entry', 'Mid', 'Senior', 'Lead', 'Director']
        invalid_exp = df[~df['experience_level'].isin(valid_levels) & df['experience_level'].notna()]
        if len(invalid_exp) > 0:
            issues.append(f"experience_level: {len(invalid_exp)} non-standard values")
        
        # 6. Job type validation
        valid_types = ['Full-time', 'Part-time', 'Contract', 'Internship', 'Remote']
        invalid_type = df[~df['job_type'].isin(valid_types) & df['job_type'].notna()]
        if len(invalid_type) > 0:
            issues.append(f"job_type: {len(invalid_type)} non-standard values")
        
        # Build validation report
        records_with_salary = len(salary_valid)
        validation_report = {
            'total_rows': total_rows,
            'issues_found': len(issues),
            'issues': issues,
            'salary_coverage': f"{records_with_salary}/{total_rows}",
            'duplicate_count': int(duplicate_count),
            'status': 'PASSED' if len(issues) <= 3 else 'WARNING'
        }
        
        logger.info(f"Validation Report: {validation_report}")
        logger.info(f"Validation {validation_report['status']} with {len(issues)} issues.")
        
        if context and 'ti' in context:
            context['ti'].xcom_push(key='validation_report', value=validation_report)
            
        return validation_report
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise
