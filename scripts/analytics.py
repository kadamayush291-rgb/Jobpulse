import pandas as pd
import logging
import os
import sys

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)

from db_utils import get_engine
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compute_analytics(**context):
    """
    Computes summary metrics from transformed data and stores results
    in the analytics_summary table in a row-per-dimension format.
    """
    try:
        engine = get_engine()
        
        logger.info("Starting analytics computation.")
        
        df = pd.read_sql('SELECT * FROM transformed_jobs', con=engine)
        
        # Try to read job_skills if available
        try:
            skills_df = pd.read_sql('SELECT * FROM job_skills', con=engine)
        except Exception:
            skills_df = pd.DataFrame()
            logger.warning("job_skills table not available, skipping skill analytics.")
        
        summary_rows = []
        
        # 1. Top skills by frequency
        if not skills_df.empty and 'skill' in skills_df.columns:
            top_skills = skills_df['skill'].value_counts().head(15)
            for skill, count in top_skills.items():
                summary_rows.append({
                    'metric_name': 'top_skills',
                    'metric_value': float(count),
                    'dimension': 'skill',
                    'dimension_value': skill
                })
        
        # 2. Average salary by job type
        if 'salary_avg' in df.columns and 'job_type' in df.columns:
            avg_sal_type = df.groupby('job_type')['salary_avg'].mean().dropna()
            for job_type, avg_sal in avg_sal_type.items():
                summary_rows.append({
                    'metric_name': 'avg_salary_by_type',
                    'metric_value': round(float(avg_sal), 2),
                    'dimension': 'job_type',
                    'dimension_value': str(job_type)
                })
        
        # 3. Average salary by experience level
        if 'salary_avg' in df.columns and 'experience_level' in df.columns:
            avg_sal_exp = df.groupby('experience_level')['salary_avg'].mean().dropna()
            for exp_level, avg_sal in avg_sal_exp.items():
                summary_rows.append({
                    'metric_name': 'avg_salary_by_experience',
                    'metric_value': round(float(avg_sal), 2),
                    'dimension': 'experience_level',
                    'dimension_value': str(exp_level)
                })
        
        # 4. Job count by city
        city_count = df['city'].value_counts().head(15)
        for city, count in city_count.items():
            summary_rows.append({
                'metric_name': 'job_count_by_city',
                'metric_value': float(count),
                'dimension': 'city',
                'dimension_value': str(city)
            })
        
        # 5. Job count by company
        company_count = df['company'].value_counts().head(10)
        for company, count in company_count.items():
            summary_rows.append({
                'metric_name': 'job_count_by_company',
                'metric_value': float(count),
                'dimension': 'company',
                'dimension_value': str(company)
            })
        
        # 6. Job count by source
        source_count = df['source'].value_counts()
        for source, count in source_count.items():
            summary_rows.append({
                'metric_name': 'job_count_by_source',
                'metric_value': float(count),
                'dimension': 'source',
                'dimension_value': str(source)
            })
        
        # 7. Job type distribution
        type_dist = df['job_type'].value_counts()
        for job_type, count in type_dist.items():
            summary_rows.append({
                'metric_name': 'job_type_distribution',
                'metric_value': float(count),
                'dimension': 'job_type',
                'dimension_value': str(job_type)
            })
        
        # 8. Experience level distribution
        exp_dist = df['experience_level'].value_counts()
        for exp_level, count in exp_dist.items():
            summary_rows.append({
                'metric_name': 'experience_distribution',
                'metric_value': float(count),
                'dimension': 'experience_level',
                'dimension_value': str(exp_level)
            })
        
        # 9. Monthly job posting trends
        try:
            df['posted_month'] = pd.to_datetime(df['posted_date'], errors='coerce').dt.to_period('M').astype(str)
            monthly = df['posted_month'].value_counts().sort_index()
            for month, count in monthly.items():
                summary_rows.append({
                    'metric_name': 'monthly_trends',
                    'metric_value': float(count),
                    'dimension': 'month',
                    'dimension_value': str(month)
                })
        except Exception as e:
            logger.warning(f"Could not compute monthly trends: {e}")
        
        # 10. Overall salary statistics
        salary_stats = df['salary_avg'].dropna().describe()
        for stat_name, stat_val in salary_stats.items():
            summary_rows.append({
                'metric_name': 'salary_statistics',
                'metric_value': round(float(stat_val), 2),
                'dimension': 'statistic',
                'dimension_value': str(stat_name)
            })
        
        # 11. Skill category distribution
        if not skills_df.empty and 'skill_category' in skills_df.columns:
            cat_dist = skills_df['skill_category'].value_counts()
            for category, count in cat_dist.items():
                summary_rows.append({
                    'metric_name': 'skill_category_distribution',
                    'metric_value': float(count),
                    'dimension': 'skill_category',
                    'dimension_value': str(category)
                })
        
        # Save to analytics_summary table
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_sql('analytics_summary', con=engine, if_exists='replace', index=False)
        
        logger.info(f"Analytics computation complete. {len(summary_rows)} metric rows saved.")
        
        if context and 'ti' in context:
            context['ti'].xcom_push(key='analytics_row_count', value=len(summary_rows))
        
        return "Analytics successful"
        
    except Exception as e:
        logger.error(f"Error computing analytics: {str(e)}")
        raise
