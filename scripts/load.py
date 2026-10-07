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

# Skill category mapping
SKILL_CATEGORIES = {
    'Programming': ['PYTHON', 'JAVA', 'C++', 'GO', 'JAVASCRIPT', 'TYPESCRIPT', 'RUBY', 'C#',
                     'SCALA', 'RUST', 'PHP', 'SWIFT', 'KOTLIN', 'R', 'NODE.JS', 'REACT',
                     'ANGULAR', 'VUE.JS', 'DJANGO', 'FLASK', 'SPRING BOOT', 'FASTAPI'],
    'Cloud': ['AWS', 'GCP', 'AZURE', 'DOCKER', 'KUBERNETES', 'TERRAFORM', 'CLOUDFORMATION',
              'HEROKU', 'LAMBDA', 'EC2', 'S3'],
    'Data/DB': ['SQL', 'SPARK', 'HADOOP', 'PANDAS', 'SNOWFLAKE', 'BIGQUERY', 'KAFKA',
                'AIRFLOW', 'MONGODB', 'POSTGRESQL', 'MYSQL', 'REDIS', 'ELASTICSEARCH',
                'CASSANDRA', 'DUCKDB', 'DBT', 'FLINK', 'HIVE'],
    'ML/AI': ['TENSORFLOW', 'PYTORCH', 'SCIKIT-LEARN', 'KERAS', 'MACHINE LEARNING',
              'DEEP LEARNING', 'NLP', 'COMPUTER VISION', 'MLFLOW', 'HUGGINGFACE'],
    'DevOps': ['CI/CD', 'JENKINS', 'GITHUB ACTIONS', 'GITLAB CI', 'ANSIBLE', 'PUPPET',
               'CHEF', 'NAGIOS', 'PROMETHEUS', 'GRAFANA', 'DATADOG'],
    'Visualization': ['TABLEAU', 'POWER BI', 'LOOKER', 'MATPLOTLIB', 'PLOTLY', 'D3.JS',
                       'SUPERSET', 'METABASE', 'CHART.JS'],
    'Other': []
}

def categorize_skill(skill_name):
    """Categorize a skill into its category."""
    skill_upper = skill_name.strip().upper()
    for category, skills in SKILL_CATEGORIES.items():
        if category == 'Other':
            continue
        if skill_upper in skills:
            return category
    return 'Other'


def load_data(**context):
    """
    Loads transformed data into final tables and creates the job_skills dimension table.
    """
    try:
        engine = get_engine()

        logger.info("Starting data load.")

        # Read transformed data
        df = pd.read_sql('SELECT * FROM transformed_jobs', con=engine)

        # Create job_skills dimensional table
        skills_data = []
        for idx, row in df.iterrows():
            skills_str = row.get('skills')
            if pd.notna(skills_str) and isinstance(skills_str, str):
                skills_list = [s.strip() for s in skills_str.split(',') if s.strip()]
                for skill in skills_list:
                    skills_data.append({
                        'job_id': idx,
                        'skill': skill,
                        'skill_category': categorize_skill(skill)
                    })

        if skills_data:
            skills_df = pd.DataFrame(skills_data)
            skills_df.to_sql('job_skills', con=engine, if_exists='replace', index=False)
            logger.info(f"Loaded {len(skills_df)} rows into job_skills.")
        else:
            logger.warning("No skills data extracted.")

        logger.info(f"Loaded {len(df)} rows in transformed_jobs.")

        if context and 'ti' in context:
            context['ti'].xcom_push(key='loaded_records_count', value=len(df))

        return "Load successful"

    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        raise
