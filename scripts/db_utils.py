import os
import logging
from sqlalchemy import create_engine
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SQLite database path (relative to project root)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_DB_PATH = os.path.join(_PROJECT_ROOT, 'data', 'jobpulse.db')

def get_engine():
    """
    Creates and returns a SQLAlchemy engine.
    - If USE_SQLITE env var is set, uses SQLite (local mode, no Docker needed).
    - Otherwise tries PostgreSQL, falls back to SQLite if connection fails.
    """
    default_sqlite = '0' if (os.environ.get('POSTGRES_HOST') or os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')) else '1'
    use_sqlite = os.environ.get('USE_SQLITE', default_sqlite).strip()

    if use_sqlite == '1':
        logger.info(f"Using SQLite database at {SQLITE_DB_PATH}")
        return create_engine(f"sqlite:///{SQLITE_DB_PATH}", echo=False)

    # Try PostgreSQL
    try:
        airflow_conn = os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        if airflow_conn:
            conn_str = airflow_conn.replace('postgresql+psycopg2://', 'postgresql://')
        else:
            db_user = os.environ.get('POSTGRES_USER', os.environ.get('DB_USER', 'jobpulse'))
            db_password = os.environ.get('POSTGRES_PASSWORD', os.environ.get('DB_PASSWORD', 'jobpulse123'))
            db_host = os.environ.get('POSTGRES_HOST', os.environ.get('DB_HOST', 'postgres'))
            db_port = os.environ.get('POSTGRES_PORT', os.environ.get('DB_PORT', '5432'))
            db_name = os.environ.get('POSTGRES_DB', os.environ.get('DB_NAME', 'jobpulse_db'))
            conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        engine = create_engine(conn_str, pool_pre_ping=True)
        # Quick connection test
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Database engine created successfully (PostgreSQL).")
        return engine
    except Exception as e:
        logger.warning(f"PostgreSQL unavailable ({e}), falling back to SQLite.")
        return create_engine(f"sqlite:///{SQLITE_DB_PATH}", echo=False)

@contextmanager
def get_connection():
    """
    Context manager for database connections.
    Automatically closes the connection when done.
    """
    engine = get_engine()
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
