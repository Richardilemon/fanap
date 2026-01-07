import os
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Get database connection - works with both Airflow and direct connections.
    
    Checks environment to determine connection method:
    - If DATABASE_URL exists → Direct connection (Neon/GitHub Actions)
    - Otherwise → Use Airflow PostgresHook (for Airflow DAGs)
    """
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        # Direct connection (Neon, GitHub Actions, etc.)
        import psycopg2
        try:
            conn = psycopg2.connect(database_url, sslmode='require')
            logger.info("✅ Connected to database (direct connection)")
            return conn
        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            raise
    else:
        # Airflow PostgresHook
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            hook = PostgresHook(postgres_conn_id='fpl_db_conn')
            conn = hook.get_conn()
            logger.info("✅ Connected to database (Airflow hook)")
            return conn
        except ImportError:
            raise RuntimeError(
                "No DATABASE_URL found and Airflow not available. "
                "Set DATABASE_URL environment variable."
            )


def db_connection_wrapper(func):
    """
    Decorator to handle database connections automatically.
    Works with both Airflow and direct connections.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = None
        try:
            conn = get_db_connection()
            result = func(conn, *args, **kwargs)
            return result
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error in {func.__name__}: {e}")
            raise
            
        finally:
            if conn:
                conn.close()
                logger.info("🔒 Database connection closed")
    
    return wrapper