# db_config.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize connection pool
def init_db_pool():
    """Initialize a PostgreSQL connection pool using environment variables."""
    required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"]
    for var in required_env_vars:
        if not os.getenv(var):
            logger.error(f"Environment variable {var} is not set")
            raise ValueError(f"Environment variable {var} is not set")
    
    try:
        pool = SimpleConnectionPool(
            minconn=1,
            maxconn=20,  # Adjust based on AWS RDS limits
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
            cursor_factory=RealDictCursor
        )
        logger.info("Database connection pool initialized successfully")
        return pool
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database connection pool: {e}")
        raise

# Global connection pool
try:
    db_pool = init_db_pool()
except Exception as e:
    logger.error(f"Failed to create connection pool: {e}")
    raise

def get_db_connection():
    """Get a connection from the pool."""
    try:
        conn = db_pool.getconn()
        logger.debug("Retrieved connection from pool")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to get connection from pool: {e}")
        raise

def release_db_connection(conn):
    """Release a connection back to the pool."""
    try:
        db_pool.putconn(conn)
        logger.debug("Released connection back to pool")
    except psycopg2.Error as e:
        logger.error(f"Failed to release connection: {e}")
        raise

def close_db_pool():
    """Close all connections in the pool (for cleanup)."""
    try:
        db_pool.closeall()
        logger.info("Database connection pool closed")
    except psycopg2.Error as e:
        logger.error(f"Failed to close connection pool: {e}")
        raise