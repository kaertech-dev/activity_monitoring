# activity_monitoring/database.py
"""Database connection and management"""
from mysql.connector.pooling import MySQLConnectionPool
from contextlib import contextmanager
from fastapi import HTTPException
import threading
import logging
from config import db_config

logger = logging.getLogger(__name__)

# Initialize connection pool
try:
    POOL = MySQLConnectionPool(
        pool_name="production_monitoring_pool_v2",
        pool_size=db_config.pool_size,
        **db_config.config
    )
    logger.info(f"Database connection pool initialized with {db_config.pool_size} connections")
except Exception as e:
    logger.error(f"Failed to initialize database pool: {e}")
    raise

# Thread lock for safe concurrent operations
lock = threading.Lock()

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    cursor = None
    try:
        conn = POOL.get_connection()
        cursor = conn.cursor(buffered=True)
        yield cursor
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()