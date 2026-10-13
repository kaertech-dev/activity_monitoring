# activity_monitoring/config.py
"""Configuration management for Production Monitoring System"""
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration management"""
    def __init__(self):
        self.config = {
            'host': os.environ['DB_HOST'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
            'autocommit': True,
            'use_unicode': True,
            'charset': 'utf8mb4',
            'connection_timeout': 60,
            'sql_mode': 'TRADITIONAL'
        }
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '15'))
        self.hidden_databases = ['smt', 'onanofflimited']

db_config = DatabaseConfig()