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
            'host': os.getenv('DB_HOST', '192.168.1.38'),
            'user': os.getenv('DB_USER', 'labeling'),
            'password': os.getenv('DB_PASSWORD', 'labeling'),
            'autocommit': True,
            'use_unicode': True,
            'charset': 'utf8mb4'
        }
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '15'))
        self.hidden_databases = ['smt', 'onanofflimited']

db_config = DatabaseConfig()