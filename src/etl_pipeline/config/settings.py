"""
Configuration Settings Module

This module handles loading and managing configuration settings for the ETL pipeline.
It supports loading from environment variables and configuration files.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def load_config():
    """
    Load configuration settings from environment variables and/or config files.
    
    Returns:
        dict: A dictionary containing all configuration settings
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Database configuration
    db_type = os.getenv('DB_TYPE', 'sqlite')
    
    # Configure database based on type
    if db_type.lower() == 'sqlite':
        db_config = {
            'type': 'sqlite',
            'db_path': os.getenv('SQLITE_DB_PATH', '../sales_database.sqlite')
        }
    elif db_type.lower() == 'postgresql':
        db_config = {
            'type': 'postgresql',
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': int(os.getenv('PG_PORT', '5432')),
            'database': os.getenv('PG_DATABASE', 'sales'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', 'password')
        }
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    # Logging configuration
    logging_config = {
        'level': os.getenv('LOG_LEVEL', 'INFO'),
        'file': os.getenv('LOG_FILE', '../etl_process.log'),
        'format': os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    }
    
    # Build complete configuration
    config = {
        'csv_path': os.getenv('CSV_PATH', '../sales_data.csv'),
        'reports_dir': os.getenv('REPORTS_DIR', '../reports'),
        'database': db_config,
        'logging': logging_config,
        'exchange_rate_api_url': os.getenv('EXCHANGE_RATE_API_URL', 'https://api.exchangerate-api.com/v4/latest/USD'),
        'exchange_rate_fallback': {
            'USD': 1.0,
            'EUR': 0.91,
            'GBP': 0.78
        }
    }
    
    return config
