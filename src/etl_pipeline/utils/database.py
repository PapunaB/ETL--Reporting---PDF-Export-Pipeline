"""
Database Utility Module

This module provides database connection and utility functions for the ETL pipeline.
"""

import logging
import sqlite3

# Try to import psycopg2 for PostgreSQL support
try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

logger = logging.getLogger('etl_pipeline.database')


def get_database_connection(db_config):
    """
    Create a database connection based on the specified configuration.
    
    Args:
        db_config (dict): Database configuration dictionary
            For SQLite: {'type': 'sqlite', 'db_path': 'path/to/db.sqlite'}
            For PostgreSQL: {'type': 'postgresql', 'host': 'localhost', 'port': 5432, 
                            'database': 'sales', 'user': 'postgres', 'password': 'password'}
    
    Returns:
        connection: A database connection object
    
    Raises:
        ValueError: If required configuration parameters are missing
        ImportError: If PostgreSQL is requested but psycopg2 is not installed
    """
    db_type = db_config.get('type', 'sqlite').lower()
    
    if db_type == 'sqlite':
        if 'db_path' not in db_config:
            raise ValueError("db_path is required for SQLite connection")
        
        logger.info(f"Creating/connecting to SQLite database at {db_config.get('db_path')}")
        return sqlite3.connect(db_config['db_path'])
    
    elif db_type == 'postgresql':
        if not POSTGRESQL_AVAILABLE:
            raise ImportError("psycopg2 is required for PostgreSQL connection but not installed")
        
        required_params = ['host', 'database', 'user', 'password']
        for param in required_params:
            if param not in db_config:
                raise ValueError(f"{param} is required for PostgreSQL connection")
        
        # Create connection string
        conn_string = f"host={db_config['host']} "
        if 'port' in db_config:
            conn_string += f"port={db_config['port']} "
        conn_string += f"dbname={db_config['database']} user={db_config['user']} password={db_config['password']}"
        
        logger.info(f"Connecting to PostgreSQL database at {db_config.get('host')}")
        return psycopg2.connect(conn_string)
    
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


class DatabaseTransaction:
    """Context manager for database transactions."""
    
    def __init__(self, connection):
        """
        Initialize with a database connection.
        
        Args:
            connection: A database connection object
        """
        self.connection = connection
    
    def __enter__(self):
        """Enter the context manager."""
        return self.connection.cursor()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, committing or rolling back as appropriate."""
        if exc_type is None:
            # No exception, commit the transaction
            self.connection.commit()
        else:
            # Exception occurred, roll back the transaction
            logger.error(f"Transaction failed: {exc_val}")
            self.connection.rollback()
            return False  # Re-raise the exception
