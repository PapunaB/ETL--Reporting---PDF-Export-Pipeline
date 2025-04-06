"""
Database Loader Module

This module provides functionality to load data into a database.
"""

import logging
from datetime import datetime
from etl_pipeline.utils.database import DatabaseTransaction

logger = logging.getLogger('etl_pipeline.loaders.database')


class DatabaseLoader:
    """Class for loading data into a database."""
    
    def __init__(self, connection, db_type='sqlite'):
        """
        Initialize with a database connection.
        
        Args:
            connection: A database connection object
            db_type (str): Type of database ('sqlite' or 'postgresql')
        """
        self.conn = connection
        self.db_type = db_type.lower()
    
    def create_tables(self):
        """
        Create database tables based on the configured database type.
        
        Raises:
            Exception: If an error occurs during table creation
        """
        try:
            logger.info("Creating database tables")
            
            if self.db_type == 'sqlite':
                self._create_sqlite_tables()
            elif self.db_type == 'postgresql':
                self._create_postgresql_tables()
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise
    
    def _create_sqlite_tables(self):
        """Create tables for SQLite database."""
        with DatabaseTransaction(self.conn) as cursor:
            # Exchange rates table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rates (
                currency TEXT PRIMARY KEY,
                rate REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            ''')
            
            # Sales table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                order_id INTEGER PRIMARY KEY,
                affiliate_name TEXT,
                sales_amount REAL,
                currency TEXT,
                order_date TEXT,
                category TEXT,
                sales_amount_usd REAL,
                month TEXT
            )
            ''')
    
    def _create_postgresql_tables(self):
        """Create tables for PostgreSQL database."""
        with DatabaseTransaction(self.conn) as cursor:
            # Exchange rates table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_rates (
                currency TEXT PRIMARY KEY,
                rate REAL NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            ''')
            
            # Sales table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                order_id INTEGER PRIMARY KEY,
                affiliate_name TEXT,
                sales_amount REAL,
                currency TEXT,
                order_date DATE,
                category TEXT,
                sales_amount_usd REAL,
                month TEXT
            )
            ''')
            
            # Aggregated data tables for reports
            # Affiliate sales aggregation table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_affiliate_sales (
                affiliate_name TEXT PRIMARY KEY,
                total_sales_usd REAL NOT NULL,
                last_updated TIMESTAMP NOT NULL
            )
            ''')
            
            # Category sales aggregation table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_category_sales (
                category TEXT PRIMARY KEY,
                total_sales_usd REAL NOT NULL,
                last_updated TIMESTAMP NOT NULL
            )
            ''')
            
            # Monthly sales aggregation table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_monthly_sales (
                month TEXT PRIMARY KEY,
                total_sales_usd REAL NOT NULL,
                last_updated TIMESTAMP NOT NULL
            )
            ''')
    
    def load_data(self, transformed_df, exchange_rates):
        """
        Load transformed data into the database.
        
        Args:
            transformed_df (pandas.DataFrame): DataFrame containing the transformed data
            exchange_rates (dict): Dictionary of currency codes to exchange rates
        
        Raises:
            Exception: If an error occurs during data loading
        """
        try:
            logger.info("Loading data into database")
            
            # Insert exchange rates
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            if self.db_type == 'sqlite':
                self._load_data_sqlite(transformed_df, exchange_rates, current_time)
            elif self.db_type == 'postgresql':
                self._load_data_postgresql(transformed_df, exchange_rates, current_time)
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            logger.info(f"Successfully loaded {len(transformed_df)} records into database")
        except Exception as e:
            logger.error(f"Error loading data into database: {str(e)}")
            raise
    
    def _load_data_sqlite(self, transformed_df, exchange_rates, current_time):
        """
        Load data into SQLite database.
        
        Args:
            transformed_df (pandas.DataFrame): DataFrame containing the transformed data
            exchange_rates (dict): Dictionary of currency codes to exchange rates
            current_time (str): Current timestamp as string
        """
        with DatabaseTransaction(self.conn) as cursor:
            # Insert exchange rates
            for currency, rate in exchange_rates.items():
                cursor.execute(
                    "INSERT OR REPLACE INTO exchange_rates (currency, rate, updated_at) VALUES (?, ?, ?)",
                    (currency, rate, current_time)
                )
            
            # Insert sales data
            for _, row in transformed_df.iterrows():
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO sales 
                    (order_id, affiliate_name, sales_amount, currency, order_date, category, sales_amount_usd, month)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row['order_id'],
                        row['affiliate_name'],
                        row['sales_amount'],
                        row['currency'],
                        row['order_date'],
                        row['category'],
                        row['sales_amount_usd'],
                        row['month']
                    )
                )
    
    def _load_data_postgresql(self, transformed_df, exchange_rates, current_time):
        """
        Load data into PostgreSQL database.
        
        Args:
            transformed_df (pandas.DataFrame): DataFrame containing the transformed data
            exchange_rates (dict): Dictionary of currency codes to exchange rates
            current_time (str): Current timestamp as string
        """
        with DatabaseTransaction(self.conn) as cursor:
            # Insert exchange rates
            for currency, rate in exchange_rates.items():
                cursor.execute(
                    """
                    INSERT INTO exchange_rates (currency, rate, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (currency) DO UPDATE
                    SET rate = EXCLUDED.rate, updated_at = EXCLUDED.updated_at
                    """,
                    (currency, rate, current_time)
                )
            
            # Insert sales data
            for _, row in transformed_df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO sales 
                    (order_id, affiliate_name, sales_amount, currency, order_date, category, sales_amount_usd, month)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET affiliate_name = EXCLUDED.affiliate_name,
                        sales_amount = EXCLUDED.sales_amount,
                        currency = EXCLUDED.currency,
                        order_date = EXCLUDED.order_date,
                        category = EXCLUDED.category,
                        sales_amount_usd = EXCLUDED.sales_amount_usd,
                        month = EXCLUDED.month
                    """,
                    (
                        row['order_id'],
                        row['affiliate_name'],
                        row['sales_amount'],
                        row['currency'],
                        row['order_date'],
                        row['category'],
                        row['sales_amount_usd'],
                        row['month']
                    )
                )
