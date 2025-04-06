#!/usr/bin/env python
"""
ETL Pipeline Main Entry Point

This script serves as the entry point for the ETL pipeline, orchestrating the extraction,
transformation, loading, and reporting processes.
"""

import logging
import sys
from etl_pipeline.config.settings import load_config
from etl_pipeline.utils.logging_config import configure_logging
from etl_pipeline.extractors.csv_extractor import CsvExtractor
from etl_pipeline.extractors.exchange_rate_extractor import ExchangeRateExtractor
from etl_pipeline.transformers.sales_transformer import SalesTransformer
from etl_pipeline.loaders.database_loader import DatabaseLoader
from etl_pipeline.reports.report_generator import ReportGenerator
from etl_pipeline.utils.database import get_database_connection


def main():
    """Main entry point for the ETL pipeline."""
    # Load configuration
    config = load_config()
    
    # Configure logging
    configure_logging(config.get('logging', {}))
    logger = logging.getLogger('etl_pipeline')
    
    try:
        logger.info("Starting ETL process")
        
        # Extract data
        logger.info("Starting extraction phase")
        csv_extractor = CsvExtractor(config.get('csv_path'))
        sales_data = csv_extractor.extract()
        
        exchange_rate_extractor = ExchangeRateExtractor()
        exchange_rates = exchange_rate_extractor.extract()
        
        # Transform data
        logger.info("Starting transformation phase")
        transformer = SalesTransformer()
        transformed_data = transformer.transform(sales_data, exchange_rates)
        
        # Load data
        logger.info("Starting loading phase")
        db_config = config.get('database', {})
        conn = get_database_connection(db_config)
        
        loader = DatabaseLoader(conn, db_config.get('type', 'sqlite'))
        loader.create_tables()
        loader.load_data(transformed_data, exchange_rates)
        
        # Generate reports
        logger.info("Starting report generation phase")
        report_generator = ReportGenerator(
            conn, 
            db_config.get('type', 'sqlite'),
            config.get('reports_dir', 'reports')
        )
        report_generator.generate_reports()
        
        # Close database connection
        conn.close()
        
        logger.info("ETL process completed successfully")
        return 0
    
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
