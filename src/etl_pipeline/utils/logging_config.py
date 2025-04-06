"""
Logging Configuration Module

This module provides functions to configure logging for the ETL pipeline.
"""

import logging
import sys
from pathlib import Path


def configure_logging(config):
    """
    Configure logging based on the provided configuration.
    
    Args:
        config (dict): Logging configuration dictionary with keys:
            - level: Logging level (INFO, DEBUG, etc.)
            - file: Path to log file
            - format: Log message format
    """
    # Get configuration values with defaults
    log_level = getattr(logging, config.get('level', 'INFO'))
    log_file = config.get('file', 'etl_process.log')
    log_format = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Ensure log directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create logger
    logger = logging.getLogger('etl_pipeline')
    logger.info(f"Logging configured with level {config.get('level', 'INFO')}")
    
    return logger
