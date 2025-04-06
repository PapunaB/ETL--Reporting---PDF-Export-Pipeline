"""
CSV Extractor Module

This module provides functionality to extract data from CSV files.
"""

import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger('etl_pipeline.extractors.csv')


class CsvExtractor:
    """Class for extracting data from CSV files."""
    
    def __init__(self, csv_path):
        """
        Initialize with the path to the CSV file.
        
        Args:
            csv_path (str): Path to the CSV file
        """
        self.csv_path = csv_path
    
    def extract(self):
        """
        Extract data from the CSV file.
        
        Returns:
            pandas.DataFrame: DataFrame containing the CSV data
        
        Raises:
            FileNotFoundError: If the CSV file does not exist
            Exception: For other errors during extraction
        """
        try:
            # Ensure the file exists
            file_path = Path(self.csv_path)
            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found at {self.csv_path}")
            
            logger.info(f"Extracting data from {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            logger.info(f"Successfully extracted {len(df)} records from CSV")
            return df
        
        except FileNotFoundError as e:
            logger.error(f"CSV file not found: {str(e)}")
            raise
        
        except Exception as e:
            logger.error(f"Error extracting CSV data: {str(e)}")
            raise
