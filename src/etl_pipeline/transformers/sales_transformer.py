"""
Sales Transformer Module

This module provides functionality to transform and clean sales data.
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger('etl_pipeline.transformers.sales')


class SalesTransformer:
    """Class for transforming and cleaning sales data."""
    
    def transform(self, df, exchange_rates):
        """
        Transform and clean the sales data.
        
        Args:
            df (pandas.DataFrame): DataFrame containing the raw sales data
            exchange_rates (dict): Dictionary of currency codes to exchange rates
        
        Returns:
            pandas.DataFrame: DataFrame containing the transformed sales data
        """
        try:
            logger.info("Starting data transformation")
            
            # Make a copy to avoid SettingWithCopyWarning
            transformed_df = df.copy()
            
            # Handle missing values
            logger.info("Handling missing values")
            # Fill missing affiliate names with 'Unknown'
            transformed_df['affiliate_name'] = transformed_df['affiliate_name'].fillna('Unknown')
            
            # Fill missing categories with 'Uncategorized'
            transformed_df['category'] = transformed_df['category'].fillna('Uncategorized')
            
            # Fill missing currencies with 'USD' (default)
            transformed_df['currency'] = transformed_df['currency'].fillna('USD')
            
            # Handle missing dates with 'Unknown' for reporting purposes
            logger.info("Standardizing date format")
            transformed_df['order_date'] = pd.to_datetime(transformed_df['order_date'], errors='coerce')
            
            # Create a month column before handling missing dates
            # For missing dates, use 'Unknown' as the month
            transformed_df['month'] = transformed_df['order_date'].apply(
                lambda x: x.strftime('%Y-%m') if pd.notna(x) else 'Unknown'
            )
            
            # Now fill missing dates with current date for database consistency
            current_date = datetime.now()
            transformed_df['order_date'] = transformed_df['order_date'].fillna(current_date)
            transformed_df['order_date'] = transformed_df['order_date'].dt.strftime('%Y-%m-%d')
            
            # Convert sales amount to numeric, replacing non-numeric values with NaN
            transformed_df['sales_amount'] = pd.to_numeric(transformed_df['sales_amount'], errors='coerce')
            
            # Fill missing sales amounts with 0 instead of mean
            transformed_df['sales_amount'] = transformed_df['sales_amount'].fillna(0)
            
            # Convert currencies to USD
            logger.info("Converting currencies to USD")
            
            # Create a new column for USD amounts
            transformed_df['sales_amount_usd'] = transformed_df.apply(
                lambda row: self._convert_to_usd(row['sales_amount'], row['currency'], exchange_rates),
                axis=1
            )
            
            # Remove duplicates
            logger.info("Removing duplicate records")
            transformed_df = transformed_df.drop_duplicates(subset=['order_id'])
            
            # Month column already created above with proper handling for missing dates
            
            logger.info(f"Transformation complete. {len(transformed_df)} records processed.")
            return transformed_df
        
        except Exception as e:
            logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def _convert_to_usd(self, amount, currency, exchange_rates):
        """
        Helper function to convert amount to USD.
        
        Args:
            amount (float): Amount in original currency
            currency (str): Currency code
            exchange_rates (dict): Dictionary of currency codes to exchange rates
        
        Returns:
            float: Amount converted to USD
        """
        if pd.isna(amount) or pd.isna(currency):
            return 0.0
        
        try:
            if currency == 'USD':
                return amount
            
            # Get the exchange rate (USD to currency)
            rate = exchange_rates.get(currency, 1.0)
            
            # Convert from currency to USD
            # If rate is USD to currency, then we divide
            return amount / rate
        except Exception as e:
            logger.error(f"Error converting {amount} {currency} to USD: {str(e)}")
            return 0.0
