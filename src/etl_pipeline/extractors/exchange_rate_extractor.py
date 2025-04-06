"""
Exchange Rate Extractor Module

This module provides functionality to extract exchange rate data from an API.
"""

import logging
import requests
from etl_pipeline.config.settings import load_config

logger = logging.getLogger('etl_pipeline.extractors.exchange_rate')


class ExchangeRateExtractor:
    """Class for extracting exchange rate data from an API."""
    
    def __init__(self, api_url=None, fallback_rates=None):
        """
        Initialize with API URL and fallback rates.
        
        Args:
            api_url (str, optional): URL of the exchange rate API
            fallback_rates (dict, optional): Fallback exchange rates to use if API fails
        """
        config = load_config()
        self.api_url = api_url or config.get('exchange_rate_api_url')
        self.fallback_rates = fallback_rates or config.get('exchange_rate_fallback')
    
    def extract(self):
        """
        Extract exchange rate data from the API.
        
        Returns:
            dict: Dictionary of currency codes to exchange rates
        """
        try:
            logger.info("Fetching exchange rates from API")
            response = requests.get(self.api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                rates = data['rates']
                logger.info(f"Successfully fetched exchange rates for {len(rates)} currencies")
                return rates
            else:
                logger.error(f"API request failed with status code {response.status_code}")
                logger.info("Using fallback exchange rates")
                return self._get_fallback_rates()
        
        except Exception as e:
            logger.error(f"Error fetching exchange rates: {str(e)}")
            logger.info("Using fallback exchange rates")
            return self._get_fallback_rates()
    
    def _get_fallback_rates(self):
        """
        Get fallback exchange rates when API request fails.
        
        Returns:
            dict: Dictionary of currency codes to exchange rates
        """
        return self.fallback_rates
