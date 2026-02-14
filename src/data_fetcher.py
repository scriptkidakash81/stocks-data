"""
Data Fetcher for Market Data Pipeline

This module handles downloading market data from Yahoo Finance
with proper API limit handling and rate limiting.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pandas as pd
import yfinance as yf
from dateutil import parser


class DataFetchError(Exception):
    """Custom exception for data fetching errors."""
    pass


class DataFetcher:
    """
    Fetches market data from Yahoo Finance with rate limiting and error handling.
    
    Handles yfinance API limits:
    - 1m: 7 days max
    - 2m, 5m, 15m: 60 days max
    - 60m: 730 days max
    - 1d: unlimited (use "max")
    """
    
    # API limits for different intervals (in days)
    INTERVAL_LIMITS = {
        '1m': 7,
        '2m': 60,
        '5m': 60,
        '15m': 60,
        '30m': 60,
        '60m': 730,
        '90m': 60,
        '1h': 730,
        '1d': None,  # Unlimited
        '5d': None,
        '1wk': None,
        '1mo': None,
        '3mo': None
    }
    
    def __init__(self, rate_limit_delay: float = 0.5, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the DataFetcher.
        
        Args:
            rate_limit_delay: Delay between API calls in seconds (default: 0.5)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Delay between retries in seconds (default: 5)
        """
        self.logger = logging.getLogger(__name__)
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.last_request_time = 0
        
        self.logger.info(
            f"DataFetcher initialized (rate_limit={rate_limit_delay}s, "
            f"max_retries={max_retries}, retry_delay={retry_delay}s)"
        )
    
    def _apply_rate_limit(self):
        """Apply rate limiting between API requests."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - elapsed
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_max_period(self, interval: str) -> Optional[int]:
        """
        Get maximum period in days for a given interval.
        
        Args:
            interval: Time interval (e.g., '1m', '5m', '1d')
            
        Returns:
            Maximum number of days allowed for the interval, or None if unlimited.
        """
        return self.INTERVAL_LIMITS.get(interval)
    
    def _validate_period(self, interval: str, period: str) -> bool:
        """
        Validate if the period is within API limits for the interval.
        
        Args:
            interval: Time interval (e.g., '1m', '5m', '1d')
            period: Period string (e.g., '7d', '60d', 'max')
            
        Returns:
            True if valid, False otherwise.
        """
        max_days = self.get_max_period(interval)
        
        # If no limit (None), any period is valid
        if max_days is None:
            return True
        
        # Special case for "max"
        if period == "max":
            return max_days is None
        
        # Parse period string (e.g., '7d', '2mo', '1y')
        try:
            if period.endswith('d'):
                requested_days = int(period[:-1])
            elif period.endswith('mo'):
                requested_days = int(period[:-2]) * 30
            elif period.endswith('y'):
                requested_days = int(period[:-1]) * 365
            else:
                self.logger.warning(f"Unknown period format: {period}")
                return False
            
            return requested_days <= max_days
            
        except (ValueError, IndexError):
            self.logger.error(f"Invalid period format: {period}")
            return False
    
    def _adjust_period(self, interval: str, period: str) -> str:
        """
        Adjust period to fit within API limits for the interval.
        
        Args:
            interval: Time interval (e.g., '1m', '5m', '1d')
            period: Requested period string
            
        Returns:
            Adjusted period string that fits within limits.
        """
        max_days = self.get_max_period(interval)
        
        if max_days is None:
            # No limit, return original or 'max'
            return period if period != 'max' else 'max'
        
        # Parse requested period
        if period == 'max':
            return f"{max_days}d"
        
        try:
            if period.endswith('d'):
                requested_days = int(period[:-1])
            elif period.endswith('mo'):
                requested_days = int(period[:-2]) * 30
            elif period.endswith('y'):
                requested_days = int(period[:-1]) * 365
            else:
                requested_days = max_days
            
            if requested_days > max_days:
                self.logger.warning(
                    f"Period {period} exceeds limit for {interval}. "
                    f"Adjusting to {max_days}d"
                )
                return f"{max_days}d"
            
            return period
            
        except (ValueError, IndexError):
            self.logger.error(f"Invalid period format: {period}, using max: {max_days}d")
            return f"{max_days}d"
    
    def fetch_data(
        self, 
        symbol: str, 
        interval: str = '1d', 
        period: str = 'max',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch market data for a given symbol and interval.
        
        Args:
            symbol: Stock/Index symbol (e.g., 'RELIANCE.NS', '^NSEI')
            interval: Time interval ('1m', '2m', '5m', '15m', '60m', '1d', etc.)
            period: Period to fetch ('1d', '5d', '1mo', '3mo', '1y', 'max', etc.)
            start_date: Optional start date (YYYY-MM-DD) - overrides period
            end_date: Optional end date (YYYY-MM-DD)
            
        Returns:
            pandas DataFrame with OHLCV data
            
        Raises:
            DataFetchError: If data cannot be fetched after retries
        """
        self.logger.info(f"Fetching data for {symbol} (interval={interval}, period={period})")
        
        # Adjust period if necessary
        adjusted_period = self._adjust_period(interval, period)
        if adjusted_period != period:
            self.logger.info(f"Period adjusted from {period} to {adjusted_period}")
        
        # Retry logic
        for attempt in range(1, self.max_retries + 1):
            try:
                # Apply rate limiting
                self._apply_rate_limit()
                
                # Create ticker object
                ticker = yf.Ticker(symbol)
                
                # Fetch data
                if start_date and end_date:
                    self.logger.debug(f"Fetching with date range: {start_date} to {end_date}")
                    df = ticker.history(
                        interval=interval,
                        start=start_date,
                        end=end_date
                    )
                elif start_date:
                    self.logger.debug(f"Fetching from {start_date}")
                    df = ticker.history(
                        interval=interval,
                        start=start_date
                    )
                else:
                    self.logger.debug(f"Fetching with period: {adjusted_period}")
                    df = ticker.history(
                        interval=interval,
                        period=adjusted_period
                    )
                
                # Check if data was returned
                if df is None or df.empty:
                    raise DataFetchError(f"No data returned for {symbol}")
                
                self.logger.info(
                    f"Successfully fetched {len(df)} rows for {symbol} "
                    f"(attempt {attempt}/{self.max_retries})"
                )
                
                # Add symbol column
                df['Symbol'] = symbol
                
                return df
                
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed for {symbol}: {str(e)}"
                )
                
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    error_msg = f"Failed to fetch data for {symbol} after {self.max_retries} attempts"
                    self.logger.error(error_msg)
                    raise DataFetchError(error_msg) from e
        
        # Should never reach here, but just in case
        raise DataFetchError(f"Failed to fetch data for {symbol}")
    
    def fetch_incremental(
        self, 
        symbol: str, 
        interval: str, 
        last_date: str
    ) -> pd.DataFrame:
        """
        Fetch incremental data since the last known date.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            last_date: Last date in existing data (YYYY-MM-DD or datetime string)
            
        Returns:
            pandas DataFrame with new data since last_date
            
        Raises:
            DataFetchError: If data cannot be fetched
        """
        self.logger.info(f"Fetching incremental data for {symbol} since {last_date}")
        
        try:
            # Parse last_date
            if isinstance(last_date, str):
                last_dt = parser.parse(last_date)
            else:
                last_dt = last_date
            
            # Start from next day/period after last_date
            start_date = (last_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            self.logger.debug(f"Incremental fetch: {start_date} to {end_date}")
            
            # Fetch data with date range
            df = self.fetch_data(
                symbol=symbol,
                interval=interval,
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                self.logger.info(f"Fetched {len(df)} new rows for {symbol}")
            else:
                self.logger.info(f"No new data available for {symbol}")
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to fetch incremental data for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataFetchError(error_msg) from e
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate fetched data for completeness and correctness.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        if df is None or df.empty:
            self.logger.warning("Validation failed: DataFrame is empty")
            return False
        
        # Check for required columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            self.logger.warning(f"Validation failed: Missing columns {missing_columns}")
            return False
        
        # Check for null values in critical columns
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            self.logger.warning(f"Validation warning: Null values found:\n{null_counts[null_counts > 0]}")
        
        # Check for negative prices or volumes
        for col in ['Open', 'High', 'Low', 'Close']:
            if (df[col] < 0).any():
                self.logger.warning(f"Validation failed: Negative values in {col}")
                return False
        
        if (df['Volume'] < 0).any():
            self.logger.warning("Validation failed: Negative volume values")
            return False
        
        # Check High >= Low
        if (df['High'] < df['Low']).any():
            self.logger.warning("Validation failed: High < Low in some rows")
            return False
        
        self.logger.debug(f"Data validation passed for {len(df)} rows")
        return True
    
    def get_ticker_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get ticker information and metadata.
        
        Args:
            symbol: Stock/Index symbol
            
        Returns:
            Dictionary with ticker information
        """
        try:
            self._apply_rate_limit()
            ticker = yf.Ticker(symbol)
            info = ticker.info
            self.logger.debug(f"Retrieved info for {symbol}")
            return info
        except Exception as e:
            self.logger.error(f"Failed to get info for {symbol}: {str(e)}")
            return {}


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create data fetcher
    fetcher = DataFetcher(rate_limit_delay=1.0)
    
    # Example 1: Fetch daily data
    print("\n=== Example 1: Fetch daily data for RELIANCE.NS ===")
    try:
        df = fetcher.fetch_data("RELIANCE.NS", interval='1d', period='1mo')
        print(f"Fetched {len(df)} rows")
        print(df.head())
        print(f"\nValidation: {fetcher.validate_data(df)}")
    except DataFetchError as e:
        print(f"Error: {e}")
    
    # Example 2: Fetch 5-minute data (limited to 60 days)
    print("\n=== Example 2: Fetch 5m data for ^NSEI ===")
    try:
        df = fetcher.fetch_data("^NSEI", interval='5m', period='5d')
        print(f"Fetched {len(df)} rows")
        print(df.tail())
    except DataFetchError as e:
        print(f"Error: {e}")
    
    # Example 3: Check max periods
    print("\n=== Example 3: API Limits ===")
    for interval in ['1m', '5m', '60m', '1d']:
        max_period = fetcher.get_max_period(interval)
        print(f"{interval}: {max_period} days" if max_period else f"{interval}: unlimited")
