"""
Retry Manager for Market Data Pipeline

This module implements retry logic with exponential backoff,
failure tracking, and reporting for failed downloads.
"""

import logging
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable, Any, Optional, List, Dict
from collections import defaultdict
from functools import wraps


class RetryError(Exception):
    """Custom exception for retry-related errors."""
    pass


class RetryManager:
    """
    Manages retry logic with exponential backoff and failure tracking.
    
    Features:
    - Exponential backoff for retries
    - Failed download tracking
    - Failure report generation
    - Alert notifications
    """
    
    def __init__(
        self,
        log_dir: str = "./logs",
        alert_callback: Optional[Callable[[str], None]] = None
    ):
        """
        Initialize the RetryManager.
        
        Args:
            log_dir: Directory for failure logs
            alert_callback: Optional function to call for alerts (e.g., email, SMS)
        """
        self.logger = logging.getLogger(__name__)
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.alert_callback = alert_callback
        self.failures: List[Dict[str, Any]] = []
        
        # Failure log file
        self.failure_log_path = self.log_dir / "download_failures.json"
        
        # Load existing failures if available
        self._load_failures()
        
        self.logger.info(f"RetryManager initialized (log_dir={self.log_dir})")
    
    def retry_with_backoff(
        self,
        func: Callable,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute a function with exponential backoff retry logic.
        
        Args:
            func: Function to execute
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            backoff_factor: Multiplier for exponential backoff
            max_delay: Maximum delay between retries
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result from successful function execution
            
        Raises:
            RetryError: If all retries are exhausted
        """
        last_exception = None
        delay = initial_delay
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"Attempt {attempt}/{max_retries} for {func.__name__}")
                result = func(*args, **kwargs)
                
                if attempt > 1:
                    self.logger.info(f"{func.__name__} succeeded on attempt {attempt}")
                
                return result
                
            except Exception as e:
                last_exception = e
                self.logger.warning(
                    f"Attempt {attempt}/{max_retries} failed for {func.__name__}: {str(e)}"
                )
                
                if attempt < max_retries:
                    # Calculate delay with exponential backoff
                    current_delay = min(delay, max_delay)
                    self.logger.info(f"Retrying in {current_delay:.1f} seconds...")
                    time.sleep(current_delay)
                    delay *= backoff_factor
                else:
                    # All retries exhausted
                    error_msg = f"{func.__name__} failed after {max_retries} attempts"
                    self.logger.error(error_msg)
                    raise RetryError(error_msg) from last_exception
        
        # Should never reach here, but just in case
        raise RetryError(f"Unexpected retry failure for {func.__name__}") from last_exception
    
    def log_failure(
        self,
        symbol: str,
        interval: str,
        error: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log a failed download attempt.
        
        Args:
            symbol: Stock/Index symbol that failed
            interval: Time interval
            error: Error message/description
            metadata: Optional additional metadata
        """
        failure_entry = {
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'interval': interval,
            'error': str(error),
            'metadata': metadata or {}
        }
        
        self.failures.append(failure_entry)
        self.logger.warning(f"Logged failure for {symbol} ({interval}): {error}")
        
        # Save to file
        self._save_failures()
        
        # Send alert if callback is configured
        if self.alert_callback:
            try:
                alert_message = f"Download failed: {symbol} ({interval}) - {error}"
                self.alert_callback(alert_message)
            except Exception as e:
                self.logger.error(f"Failed to send alert: {str(e)}")
    
    def get_failed_downloads(
        self,
        since: Optional[datetime] = None,
        symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of failed downloads with optional filtering.
        
        Args:
            since: Optional datetime to filter failures after this time
            symbol: Optional symbol to filter by
            
        Returns:
            List of failure dictionaries
        """
        filtered_failures = self.failures
        
        # Filter by timestamp
        if since:
            filtered_failures = [
                f for f in filtered_failures
                if datetime.fromisoformat(f['timestamp']) >= since
            ]
        
        # Filter by symbol
        if symbol:
            filtered_failures = [
                f for f in filtered_failures
                if f['symbol'] == symbol
            ]
        
        self.logger.info(f"Retrieved {len(filtered_failures)} failed downloads")
        return filtered_failures
    
    def generate_failure_report(
        self,
        output_path: Optional[str] = None,
        format: str = 'text'
    ) -> str:
        """
        Generate a report of all failures.
        
        Args:
            output_path: Optional path to save report to file
            format: Report format ('text' or 'json')
            
        Returns:
            Report string
        """
        if format == 'json':
            report = json.dumps(self.failures, indent=2, ensure_ascii=False)
        else:
            # Text format
            lines = [
                "=" * 80,
                f"DOWNLOAD FAILURE REPORT",
                f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"Total Failures: {len(self.failures)}",
                "=" * 80,
                ""
            ]
            
            if not self.failures:
                lines.append("No failures recorded.")
            else:
                # Group by symbol
                failures_by_symbol = {}
                for failure in self.failures:
                    symbol = failure['symbol']
                    if symbol not in failures_by_symbol:
                        failures_by_symbol[symbol] = []
                    failures_by_symbol[symbol].append(failure)
                
                for symbol, symbol_failures in failures_by_symbol.items():
                    lines.append(f"\n{symbol} - {len(symbol_failures)} failures:")
                    lines.append("-" * 80)
                    
                    for failure in symbol_failures:
                        lines.append(f"  Time: {failure['timestamp']}")
                        lines.append(f"  Interval: {failure['interval']}")
                        lines.append(f"  Error: {failure['error']}")
                        if failure.get('metadata'):
                            lines.append(f"  Metadata: {failure['metadata']}")
                        lines.append("")
            
            lines.append("=" * 80)
            report = '\n'.join(lines)
        
        # Save to file if path provided
        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report)
                self.logger.info(f"Saved failure report to {output_path}")
            except Exception as e:
                self.logger.error(f"Failed to save report: {str(e)}")
        
        return report
    
    def _save_failures(self):
        """Save failures to JSON file."""
        try:
            with open(self.failure_log_path, 'w', encoding='utf-8') as f:
                json.dump(self.failures, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"Saved failures to {self.failure_log_path}")
        except Exception as e:
            self.logger.error(f"Failed to save failures: {str(e)}")
    
    def _load_failures(self):
        """Load failures from JSON file."""
        if not self.failure_log_path.exists():
            self.logger.debug("No existing failure log found")
            return
        
        try:
            with open(self.failure_log_path, 'r', encoding='utf-8') as f:
                self.failures = json.load(f)
            self.logger.info(f"Loaded {len(self.failures)} previous failures")
        except Exception as e:
            self.logger.error(f"Failed to load failures: {str(e)}")
            self.failures = []
    
    def clear_failures(self, older_than_days: Optional[int] = None):
        """
        Clear failure records.
        
        Args:
            older_than_days: Optional - only clear failures older than N days
        """
        if older_than_days is None:
            # Clear all
            self.failures = []
            self.logger.info("Cleared all failure records")
        else:
            # Clear old failures
            cutoff_date = datetime.now() - timedelta(days=older_than_days)
            original_count = len(self.failures)
            
            self.failures = [
                f for f in self.failures
                if datetime.fromisoformat(f['timestamp']) >= cutoff_date
            ]
            
            cleared_count = original_count - len(self.failures)
            self.logger.info(f"Cleared {cleared_count} failures older than {older_than_days} days")
        
        self._save_failures()
    
    def get_failure_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about failures.
        
        Returns:
            Dictionary with failure statistics
        """
        if not self.failures:
            return {
                'total_failures': 0,
                'unique_symbols': 0,
                'most_failed_symbol': None,
                'recent_failures_24h': 0
            }
        
        # Count by symbol
        symbol_counts = {}
        for failure in self.failures:
            symbol = failure['symbol']
            symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        
        # Most failed symbol
        most_failed = max(symbol_counts.items(), key=lambda x: x[1]) if symbol_counts else (None, 0)
        
        # Recent failures (last 24 hours)
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_failures = sum(
            1 for f in self.failures
            if datetime.fromisoformat(f['timestamp']) >= recent_cutoff
        )
        
        stats = {
            'total_failures': len(self.failures),
            'unique_symbols': len(symbol_counts),
            'most_failed_symbol': most_failed[0],
            'most_failed_count': most_failed[1],
            'recent_failures_24h': recent_failures,
            'failures_by_symbol': symbol_counts
        }
        
        return stats
    
    def send_alert(self, message: str):
        """
        Send an alert notification.
        
        Args:
            message: Alert message to send
        """
        if self.alert_callback:
            try:
                self.alert_callback(message)
                self.logger.info(f"Alert sent: {message}")
            except Exception as e:
                self.logger.error(f"Failed to send alert: {str(e)}")
        else:
            self.logger.warning(f"No alert callback configured. Alert: {message}")


def with_retry(max_retries: int = 3, initial_delay: float = 1.0):
    """
    Decorator for adding retry logic to functions.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        
    Example:
        @with_retry(max_retries=5, initial_delay=2.0)
        def download_data(symbol):
            # Download logic here
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_manager = RetryManager()
            return retry_manager.retry_with_backoff(
                func,
                max_retries=max_retries,
                initial_delay=initial_delay,
                *args,
                **kwargs
            )
        return wrapper
    return decorator


# Example usage
if __name__ == "__main__":
    from datetime import timedelta
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Custom alert callback
    def send_email_alert(message: str):
        print(f"[EMAIL ALERT] {message}")
    
    # Create retry manager
    rm = RetryManager(log_dir="./logs", alert_callback=send_email_alert)
    
    # Example 1: Retry a function
    print("\n=== Example 1: Retry with backoff ===")
    
    attempt_count = 0
    def unreliable_function():
        global attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise Exception(f"Simulated failure #{attempt_count}")
        return "Success!"
    
    try:
        result = rm.retry_with_backoff(unreliable_function, max_retries=5)
        print(f"Result: {result}")
    except RetryError as e:
        print(f"Failed: {e}")
    
    # Example 2: Log failures
    print("\n=== Example 2: Log failures ===")
    rm.log_failure("RELIANCE.NS", "1d", "Connection timeout")
    rm.log_failure("TCS.NS", "5m", "Invalid data")
    rm.log_failure("RELIANCE.NS", "1d", "Rate limit exceeded")
    
    # Example 3: Get failed downloads
    print("\n=== Example 3: Failed downloads ===")
    failed = rm.get_failed_downloads(symbol="RELIANCE.NS")
    print(f"Failures for RELIANCE.NS: {len(failed)}")
    
    # Example 4: Generate report
    print("\n=== Example 4: Failure report ===")
    report = rm.generate_failure_report(output_path="./logs/failure_report.txt")
    print(report)
    
    # Example 5: Statistics
    print("\n=== Example 5: Statistics ===")
    stats = rm.get_failure_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
