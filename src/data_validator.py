"""
Data Validator for Market Data Pipeline

This module provides functionality to validate market data quality,
check for duplicates, gaps, and data integrity issues.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import numpy as np


class ValidationReport:
    """Container for validation results."""
    
    def __init__(self):
        self.issues: List[Dict[str, Any]] = []
        self.warnings: List[str] = []
        self.stats: Dict[str, Any] = {}
        self.is_valid: bool = True
    
    def add_issue(self, category: str, severity: str, message: str, details: Any = None):
        """Add a validation issue."""
        self.issues.append({
            'category': category,
            'severity': severity,
            'message': message,
            'details': details
        })
        if severity in ['error', 'critical']:
            self.is_valid = False
    
    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)
    
    def add_stat(self, key: str, value: Any):
        """Add a statistic."""
        self.stats[key] = value
    
    def __str__(self) -> str:
        """String representation of the validation report."""
        lines = ["\n=== VALIDATION REPORT ==="]
        lines.append(f"Status: {'PASSED' if self.is_valid else 'FAILED'}")
        lines.append(f"Issues Found: {len(self.issues)}")
        lines.append(f"Warnings: {len(self.warnings)}")
        
        if self.stats:
            lines.append("\n--- Statistics ---")
            for key, value in self.stats.items():
                lines.append(f"  {key}: {value}")
        
        if self.warnings:
            lines.append("\n--- Warnings ---")
            for warning in self.warnings:
                lines.append(f"  ⚠ {warning}")
        
        if self.issues:
            lines.append("\n--- Issues ---")
            for issue in self.issues:
                severity_symbol = {
                    'info': 'ℹ',
                    'warning': '⚠',
                    'error': '✗',
                    'critical': '✗✗'
                }.get(issue['severity'], '•')
                lines.append(
                    f"  {severity_symbol} [{issue['category']}] "
                    f"{issue['severity'].upper()}: {issue['message']}"
                )
                if issue['details'] is not None:
                    lines.append(f"      Details: {issue['details']}")
        
        return '\n'.join(lines)


class DataValidator:
    """
    Validates market data quality and integrity.
    
    Checks for:
    - Duplicate timestamps
    - Missing data gaps
    - Invalid prices and volumes
    - Timezone consistency
    """
    
    # Market hours for NSE (Indian Stock Exchange)
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 15
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 30
    
    def __init__(self, timezone: str = 'Asia/Kolkata'):
        """
        Initialize the DataValidator.
        
        Args:
            timezone: Expected timezone for the data
        """
        self.logger = logging.getLogger(__name__)
        self.timezone = timezone
        self.logger.info(f"DataValidator initialized with timezone: {timezone}")
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame, 
        interval: str,
        auto_fix: bool = False
    ) -> Tuple[pd.DataFrame, ValidationReport]:
        """
        Perform comprehensive validation on a DataFrame.
        
        Args:
            df: DataFrame to validate
            interval: Time interval of the data (e.g., '1m', '5m', '1d')
            auto_fix: Whether to automatically fix simple issues
            
        Returns:
            Tuple of (potentially fixed DataFrame, ValidationReport)
        """
        self.logger.info(f"Validating DataFrame with {len(df)} rows (interval={interval})")
        
        report = ValidationReport()
        validated_df = df.copy()
        
        # Basic checks
        if df is None or df.empty:
            report.add_issue('data', 'critical', 'DataFrame is empty or None')
            return validated_df, report
        
        # Add statistics
        report.add_stat('total_rows', len(df))
        report.add_stat('interval', interval)
        report.add_stat('date_range', f"{df.index.min()} to {df.index.max()}")
        
        # Check for required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            report.add_issue(
                'structure', 'critical', 
                f'Missing required columns: {missing_cols}'
            )
            return validated_df, report
        
        # Check duplicates
        duplicates = self.check_duplicates(validated_df)
        if duplicates > 0:
            report.add_issue(
                'duplicates', 'error',
                f'Found {duplicates} duplicate timestamps',
                details=f'{duplicates} rows'
            )
            if auto_fix:
                validated_df = self.fix_duplicates(validated_df)
                report.add_warning(f'Auto-fixed: Removed {duplicates} duplicates')
        
        # Check data quality
        quality_issues = self.check_data_quality(validated_df)
        for issue in quality_issues:
            report.add_issue('data_quality', issue['severity'], issue['message'], issue.get('details'))
        
        # Check for gaps
        gap_issues = self.check_gaps(validated_df, interval)
        for gap in gap_issues:
            report.add_issue('gaps', 'warning', gap['message'], gap.get('details'))
        
        # Check timezone
        tz_issue = self.check_timezone(validated_df)
        if tz_issue:
            report.add_warning(tz_issue)
        
        # Add final statistics
        report.add_stat('validated_rows', len(validated_df))
        if auto_fix:
            report.add_stat('rows_removed', len(df) - len(validated_df))
        
        self.logger.info(f"Validation complete: {'PASSED' if report.is_valid else 'FAILED'}")
        
        return validated_df, report
    
    def check_duplicates(self, df: pd.DataFrame) -> int:
        """
        Check for duplicate timestamps.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Number of duplicate rows found
        """
        if df.index.duplicated().any():
            duplicate_count = df.index.duplicated().sum()
            self.logger.warning(f"Found {duplicate_count} duplicate timestamps")
            return duplicate_count
        return 0
    
    def fix_duplicates(self, df: pd.DataFrame, method: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate timestamps.
        
        Args:
            df: DataFrame with duplicates
            method: How to handle duplicates ('first', 'last', 'mean')
                   - 'first': Keep first occurrence
                   - 'last': Keep last occurrence
                   - 'mean': Average duplicate values
            
        Returns:
            DataFrame with duplicates removed
        """
        if not df.index.duplicated().any():
            return df
        
        initial_count = len(df)
        
        if method == 'mean':
            # Group by index and take mean of numeric columns
            df_fixed = df.groupby(df.index).mean()
        elif method == 'last':
            df_fixed = df[~df.index.duplicated(keep='last')]
        else:  # 'first' is default
            df_fixed = df[~df.index.duplicated(keep='first')]
        
        removed = initial_count - len(df_fixed)
        self.logger.info(f"Removed {removed} duplicate rows using method '{method}'")
        
        return df_fixed
    
    def check_gaps(self, df: pd.DataFrame, interval: str) -> List[Dict[str, Any]]:
        """
        Check for missing data gaps based on the interval.
        
        Args:
            df: DataFrame to check
            interval: Time interval ('1m', '5m', '1d', etc.)
            
        Returns:
            List of gap information dictionaries
        """
        if len(df) < 2:
            return []
        
        gaps = []
        
        # Convert interval to timedelta
        interval_td = self._parse_interval(interval)
        if interval_td is None:
            self.logger.warning(f"Could not parse interval: {interval}")
            return gaps
        
        # Check consecutive timestamps
        time_diffs = df.index.to_series().diff()
        
        # For intraday data, consider market hours
        if interval in ['1m', '2m', '5m', '15m', '30m', '60m', '1h']:
            # Allow for overnight gaps and weekends
            # Flag only gaps during market hours
            tolerance = interval_td * 2  # Allow some flexibility
            
            for i in range(1, len(df)):
                diff = time_diffs.iloc[i]
                
                # Skip if weekend or different days
                prev_date = df.index[i-1].date()
                curr_date = df.index[i].date()
                
                if prev_date != curr_date:
                    continue  # Different days, gap is expected
                
                if diff > tolerance:
                    gaps.append({
                        'message': f'Gap detected: {diff} at {df.index[i]}',
                        'details': f'Expected ~{interval_td}, got {diff}',
                        'index': i
                    })
        else:
            # For daily or longer intervals
            if interval == '1d':
                # Allow for weekends (up to 3 days)
                max_gap = timedelta(days=4)
            else:
                max_gap = interval_td * 2
            
            large_gaps = time_diffs[time_diffs > max_gap]
            for idx in large_gaps.index:
                gaps.append({
                    'message': f'Large gap detected at {idx}',
                    'details': f'Gap size: {time_diffs[idx]}',
                    'index': df.index.get_loc(idx)
                })
        
        if gaps:
            self.logger.info(f"Found {len(gaps)} data gaps")
        
        return gaps
    
    def check_data_quality(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Check data quality issues (invalid prices, volumes, etc.).
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality issue dictionaries
        """
        issues = []
        
        # Check for negative prices
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append({
                        'severity': 'error',
                        'message': f'Negative {col} prices found',
                        'details': f'{negative_count} rows'
                    })
        
        # Check for zero prices (unusual)
        for col in price_cols:
            if col in df.columns:
                zero_count = (df[col] == 0).sum()
                if zero_count > 0:
                    issues.append({
                        'severity': 'warning',
                        'message': f'Zero {col} prices found',
                        'details': f'{zero_count} rows'
                    })
        
        # Check for negative volume
        if 'Volume' in df.columns:
            negative_vol = (df['Volume'] < 0).sum()
            if negative_vol > 0:
                issues.append({
                    'severity': 'error',
                    'message': 'Negative volume found',
                    'details': f'{negative_vol} rows'
                })
        
        # Check High >= Low
        if 'High' in df.columns and 'Low' in df.columns:
            invalid_hl = (df['High'] < df['Low']).sum()
            if invalid_hl > 0:
                issues.append({
                    'severity': 'error',
                    'message': 'High < Low detected',
                    'details': f'{invalid_hl} rows'
                })
        
        # Check if Open, Close are within High-Low range
        if all(col in df.columns for col in ['Open', 'High', 'Low', 'Close']):
            invalid_open = ((df['Open'] > df['High']) | (df['Open'] < df['Low'])).sum()
            if invalid_open > 0:
                issues.append({
                    'severity': 'warning',
                    'message': 'Open price outside High-Low range',
                    'details': f'{invalid_open} rows'
                })
            
            invalid_close = ((df['Close'] > df['High']) | (df['Close'] < df['Low'])).sum()
            if invalid_close > 0:
                issues.append({
                    'severity': 'warning',
                    'message': 'Close price outside High-Low range',
                    'details': f'{invalid_close} rows'
                })
        
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            for col, count in null_counts[null_counts > 0].items():
                issues.append({
                    'severity': 'warning',
                    'message': f'Null values in {col}',
                    'details': f'{count} rows'
                })
        
        # Check for suspiciously low volume on trading days
        if 'Volume' in df.columns:
            zero_volume = (df['Volume'] == 0).sum()
            if zero_volume > 0:
                issues.append({
                    'severity': 'info',
                    'message': 'Zero volume detected (may be non-trading days)',
                    'details': f'{zero_volume} rows'
                })
        
        if issues:
            self.logger.info(f"Found {len(issues)} data quality issues")
        
        return issues
    
    def check_timezone(self, df: pd.DataFrame) -> Optional[str]:
        """
        Check if DataFrame has timezone information.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Warning message if timezone issue detected, None otherwise
        """
        if not isinstance(df.index, pd.DatetimeIndex):
            return "Index is not a DatetimeIndex"
        
        if df.index.tz is None:
            return f"Data is timezone-naive, expected {self.timezone}"
        
        if str(df.index.tz) != self.timezone:
            return f"Timezone mismatch: got {df.index.tz}, expected {self.timezone}"
        
        return None
    
    def _parse_interval(self, interval: str) -> Optional[timedelta]:
        """
        Parse interval string to timedelta.
        
        Args:
            interval: Interval string (e.g., '1m', '5m', '1d')
            
        Returns:
            timedelta object or None if parsing fails
        """
        try:
            if interval.endswith('m'):
                minutes = int(interval[:-1])
                return timedelta(minutes=minutes)
            elif interval.endswith('h'):
                hours = int(interval[:-1])
                return timedelta(hours=hours)
            elif interval.endswith('d'):
                days = int(interval[:-1])
                return timedelta(days=days)
            elif interval.endswith('wk'):
                weeks = int(interval[:-2])
                return timedelta(weeks=weeks)
            elif interval.endswith('mo'):
                months = int(interval[:-2])
                return timedelta(days=months * 30)  # Approximate
            else:
                return None
        except (ValueError, IndexError):
            return None
    
    def get_data_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistical summary of the data.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with statistics
        """
        stats = {
            'total_rows': len(df),
            'date_range': {
                'start': str(df.index.min()),
                'end': str(df.index.max())
            },
            'columns': list(df.columns)
        }
        
        if 'Volume' in df.columns:
            stats['volume'] = {
                'mean': float(df['Volume'].mean()),
                'median': float(df['Volume'].median()),
                'total': float(df['Volume'].sum())
            }
        
        if 'Close' in df.columns:
            stats['price'] = {
                'min': float(df['Close'].min()),
                'max': float(df['Close'].max()),
                'mean': float(df['Close'].mean())
            }
        
        return stats


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create sample data with issues
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    data = {
        'Open': [100, 101, 102, 103, -5, 105, 106, 107, 108, 109],  # Negative price
        'High': [102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        'Low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
        'Close': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        'Volume': [1000, 1100, 1200, 1300, 1400, 1500, 0, 1700, 1800, 1900]  # Zero volume
    }
    
    df = pd.DataFrame(data, index=dates)
    
    # Add a duplicate
    df = pd.concat([df, df.iloc[[0]]])
    
    print("Sample DataFrame:")
    print(df)
    
    # Validate
    validator = DataValidator()
    validated_df, report = validator.validate_dataframe(df, interval='1d', auto_fix=True)
    
    print(report)
    print(f"\nOriginal rows: {len(df)}")
    print(f"Validated rows: {len(validated_df)}")
