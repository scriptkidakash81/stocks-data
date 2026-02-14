"""
Data Merger for Market Data Pipeline

This module handles merging new data with existing CSV files,
maintaining data integrity and creating backups.
"""

import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
import pandas as pd


class MergeError(Exception):
    """Custom exception for data merging errors."""
    pass


class DataMerger:
    """
    Manages merging of new market data with existing CSV files.
    
    Features:
    - Load and merge data
    - Automatic deduplication
    - Chronological ordering
    - Backup creation
    - Data validation
    """
    
    def __init__(self, backup_enabled: bool = True, backup_dir: Optional[str] = None):
        """
        Initialize the DataMerger.
        
        Args:
            backup_enabled: Whether to create backups before overwriting
            backup_dir: Directory for backups (default: same dir as original with .bak extension)
        """
        self.logger = logging.getLogger(__name__)
        self.backup_enabled = backup_enabled
        self.backup_dir = Path(backup_dir) if backup_dir else None
        
        if self.backup_dir:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(
            f"DataMerger initialized (backups={'enabled' if backup_enabled else 'disabled'})"
        )
    
    def load_existing_data(self, filepath: str) -> Optional[pd.DataFrame]:
        """
        Load existing data from CSV file.
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            DataFrame if file exists and is valid, None otherwise
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            self.logger.info(f"File does not exist: {filepath}")
            return None
        
        if filepath.stat().st_size == 0:
            self.logger.warning(f"File is empty: {filepath}")
            return None
        
        try:
            df = pd.read_csv(filepath, index_col=0, parse_dates=True)
            self.logger.info(f"Loaded {len(df)} rows from {filepath}")
            
            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                self.logger.warning("Converting index to DatetimeIndex")
                df.index = pd.to_datetime(df.index)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load {filepath}: {str(e)}")
            raise MergeError(f"Error loading file: {str(e)}") from e
    
    def get_last_timestamp(self, filepath: str) -> Optional[datetime]:
        """
        Get the last (most recent) timestamp from an existing CSV file.
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Last timestamp as datetime, or None if file doesn't exist or is empty
        """
        df = self.load_existing_data(filepath)
        
        if df is None or df.empty:
            return None
        
        # Ensure data is sorted
        df_sorted = df.sort_index()
        last_timestamp = df_sorted.index[-1]
        
        self.logger.info(f"Last timestamp in {filepath}: {last_timestamp}")
        
        # Convert to datetime if it's a Timestamp
        if isinstance(last_timestamp, pd.Timestamp):
            last_timestamp = last_timestamp.to_pydatetime()
        
        return last_timestamp
    
    def merge_data(
        self, 
        existing_df: Optional[pd.DataFrame], 
        new_df: pd.DataFrame,
        deduplicate: bool = True,
        sort: bool = True
    ) -> pd.DataFrame:
        """
        Merge new data with existing data.
        
        Args:
            existing_df: Existing DataFrame (can be None)
            new_df: New DataFrame to merge
            deduplicate: Whether to remove duplicate timestamps (default: True)
            sort: Whether to sort by timestamp (default: True)
            
        Returns:
            Merged DataFrame
        """
        if new_df is None or new_df.empty:
            self.logger.warning("New data is empty, returning existing data")
            return existing_df if existing_df is not None else pd.DataFrame()
        
        # If no existing data, return new data
        if existing_df is None or existing_df.empty:
            self.logger.info("No existing data, using new data only")
            merged_df = new_df.copy()
        else:
            # Concatenate dataframes
            self.logger.info(
                f"Merging: existing={len(existing_df)} rows, new={len(new_df)} rows"
            )
            merged_df = pd.concat([existing_df, new_df])
        
        # Remove duplicates if requested
        if deduplicate:
            initial_count = len(merged_df)
            merged_df = self._deduplicate(merged_df)
            removed = initial_count - len(merged_df)
            if removed > 0:
                self.logger.info(f"Removed {removed} duplicate rows")
        
        # Sort by timestamp if requested
        if sort:
            merged_df = merged_df.sort_index()
            self.logger.debug("Data sorted chronologically")
        
        self.logger.info(f"Merge complete: {len(merged_df)} total rows")
        
        return merged_df
    
    def _deduplicate(self, df: pd.DataFrame, keep: str = 'last') -> pd.DataFrame:
        """
        Remove duplicate timestamps from DataFrame.
        
        Args:
            df: DataFrame with potential duplicates
            keep: Which duplicate to keep ('first', 'last', or False to drop all)
            
        Returns:
            DataFrame with duplicates removed
        """
        if df.index.duplicated().any():
            duplicate_count = df.index.duplicated().sum()
            self.logger.debug(f"Found {duplicate_count} duplicates, keeping '{keep}'")
            
            # Remove duplicates, keeping the specified one
            df_dedup = df[~df.index.duplicated(keep=keep)]
            
            return df_dedup
        
        return df
    
    def save_data(
        self, 
        df: pd.DataFrame, 
        filepath: str,
        create_backup: Optional[bool] = None
    ) -> bool:
        """
        Save DataFrame to CSV file with optional backup.
        
        Args:
            df: DataFrame to save
            filepath: Destination file path
            create_backup: Override backup setting (None uses instance setting)
            
        Returns:
            True if successful, False otherwise
        """
        filepath = Path(filepath)
        
        # Determine if we should create backup
        should_backup = create_backup if create_backup is not None else self.backup_enabled
        
        try:
            # Create parent directories if needed
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Create backup if file exists and backup is enabled
            if should_backup and filepath.exists():
                self._create_backup(filepath)
            
            # Save to CSV
            df.to_csv(filepath, index=True)
            self.logger.info(f"Saved {len(df)} rows to {filepath}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save data to {filepath}: {str(e)}")
            raise MergeError(f"Error saving file: {str(e)}") from e
    
    def _create_backup(self, filepath: Path) -> Path:
        """
        Create a backup of the existing file.
        
        Args:
            filepath: Path to file to backup
            
        Returns:
            Path to backup file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if self.backup_dir:
            # Use dedicated backup directory
            backup_path = self.backup_dir / f"{filepath.stem}_{timestamp}{filepath.suffix}.bak"
        else:
            # Place backup next to original file
            backup_path = filepath.with_suffix(f'.{timestamp}.bak')
        
        try:
            shutil.copy2(filepath, backup_path)
            self.logger.info(f"Created backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Failed to create backup: {str(e)}")
            # Don't raise error, just log it - backup failure shouldn't stop the operation
            return None
    
    def merge_and_save(
        self,
        filepath: str,
        new_df: pd.DataFrame,
        validate: bool = True
    ) -> Tuple[bool, int]:
        """
        Convenience method to load, merge, and save data in one operation.
        
        Args:
            filepath: Path to CSV file
            new_df: New data to merge
            validate: Whether to validate merged data
            
        Returns:
            Tuple of (success: bool, total_rows: int)
        """
        try:
            # Load existing data
            existing_df = self.load_existing_data(filepath)
            
            # Merge data
            merged_df = self.merge_data(existing_df, new_df)
            
            # Validate if requested
            if validate:
                if not self._quick_validate(merged_df):
                    raise MergeError("Merged data failed validation")
            
            # Save merged data
            success = self.save_data(merged_df, filepath)
            
            return success, len(merged_df)
            
        except Exception as e:
            self.logger.error(f"merge_and_save failed: {str(e)}")
            return False, 0
    
    def _quick_validate(self, df: pd.DataFrame) -> bool:
        """
        Quick validation of merged data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if data passes basic validation
        """
        if df is None or df.empty:
            self.logger.warning("Validation: DataFrame is empty")
            return False
        
        # Check index is sorted
        if not df.index.is_monotonic_increasing:
            self.logger.warning("Validation: Index is not sorted")
            return False
        
        # Check for duplicates
        if df.index.duplicated().any():
            self.logger.warning("Validation: Duplicate timestamps found")
            return False
        
        # Check for required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            self.logger.warning(f"Validation: Missing columns {missing_cols}")
            return False
        
        self.logger.debug("Quick validation passed")
        return True
    
    def cleanup_old_backups(self, filepath: str, keep_count: int = 5):
        """
        Remove old backup files, keeping only the most recent ones.
        
        Args:
            filepath: Original file path
            keep_count: Number of recent backups to keep
        """
        filepath = Path(filepath)
        
        # Find all backup files
        if self.backup_dir:
            pattern = f"{filepath.stem}_*.bak"
            backup_files = sorted(self.backup_dir.glob(pattern), key=lambda p: p.stat().st_mtime)
        else:
            pattern = f"{filepath.stem}.*.bak"
            backup_files = sorted(filepath.parent.glob(pattern), key=lambda p: p.stat().st_mtime)
        
        # Remove old backups
        if len(backup_files) > keep_count:
            to_remove = backup_files[:-keep_count]
            for backup_file in to_remove:
                try:
                    backup_file.unlink()
                    self.logger.info(f"Removed old backup: {backup_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove {backup_file}: {str(e)}")
    
    def get_data_summary(self, df: pd.DataFrame) -> dict:
        """
        Get summary statistics for merged data.
        
        Args:
            df: DataFrame to summarize
            
        Returns:
            Dictionary with summary statistics
        """
        if df is None or df.empty:
            return {'rows': 0, 'status': 'empty'}
        
        summary = {
            'rows': len(df),
            'start_date': str(df.index.min()),
            'end_date': str(df.index.max()),
            'columns': list(df.columns),
            'has_duplicates': df.index.duplicated().any(),
            'is_sorted': df.index.is_monotonic_increasing
        }
        
        return summary


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create sample data
    dates1 = pd.date_range('2024-01-01', periods=5, freq='D')
    df1 = pd.DataFrame({
        'Open': [100, 101, 102, 103, 104],
        'High': [102, 103, 104, 105, 106],
        'Low': [99, 100, 101, 102, 103],
        'Close': [101, 102, 103, 104, 105],
        'Volume': [1000, 1100, 1200, 1300, 1400]
    }, index=dates1)
    
    dates2 = pd.date_range('2024-01-04', periods=5, freq='D')
    df2 = pd.DataFrame({
        'Open': [103, 104, 105, 106, 107],
        'High': [105, 106, 107, 108, 109],
        'Low': [102, 103, 104, 105, 106],
        'Close': [104, 105, 106, 107, 108],
        'Volume': [1300, 1400, 1500, 1600, 1700]
    }, index=dates2)
    
    print("Existing data (df1):")
    print(df1)
    print("\nNew data (df2):")
    print(df2)
    
    # Create merger
    merger = DataMerger(backup_enabled=True)
    
    # Merge data
    merged = merger.merge_data(df1, df2)
    print("\nMerged data:")
    print(merged)
    
    # Get summary
    summary = merger.get_data_summary(merged)
    print("\nSummary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    # Save to file
    test_file = Path("./data/test_merge.csv")
    merger.save_data(merged, test_file)
    
    # Get last timestamp
    last_ts = merger.get_last_timestamp(test_file)
    print(f"\nLast timestamp: {last_ts}")
