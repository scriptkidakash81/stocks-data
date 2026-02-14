"""
Metadata Manager for Market Data Pipeline

This module manages metadata for downloaded market data,
tracking download history, data quality, and update schedules.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List


class MetadataError(Exception):
    """Custom exception for metadata-related errors."""
    pass


class MetadataManager:
    """
    Manages metadata for market data files.
    
    Tracks download history, data quality, and helps determine
    what data needs updating.
    """
    
    def __init__(self, metadata_dir: Optional[str] = None):
        """
        Initialize the MetadataManager.
        
        Args:
            metadata_dir: Directory to store metadata files (default: ./data/metadata)
        """
        self.logger = logging.getLogger(__name__)
        
        if metadata_dir is None:
            self.metadata_dir = Path("./data/metadata")
        else:
            self.metadata_dir = Path(metadata_dir)
        
        # Create metadata directory if it doesn't exist
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"MetadataManager initialized (metadata_dir={self.metadata_dir})")
    
    def _get_metadata_path(self, symbol: str, interval: str) -> Path:
        """
        Get the metadata file path for a symbol and interval.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            
        Returns:
            Path to metadata JSON file
        """
        # Sanitize symbol for filename (replace characters that might cause issues)
        safe_symbol = symbol.replace('^', '_').replace('/', '_').replace('\\', '_')
        filename = f"{safe_symbol}_{interval}.json"
        return self.metadata_dir / filename
    
    def load_metadata(self, symbol: str, interval: str) -> Dict[str, Any]:
        """
        Load metadata for a specific symbol and interval.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            
        Returns:
            Metadata dictionary, or default structure if not found
        """
        metadata_path = self._get_metadata_path(symbol, interval)
        
        if not metadata_path.exists():
            self.logger.debug(f"No metadata found for {symbol} ({interval}), creating new")
            return self._create_default_metadata(symbol, interval)
        
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            self.logger.debug(f"Loaded metadata for {symbol} ({interval})")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to load metadata for {symbol} ({interval}): {str(e)}")
            # Return default metadata if loading fails
            return self._create_default_metadata(symbol, interval)
    
    def _create_default_metadata(self, symbol: str, interval: str) -> Dict[str, Any]:
        """
        Create default metadata structure.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            
        Returns:
            Default metadata dictionary
        """
        return {
            'symbol': symbol,
            'interval': interval,
            'created_at': datetime.now().isoformat(),
            'last_update': None,
            'total_rows': 0,
            'date_range': {
                'start': None,
                'end': None
            },
            'data_quality': {
                'status': 'unknown',
                'last_validated': None,
                'issues_count': 0,
                'validation_details': {}
            },
            'download_history': []
        }
    
    def update_metadata(
        self,
        symbol: str,
        interval: str,
        stats: Dict[str, Any],
        validation_report: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update metadata with new statistics.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            stats: Statistics dictionary with keys:
                   - total_rows: int
                   - date_range: dict with 'start' and 'end'
                   - rows_added: int (optional)
            validation_report: Optional validation report details
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load existing metadata
            metadata = self.load_metadata(symbol, interval)
            
            # Update basic info
            metadata['last_update'] = datetime.now().isoformat()
            metadata['total_rows'] = stats.get('total_rows', metadata['total_rows'])
            
            # Update date range
            if 'date_range' in stats:
                metadata['date_range']['start'] = stats['date_range'].get('start')
                metadata['date_range']['end'] = stats['date_range'].get('end')
            
            # Update data quality if validation report provided
            if validation_report is not None:
                metadata['data_quality']['status'] = validation_report.get('status', 'unknown')
                metadata['data_quality']['last_validated'] = datetime.now().isoformat()
                metadata['data_quality']['issues_count'] = validation_report.get('issues_count', 0)
                metadata['data_quality']['validation_details'] = validation_report.get('details', {})
            
            # Add to download history
            history_entry = {
                'timestamp': datetime.now().isoformat(),
                'rows_added': stats.get('rows_added', 0),
                'total_rows': stats.get('total_rows', 0),
                'success': True
            }
            
            metadata['download_history'].append(history_entry)
            
            # Keep only last 100 history entries
            if len(metadata['download_history']) > 100:
                metadata['download_history'] = metadata['download_history'][-100:]
            
            # Save metadata
            self._save_metadata(symbol, interval, metadata)
            
            self.logger.info(f"Updated metadata for {symbol} ({interval})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update metadata for {symbol} ({interval}): {str(e)}")
            return False
    
    def _save_metadata(self, symbol: str, interval: str, metadata: Dict[str, Any]):
        """
        Save metadata to JSON file.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            metadata: Metadata dictionary to save
        """
        metadata_path = self._get_metadata_path(symbol, interval)
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"Saved metadata to {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata to {metadata_path}: {str(e)}")
            raise MetadataError(f"Error saving metadata: {str(e)}") from e
    
    def get_next_fetch_date(self, symbol: str, interval: str) -> Optional[datetime]:
        """
        Determine the next date to start fetching data from.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            
        Returns:
            Next date to fetch from, or None if no existing data
        """
        metadata = self.load_metadata(symbol, interval)
        
        # If no data exists yet, return None (fetch all available)
        if metadata['date_range']['end'] is None:
            self.logger.info(f"No existing data for {symbol} ({interval}), fetch from beginning")
            return None
        
        try:
            # Parse the last date
            last_date = datetime.fromisoformat(metadata['date_range']['end'])
            
            # Add one interval to get next fetch date
            next_date = self._calculate_next_date(last_date, interval)
            
            self.logger.info(f"Next fetch date for {symbol} ({interval}): {next_date}")
            return next_date
            
        except Exception as e:
            self.logger.error(f"Failed to calculate next fetch date: {str(e)}")
            return None
    
    def _calculate_next_date(self, last_date: datetime, interval: str) -> datetime:
        """
        Calculate the next date based on interval.
        
        Args:
            last_date: Last known date in data
            interval: Time interval
            
        Returns:
            Next date to fetch from
        """
        # For simplicity, just add 1 day for most intervals
        # In practice, for intraday data, you might want more sophisticated logic
        
        if interval.endswith('m') or interval.endswith('h'):
            # For intraday data, start from next day
            return last_date + timedelta(days=1)
        elif interval == '1d':
            return last_date + timedelta(days=1)
        elif interval == '1wk':
            return last_date + timedelta(weeks=1)
        elif interval == '1mo':
            return last_date + timedelta(days=30)
        else:
            # Default: next day
            return last_date + timedelta(days=1)
    
    def needs_update(
        self,
        symbol: str,
        interval: str,
        max_age_hours: int = 24
    ) -> bool:
        """
        Check if data needs updating based on last update time.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            max_age_hours: Maximum age in hours before update is needed
            
        Returns:
            True if data needs updating, False otherwise
        """
        metadata = self.load_metadata(symbol, interval)
        
        # If never updated, needs update
        if metadata['last_update'] is None:
            self.logger.info(f"{symbol} ({interval}) never updated, needs update")
            return True
        
        try:
            last_update = datetime.fromisoformat(metadata['last_update'])
            age = datetime.now() - last_update
            age_hours = age.total_seconds() / 3600
            
            needs_update = age_hours >= max_age_hours
            
            if needs_update:
                self.logger.info(
                    f"{symbol} ({interval}) last updated {age_hours:.1f}h ago, needs update"
                )
            else:
                self.logger.debug(
                    f"{symbol} ({interval}) last updated {age_hours:.1f}h ago, no update needed"
                )
            
            return needs_update
            
        except Exception as e:
            self.logger.error(f"Error checking update status: {str(e)}")
            return True  # If error, assume update is needed
    
    def get_all_metadata(self) -> List[Dict[str, Any]]:
        """
        Get metadata for all symbols and intervals.
        
        Returns:
            List of all metadata dictionaries
        """
        all_metadata = []
        
        # Find all JSON files in metadata directory
        for metadata_file in self.metadata_dir.glob("*.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    all_metadata.append(metadata)
            except Exception as e:
                self.logger.warning(f"Failed to load {metadata_file}: {str(e)}")
        
        self.logger.info(f"Loaded metadata for {len(all_metadata)} symbol/interval combinations")
        return all_metadata
    
    def get_symbols_needing_update(self, max_age_hours: int = 24) -> List[Dict[str, str]]:
        """
        Get list of symbols that need updating.
        
        Args:
            max_age_hours: Maximum age in hours before update is needed
            
        Returns:
            List of dictionaries with 'symbol' and 'interval' keys
        """
        all_metadata = self.get_all_metadata()
        needs_update = []
        
        for metadata in all_metadata:
            symbol = metadata['symbol']
            interval = metadata['interval']
            
            if self.needs_update(symbol, interval, max_age_hours):
                needs_update.append({
                    'symbol': symbol,
                    'interval': interval,
                    'last_update': metadata['last_update'],
                    'total_rows': metadata['total_rows']
                })
        
        self.logger.info(f"Found {len(needs_update)} symbols needing update")
        return needs_update
    
    def record_download_failure(
        self,
        symbol: str,
        interval: str,
        error_message: str
    ):
        """
        Record a failed download attempt.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            error_message: Error description
        """
        try:
            metadata = self.load_metadata(symbol, interval)
            
            # Add to download history
            history_entry = {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': error_message
            }
            
            metadata['download_history'].append(history_entry)
            
            # Keep only last 100 history entries
            if len(metadata['download_history']) > 100:
                metadata['download_history'] = metadata['download_history'][-100:]
            
            self._save_metadata(symbol, interval, metadata)
            
            self.logger.info(f"Recorded download failure for {symbol} ({interval})")
            
        except Exception as e:
            self.logger.error(f"Failed to record download failure: {str(e)}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics across all tracked symbols.
        
        Returns:
            Dictionary with aggregate statistics
        """
        all_metadata = self.get_all_metadata()
        
        total_symbols = len(all_metadata)
        total_rows = sum(m.get('total_rows', 0) for m in all_metadata)
        
        # Count by status
        status_counts = {}
        for metadata in all_metadata:
            status = metadata.get('data_quality', {}).get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Recent downloads (last 24 hours)
        recent_downloads = 0
        for metadata in all_metadata:
            if metadata.get('last_update'):
                try:
                    last_update = datetime.fromisoformat(metadata['last_update'])
                    if (datetime.now() - last_update).total_seconds() < 86400:
                        recent_downloads += 1
                except:
                    pass
        
        stats = {
            'total_symbols': total_symbols,
            'total_rows': total_rows,
            'status_counts': status_counts,
            'recent_downloads_24h': recent_downloads,
            'metadata_dir': str(self.metadata_dir)
        }
        
        return stats


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create metadata manager
    mm = MetadataManager(metadata_dir="./data/metadata")
    
    # Example 1: Update metadata
    print("\n=== Example 1: Update metadata ===")
    stats = {
        'total_rows': 100,
        'rows_added': 10,
        'date_range': {
            'start': '2024-01-01',
            'end': '2024-01-31'
        }
    }
    
    validation = {
        'status': 'passed',
        'issues_count': 0,
        'details': {'duplicates': 0, 'gaps': 0}
    }
    
    mm.update_metadata('RELIANCE.NS', '1d', stats, validation)
    
    # Example 2: Load metadata
    print("\n=== Example 2: Load metadata ===")
    metadata = mm.load_metadata('RELIANCE.NS', '1d')
    print(f"Symbol: {metadata['symbol']}")
    print(f"Total rows: {metadata['total_rows']}")
    print(f"Last update: {metadata['last_update']}")
    print(f"Data quality: {metadata['data_quality']['status']}")
    
    # Example 3: Check if needs update
    print("\n=== Example 3: Check update status ===")
    needs_update = mm.needs_update('RELIANCE.NS', '1d', max_age_hours=1)
    print(f"Needs update: {needs_update}")
    
    # Example 4: Get next fetch date
    print("\n=== Example 4: Get next fetch date ===")
    next_date = mm.get_next_fetch_date('RELIANCE.NS', '1d')
    print(f"Next fetch date: {next_date}")
    
    # Example 5: Overall statistics
    print("\n=== Example 5: Overall statistics ===")
    stats = mm.get_statistics()
    for key, value in stats.items():
        print(f"{key}: {value}")
