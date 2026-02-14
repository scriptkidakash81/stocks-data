#!/usr/bin/env python3
"""
Daily Update Script for Market Data Pipeline

Incrementally updates market data since last download.
"""

import sys
import os
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config_manager import ConfigManager
from data_fetcher import DataFetcher
from data_validator import DataValidator
from data_merger import DataMerger
from metadata_manager import MetadataManager
from retry_manager import RetryManager

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class DailyUpdater:
    """Handles incremental updates of market data."""
    
    def __init__(
        self,
        config_dir: str = "./config",
        data_dir: str = "./data",
        log_dir: str = "./logs"
    ):
        """Initialize the DailyUpdater."""
        # Setup logging
        self._setup_logging(log_dir)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("=" * 80)
        self.logger.info("DAILY UPDATE STARTED")
        self.logger.info("=" * 80)
        
        # Initialize components
        self.config_manager = ConfigManager(config_dir)
        self.config_manager.load_config()
        
        self.data_fetcher = DataFetcher(
            rate_limit_delay=0.5,
            max_retries=self.config_manager.get_max_retries(),
            retry_delay=self.config_manager.get_retry_delay()
        )
        
        self.validator = DataValidator(
            timezone=self.config_manager.get_timezone()
        )
        
        self.merger = DataMerger(backup_enabled=True)
        self.metadata_manager = MetadataManager(metadata_dir=f"{data_dir}/metadata")
        self.retry_manager = RetryManager(log_dir=log_dir)
        
        self.data_dir = Path(data_dir)
        self.start_time = time.time()
        
        # Statistics
        self.stats = {
            'total_symbols': 0,
            'symbols_updated': 0,
            'symbols_skipped': 0,
            'symbols_failed': 0,
            'total_rows_added': 0,
            'total_size_mb': 0.0
        }
    
    def _setup_logging(self, log_dir: str):
        """Setup logging configuration."""
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        log_file = log_path / f"daily_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _needs_update(self, symbol: str, interval: str, max_age_hours: int = 24) -> bool:
        """
        Check if symbol/interval needs updating based on last update time.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            max_age_hours: Maximum age before update is needed
            
        Returns:
            True if update is needed
        """
        return self.metadata_manager.needs_update(symbol, interval, max_age_hours)
    
    def _calculate_fetch_range(
        self,
        symbol: str,
        interval: str,
        specific_date: Optional[datetime] = None
    ) -> tuple:
        """
        Calculate the date range to fetch based on metadata and interval.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            specific_date: Optional specific date to update
            
        Returns:
            Tuple of (start_date, end_date, period) - period can be None if using dates
        """
        if specific_date:
            # Fetch specific date
            start_date = specific_date.strftime('%Y-%m-%d')
            end_date = (specific_date + timedelta(days=1)).strftime('%Y-%m-%d')
            return start_date, end_date, None
        
        # Get last update from metadata
        last_date = self.metadata_manager.get_next_fetch_date(symbol, interval)
        
        if last_date is None:
            # No existing data, fetch maximum
            max_period = self.data_fetcher.get_max_period(interval)
            if max_period is None:
                return None, None, "max"
            else:
                return None, None, f"{max_period}d"
        
        # Incremental update from last date to now (all intervals)
        start_date = last_date.strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        return start_date, end_date, None
    
    def update_symbol(
        self,
        symbol: str,
        interval: str,
        category: str = 'stocks',
        dry_run: bool = False,
        specific_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Update data for a single symbol and interval.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            category: 'stocks' or 'indices'
            dry_run: If True, preview without downloading
            specific_date: Optional specific date to update
            
        Returns:
            Dictionary with update results
        """
        result = {
            'success': False,
            'rows_added': 0,
            'skipped': False,
            'error': None
        }
        
        try:
            # Check if update is needed (unless specific date requested)
            if not specific_date and not self._needs_update(symbol, interval):
                self.logger.info(f"Skipping {symbol} ({interval}) - recently updated")
                result['skipped'] = True
                return result
            
            # Get symbol directory and CSV path
            symbol_dir = self.data_dir / category / symbol.replace('^', '_').replace('/', '_')
            csv_path = symbol_dir / f"{interval}.csv"
            
            # Calculate fetch range
            start_date, end_date, period = self._calculate_fetch_range(
                symbol, interval, specific_date
            )
            
            if dry_run:
                print(f"[DRY RUN] Would fetch {symbol} ({interval}):")
                if start_date and end_date:
                    print(f"  Date range: {start_date} to {end_date}")
                else:
                    print(f"  Period: {period}")
                result['success'] = True
                return result
            
            # Fetch new data
            self.logger.info(
                f"Fetching {symbol} ({interval}): "
                f"range={start_date or 'N/A'} to {end_date or 'N/A'}, period={period or 'N/A'}"
            )
            
            if start_date and end_date:
                new_df = self.retry_manager.retry_with_backoff(
                    self.data_fetcher.fetch_data,
                    max_retries=3,
                    initial_delay=2.0,
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                new_df = self.retry_manager.retry_with_backoff(
                    self.data_fetcher.fetch_data,
                    max_retries=3,
                    initial_delay=2.0,
                    symbol=symbol,
                    interval=interval,
                    period=period
                )
            
            if new_df is None or new_df.empty:
                self.logger.info(f"No new data for {symbol} ({interval})")
                result['success'] = True
                result['rows_added'] = 0
                return result
            
            # Load existing data
            existing_df = self.merger.load_existing_data(csv_path)
            
            # Merge new data with existing (all intervals)
            merged_df = self.merger.merge_data(existing_df, new_df)
            rows_added = len(merged_df) - (len(existing_df) if existing_df is not None else 0)
            
            # Validate merged data
            validated_df, report = self.validator.validate_dataframe(
                merged_df, interval, auto_fix=True
            )
            
            if not report.is_valid:
                self.logger.warning(
                    f"Validation issues for {symbol} ({interval}): "
                    f"{len(report.issues)} issues"
                )
            
            # Save merged data
            self.merger.save_data(validated_df, csv_path)
            
            # Update metadata
            stats = {
                'total_rows': len(validated_df),
                'rows_added': rows_added,
                'date_range': {
                    'start': str(validated_df.index.min()),
                    'end': str(validated_df.index.max())
                }
            }
            
            validation_report = {
                'status': 'passed' if report.is_valid else 'issues',
                'issues_count': len(report.issues)
            }
            
            self.metadata_manager.update_metadata(
                symbol, interval, stats, validation_report
            )
            
            # Update result
            result['success'] = True
            result['rows_added'] = rows_added
            
            self.logger.info(
                f"✓ Updated {symbol} ({interval}): +{rows_added} rows, "
                f"total={len(validated_df)}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to update {symbol} ({interval}): {str(e)}")
            result['error'] = str(e)
            self.retry_manager.log_failure(symbol, interval, str(e))
        
        return result
    
    def run(
        self,
        symbols: Optional[List[str]] = None,
        intervals: Optional[List[str]] = None,
        dry_run: bool = False,
        specific_date: Optional[str] = None
    ):
        """
        Run the daily update process.
        
        Args:
            symbols: Optional list of specific symbols to update
            intervals: Optional list of specific intervals to update
            dry_run: If True, preview without downloading
            specific_date: Optional specific date to update (YYYY-MM-DD)
        """
        # Parse specific date if provided
        target_date = None
        if specific_date:
            try:
                target_date = datetime.strptime(specific_date, '%Y-%m-%d')
                self.logger.info(f"Updating data for specific date: {specific_date}")
            except ValueError:
                self.logger.error(f"Invalid date format: {specific_date}. Use YYYY-MM-DD")
                return
        
        # Get symbols to update
        all_stocks = self.config_manager.get_stock_list()
        all_indices = self.config_manager.get_indices_list()
        
        if symbols:
            all_stocks = [s for s in all_stocks if s in symbols]
            all_indices = [s for s in all_indices if s in symbols]
        
        # Get intervals to update
        if intervals is None:
            intervals = self.config_manager.get_intervals()
        
        total_symbols = len(all_stocks) + len(all_indices)
        self.stats['total_symbols'] = total_symbols
        
        self.logger.info(f"Updating {len(all_stocks)} stocks and {len(all_indices)} indices")
        self.logger.info(f"Intervals: {intervals}")
        self.logger.info(f"Dry run: {dry_run}")
        
        if dry_run:
            print("\n*** DRY RUN MODE - No data will be downloaded ***\n")
        
        # Process stocks
        print("\n" + "=" * 80)
        print("UPDATING STOCKS")
        print("=" * 80)
        
        if HAS_TQDM:
            stock_iterator = tqdm(all_stocks, desc="Stocks", unit="symbol")
        else:
            stock_iterator = all_stocks
        
        for stock in stock_iterator:
            if HAS_TQDM:
                stock_iterator.set_description(f"Stocks: {stock}")
            
            for interval in intervals:
                result = self.update_symbol(
                    stock, interval, 'stocks', dry_run, target_date
                )
                
                if result['skipped']:
                    self.stats['symbols_skipped'] += 1
                elif result['success']:
                    self.stats['symbols_updated'] += 1
                    self.stats['total_rows_added'] += result['rows_added']
                else:
                    self.stats['symbols_failed'] += 1
        
        # Process indices
        print("\n" + "=" * 80)
        print("UPDATING INDICES")
        print("=" * 80)
        
        if HAS_TQDM:
            index_iterator = tqdm(all_indices, desc="Indices", unit="symbol")
        else:
            index_iterator = all_indices
        
        for index in index_iterator:
            if HAS_TQDM:
                index_iterator.set_description(f"Indices: {index}")
            
            for interval in intervals:
                result = self.update_symbol(
                    index, interval, 'indices', dry_run, target_date
                )
                
                if result['skipped']:
                    self.stats['symbols_skipped'] += 1
                elif result['success']:
                    self.stats['symbols_updated'] += 1
                    self.stats['total_rows_added'] += result['rows_added']
                else:
                    self.stats['symbols_failed'] += 1
        
        # Generate summary report
        self._generate_summary_report(dry_run)
    
    def _generate_summary_report(self, dry_run: bool = False):
        """Generate and display summary report."""
        elapsed_time = time.time() - self.start_time
        
        print("\n" + "=" * 80)
        print("UPDATE SUMMARY")
        print("=" * 80)
        
        summary = [
            f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}",
            f"Total Symbols Processed: {self.stats['total_symbols']}",
            f"Symbols Updated: {self.stats['symbols_updated']}",
            f"Symbols Skipped: {self.stats['symbols_skipped']}",
            f"Symbols Failed: {self.stats['symbols_failed']}",
            f"Total Rows Added: {self.stats['total_rows_added']:,}",
            f"Time Taken: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)"
        ]
        
        for line in summary:
            print(line)
            self.logger.info(line)
        
        # Failure report
        failure_stats = self.retry_manager.get_failure_statistics()
        if failure_stats['total_failures'] > 0:
            print(f"\nFailure Details:")
            print(f"  Total Failures: {failure_stats['total_failures']}")
            print(f"  Unique Symbols: {failure_stats['unique_symbols']}")
            
            if failure_stats['most_failed_symbol']:
                print(
                    f"  Most Failed: {failure_stats['most_failed_symbol']} "
                    f"({failure_stats['most_failed_count']} times)"
                )
            
            # Save failure report
            report_path = self.data_dir.parent / "logs" / "update_failures.txt"
            self.retry_manager.generate_failure_report(str(report_path))
            print(f"\nFailure report: {report_path}")
        
        print("=" * 80)
        self.logger.info("DAILY UPDATE COMPLETED")
        self.logger.info("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Incrementally update market data"
    )
    
    parser.add_argument(
        '--date',
        help='Specific date to update (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Specific symbols to update'
    )
    
    parser.add_argument(
        '--intervals',
        nargs='+',
        help='Specific intervals to update'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview without downloading'
    )
    
    parser.add_argument(
        '--config-dir',
        default='./config',
        help='Configuration directory (default: ./config)'
    )
    
    parser.add_argument(
        '--data-dir',
        default='./data',
        help='Data directory (default: ./data)'
    )
    
    parser.add_argument(
        '--log-dir',
        default='./logs',
        help='Log directory (default: ./logs)'
    )
    
    args = parser.parse_args()
    
    # Create updater
    updater = DailyUpdater(
        config_dir=args.config_dir,
        data_dir=args.data_dir,
        log_dir=args.log_dir
    )
    
    # Run update
    try:
        updater.run(
            symbols=args.symbols,
            intervals=args.intervals,
            dry_run=args.dry_run,
            specific_date=args.date
        )
    except KeyboardInterrupt:
        print("\n\nUpdate interrupted by user")
        updater._generate_summary_report()
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {str(e)}")
        logging.exception("Fatal error during update")
        sys.exit(1)


if __name__ == "__main__":
    main()
