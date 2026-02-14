#!/usr/bin/env python3
"""
Initial Download Script for Market Data Pipeline

Downloads maximum available history for all configured symbols and intervals.
"""

import sys
import os
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

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
    print("Warning: tqdm not installed. Install with: pip install tqdm")


class InitialDownloader:
    """Handles initial download of market data for all symbols and intervals."""
    
    def __init__(
        self,
        config_dir: str = "./config",
        data_dir: str = "./data",
        log_dir: str = "./logs"
    ):
        """Initialize the InitialDownloader."""
        # Setup logging first
        self._setup_logging(log_dir)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("=" * 80)
        self.logger.info("INITIAL DOWNLOAD STARTED")
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
            'successful_downloads': 0,
            'failed_downloads': 0,
            'total_rows': 0,
            'total_size_mb': 0.0
        }
    
    def _setup_logging(self, log_dir: str):
        """Setup logging configuration."""
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        log_file = log_path / f"initial_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def download_symbol(
        self,
        symbol: str,
        intervals: List[str],
        category: str = 'stocks',
        force: bool = False
    ) -> Dict[str, bool]:
        """
        Download data for a single symbol across multiple intervals.
        
        Args:
            symbol: Stock/Index symbol
            intervals: List of intervals to download
            category: 'stocks' or 'indices'
            force: Whether to overwrite existing data
            
        Returns:
            Dictionary mapping interval to success status
        """
        results = {}
        
        # Create symbol directory
        symbol_dir = self.data_dir / category / symbol.replace('^', '_').replace('/', '_')
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        for interval in intervals:
            try:
                csv_path = symbol_dir / f"{interval}.csv"
                
                # Check if file exists and skip if not forcing
                if not force and csv_path.exists():
                    existing_data = self.merger.load_existing_data(csv_path)
                    if existing_data is not None and not existing_data.empty:
                        self.logger.info(f"Skipping {symbol} ({interval}) - already exists")
                        results[interval] = True
                        continue
                
                # Determine period based on interval limits
                max_period = self.data_fetcher.get_max_period(interval)
                if max_period is None:
                    period = "max"
                else:
                    period = f"{max_period}d"
                
                self.logger.info(f"Downloading {symbol} ({interval}) with period={period}")
                
                # Download data with retry
                df = self.retry_manager.retry_with_backoff(
                    self.data_fetcher.fetch_data,
                    max_retries=3,
                    initial_delay=2.0,
                    symbol=symbol,
                    interval=interval,
                    period=period
                )
                
                if df is None or df.empty:
                    self.logger.warning(f"No data returned for {symbol} ({interval})")
                    results[interval] = False
                    self.retry_manager.log_failure(symbol, interval, "No data returned")
                    continue
                
                # Validate data
                validated_df, report = self.validator.validate_dataframe(
                    df, interval, auto_fix=True
                )
                
                if not report.is_valid:
                    self.logger.warning(
                        f"Data validation issues for {symbol} ({interval}): "
                        f"{len(report.issues)} issues"
                    )
                
                # Save data
                self.merger.save_data(validated_df, csv_path)
                
                # Update metadata
                stats = {
                    'total_rows': len(validated_df),
                    'rows_added': len(validated_df),
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
                
                # Update statistics
                self.stats['total_rows'] += len(validated_df)
                file_size = csv_path.stat().st_size / (1024 * 1024)  # MB
                self.stats['total_size_mb'] += file_size
                
                results[interval] = True
                self.logger.info(
                    f"✓ {symbol} ({interval}): {len(validated_df)} rows, "
                    f"{file_size:.2f} MB"
                )
                
            except Exception as e:
                self.logger.error(f"Failed to download {symbol} ({interval}): {str(e)}")
                results[interval] = False
                self.retry_manager.log_failure(symbol, interval, str(e))
        
        return results
    
    def run(
        self,
        symbols: List[str] = None,
        intervals: List[str] = None,
        force: bool = False
    ):
        """
        Run the initial download process.
        
        Args:
            symbols: Optional list of specific symbols to download
            intervals: Optional list of specific intervals to download
            force: Whether to overwrite existing data
        """
        # Get symbols to download
        all_stocks = self.config_manager.get_stock_list()
        all_indices = self.config_manager.get_indices_list()
        
        if symbols:
            # Filter to specified symbols
            all_stocks = [s for s in all_stocks if s in symbols]
            all_indices = [s for s in all_indices if s in symbols]
        
        # Get intervals to download
        if intervals is None:
            intervals = self.config_manager.get_intervals()
        
        # Calculate total
        total_symbols = len(all_stocks) + len(all_indices)
        self.stats['total_symbols'] = total_symbols
        
        self.logger.info(f"Processing {len(all_stocks)} stocks and {len(all_indices)} indices")
        self.logger.info(f"Intervals: {intervals}")
        self.logger.info(f"Force overwrite: {force}")
        
        # Process stocks
        print("\n" + "=" * 80)
        print("DOWNLOADING STOCKS")
        print("=" * 80)
        
        if HAS_TQDM:
            stock_iterator = tqdm(all_stocks, desc="Stocks", unit="symbol")
        else:
            stock_iterator = all_stocks
        
        for stock in stock_iterator:
            if HAS_TQDM:
                stock_iterator.set_description(f"Stocks: {stock}")
            else:
                print(f"Processing stock: {stock}")
            
            results = self.download_symbol(stock, intervals, 'stocks', force)
            
            success_count = sum(1 for v in results.values() if v)
            if success_count == len(intervals):
                self.stats['successful_downloads'] += 1
            else:
                self.stats['failed_downloads'] += 1
        
        # Process indices
        print("\n" + "=" * 80)
        print("DOWNLOADING INDICES")
        print("=" * 80)
        
        if HAS_TQDM:
            index_iterator = tqdm(all_indices, desc="Indices", unit="symbol")
        else:
            index_iterator = all_indices
        
        for index in index_iterator:
            if HAS_TQDM:
                index_iterator.set_description(f"Indices: {index}")
            else:
                print(f"Processing index: {index}")
            
            results = self.download_symbol(index, intervals, 'indices', force)
            
            success_count = sum(1 for v in results.values() if v)
            if success_count == len(intervals):
                self.stats['successful_downloads'] += 1
            else:
                self.stats['failed_downloads'] += 1
        
        # Generate summary report
        self._generate_summary_report()
    
    def _generate_summary_report(self):
        """Generate and display summary report."""
        elapsed_time = time.time() - self.start_time
        
        print("\n" + "=" * 80)
        print("DOWNLOAD SUMMARY")
        print("=" * 80)
        
        summary = [
            f"Total Symbols Processed: {self.stats['total_symbols']}",
            f"Successful Downloads: {self.stats['successful_downloads']}",
            f"Failed Downloads: {self.stats['failed_downloads']}",
            f"Total Rows Downloaded: {self.stats['total_rows']:,}",
            f"Total Data Size: {self.stats['total_size_mb']:.2f} MB",
            f"Time Taken: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)",
        ]
        
        for line in summary:
            print(line)
            self.logger.info(line)
        
        # Get failure statistics
        failure_stats = self.retry_manager.get_failure_statistics()
        if failure_stats['total_failures'] > 0:
            print(f"\nFailure Details:")
            print(f"  Total Failures: {failure_stats['total_failures']}")
            print(f"  Unique Symbols Failed: {failure_stats['unique_symbols']}")
            
            if failure_stats['most_failed_symbol']:
                print(
                    f"  Most Failed Symbol: {failure_stats['most_failed_symbol']} "
                    f"({failure_stats['most_failed_count']} failures)"
                )
            
            # Generate failure report
            report_path = self.data_dir.parent / "logs" / "download_failures.txt"
            self.retry_manager.generate_failure_report(str(report_path))
            print(f"\nDetailed failure report saved to: {report_path}")
        
        print("=" * 80)
        self.logger.info("INITIAL DOWNLOAD COMPLETED")
        self.logger.info("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download initial market data for all configured symbols"
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Specific symbols to download (e.g., RELIANCE.NS TCS.NS)'
    )
    
    parser.add_argument(
        '--intervals',
        nargs='+',
        help='Specific intervals to download (e.g., 1d 5m)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing data'
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
    
    # Create downloader
    downloader = InitialDownloader(
        config_dir=args.config_dir,
        data_dir=args.data_dir,
        log_dir=args.log_dir
    )
    
    # Run download
    try:
        downloader.run(
            symbols=args.symbols,
            intervals=args.intervals,
            force=args.force
        )
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        downloader._generate_summary_report()
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {str(e)}")
        logging.exception("Fatal error during download")
        sys.exit(1)


if __name__ == "__main__":
    main()
