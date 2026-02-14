#!/usr/bin/env python3
"""
Gap Fixing Script for Market Data Pipeline

Identifies and fills gaps in historical market data.
"""

import sys
import os
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config_manager import ConfigManager
from data_fetcher import DataFetcher
from data_validator import DataValidator
from data_merger import DataMerger
from metadata_manager import MetadataManager
from retry_manager import RetryManager

import pandas as pd

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class GapFixer:
    """Identifies and fixes gaps in market data."""
    
    # NSE Market Holidays for 2024-2026 (sample - should be updated)
    MARKET_HOLIDAYS = [
        '2024-01-26',  # Republic Day
        '2024-03-08',  # Mahashivratri
        '2024-03-25',  # Holi
        '2024-03-29',  # Good Friday
        '2024-04-11',  # Eid al-Fitr
        '2024-04-17',  # Ram Navami
        '2024-04-21',  # Mahavir Jayanti
        '2024-05-01',  # Maharashtra Day
        '2024-06-17',  # Eid al-Adha
        '2024-07-17',  # Muharram
        '2024-08-15',  # Independence Day
        '2024-08-26',  # Janmashtami
        '2024-10-02',  # Gandhi Jayanti
        '2024-10-12',  # Dussehra
        '2024-11-01',  # Diwali
        '2024-11-15',  # Guru Nanak Jayanti
        '2024-12-25',  # Christmas
        # Add 2025-2026 holidays as needed
    ]
    
    def __init__(
        self,
        config_dir: str = "./config",
        data_dir: str = "./data",
        log_dir: str = "./logs"
    ):
        """Initialize the GapFixer."""
        # Setup logging
        self._setup_logging(log_dir)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("=" * 80)
        self.logger.info("GAP FIXING STARTED")
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
        
        # Parse holidays
        self.holidays = set(pd.to_datetime(self.MARKET_HOLIDAYS).date)
        
        # Results
        self.results = {
            'total_gaps_found': 0,
            'valid_gaps': 0,  # Holidays, weekends
            'fixable_gaps': 0,
            'gaps_fixed': 0,
            'gaps_unfixable': 0,
            'gap_details': []
        }
    
    def _setup_logging(self, log_dir: str):
        """Setup logging configuration."""
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        log_file = log_path / f"gap_fixing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _is_valid_gap(self, gap_date: datetime) -> Tuple[bool, str]:
        """
        Check if a gap is valid (holiday, weekend).
        
        Args:
            gap_date: Date to check
            
        Returns:
            Tuple of (is_valid, reason)
        """
        # Check if weekend
        if gap_date.weekday() >= 5:  # Saturday=5, Sunday=6
            return True, "weekend"
        
        # Check if market holiday
        if gap_date.date() in self.holidays:
            return True, "market_holiday"
        
        return False, "trading_day"
    
    def identify_gaps(
        self,
        symbol: str,
        interval: str,
        category: str = 'stocks'
    ) -> List[Dict[str, Any]]:
        """
        Identify gaps in data for a symbol/interval.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            category: 'stocks' or 'indices'
            
        Returns:
            List of gap information dictionaries
        """
        symbol_dir = self.data_dir / category / symbol.replace('^', '_').replace('/', '_')
        csv_path = symbol_dir / f"{interval}.csv"
        
        if not csv_path.exists():
            self.logger.warning(f"File not found: {csv_path}")
            return []
        
        try:
            df = self.merger.load_existing_data(csv_path)
            
            if df is None or df.empty:
                return []
            
            # Use validator to find gaps
            gap_issues = self.validator.check_gaps(df, interval)
            
            gaps = []
            for gap in gap_issues:
                # Extract gap information
                gap_info = {
                    'symbol': symbol,
                    'interval': interval,
                    'category': category,
                    'message': gap['message'],
                    'details': gap.get('details', ''),
                    'index': gap.get('index'),
                    'is_valid_gap': False,
                    'gap_reason': None,
                    'fixable': True
                }
                
                # For daily data, check if gap is valid
                if interval == '1d' and gap_info['index'] is not None:
                    try:
                        gap_date = df.index[gap_info['index']]
                        is_valid, reason = self._is_valid_gap(gap_date)
                        gap_info['is_valid_gap'] = is_valid
                        gap_info['gap_reason'] = reason
                        
                        if is_valid:
                            gap_info['fixable'] = False
                    except:
                        pass
                
                gaps.append(gap_info)
            
            return gaps
            
        except Exception as e:
            self.logger.error(f"Error identifying gaps for {symbol} ({interval}): {str(e)}")
            return []
    
    def fill_gap(
        self,
        symbol: str,
        interval: str,
        category: str,
        gap_start: Optional[str] = None,
        gap_end: Optional[str] = None
    ) -> bool:
        """
        Attempt to fill a gap by re-downloading data.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            category: 'stocks' or 'indices'
            gap_start: Start date of gap (optional)
            gap_end: End date of gap (optional)
            
        Returns:
            True if gap was filled successfully
        """
        symbol_dir = self.data_dir / category / symbol.replace('^', '_').replace('/', '_')
        csv_path = symbol_dir / f"{interval}.csv"
        
        try:
            # Load existing data
            existing_df = self.merger.load_existing_data(csv_path)
            
            # Determine fetch parameters
            if gap_start and gap_end:
                self.logger.info(f"Fetching gap data for {symbol} ({interval}): {gap_start} to {gap_end}")
                new_df = self.data_fetcher.fetch_data(
                    symbol=symbol,
                    interval=interval,
                    start_date=gap_start,
                    end_date=gap_end
                )
            else:
                # Fetch all data and merge
                max_period = self.data_fetcher.get_max_period(interval)
                period = f"{max_period}d" if max_period else "max"
                
                self.logger.info(f"Fetching all data for {symbol} ({interval}) with period={period}")
                new_df = self.data_fetcher.fetch_data(
                    symbol=symbol,
                    interval=interval,
                    period=period
                )
            
            if new_df is None or new_df.empty:
                self.logger.warning(f"No data returned for {symbol} ({interval})")
                return False
            
            # Merge with existing data
            merged_df = self.merger.merge_data(existing_df, new_df)
            
            # Validate
            validated_df, report = self.validator.validate_dataframe(
                merged_df, interval, auto_fix=True
            )
            
            # Save
            self.merger.save_data(validated_df, csv_path)
            
            # Update metadata
            stats = {
                'total_rows': len(validated_df),
                'rows_added': len(validated_df) - (len(existing_df) if existing_df is not None else 0),
                'date_range': {
                    'start': str(validated_df.index.min()),
                    'end': str(validated_df.index.max())
                }
            }
            
            self.metadata_manager.update_metadata(symbol, interval, stats)
            
            self.logger.info(f"✓ Successfully filled gap for {symbol} ({interval})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to fill gap for {symbol} ({interval}): {str(e)}")
            self.retry_manager.log_failure(symbol, interval, f"Gap fill failed: {str(e)}")
            return False
    
    def run(
        self,
        symbols: Optional[List[str]] = None,
        intervals: Optional[List[str]] = None,
        auto_fix: bool = False,
        dry_run: bool = False
    ):
        """
        Run gap analysis and fixing.
        
        Args:
            symbols: Optional specific symbols to check
            intervals: Optional specific intervals to check
            auto_fix: Automatically attempt to fix gaps
            dry_run: Preview without fixing
        """
        # Get symbols
        all_stocks = self.config_manager.get_stock_list()
        all_indices = self.config_manager.get_indices_list()
        
        if symbols:
            all_stocks = [s for s in all_stocks if s in symbols]
            all_indices = [s for s in all_indices if s in symbols]
        
        # Get intervals
        if intervals is None:
            intervals = self.config_manager.get_intervals()
        
        self.logger.info(f"Analyzing {len(all_stocks)} stocks and {len(all_indices)} indices")
        self.logger.info(f"Auto-fix: {auto_fix}, Dry-run: {dry_run}")
        
        # Analyze stocks
        print("\n" + "=" * 80)
        print("ANALYZING STOCKS FOR GAPS")
        print("=" * 80)
        
        if HAS_TQDM:
            stock_iterator = tqdm(all_stocks, desc="Stocks", unit="symbol")
        else:
            stock_iterator = all_stocks
        
        for stock in stock_iterator:
            for interval in intervals:
                gaps = self.identify_gaps(stock, interval, 'stocks')
                
                if gaps:
                    self.results['total_gaps_found'] += len(gaps)
                    
                    for gap in gaps:
                        self.results['gap_details'].append(gap)
                        
                        if gap['is_valid_gap']:
                            self.results['valid_gaps'] += 1
                        elif gap['fixable']:
                            self.results['fixable_gaps'] += 1
                            
                            if auto_fix and not dry_run:
                                if self.fill_gap(stock, interval, 'stocks'):
                                    self.results['gaps_fixed'] += 1
                                else:
                                    self.results['gaps_unfixable'] += 1
                        else:
                            self.results['gaps_unfixable'] += 1
        
        # Analyze indices
        print("\n" + "=" * 80)
        print("ANALYZING INDICES FOR GAPS")
        print("=" * 80)
        
        if HAS_TQDM:
            index_iterator = tqdm(all_indices, desc="Indices", unit="symbol")
        else:
            index_iterator = all_indices
        
        for index in index_iterator:
            for interval in intervals:
                gaps = self.identify_gaps(index, interval, 'indices')
                
                if gaps:
                    self.results['total_gaps_found'] += len(gaps)
                    
                    for gap in gaps:
                        self.results['gap_details'].append(gap)
                        
                        if gap['is_valid_gap']:
                            self.results['valid_gaps'] += 1
                        elif gap['fixable']:
                            self.results['fixable_gaps'] += 1
                            
                            if auto_fix and not dry_run:
                                if self.fill_gap(index, interval, 'indices'):
                                    self.results['gaps_fixed'] += 1
                                else:
                                    self.results['gaps_unfixable'] += 1
                        else:
                            self.results['gaps_unfixable'] += 1
        
        # Generate report
        self._generate_report(auto_fix, dry_run)
    
    def _generate_report(self, auto_fix: bool = False, dry_run: bool = False):
        """Generate gap analysis report."""
        print("\n" + "=" * 80)
        print("GAP ANALYSIS REPORT")
        print("=" * 80)
        
        summary = [
            f"Mode: {'AUTO-FIX' if auto_fix else 'ANALYSIS ONLY'} {'(DRY RUN)' if dry_run else ''}",
            f"Total Gaps Found: {self.results['total_gaps_found']}",
            f"Valid Gaps (Holidays/Weekends): {self.results['valid_gaps']}",
            f"Fixable Gaps: {self.results['fixable_gaps']}",
            f"Gaps Fixed: {self.results['gaps_fixed']}",
            f"Unfixable Gaps: {self.results['gaps_unfixable']}"
        ]
        
        for line in summary:
            print(line)
            self.logger.info(line)
        
        # Gap details by type
        if self.results['gap_details']:
            print("\nGap Summary by Symbol (showing first 20):")
            
            # Group by symbol
            gaps_by_symbol = {}
            for gap in self.results['gap_details']:
                symbol = gap['symbol']
                if symbol not in gaps_by_symbol:
                    gaps_by_symbol[symbol] = []
                gaps_by_symbol[symbol].append(gap)
            
            for idx, (symbol, gaps) in enumerate(list(gaps_by_symbol.items())[:20]):
                fixable = sum(1 for g in gaps if g['fixable'] and not g['is_valid_gap'])
                valid = sum(1 for g in gaps if g['is_valid_gap'])
                print(f"  {symbol}: {len(gaps)} gaps ({fixable} fixable, {valid} valid)")
            
            if len(gaps_by_symbol) > 20:
                print(f"  ... and {len(gaps_by_symbol) - 20} more symbols")
        
        # Save detailed report
        report_path = self.data_dir.parent / "logs" / f"gap_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\nDetailed report saved to: {report_path}")
        
        # Generate fix script if needed
        if not auto_fix and self.results['fixable_gaps'] > 0:
            self._generate_fix_script()
        
        print("=" * 80)
        self.logger.info("GAP ANALYSIS COMPLETED")
    
    def _generate_fix_script(self):
        """Generate a script to fix gaps."""
        script_path = self.data_dir.parent / "logs" / "fix_gaps_script.sh"
        
        # Get unique symbol/interval combinations that need fixing
        to_fix = set()
        for gap in self.results['gap_details']:
            if gap['fixable'] and not gap['is_valid_gap']:
                to_fix.add((gap['symbol'], gap['interval']))
        
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write("#!/bin/bash\n")
            f.write("# Auto-generated script to fix gaps\n\n")
            
            for symbol, interval in sorted(to_fix):
                f.write(f"# Fix gaps for {symbol} ({interval})\n")
                f.write(
                    f"python scripts/fix_gaps.py --symbols {symbol} "
                    f"--intervals {interval} --auto-fix\n\n"
                )
        
        if os.name != 'nt':
            os.chmod(script_path, 0o755)
        
        print(f"Fix script saved to: {script_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Identify and fix gaps in market data"
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Specific symbols to check'
    )
    
    parser.add_argument(
        '--intervals',
        nargs='+',
        help='Specific intervals to check'
    )
    
    parser.add_argument(
        '--auto-fix',
        action='store_true',
        help='Automatically attempt to fix gaps'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview without fixing'
    )
    
    parser.add_argument(
        '--config-dir',
        default='./config',
        help='Configuration directory'
    )
    
    parser.add_argument(
        '--data-dir',
        default='./data',
        help='Data directory'
    )
    
    parser.add_argument(
        '--log-dir',
        default='./logs',
        help='Log directory'
    )
    
    args = parser.parse_args()
    
    # Create gap fixer
    fixer = GapFixer(
        config_dir=args.config_dir,
        data_dir=args.data_dir,
        log_dir=args.log_dir
    )
    
    # Run gap fixing
    try:
        fixer.run(
            symbols=args.symbols,
            intervals=args.intervals,
            auto_fix=args.auto_fix,
            dry_run=args.dry_run
        )
    except KeyboardInterrupt:
        print("\n\nGap fixing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {str(e)}")
        logging.exception("Fatal error during gap fixing")
        sys.exit(1)


if __name__ == "__main__":
    main()
