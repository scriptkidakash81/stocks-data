#!/usr/bin/env python3
"""
Validation Script for Market Data Pipeline

Validates all existing data files and generates comprehensive reports.
"""

import sys
import os
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config_manager import ConfigManager
from data_validator import DataValidator, ValidationReport
from data_merger import DataMerger
from metadata_manager import MetadataManager

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class DataValidationSuite:
    """Comprehensive validation suite for market data files."""
    
    def __init__(
        self,
        config_dir: str = "./config",
        data_dir: str = "./data",
        log_dir: str = "./logs"
    ):
        """Initialize the validation suite."""
        # Setup logging
        self._setup_logging(log_dir)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("=" * 80)
        self.logger.info("VALIDATION SUITE STARTED")
        self.logger.info("=" * 80)
        
        # Initialize components
        self.config_manager = ConfigManager(config_dir)
        self.config_manager.load_config()
        
        self.validator = DataValidator(
            timezone=self.config_manager.get_timezone()
        )
        
        self.merger = DataMerger(backup_enabled=True)
        self.metadata_manager = MetadataManager(metadata_dir=f"{data_dir}/metadata")
        
        self.data_dir = Path(data_dir)
        
        # Validation results
        self.results = {
            'total_files': 0,
            'valid_files': 0,
            'files_with_issues': 0,
            'missing_files': 0,
            'corrupted_files': 0,
            'files_needing_redownload': [],
            'issues_by_type': {},
            'detailed_results': []
        }
    
    def _setup_logging(self, log_dir: str):
        """Setup logging configuration."""
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        log_file = log_path / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def validate_file(
        self,
        filepath: Path,
        symbol: str,
        interval: str,
        fix: bool = False
    ) -> Dict[str, Any]:
        """
        Validate a single data file.
        
        Args:
            filepath: Path to CSV file
            symbol: Stock/Index symbol
            interval: Time interval
            fix: Whether to fix issues automatically
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'filepath': str(filepath),
            'symbol': symbol,
            'interval': interval,
            'exists': False,
            'valid': False,
            'issues': [],
            'stats': {},
            'needs_redownload': False
        }
        
        # Check file exists
        if not filepath.exists():
            result['issues'].append({
                'severity': 'error',
                'category': 'missing',
                'message': 'File does not exist'
            })
            result['needs_redownload'] = True
            return result
        
        result['exists'] = True
        
        # Check file size
        file_size = filepath.stat().st_size
        if file_size == 0:
            result['issues'].append({
                'severity': 'error',
                'category': 'corrupted',
                'message': 'File is empty'
            })
            result['needs_redownload'] = True
            return result
        
        result['stats']['file_size_mb'] = file_size / (1024 * 1024)
        
        try:
            # Load data
            df = self.merger.load_existing_data(filepath)
            
            if df is None or df.empty:
                result['issues'].append({
                    'severity': 'error',
                    'category': 'corrupted',
                    'message': 'Cannot load data or data is empty'
                })
                result['needs_redownload'] = True
                return result
            
            result['stats']['row_count'] = len(df)
            result['stats']['date_range'] = {
                'start': str(df.index.min()),
                'end': str(df.index.max())
            }
            
            # Validate data
            validated_df, report = self.validator.validate_dataframe(
                df, interval, auto_fix=fix
            )
            
            # Process validation report
            result['valid'] = report.is_valid
            
            for issue in report.issues:
                result['issues'].append({
                    'severity': issue['severity'],
                    'category': issue['category'],
                    'message': issue['message'],
                    'details': issue.get('details')
                })
            
            # Check metadata consistency
            metadata_issues = self._check_metadata_consistency(
                symbol, interval, df
            )
            result['issues'].extend(metadata_issues)
            
            # If fixed, save the corrected data
            if fix and not report.is_valid:
                self.logger.info(f"Saving fixed data for {symbol} ({interval})")
                self.merger.save_data(validated_df, filepath)
            
            # Determine if redownload is needed
            critical_issues = [i for i in result['issues'] if i['severity'] in ['error', 'critical']]
            if len(critical_issues) > 5:  # Threshold for redownload
                result['needs_redownload'] = True
            
        except Exception as e:
            result['issues'].append({
                'severity': 'critical',
                'category': 'error',
                'message': f'Unexpected error: {str(e)}'
            })
            result['needs_redownload'] = True
            self.logger.error(f"Error validating {filepath}: {str(e)}")
        
        return result
    
    def _check_metadata_consistency(
        self,
        symbol: str,
        interval: str,
        df
    ) -> List[Dict[str, Any]]:
        """
        Check if metadata is consistent with actual data.
        
        Args:
            symbol: Stock/Index symbol
            interval: Time interval
            df: DataFrame with actual data
            
        Returns:
            List of metadata-related issues
        """
        issues = []
        
        try:
            metadata = self.metadata_manager.load_metadata(symbol, interval)
            
            # Check row count
            actual_rows = len(df)
            metadata_rows = metadata.get('total_rows', 0)
            
            if actual_rows != metadata_rows:
                issues.append({
                    'severity': 'warning',
                    'category': 'metadata',
                    'message': f'Row count mismatch: actual={actual_rows}, metadata={metadata_rows}'
                })
            
            # Check date range
            actual_start = str(df.index.min())
            actual_end = str(df.index.max())
            
            metadata_start = metadata.get('date_range', {}).get('start')
            metadata_end = metadata.get('date_range', {}).get('end')
            
            if metadata_start and actual_start != metadata_start:
                issues.append({
                    'severity': 'warning',
                    'category': 'metadata',
                    'message': f'Start date mismatch: actual={actual_start}, metadata={metadata_start}'
                })
            
            if metadata_end and actual_end != metadata_end:
                issues.append({
                    'severity': 'warning',
                    'category': 'metadata',
                    'message': f'End date mismatch: actual={actual_end}, metadata={metadata_end}'
                })
            
        except Exception as e:
            issues.append({
                'severity': 'warning',
                'category': 'metadata',
                'message': f'Metadata check failed: {str(e)}'
            })
        
        return issues
    
    def check_missing_intervals(self, symbol: str, category: str) -> List[str]:
        """
        Check for missing interval files for a symbol.
        
        Args:
            symbol: Stock/Index symbol
            category: 'stocks' or 'indices'
            
        Returns:
            List of missing intervals
        """
        expected_intervals = self.config_manager.get_intervals()
        symbol_dir = self.data_dir / category / symbol.replace('^', '_').replace('/', '_')
        
        missing = []
        
        if not symbol_dir.exists():
            return expected_intervals  # All missing
        
        for interval in expected_intervals:
            csv_path = symbol_dir / f"{interval}.csv"
            if not csv_path.exists():
                missing.append(interval)
        
        return missing
    
    def run(self, fix: bool = False, symbols: List[str] = None):
        """
        Run comprehensive validation on all data files.
        
        Args:
            fix: Whether to fix issues automatically
            symbols: Optional list of specific symbols to validate
        """
        # Get symbols
        all_stocks = self.config_manager.get_stock_list()
        all_indices = self.config_manager.get_indices_list()
        
        if symbols:
            all_stocks = [s for s in all_stocks if s in symbols]
            all_indices = [s for s in all_indices if s in symbols]
        
        intervals = self.config_manager.get_intervals()
        
        self.logger.info(f"Validating {len(all_stocks)} stocks and {len(all_indices)} indices")
        self.logger.info(f"Fix mode: {fix}")
        
        # Validate stocks
        print("\n" + "=" * 80)
        print("VALIDATING STOCKS")
        print("=" * 80)
        
        if HAS_TQDM:
            stock_iterator = tqdm(all_stocks, desc="Stocks", unit="symbol")
        else:
            stock_iterator = all_stocks
        
        for stock in stock_iterator:
            # Check for missing intervals
            missing_intervals = self.check_missing_intervals(stock, 'stocks')
            
            if missing_intervals:
                self.results['missing_files'] += len(missing_intervals)
                self.logger.warning(f"{stock}: Missing intervals: {missing_intervals}")
            
            # Validate each interval
            for interval in intervals:
                symbol_dir = self.data_dir / 'stocks' / stock.replace('^', '_').replace('/', '_')
                csv_path = symbol_dir / f"{interval}.csv"
                
                result = self.validate_file(csv_path, stock, interval, fix)
                
                self.results['total_files'] += 1
                self.results['detailed_results'].append(result)
                
                if result['valid'] and result['exists']:
                    self.results['valid_files'] += 1
                elif result['issues']:
                    self.results['files_with_issues'] += 1
                    
                    # Track issues by type
                    for issue in result['issues']:
                        category = issue['category']
                        self.results['issues_by_type'][category] = \
                            self.results['issues_by_type'].get(category, 0) + 1
                
                if result['needs_redownload']:
                    self.results['files_needing_redownload'].append({
                        'symbol': stock,
                        'interval': interval,
                        'category': 'stocks',
                        'reason': [i['message'] for i in result['issues'][:3]]  # First 3 issues
                    })
                
                if not result['exists']:
                    self.results['corrupted_files'] += 1
        
        # Validate indices
        print("\n" + "=" * 80)
        print("VALIDATING INDICES")
        print("=" * 80)
        
        if HAS_TQDM:
            index_iterator = tqdm(all_indices, desc="Indices", unit="symbol")
        else:
            index_iterator = all_indices
        
        for index in index_iterator:
            # Check for missing intervals
            missing_intervals = self.check_missing_intervals(index, 'indices')
            
            if missing_intervals:
                self.results['missing_files'] += len(missing_intervals)
                self.logger.warning(f"{index}: Missing intervals: {missing_intervals}")
            
            # Validate each interval
            for interval in intervals:
                symbol_dir = self.data_dir / 'indices' / index.replace('^', '_').replace('/', '_')
                csv_path = symbol_dir / f"{interval}.csv"
                
                result = self.validate_file(csv_path, index, interval, fix)
                
                self.results['total_files'] += 1
                self.results['detailed_results'].append(result)
                
                if result['valid'] and result['exists']:
                    self.results['valid_files'] += 1
                elif result['issues']:
                    self.results['files_with_issues'] += 1
                    
                    for issue in result['issues']:
                        category = issue['category']
                        self.results['issues_by_type'][category] = \
                            self.results['issues_by_type'].get(category, 0) + 1
                
                if result['needs_redownload']:
                    self.results['files_needing_redownload'].append({
                        'symbol': index,
                        'interval': interval,
                        'category': 'indices',
                        'reason': [i['message'] for i in result['issues'][:3]]
                    })
                
                if not result['exists']:
                    self.results['corrupted_files'] += 1
        
        # Generate report
        self._generate_report(fix)
    
    def _generate_report(self, fix: bool = False):
        """Generate comprehensive validation report."""
        print("\n" + "=" * 80)
        print("VALIDATION REPORT")
        print("=" * 80)
        
        summary = [
            f"Fix Mode: {'ENABLED' if fix else 'DISABLED'}",
            f"Total Files Checked: {self.results['total_files']}",
            f"Valid Files: {self.results['valid_files']}",
            f"Files with Issues: {self.results['files_with_issues']}",
            f"Missing Files: {self.results['missing_files']}",
            f"Corrupted Files: {self.results['corrupted_files']}",
            f"Files Needing Re-download: {len(self.results['files_needing_redownload'])}"
        ]
        
        for line in summary:
            print(line)
            self.logger.info(line)
        
        # Issues by type
        if self.results['issues_by_type']:
            print("\nIssues by Type:")
            for issue_type, count in sorted(self.results['issues_by_type'].items()):
                print(f"  {issue_type}: {count}")
        
        # Files needing redownload
        if self.results['files_needing_redownload']:
            print(f"\nFiles Needing Re-download ({len(self.results['files_needing_redownload'])}):")
            for item in self.results['files_needing_redownload'][:10]:  # Show first 10
                print(f"  {item['symbol']} ({item['interval']}): {item['reason'][0] if item['reason'] else 'Unknown'}")
            
            if len(self.results['files_needing_redownload']) > 10:
                print(f"  ... and {len(self.results['files_needing_redownload']) - 10} more")
        
        # Save detailed report
        report_path = self.data_dir.parent / "logs" / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\nDetailed report saved to: {report_path}")
        
        # Generate redownload script
        if self.results['files_needing_redownload']:
            self._generate_redownload_script()
        
        print("=" * 80)
        self.logger.info("VALIDATION COMPLETED")
    
    def _generate_redownload_script(self):
        """Generate a script to redownload problematic files."""
        script_path = self.data_dir.parent / "logs" / "redownload_needed.sh"
        
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write("#!/bin/bash\n")
            f.write("# Auto-generated script to re-download problematic files\n\n")
            
            for item in self.results['files_needing_redownload']:
                symbol = item['symbol']
                interval = item['interval']
                f.write(f"# Re-download {symbol} ({interval})\n")
                f.write(
                    f"python scripts/daily_update.py --symbols {symbol} "
                    f"--intervals {interval}\n\n"
                )
        
        # Make executable on Unix
        if os.name != 'nt':  # Not Windows
            os.chmod(script_path, 0o755)
        
        print(f"Re-download script saved to: {script_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate all market data files"
    )
    
    parser.add_argument(
        '--fix',
        action='store_true',
        help='Automatically fix issues where possible'
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Specific symbols to validate'
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
    
    # Create validation suite
    validator = DataValidationSuite(
        config_dir=args.config_dir,
        data_dir=args.data_dir,
        log_dir=args.log_dir
    )
    
    # Run validation
    try:
        validator.run(fix=args.fix, symbols=args.symbols)
    except KeyboardInterrupt:
        print("\n\nValidation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {str(e)}")
        logging.exception("Fatal error during validation")
        sys.exit(1)


if __name__ == "__main__":
    main()
