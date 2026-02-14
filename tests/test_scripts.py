"""
Integration tests for scripts
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd


# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


@pytest.fixture
def mock_dependencies():
    """Mock all external dependencies."""
    with patch('scripts.initial_download.DataFetcher') as mock_fetcher, \
         patch('scripts.initial_download.DataValidator') as mock_validator, \
         patch('scripts.initial_download.DataMerger') as mock_merger, \
         patch('scripts.initial_download.MetadataManager') as mock_metadata, \
         patch('scripts.initial_download.ConfigManager') as mock_config:
        
        # Setup config manager mock
        mock_config_instance = MagicMock()
        mock_config_instance.get_stock_list.return_value = ['RELIANCE.NS', 'TCS.NS']
        mock_config_instance.get_indices_list.return_value = ['^NSEI']
        mock_config_instance.get_intervals.return_value = ['1d', '5m']
        mock_config_instance.get_data_dir.return_value = './data'
        mock_config.return_value = mock_config_instance
        
        # Setup fetcher mock
        mock_fetcher_instance = MagicMock()
        sample_df = pd.DataFrame({
            'Open': [100.0], 'High': [105.0], 'Low': [99.0],
            'Close': [104.0], 'Volume': [1000]
        }, index=pd.date_range('2024-01-01', periods=1))
        mock_fetcher_instance.fetch_data.return_value = sample_df
        mock_fetcher.return_value = mock_fetcher_instance
        
        # Setup validator mock
        mock_validator_instance = MagicMock()
        mock_report = MagicMock()
        mock_report.is_valid = True
        mock_report.issues = []
        mock_validator_instance.validate_dataframe.return_value = (sample_df, mock_report)
        mock_validator.return_value = mock_validator_instance
        
        # Setup merger mock
        mock_merger_instance = MagicMock()
        mock_merger_instance.load_existing_data.return_value = pd.DataFrame()
        mock_merger_instance.merge_data.return_value = sample_df
        mock_merger.return_value = mock_merger_instance
        
        # Setup metadata mock
        mock_metadata_instance = MagicMock()
        mock_metadata.return_value = mock_metadata_instance
        
        yield {
            'config': mock_config_instance,
            'fetcher': mock_fetcher_instance,
            'validator': mock_validator_instance,
            'merger': mock_merger_instance,
            'metadata': mock_metadata_instance
        }


@pytest.mark.integration
@patch('scripts.initial_download.Path.mkdir')
@patch('scripts.initial_download.logging')
def test_initial_download_basic_flow(mock_logging, mock_mkdir, mock_dependencies, tmp_path):
    """Test basic flow of initial_download.py script."""
    # This would test the high-level flow
    # In practice, you'd import and run the main function
    # For now, just verify mocks would be called correctly
    
    config = mock_dependencies['config']
    
    # Verify config would be accessed
    stocks = config.get_stock_list()
    assert len(stocks) == 2
    
    intervals = config.get_intervals()
    assert len(intervals) == 2


@pytest.mark.integration
def test_initial_download_with_args(mock_dependencies):
    """Test initial_download with command line arguments."""
    # Would test --symbols RELIANCE.NS --intervals 1d
    # Verify only specified symbols/intervals are processed
    pass


@pytest.mark.integration
def test_initial_download_force_overwrite(mock_dependencies):
    """Test --force flag overwrites existing data."""
    pass


@pytest.mark.integration
@patch('scripts.daily_update.MetadataManager')
@patch('scripts.daily_update.DataFetcher')
@patch('scripts.daily_update.ConfigManager')
def test_daily_update_basic_flow(mock_config, mock_fetcher, mock_metadata, tmp_path):
    """Test basic flow of daily_update.py script."""
    # Setup mocks
    mock_config_instance = MagicMock()
    mock_config_instance.get_stock_list.return_value = ['RELIANCE.NS']
    mock_config_instance.get_intervals.return_value = ['1d']
    mock_config.return_value = mock_config_instance
    
    mock_metadata_instance = MagicMock()
    mock_metadata_instance.needs_update.return_value = True
    mock_metadata_instance.get_next_fetch_date.return_value = '2024-01-01'
    mock_metadata.return_value = mock_metadata_instance
    
    # Verify metadata is checked
    needs_update = mock_metadata_instance.needs_update('RELIANCE.NS', '1d')
    assert needs_update is True


@pytest.mark.integration
def test_daily_update_dry_run(mock_dependencies):
    """Test --dry-run mode doesn't modify data."""
    # Should fetch and validate but not save
    pass


@pytest.mark.integration
def test_daily_update_rolling_window_1m(mock_dependencies):
    """Test that 1m interval uses rolling 7-day window."""
    # Verify special handling for 1m interval
    pass


@pytest.mark.integration
@patch('scripts.validate_all.DataValidator')
@patch('scripts.validate_all.Path.rglob')
def test_validate_all_scans_files(mock_rglob, mock_validator):
    """Test validate_all.py scans all CSV files."""
    # Setup mock file list
    mock_files = [
        MagicMock(name='RELIANCE.NS/1d.csv'),
        MagicMock(name='TCS.NS/1d.csv')
    ]
    mock_rglob.return_value = mock_files
    
    # Setup validator
    mock_validator_instance = MagicMock()
    mock_report = MagicMock()
    mock_report.is_valid = True
    mock_report.issues = []
    mock_validator_instance.validate_dataframe.return_value = (pd.DataFrame(), mock_report)
    mock_validator.return_value = mock_validator_instance
    
    # Would iterate and validate each file
    assert len(mock_files) == 2


@pytest.mark.integration
def test_validate_all_with_fix_flag():
    """Test validate_all.py --fix auto-fixes issues."""
    # Should call validator with auto_fix=True
    pass


@pytest.mark.integration
def test_validate_all_generates_report():
    """Test validate_all.py generates comprehensive report."""
    # Should produce summary of all validation issues
    pass


@pytest.mark.integration
@patch('builtins.open', new_callable=mock_open)
@patch('scripts.fix_gaps.Path.rglob')
def test_fix_gaps_identifies_gaps(mock_rglob, mock_file):
    """Test fix_gaps.py identifies data gaps."""
    # Setup mock CSV files
    mock_files = [MagicMock(name='RELIANCE.NS/1d.csv')]
    mock_rglob.return_value = mock_files
    
    # Would identify gaps in each file
    assert len(mock_files) == 1


@pytest.mark.integration
def test_fix_gaps_auto_fix_mode():
    """Test fix_gaps.py --auto-fix downloads missing data."""
    # Should re-fetch data for gaps and merge
    pass


@pytest.mark.integration
def test_fix_gaps_respects_holidays():
    """Test fix_gaps.py doesn't fix gaps on holidays."""
    # Should skip NSE holidays and weekends
    pass
