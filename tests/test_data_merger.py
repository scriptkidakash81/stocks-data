"""
Tests for DataMerger
"""

import pytest
import pandas as pd
from pathlib import Path
from src.data_merger import DataMerger


@pytest.fixture
def merger():
    """Create DataMerger instance."""
    return DataMerger(backup_enabled=False)  # Disable backups for tests


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='Asia/Kolkata')
    return pd.DataFrame(data, index=index)


@pytest.mark.unit
def test_load_existing_data_file_exists(merger, sample_dataframe, tmp_path):
    """Test loading existing CSV file."""
    csv_path = tmp_path / "test.csv"
    sample_dataframe.to_csv(csv_path)
    
    loaded_df = merger.load_existing_data(csv_path)
    
    assert loaded_df is not None
    assert len(loaded_df) == 3


@pytest.mark.unit
def test_load_existing_data_file_missing(merger, tmp_path):
    """Test loading when file doesn't exist."""
    csv_path = tmp_path / "nonexistent.csv"
    
    loaded_df = merger.load_existing_data(csv_path)
    
    assert loaded_df is None or loaded_df.empty


@pytest.mark.unit
def test_merge_data_append_new(merger):
    """Test merging with new data."""
    existing_data = {
        'Open': [100.0, 101.0],
        'High': [105.0, 106.0],
        'Low': [99.0, 100.0],
        'Close': [104.0, 105.0],
        'Volume': [1000, 1100]
    }
    existing_index = pd.date_range(start='2024-01-01', periods=2, freq='1D', tz='Asia/Kolkata')
    existing_df = pd.DataFrame(existing_data, index=existing_index)
    
    new_data = {
        'Open': [102.0],
        'High': [107.0],
        'Low': [101.0],
        'Close': [106.0],
        'Volume': [1200]
    }
    new_index = pd.date_range(start='2024-01-03', periods=1, freq='1D', tz='Asia/Kolkata')
    new_df = pd.DataFrame(new_data, index=new_index)
    
    merged_df = merger.merge_data(existing_df, new_df)
    
    assert len(merged_df) == 3
    assert merged_df.index.is_monotonic_increasing


@pytest.mark.unit
def test_merge_data_remove_duplicates(merger):
    """Test merging removes duplicates."""
    existing_data = {
        'Open': [100.0, 101.0],
        'High': [105.0, 106.0],
        'Low': [99.0, 100.0],
        'Close': [104.0, 105.0],
        'Volume': [1000, 1100]
    }
    existing_index = pd.date_range(start='2024-01-01', periods=2, freq='1D', tz='Asia/Kolkata')
    existing_df = pd.DataFrame(existing_data, index=existing_index)
    
    # New data with overlapping date
    new_data = {
        'Open': [101.5, 102.0],
        'High': [106.5, 107.0],
        'Low': [100.5, 101.0],
        'Close': [105.5, 106.0],
        'Volume': [1150, 1200]
    }
    new_index = pd.DatetimeIndex(['2024-01-02', '2024-01-03'], tz='Asia/Kolkata')
    new_df = pd.DataFrame(new_data, index=new_index)
    
    merged_df = merger.merge_data(existing_df, new_df)
    
    # Should not have duplicates
    assert not merged_df.index.duplicated().any()
    # Should prefer newer data (from new_df)
    assert merged_df.loc['2024-01-02', 'Open'] == 101.5


@pytest.mark.unit
def test_save_data(merger, sample_dataframe, tmp_path):
    """Test saving DataFrame to CSV."""
    csv_path = tmp_path / "test.csv"
    
    merger.save_data(sample_dataframe, csv_path)
    
    assert csv_path.exists()
    # Verify can be loaded back
    loaded_df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    assert len(loaded_df) == 3


@pytest.mark.unit
def test_get_last_timestamp_exists(merger, sample_dataframe, tmp_path):
    """Test getting last timestamp from existing file."""
    csv_path = tmp_path / "test.csv"
    sample_dataframe.to_csv(csv_path)
    
    last_ts = merger.get_last_timestamp(csv_path)
    
    assert last_ts is not None


@pytest.mark.unit
def test_get_last_timestamp_missing_file(merger, tmp_path):
    """Test getting last timestamp from missing file."""
    csv_path = tmp_path / "nonexistent.csv"
    
    last_ts = merger.get_last_timestamp(csv_path)
    
    assert last_ts is None


@pytest.mark.unit
def test_merge_empty_existing(merger, sample_dataframe):
    """Test merging when existing data is empty."""
    existing_df = pd.DataFrame()
    
    merged_df = merger.merge_data(existing_df, sample_dataframe)
    
    assert len(merged_df) == 3
    assert merged_df.equals(sample_dataframe)


@pytest.mark.unit
def test_merge_sorted_output(merger):
    """Test that merged data is always sorted."""
    # Create unsorted existing data
    existing_data = {
        'Open': [102.0, 100.0],
        'High': [107.0, 105.0],
        'Low': [101.0, 99.0],
        'Close': [106.0, 104.0],
        'Volume': [1200, 1000]
    }
    existing_index = pd.DatetimeIndex(['2024-01-03', '2024-01-01'], tz='Asia/Kolkata')
    existing_df = pd.DataFrame(existing_data, index=existing_index)
    
    new_data = {
        'Open': [101.0],
        'High': [106.0],
        'Low': [100.0],
        'Close': [105.0],
        'Volume': [1100]
    }
    new_index = pd.DatetimeIndex(['2024-01-02'], tz='Asia/Kolkata')
    new_df = pd.DataFrame(new_data, index=new_index)
    
    merged_df = merger.merge_data(existing_df, new_df)
    
    assert merged_df.index.is_monotonic_increasing
