"""
Common test fixtures and utilities
"""

import pytest
import pandas as pd
from pathlib import Path


@pytest.fixture
def sample_ohlcv_data():
    """Create sample OHLCV DataFrame."""
    data = {
        'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
        'High': [105.0, 106.0, 107.0, 108.0, 109.0],
        'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
        'Close': [104.0, 105.0, 106.0, 107.0, 108.0],
        'Volume': [1000, 1100, 1200, 1300, 1400]
    }
    index = pd.date_range(
        start='2024-01-01',
        periods=5,
        freq='1D',
        tz='Asia/Kolkata'
    )
    return pd.DataFrame(data, index=index)


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory structure."""
    data_dir = tmp_path / "data"
    stocks_dir = data_dir / "stocks"
    indices_dir = data_dir / "indices"
    metadata_dir = data_dir / "metadata"
    
    stocks_dir.mkdir(parents=True)
    indices_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)
    
    return {
        'data': data_dir,
        'stocks': stocks_dir,
        'indices': indices_dir,
        'metadata': metadata_dir
    }


@pytest.fixture
def temp_log_dir(tmp_path):
    """Create temporary log directory."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return log_dir


def create_test_csv(path, dataframe):
    """Helper to create test CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path)


def assert_dataframe_equal(df1, df2, check_dtype=True):
    """Helper to assert DataFrames are equal."""
    pd.testing.assert_frame_equal(df1, df2, check_dtype=check_dtype)
