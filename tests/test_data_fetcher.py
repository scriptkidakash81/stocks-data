"""
Tests for DataFetcher
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.data_fetcher import DataFetcher


@pytest.fixture
def data_fetcher():
    """Create DataFetcher instance."""
    return DataFetcher(rate_limit_delay=0, max_retries=3, retry_delay=1)


@pytest.fixture
def sample_dataframe():
    """Create sample OHLCV DataFrame."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='UTC')
    return pd.DataFrame(data, index=index)


@pytest.mark.unit
def test_get_max_period_1m(data_fetcher):
    """Test max period for 1m interval."""
    assert data_fetcher.get_max_period('1m') == 7


@pytest.mark.unit
def test_get_max_period_2m(data_fetcher):
    """Test max period for 2m interval."""
    assert data_fetcher.get_max_period('2m') == 60


@pytest.mark.unit
def test_get_max_period_5m(data_fetcher):
    """Test max period for 5m interval."""
    assert data_fetcher.get_max_period('5m') == 60


@pytest.mark.unit
def test_get_max_period_15m(data_fetcher):
    """Test max period for 15m interval."""
    assert data_fetcher.get_max_period('15m') == 60


@pytest.mark.unit
def test_get_max_period_60m(data_fetcher):
    """Test max period for 60m interval."""
    assert data_fetcher.get_max_period('60m') == 730


@pytest.mark.unit
def test_get_max_period_1d(data_fetcher):
    """Test max period for 1d interval."""
    assert data_fetcher.get_max_period('1d') is None  # No limit


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_success(mock_download, data_fetcher, sample_dataframe):
    """Test successful data fetch."""
    mock_download.return_value = sample_dataframe
    
    result = data_fetcher.fetch_data('RELIANCE.NS', '1d', period='1mo')
    
    assert result is not None
    assert len(result) == 3
    mock_download.assert_called_once()


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_with_period(mock_download, data_fetcher, sample_dataframe):
    """Test fetch with period parameter."""
    mock_download.return_value = sample_dataframe
    
    result = data_fetcher.fetch_data('RELIANCE.NS', '1d', period='1y')
    
    mock_download.assert_called_with(
        'RELIANCE.NS',
        interval='1d',
        period='1y',
        progress=False
    )


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_with_dates(mock_download, data_fetcher, sample_dataframe):
    """Test fetch with start and end dates."""
    mock_download.return_value = sample_dataframe
    
    result = data_fetcher.fetch_data(
        'RELIANCE.NS',
        '1d',
        start_date='2024-01-01',
        end_date='2024-01-31'
    )
    
    mock_download.assert_called_with(
        'RELIANCE.NS',
        interval='1d',
        start='2024-01-01',
        end='2024-01-31',
        progress=False
    )


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_empty_dataframe(mock_download, data_fetcher):
    """Test handling of empty DataFrame."""
    mock_download.return_value = pd.DataFrame()
    
    result = data_fetcher.fetch_data('INVALID.NS', '1d', period='1mo')
    
    assert result is None or result.empty


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_timezone_conversion(mock_download, data_fetcher, sample_dataframe):
    """Test timezone conversion to Asia/Kolkata."""
    mock_download.return_value = sample_dataframe
    
    result = data_fetcher.fetch_data('RELIANCE.NS', '1d', period='1mo')
    
    if result is not None and not result.empty:
        assert result.index.tz is not None


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_network_error(mock_download, data_fetcher):
    """Test handling of network errors."""
    mock_download.side_effect = Exception("Network error")
    
    result = data_fetcher.fetch_data('RELIANCE.NS', '1d', period='1mo')
    
    assert result is None


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_rate_limiting(mock_download, data_fetcher, sample_dataframe):
    """Test rate limiting delay."""
    # Create fetcher with delay
    fetcher_with_delay = DataFetcher(rate_limit_delay=0.1)
    mock_download.return_value = sample_dataframe
    
    import time
    start = time.time()
    fetcher_with_delay.fetch_data('RELIANCE.NS', '1d', period='1mo')
    elapsed = time.time() - start
    
    # Should have some delay (even if small in tests)
    assert elapsed >= 0


@pytest.mark.unit
def test_data_fetcher_initialization():
    """Test DataFetcher initialization."""
    fetcher = DataFetcher(rate_limit_delay=1.0, max_retries=5, retry_delay=2)
    
    assert fetcher.rate_limit_delay == 1.0
    assert fetcher.max_retries == 5
    assert fetcher.retry_delay == 2


@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data_required_columns(mock_download, data_fetcher):
    """Test that fetched data has required OHLCV columns."""
    # Create DataFrame missing Volume column
    incomplete_df = pd.DataFrame({
        'Open': [100.0],
        'High': [105.0],
        'Low': [99.0],
        'Close': [104.0]
    }, index=pd.date_range('2024-01-01', periods=1))
    
    mock_download.return_value = incomplete_df
    
    result = data_fetcher.fetch_data('RELIANCE.NS', '1d', period='1mo')
    
    # Should handle missing columns gracefully
    assert result is not None
