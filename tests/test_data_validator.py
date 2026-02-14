"""
Tests for DataValidator
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.data_validator import DataValidator, ValidationReport


@pytest.fixture
def validator():
    """Create DataValidator instance."""
    return DataValidator(timezone='Asia/Kolkata')


@pytest.fixture
def valid_dataframe():
    """Create valid OHLCV DataFrame."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01 09:15:00', periods=3, freq='1D', tz='Asia/Kolkata')
    return pd.DataFrame(data, index=index)


@pytest.mark.unit
def test_validate_valid_dataframe(validator, valid_dataframe):
    """Test validation of valid DataFrame."""
    validated_df, report = validator.validate_dataframe(valid_dataframe, '1d')
    
    assert report.is_valid is True
    assert len(report.issues) == 0


@pytest.mark.unit
def test_detect_duplicates(validator):
    """Test detection of duplicate timestamps."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    # Create duplicates
    index = pd.DatetimeIndex([
        '2024-01-01 09:15:00',
        '2024-01-01 09:15:00',  # Duplicate
        '2024-01-02 09:15:00'
    ], tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False
    assert any(issue['category'] == 'duplicates' for issue in report.issues)


@pytest.mark.unit
def test_fix_duplicates(validator):
    """Test fixing duplicate timestamps."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.DatetimeIndex([
        '2024-01-01 09:15:00',
        '2024-01-01 09:15:00',
        '2024-01-02 09:15:00'
    ], tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d', auto_fix=True)
    
    # Should remove duplicates
    assert not validated_df.index.duplicated().any()
    assert len(validated_df) == 2


@pytest.mark.unit
def test_detect_null_values(validator):
    """Test detection of null values."""
    data = {
        'Open': [100.0, np.nan, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False
    assert any(issue['category'] == 'nulls' for issue in report.issues)


@pytest.mark.unit
def test_detect_negative_prices(validator):
    """Test detection of negative prices."""
    data = {
        'Open': [100.0, -101.0, 102.0],  # Negative price
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False


@pytest.mark.unit
def test_detect_ohlc_logic_violation(validator):
    """Test detection of OHLC logic violations."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [99.0, 106.0, 107.0],  # High < Open - invalid!
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False
    assert any(issue['category'] == 'ohlc_logic' for issue in report.issues)


@pytest.mark.unit
def test_detect_negative_volume(validator):
    """Test detection of negative volume."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, -1100, 1200]  # Negative volume
    }
    index = pd.date_range(start='2024-01-01', periods=3, freq='1D', tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False
    assert any(issue['category'] == 'volume' for issue in report.issues)


@pytest.mark.unit
def test_check_sorting(validator):
    """Test detection of unsorted data."""
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    # Unsorted index
    index = pd.DatetimeIndex([
        '2024-01-03',
        '2024-01-01',
        '2024-01-02'
    ], tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d', auto_fix=True)
    
    # Should be sorted after fix
    assert validated_df.index.is_monotonic_increasing


@pytest.mark.unit
def test_check_gaps(validator):
    """Test gap detection."""
    # Create data with a gap
    dates = pd.DatetimeIndex([
        '2024-01-01 09:15:00',
        '2024-01-01 09:20:00',
        '2024-01-01 09:30:00'  # Gap - missing 09:25
    ], tz='Asia/Kolkata')
    
    data = {
        'Open': [100.0, 101.0, 102.0],
        'High': [105.0, 106.0, 107.0],
        'Low': [99.0, 100.0, 101.0],
        'Close': [104.0, 105.0, 106.0],
        'Volume': [1000, 1100, 1200]
    }
    df = pd.DataFrame(data, index=dates)
    
    gaps = validator.check_gaps(df, '5m')
    
    assert len(gaps) > 0


@pytest.mark.unit
def test_required_columns_missing(validator):
    """Test detection of missing required columns."""
    # DataFrame missing Volume column
    data = {
        'Open': [100.0, 101.0],
        'High': [105.0, 106.0],
        'Low': [99.0, 100.0],
        'Close': [104.0, 105.0]
    }
    index = pd.date_range(start='2024-01-01', periods=2, freq='1D', tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d')
    
    assert report.is_valid is False


@pytest.mark.unit
def test_validation_report_structure():
    """Test ValidationReport structure."""
    report = ValidationReport(is_valid=True, issues=[])
    
    assert hasattr(report, 'is_valid')
    assert hasattr(report, 'issues')
    assert report.is_valid is True
    assert isinstance(report.issues, list)


@pytest.mark.unit
def test_auto_fix_multiple_issues(validator):
    """Test auto-fixing multiple issues at once."""
    data = {
        'Open': [100.0, 101.0, 102.0, 101.0],
        'High': [105.0, 106.0, 107.0, 106.0],
        'Low': [99.0, 100.0, 101.0, 100.0],
        'Close': [104.0, 105.0, 106.0, 105.0],
        'Volume': [1000, 1100, 1200, 1100]
    }
    # Unsorted with duplicates
    index = pd.DatetimeIndex([
        '2024-01-03',
        '2024-01-01',
        '2024-01-02',
        '2024-01-01'  # Duplicate
    ], tz='Asia/Kolkata')
    df = pd.DataFrame(data, index=index)
    
    validated_df, report = validator.validate_dataframe(df, '1d', auto_fix=True)
    
    # Should be sorted and deduplicated
    assert validated_df.index.is_monotonic_increasing
    assert not validated_df.index.duplicated().any()
