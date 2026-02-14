"""
Tests for ConfigManager
"""

import pytest
import yaml
from pathlib import Path
from src.config_manager import ConfigManager, ConfigurationError


@pytest.fixture
def temp_config_dir(tmp_path):
    """Create temporary config directory with test files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    
    # Create config.yaml
    config_data = {
        'data_dir': './data',
        'log_dir': './logs',
        'intervals': ['1m', '5m', '1d'],
        'max_retries': 3,
        'retry_delay': 5,
        'chunk_size': 10,
        'validate_data': True,
        'timezone': 'Asia/Kolkata'
    }
    with open(config_dir / 'config.yaml', 'w') as f:
        yaml.dump(config_data, f)
    
    # Create stocks.yaml
    stocks_data = {
        'stocks': [
            {'symbol': 'RELIANCE.NS', 'name': 'Reliance Industries'},
            {'symbol': 'TCS.NS', 'name': 'Tata Consultancy Services'}
        ]
    }
    with open(config_dir / 'stocks.yaml', 'w') as f:
        yaml.dump(stocks_data, f)
    
    # Create indices.yaml
    indices_data = {
        'indices': [
            {'symbol': '^NSEI', 'name': 'Nifty 50'},
            {'symbol': '^NSEBANK', 'name': 'Bank Nifty'}
        ]
    }
    with open(config_dir / 'indices.yaml', 'w') as f:
        yaml.dump(indices_data, f)
    
    return config_dir


@pytest.mark.unit
def test_load_config_success(temp_config_dir):
    """Test successful configuration loading."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.config is not None
    assert cm.stocks is not None
    assert cm.indices is not None


@pytest.mark.unit
def test_get_stock_list(temp_config_dir):
    """Test getting stock list."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    stocks = cm.get_stock_list()
    assert len(stocks) == 2
    assert 'RELIANCE.NS' in stocks
    assert 'TCS.NS' in stocks


@pytest.mark.unit
def test_get_indices_list(temp_config_dir):
    """Test getting indices list."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    indices = cm.get_indices_list()
    assert len(indices) == 2
    assert '^NSEI' in indices
    assert '^NSEBANK' in indices


@pytest.mark.unit
def test_get_intervals(temp_config_dir):
    """Test getting intervals."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    intervals = cm.get_intervals()
    assert intervals == ['1m', '5m', '1d']


@pytest.mark.unit
def test_get_config_value(temp_config_dir):
    """Test getting specific config values."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.get_config('max_retries') == 3
    assert cm.get_config('timezone') == 'Asia/Kolkata'
    assert cm.get_config('nonexistent', 'default') == 'default'


@pytest.mark.unit
def test_get_data_dir(temp_config_dir):
    """Test getting data directory."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.get_data_dir() == './data'


@pytest.mark.unit
def test_get_timezone(temp_config_dir):
    """Test getting timezone."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.get_timezone() == 'Asia/Kolkata'


@pytest.mark.unit
def test_validate_config_success(temp_config_dir):
    """Test configuration validation passes."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    # Should not raise
    cm.validate_config()


@pytest.mark.unit
def test_missing_config_file(tmp_path):
    """Test error when config.yaml is missing."""
    config_dir = tmp_path / "empty_config"
    config_dir.mkdir()
    
    cm = ConfigManager(str(config_dir))
    
    with pytest.raises(ConfigurationError):
        cm.load_config()


@pytest.mark.unit
def test_malformed_yaml(tmp_path):
    """Test error with malformed YAML."""
    config_dir = tmp_path / "bad_config"
    config_dir.mkdir()
    
    # Create malformed YAML
    with open(config_dir / 'config.yaml', 'w') as f:
        f.write("invalid: yaml: content: [")
    
    cm = ConfigManager(str(config_dir))
    
    with pytest.raises(ConfigurationError):
        cm.load_config()


@pytest.mark.unit
def test_missing_required_field(tmp_path):
    """Test error when required field is missing."""
    config_dir = tmp_path / "incomplete_config"
    config_dir.mkdir()
    
    # Create config without required fields
    config_data = {
        'data_dir': './data'
        # Missing intervals and other required fields
    }
    with open(config_dir / 'config.yaml', 'w') as f:
        yaml.dump(config_data, f)
    
    # Create empty stocks and indices
    with open(config_dir / 'stocks.yaml', 'w') as f:
        yaml.dump({'stocks': []}, f)
    with open(config_dir / 'indices.yaml', 'w') as f:
        yaml.dump({'indices': []}, f)
    
    cm = ConfigManager(str(config_dir))
    cm.load_config()
    
    with pytest.raises(ConfigurationError):
        cm.validate_config()


@pytest.mark.unit
def test_get_max_retries(temp_config_dir):
    """Test getting max retries."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.get_max_retries() == 3


@pytest.mark.unit
def test_get_retry_delay(temp_config_dir):
    """Test getting retry delay."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.get_retry_delay() == 5


@pytest.mark.unit
def test_should_validate_data(temp_config_dir):
    """Test getting validate_data flag."""
    cm = ConfigManager(str(temp_config_dir))
    cm.load_config()
    
    assert cm.should_validate_data() is True
