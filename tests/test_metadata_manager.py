"""
Tests for MetadataManager
"""

import pytest
import json
from pathlib import Path
from datetime import datetime, timedelta
from src.metadata_manager import MetadataManager


@pytest.fixture
def metadata_manager(tmp_path):
    """Create MetadataManager with temp directory."""
    metadata_dir = tmp_path / "metadata"
    return MetadataManager(metadata_dir=str(metadata_dir))


@pytest.mark.unit
def test_load_metadata_file_exists(metadata_manager, tmp_path):
    """Test loading existing metadata file."""
    # Create metadata file
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(exist_ok=True)
    
    metadata = {
        'symbol': 'RELIANCE.NS',
        'interval': '1d',
        'last_update': '2024-01-15T20:00:00',
        'total_rows': 100
    }
    
    metadata_file = metadata_dir / "RELIANCE.NS_1d.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)
    
    loaded = metadata_manager.load_metadata('RELIANCE.NS', '1d')
    
    assert loaded['symbol'] == 'RELIANCE.NS'
    assert loaded['total_rows'] == 100


@pytest.mark.unit
def test_load_metadata_file_missing(metadata_manager):
    """Test loading metadata when file doesn't exist."""
    metadata = metadata_manager.load_metadata('NONEXISTENT.NS', '1d')
    
    # Should return default metadata
    assert metadata is not None
    assert metadata['symbol'] == 'NONEXISTENT.NS'
    assert metadata['interval'] == '1d'


@pytest.mark.unit
def test_update_metadata(metadata_manager):
    """Test updating metadata."""
    stats = {
        'total_rows': 150,
        'rows_added': 50,
        'date_range': {
            'start': '2024-01-01',
            'end': '2024-01-15'
        }
    }
    
    validation_report = {
        'status': 'passed',
        'issues_count': 0
    }
    
    metadata_manager.update_metadata('RELIANCE.NS', '1d', stats, validation_report)
    
    # Load and verify
    loaded = metadata_manager.load_metadata('RELIANCE.NS', '1d')
    assert loaded['total_rows'] == 150
    assert loaded['date_range']['end'] == '2024-01-15'


@pytest.mark.unit
def test_get_last_update(metadata_manager):
    """Test getting last update timestamp."""
    # Update metadata first
    stats = {'total_rows': 100}
    metadata_manager.update_metadata('RELIANCE.NS', '1d', stats)
    
    last_update = metadata_manager.get_last_update('RELIANCE.NS', '1d')
    
    assert last_update is not None
    assert isinstance(last_update, datetime)


@pytest.mark.unit
def test_get_last_update_no_metadata(metadata_manager):
    """Test getting last update when no metadata exists."""
    last_update = metadata_manager.get_last_update('NONEXISTENT.NS', '1d')
    
    assert last_update is None


@pytest.mark.unit
def test_needs_update_recent(metadata_manager):
    """Test needs_update returns False for recent updates."""
    # Update metadata with recent timestamp
    stats = {'total_rows': 100}
    metadata_manager.update_metadata('RELIANCE.NS', '1d', stats)
    
    needs_update = metadata_manager.needs_update('RELIANCE.NS', '1d', max_age_hours=24)
    
    assert needs_update is False


@pytest.mark.unit
def test_needs_update_old(metadata_manager, tmp_path):
    """Test needs_update returns True for old updates."""
    # Create old metadata
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(exist_ok=True)
    
    old_time = datetime.now() - timedelta(hours=48)
    metadata = {
        'symbol': 'RELIANCE.NS',
        'interval': '1d',
        'last_update': old_time.isoformat(),
        'total_rows': 100
    }
    
    metadata_file = metadata_dir / "RELIANCE.NS_1d.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)
    
    needs_update = metadata_manager.needs_update('RELIANCE.NS', '1d', max_age_hours=24)
    
    assert needs_update is True


@pytest.mark.unit
def test_get_next_fetch_date(metadata_manager, tmp_path):
    """Test calculating next fetch date."""
    # Create metadata with date range
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(exist_ok=True)
    
    metadata = {
        'symbol': 'RELIANCE.NS',
        'interval': '1d',
        'date_range': {
            'start': '2024-01-01',
            'end': '2024-01-15'
        }
    }
    
    metadata_file = metadata_dir / "RELIANCE.NS_1d.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)
    
    next_date = metadata_manager.get_next_fetch_date('RELIANCE.NS', '1d')
    
    assert next_date is not None


@pytest.mark.unit
def test_metadata_file_structure(metadata_manager):
    """Test that saved metadata has correct structure."""
    stats = {
        'total_rows': 100,
        'rows_added': 10,
        'date_range': {'start': '2024-01-01', 'end': '2024-01-10'}
    }
    validation_report = {'status': 'passed', 'issues_count': 0}
    
    metadata_manager.update_metadata('TEST.NS', '1d', stats, validation_report)
    
    # Load raw file and check structure
    metadata = metadata_manager.load_metadata('TEST.NS', '1d')
    
    assert 'symbol' in metadata
    assert 'interval' in metadata
    assert 'last_update' in metadata
    assert 'total_rows' in metadata
    assert 'date_range' in metadata
    assert 'validation' in metadata


@pytest.mark.unit
def test_download_history_tracking(metadata_manager):
    """Test that download history is tracked."""
    stats = {'total_rows': 100, 'rows_added': 100}
    metadata_manager.update_metadata('TEST.NS', '1d', stats)
    
    # Update again
    stats = {'total_rows': 110, 'rows_added': 10}
    metadata_manager.update_metadata('TEST.NS', '1d', stats)
    
    metadata = metadata_manager.load_metadata('TEST.NS', '1d')
    
    assert 'download_history' in metadata
    assert len(metadata['download_history']) >= 1
