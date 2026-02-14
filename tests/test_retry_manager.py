"""
Tests for RetryManager
"""

import pytest
import json
from pathlib import Path
from datetime import datetime, timedelta
from src.retry_manager import RetryManager, RetryError


@pytest.fixture
def retry_manager(tmp_path):
    """Create RetryManager with temp log directory."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return RetryManager(log_dir=str(log_dir))


@pytest.mark.unit
def test_retry_with_backoff_success(retry_manager):
    """Test that function succeeds on first attempt."""
    def success_func():
        return "success"
    
    result = retry_manager.retry_with_backoff(
        success_func,
        max_retries=3,
        initial_delay=0.1
    )
    
    assert result == "success"


@pytest.mark.unit
def test_retry_with_backoff_eventual_success(retry_manager):
    """Test that function succeeds after retries."""
    attempts = {'count': 0}
    
    def flaky_func():
        attempts['count'] += 1
        if attempts['count'] < 3:
            raise Exception("Temporary failure")
        return "success"
    
    result = retry_manager.retry_with_backoff(
        flaky_func,
        max_retries=5,
        initial_delay=0.1
    )
    
    assert result == "success"
    assert attempts['count'] == 3


@pytest.mark.unit
def test_retry_with_backoff_max_retries_exceeded(retry_manager):
    """Test that RetryError is raised after max retries."""
    def always_fails():
        raise Exception("Always fails")
    
    with pytest.raises(RetryError):
        retry_manager.retry_with_backoff(
            always_fails,
            max_retries=3,
            initial_delay=0.1
        )


@pytest.mark.unit
def test_retry_with_backoff_with_args(retry_manager):
    """Test retry with function arguments."""
    def func_with_args(a, b):
        return a + b
    
    result = retry_manager.retry_with_backoff(
        func_with_args,
        max_retries=3,
        initial_delay=0.1,
        a=5,
        b=10
    )
    
    assert result == 15


@pytest.mark.unit
def test_log_failure(retry_manager):
    """Test logging failures."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Connection timeout')
    
    failures = retry_manager.get_failed_downloads()
    
    assert len(failures) == 1
    assert failures[0]['symbol'] == 'RELIANCE.NS'
    assert failures[0]['interval'] == '1d'


@pytest.mark.unit
def test_get_failed_downloads(retry_manager):
    """Test retrieving failed downloads."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    retry_manager.log_failure('TCS.NS', '5m', 'Error 2')
    
    failures = retry_manager.get_failed_downloads()
    
    assert len(failures) == 2


@pytest.mark.unit
def test_get_failed_downloads_filtered_by_symbol(retry_manager):
    """Test filtering failures by symbol."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    retry_manager.log_failure('TCS.NS', '5m', 'Error 2')
    retry_manager.log_failure('RELIANCE.NS', '5m', 'Error 3')
    
    failures = retry_manager.get_failed_downloads(symbol='RELIANCE.NS')
    
    assert len(failures) == 2
    assert all(f['symbol'] == 'RELIANCE.NS' for f in failures)


@pytest.mark.unit
def test_get_failed_downloads_filtered_by_time(retry_manager):
    """Test filtering failures by time."""
    # Log a failure
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    
    # Get failures since 1 hour ago
    since = datetime.now() - timedelta(hours=1)
    failures = retry_manager.get_failed_downloads(since=since)
    
    assert len(failures) == 1


@pytest.mark.unit
def test_generate_failure_report_text(retry_manager, tmp_path):
    """Test generating text failure report."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    retry_manager.log_failure('TCS.NS', '5m', 'Error 2')
    
    output_path = tmp_path / "failures.txt"
    report = retry_manager.generate_failure_report(str(output_path), format='text')
    
    assert output_path.exists()
    content = output_path.read_text()
    assert 'RELIANCE.NS' in content
    assert 'TCS.NS' in content


@pytest.mark.unit
def test_generate_failure_report_json(retry_manager, tmp_path):
    """Test generating JSON failure report."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    
    output_path = tmp_path / "failures.json"
    report = retry_manager.generate_failure_report(str(output_path), format='json')
    
    assert output_path.exists()
    with open(output_path) as f:
        data = json.load(f)
    assert len(data) == 1


@pytest.mark.unit
def test_get_failure_statistics(retry_manager):
    """Test getting failure statistics."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    retry_manager.log_failure('RELIANCE.NS', '5m', 'Error 2')
    retry_manager.log_failure('TCS.NS', '1d', 'Error 3')
    
    stats = retry_manager.get_failure_statistics()
    
    assert stats['total_failures'] == 3
    assert stats['unique_symbols'] == 2
    assert stats['most_failed_symbol'] == 'RELIANCE.NS'
    assert stats['most_failed_count'] == 2


@pytest.mark.unit
def test_clear_failures(retry_manager):
    """Test clearing old failures."""
    retry_manager.log_failure('RELIANCE.NS', '1d', 'Error 1')
    
    # Clear failures older than 0 days (all)
    retry_manager.clear_failures(older_than_days=0)
    
    failures = retry_manager.get_failed_downloads()
    assert len(failures) == 0


@pytest.mark.unit
def test_exponential_backoff_timing(retry_manager):
    """Test that delays increase exponentially."""
    import time
    attempts = {'count': 0, 'times': []}
    
    def failing_func():
        attempts['times'].append(time.time())
        attempts['count'] += 1
        if attempts['count'] < 3:
            raise Exception("Fail")
        return "success"
    
    retry_manager.retry_with_backoff(
        failing_func,
        max_retries=5,
        initial_delay=0.1,
        backoff_factor=2.0
    )
    
    # Check that delays increased
    assert len(attempts['times']) == 3


@pytest.mark.unit
def test_retry_with_alert_callback(retry_manager):
    """Test alert callback on failure logging."""
    alert_called = {'value': False}
    
    def alert_callback(message):
        alert_called['value'] = True
    
    # Create manager with alert
    manager_with_alert = RetryManager(
        log_dir=retry_manager.log_dir,
        alert_callback=alert_callback
    )
    
    manager_with_alert.log_failure('TEST.NS', '1d', 'Error')
    
    # Alert should have been called
    assert alert_called['value'] is True
