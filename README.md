# Market Data Pipeline

A robust, production-ready Python pipeline for fetching, processing, and storing historical market data from Yahoo Finance for Indian markets (NSE stocks and indices).

![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

## 🎯 Overview

This project provides a comprehensive solution for:
- **Downloading** maximum available historical data for NSE 200 stocks and major Indian indices
- **Incremental updates** with smart date range calculation
- **Data validation** with automatic quality checks and fixes
- **Gap detection and filling** with market holiday awareness
- **Automated workflows** via GitHub Actions for daily updates at 8 PM IST
- **Retry logic** with exponential backoff for resilient data fetching
- **Metadata tracking** for all downloads with update history

## ✨ Features

### Core Capabilities
- ✅ **200+ NSE Stocks** - Pre-configured with NSE 200 symbols
- ✅ **Multiple Intervals** - 1m, 2m, 5m, 15m, 60m, 1d
- ✅ **Smart Updates** - Incremental downloads since last update
- ✅ **Rolling Windows** - 7-day rolling window for 1-minute data
- ✅ **Data Validation** - Automatic OHLCV validation with auto-fix
- ✅ **Gap Detection** - Identifies and fills data gaps
- ✅ **Holiday Detection** - NSE market holiday calendar built-in
- ✅ **Failure Tracking** - Comprehensive retry and failure logging

### Automation
- ✅ **GitHub Actions** - Automated daily updates
- ✅ **Cron Support** - Linux/Windows scheduling examples
- ✅ **Docker Ready** - Can run in containerized environments

## 📦 Installation

### Prerequisites
- Python 3.11+
- pip package manager
- Git

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd market-data-pipeline
```

2. **Create virtual environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
pip install tqdm  # For progress bars (optional)
```

4. **Verify installation**
```bash
python -c "import yfinance; import pandas; print('✓ All dependencies installed')"
```

## 🚀 Usage

### Initial Setup

1. **Configure symbols** (optional - defaults provided)
   - Edit `config/stocks.yaml` for custom stock list
   - Edit `config/indices.yaml` for custom indices
   - Edit `config/config.yaml` for pipeline settings

2. **Initial data download**
```bash
# Download all configured symbols and intervals
python scripts/initial_download.py

# Download specific symbols only
python scripts/initial_download.py --symbols RELIANCE.NS TCS.NS

# Download specific intervals only
python scripts/initial_download.py --intervals 1d 5m

# Force overwrite existing data
python scripts/initial_download.py --force
```

**⏱️ Time estimate:** 15-30 minutes for all 200+ symbols across 6 intervals

### Daily Updates

```bash
# Regular incremental update
python scripts/daily_update.py

# Dry run (preview without downloading)
python scripts/daily_update.py --dry-run

# Update specific date
python scripts/daily_update.py --date 2024-01-15

# Update specific symbols
python scripts/daily_update.py --symbols RELIANCE.NS TCS.NS
```

### Data Validation

```bash
# Validate all data files
python scripts/validate_all.py

# Auto-fix issues
python scripts/validate_all.py --fix

# Validate specific symbols
python scripts/validate_all.py --symbols RELIANCE.NS
```

### Gap Fixing

```bash
# Analyze gaps (no fixing)
python scripts/fix_gaps.py

# Auto-fix gaps
python scripts/fix_gaps.py --auto-fix

# Dry run
python scripts/fix_gaps.py --auto-fix --dry-run
```

## 📁 Data Structure

```
market-data-pipeline/
├── config/
│   ├── config.yaml          # Main configuration
│   ├── stocks.yaml          # Stock symbols (NSE 200)
│   └── indices.yaml         # Index symbols
├── data/
│   ├── stocks/
│   │   ├── RELIANCE.NS/
│   │   │   ├── 1m.csv
│   │   │   ├── 5m.csv
│   │   │   ├── 1d.csv
│   │   │   └── ...
│   │   └── ...
│   ├── indices/
│   │   ├── _NSEI/           # ^NSEI sanitized
│   │   │   ├── 1m.csv
│   │   │   └── ...
│   │   └── ...
│   └── metadata/
│       ├── RELIANCE.NS_1d.json
│       └── ...
├── logs/
│   ├── initial_download_*.log
│   ├── daily_update_*.log
│   └── validation_*.log
├── scripts/
│   ├── initial_download.py
│   ├── daily_update.py
│   ├── validate_all.py
│   └── fix_gaps.py
└── src/
    ├── config_manager.py
    ├── data_fetcher.py
    ├── data_validator.py
    ├── data_merger.py
    ├── metadata_manager.py
    └── retry_manager.py
```

### CSV Format

All data files follow this structure:

| Datetime | Open | High | Low | Close | Volume |
|----------|------|------|-----|-------|--------|
| 2024-01-15 09:15:00+05:30 | 2850.50 | 2855.00 | 2848.00 | 2852.75 | 125000 |

- **Datetime**: Timezone-aware (Asia/Kolkata)
- **OHLCV**: Standard candlestick data
- **Index**: Sorted chronologically

## ⚙️ Configuration

### Main Configuration (`config/config.yaml`)

```yaml
data_dir: "./data"
log_dir: "./logs"
intervals: [1m, 2m, 5m, 15m, 60m, 1d]
max_retries: 3
retry_delay: 5
chunk_size: 10
validate_data: true
timezone: "Asia/Kolkata"
```

### Adding Stocks (`config/stocks.yaml`)

```yaml
stocks:
  - symbol: "NEWSTOCK.NS"
    name: "New Stock Ltd"
```

### API Limits

See [docs/api_limits.md](docs/api_limits.md) for Yahoo Finance rate limits and best practices.

## 🤖 GitHub Actions Setup

The pipeline includes automated daily updates via GitHub Actions.

### Quick Setup

1. Push code to GitHub repository
2. Go to **Settings** → **Actions** → **General**
3. Enable workflows
4. Workflow runs daily at 8:00 PM IST automatically

### Manual Trigger

1. Go to **Actions** tab
2. Select "Daily Data Update"
3. Click "Run workflow"
4. Optional: specify symbols, intervals, or enable dry-run

See [docs/GITHUB_ACTIONS_SETUP.md](docs/GITHUB_ACTIONS_SETUP.md) for detailed configuration.

## 🔧 Troubleshooting

### Common Issues

**1. Import Error: No module named 'yfinance'**
```bash
pip install -r requirements.txt
```

**2. Permission Denied (git push)**
- Check repository write permissions
- For GitHub Actions, ensure workflow has `contents: write` permission

**3. Rate Limit Errors**
- yfinance has rate limits (~2000 requests/hour)
- Use `--symbols` to download in batches
- Add delays between requests (configured in data_fetcher.py)

**4. Empty Data Returned**
- Check if market is open (weekends/holidays have no data)
- Verify symbol format (NSE: `.NS`, BSE: `.BO`)
- Try with `period="1d"` to test connectivity

**5. Validation Errors**
- Run with `--fix` flag to auto-correct issues
- Check logs for detailed error messages
- Re-download problematic files using fix_gaps.py

### Debug Mode

Enable detailed logging:
```python
# In any script, add at the top:
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Getting Help

1. Check logs in `logs/` directory
2. Review validation reports
3. Check [docs/](docs/) for detailed documentation
4. Open an issue on GitHub

## 📊 Architecture

The pipeline uses a modular architecture:

- **ConfigManager**: YAML configuration loading and validation
- **DataFetcher**: Yahoo Finance API integration with rate limiting
- **DataValidator**: OHLCV validation and quality checks
- **DataMerger**: Smart data merging with deduplication
- **MetadataManager**: Download history and statistics tracking
- **RetryManager**: Failure handling with exponential backoff

See [docs/architecture.md](docs/architecture.md) for detailed design.

## 🧪 Testing

The project includes a comprehensive test suite with 77+ unit tests covering all core modules.

### Running Tests

**Install test dependencies:**
```bash
pip install -r requirements.txt  # Includes pytest, pytest-mock, pytest-cov
```

**Run all tests:**
```bash
# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=src --cov-report=term-missing

# Run specific test file
pytest tests/test_config_manager.py -v

# Run tests matching pattern
pytest -k "test_fetch" -v
```

**Run tests by marker:**
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration
```

### Test Coverage

Current test coverage:
- **ConfigManager**: 12 tests (100% coverage)
- **DataFetcher**: 14 tests (95% coverage)
- **DataValidator**: 15 tests (98% coverage)
- **DataMerger**: 10 tests (95% coverage)
- **MetadataManager**: 11 tests (92% coverage)
- **RetryManager**: 15 tests (98% coverage)
- **Scripts**: Integration test framework

**Generate HTML coverage report:**
```bash
pytest --cov=src --cov-report=html
# Open htmlcov/index.html in browser
```

### Continuous Integration

Tests run automatically on:
- Every push to `main` or `develop`
- Every pull request
- Manual workflow dispatch

**CI Workflow** (`.github/workflows/tests.yml`):
- Tests on Python 3.9, 3.10, 3.11
- Coverage reporting with Codecov
- Linting with flake8, black, isort
- Fails if coverage < 70%

**View CI results:** GitHub Actions tab

### Writing Tests

All tests use mocks - no real network calls or file system modifications to production directories.

**Example test:**
```python
import pytest
from unittest.mock import patch

@pytest.mark.unit
@patch('src.data_fetcher.yf.download')
def test_fetch_data(mock_download, sample_dataframe):
    """Test data fetching with mocked yfinance."""
    mock_download.return_value = sample_dataframe
    fetcher = DataFetcher()
    result = fetcher.fetch_data('RELIANCE.NS', '1d')
    assert result is not None
```

### Manual Validation

Beyond automated tests, you can validate the pipeline:

**Test configuration:**
```bash
python -c "from src.config_manager import ConfigManager; cm = ConfigManager(); cm.load_config(); print('✓ Config valid')"
```

**Test with single symbol (dry-run):**
```bash
python scripts/daily_update.py --symbols RELIANCE.NS --dry-run
```

**Validate existing data:**
```bash
python scripts/validate_all.py
```

## 📈 Performance Tips

1. **Parallel Downloads**: Modify chunk_size in config.yaml
2. **Reduce Intervals**: Focus on needed timeframes only
3. **Symbol Batching**: Download in groups for large lists
4. **SSD Storage**: Use SSD for faster I/O operations
5. **Memory**: Recommended 4GB+ RAM for 200+ symbols

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
pip install -r requirements.txt
pip install pytest pytest-cov  # For testing
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Known Limitations

### Yahoo Finance Data Limitations

Due to Yahoo Finance API restrictions and data availability, the following limitations exist:

#### 1. Delisted Symbols (No Data Available)
These symbols are no longer available on Yahoo Finance and cannot be downloaded:

- **TATAMOTORS.NS** - Delisted from Yahoo Finance
- **PEL.NS** (Piramal Enterprises) - Delisted from Yahoo Finance

**Action**: These symbols are not included in the configuration. If you see download failures for these symbols, they should be removed from `config/stocks.yaml`.

#### 2. BSE Index Intraday Limitations
BSE indices do not support 1-minute interval data:

- **BSE-MIDCAP.BO** - No 1m data available (2m, 5m, 15m, 60m, 1d work fine)
- **BSE-SMLCAP.BO** - No 1m data available (2m, 5m, 15m, 60m, 1d work fine)

**Workaround**: Use 2-minute data as a substitute for 1-minute data for BSE indices.

#### 3. Intermittent 60m Data Gaps
Some stocks may have missing 60-minute interval data due to Yahoo data gaps:

- **ADANIENSOL.NS** - 60m interval may be missing
- **ARE&M.NS** (Amara Raja) - 60m interval may be missing

**Note**: Other intervals (1m, 2m, 5m, 15m, 1d) work fine for these symbols.

#### 4. Symbol Changes & Updates
Over time, companies may rename, merge, or get delisted. The pipeline tracks these changes:

- **GMRINFRA.NS** → **GMRAIRPORT.NS** (Company renamed to GMR Airports Infrastructure)
- **^CNXMIDCAP** → **^NSEMDCP50** (Index symbol updated by NSE)
- **NIFTY_SMLCAP_100.NS** → **^CNXSC** (Index symbol updated)
- **NIFTY_NEXT_50.NS** → **^NSMIDCP** (Index symbol updated)

**Expected Coverage**: With these limitations, expect **98.5% data coverage** (1,034 out of 1,050 expected files).

### Rate Limiting

Yahoo Finance may impose rate limits:
- The pipeline implements automatic retry with exponential backoff
- Recommended: Run initial download during off-peak hours
- Daily updates are lightweight and rarely hit rate limits

## ⚠️ Disclaimer

This software is for educational and research purposes only. 

- **Not Financial Advice**: Do not use for actual trading decisions
- **Data Accuracy**: No guarantee of data accuracy or completeness
- **Yahoo Finance TOS**: Ensure compliance with Yahoo Finance Terms of Service
- **Market Risk**: Past performance does not guarantee future results

## 🙏 Acknowledgments

- [yfinance](https://github.com/ranaroussi/yfinance) - Yahoo Finance API wrapper
- [pandas](https://pandas.pydata.org/) - Data manipulation
- NSE India - Market data source

## 📞 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing documentation in `docs/`
- Review logs for error details

---

**Made with ❤️ for the trading community**
