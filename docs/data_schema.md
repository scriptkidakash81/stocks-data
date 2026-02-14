# Data Schema Documentation

This document describes the data formats, schemas, and structures used in the Market Data Pipeline.

## CSV Data Format

### OHLCV Schema

All market data CSV files follow this standardized schema:

| Column | Type | Description | Example | Constraints |
|--------|------|-------------|---------|-------------|
| **Datetime** | Timestamp | Trading datetime with timezone | `2024-01-15 09:15:00+05:30` | Index, timezone-aware (Asia/Kolkata) |
| **Open** | Float64 | Opening price | `2850.50` | > 0 |
| **High** | Float64 | Highest price | `2855.00` | ≥ max(Open, Close) |
| **Low** | Float64 | Lowest price | `2848.00` | ≤ min(Open, Close) |
| **Close** | Float64 | Closing price | `2852.75` | > 0 |
| **Volume** | Int64 | Trading volume | `125000` | ≥ 0 |

### File Structure

**Format**: CSV (Comma-Separated Values)  
**Encoding**: UTF-8  
**Delimiter**: `,` (comma)  
**Header**: First row contains column names  
**Index**: Datetime column (not a separate column in CSV)  

### Example CSV Content

```csv
Datetime,Open,High,Low,Close,Volume
2024-01-15 09:15:00+05:30,2850.50,2855.00,2848.00,2852.75,125000
2024-01-15 09:16:00+05:30,2852.75,2856.25,2851.50,2854.00,98000
2024-01-15 09:17:00+05:30,2854.00,2858.00,2853.00,2856.50,112000
```

## Metadata Schema

### Metadata File Format

**Format**: JSON  
**Location**: `data/metadata/{SYMBOL}_{INTERVAL}.json`  
**Encoding**: UTF-8

### Metadata Structure

```json
{
  "symbol": "RELIANCE.NS",
  "interval": "1d",
  "last_update": "2024-01-15T20:30:00+05:30",
  "total_rows": 5000,
  "rows_added": 1,
  "date_range": {
    "start": "2020-01-01",
    "end": "2024-01-15"
  },
  "validation": {
    "status": "passed",
    "issues_count": 0,
    "last_validated": "2024-01-15T20:30:00+05:30",
    "issues": []
  },
  "download_history": [
    {
      "timestamp": "2024-01-15T20:30:00+05:30",
      "rows_added": 1,
      "success": true,
      "error": null
    }
  ],
  "file_info": {
    "size_bytes": 245000,
    "size_mb": 0.23,
    "path": "data/stocks/RELIANCE.NS/1d.csv"
  }
}
```

### Metadata Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | String | Stock/Index symbol |
| `interval` | String | Time interval (1m, 5m, 1d, etc.) |
| `last_update` | ISO8601 | Last successful update timestamp |
| `total_rows` | Integer | Current total number of rows |
| `rows_added` | Integer | Rows added in last update |
| `date_range.start` | ISO8601 Date | Earliest data point |
| `date_range.end` | ISO8601 Date | Latest data point |
| `validation.status` | String | "passed" or "issues" |
| `validation.issues_count` | Integer | Number of validation issues |
| `validation.last_validated` | ISO8601 | Last validation timestamp |
| `download_history` | Array | Historical download records |

## Configuration Schemas

### Main Configuration (`config.yaml`)

```yaml
# Data storage directories
data_dir: "./data"
log_dir: "./logs"

# Time intervals to download
intervals:
  - "1m"
  - "2m"
  - "5m"
  - "15m"
  - "60m"
  - "1d"

# API retry settings
max_retries: 3
retry_delay: 5

# Processing settings
chunk_size: 10
validate_data: true

# Timezone for data
timezone: "Asia/Kolkata"
```

**Schema**:
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `data_dir` | String | Yes | "./data" | Data storage directory |
| `log_dir` | String | Yes | "./logs" | Log storage directory |
| `intervals` | Array[String] | Yes | - | List of intervals to fetch |
| `max_retries` | Integer | Yes | 3 | Max retry attempts |
| `retry_delay` | Integer | Yes | 5 | Delay between retries (seconds) |
| `chunk_size` | Integer | No | 10 | Batch processing size |
| `validate_data` | Boolean | No | true | Enable validation |
| `timezone` | String | Yes | "Asia/Kolkata" | Timezone for data |

### Stocks Configuration (`stocks.yaml`)

```yaml
stocks:
  - symbol: "RELIANCE.NS"
    name: "Reliance Industries Ltd"
    sector: "Energy"
  
  - symbol: "TCS.NS"
    name: "Tata Consultancy Services"
    sector: "IT"
```

**Schema**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | String | Yes | Yahoo Finance symbol (with exchange suffix) |
| `name` | String | No | Company full name |
| `sector` | String | No | Industry sector |

### Indices Configuration (`indices.yaml`)

```yaml
indices:
  - symbol: "^NSEI"
    name: "Nifty 50"
    description: "NSE's benchmark index"
    region: "India"
  
  - symbol: "^NSEBANK"
    name: "Bank Nifty"
    description: "Banking sector index"
    region: "India"
```

**Schema**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | String | Yes | Index symbol (Yahoo Finance format) |
| `name` | String | No | Index name |
| `description` | String | No | Index description |
| `region` | String | No | Geographic region |

## Validation Report Schema

### Structure

```json
{
  "is_valid": true,
  "issues": [
    {
      "severity": "warning",
      "category": "gaps",
      "message": "Gap detected between rows",
      "details": {
        "index": 1234,
        "gap_size": "2 hours",
        "expected": "2024-01-15 11:00:00+05:30",
        "actual": "2024-01-15 13:00:00+05:30"
      }
    }
  ]
}
```

### Field Descriptions

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `is_valid` | Boolean | true/false | Overall validation result |
| `issues` | Array | - | List of validation issues |
| `issues[].severity` | String | error, warning, info | Issue severity level |
| `issues[].category` | String | duplicates, gaps, nulls, ohlc_logic, volume, sorting | Issue category |
| `issues[].message` | String | - | Human-readable message |
| `issues[].details` | Object | - | Additional issue details |

### Issue Categories

| Category | Description | Example |
|----------|-------------|---------|
| `duplicates` | Duplicate timestamps | Multiple rows with same datetime |
| `gaps` | Missing data points | Expected row not present |
| `nulls` | Null/NaN values | Missing OHLCV values |
| `ohlc_logic` | OHLC validation failed | High < Close |
| `volume` | Volume issues | Negative volume |
| `sorting` | Data not sorted | Rows out of order |

## Failure Report Schema

### Structure

```json
{
  "symbol": "TCS.NS",
  "interval": "1d",
  "timestamp": "2024-01-15T20:30:00+05:30",
  "error": "Connection timeout",
  "metadata": {
    "attempt": 3,
    "last_successful_download": "2024-01-14T20:30:00+05:30"
  }
}
```

## Directory Structure

```
market-data-pipeline/
│
├── config/
│   ├── config.yaml              # Main configuration
│   ├── stocks.yaml              # Stock symbols list
│   └── indices.yaml             # Index symbols list
│
├── data/
│   ├── stocks/
│   │   ├── RELIANCE.NS/
│   │   │   ├── 1m.csv           # 1-minute data
│   │   │   ├── 2m.csv           # 2-minute data
│   │   │   ├── 5m.csv           # 5-minute data
│   │   │   ├── 15m.csv          # 15-minute data
│   │   │   ├── 60m.csv          # 60-minute data
│   │   │   └── 1d.csv           # Daily data
│   │   │
│   │   ├── TCS.NS/
│   │   │   └── ...
│   │   └── ...
│   │
│   ├── indices/
│   │   ├── _NSEI/               # ^NSEI sanitized to _NSEI
│   │   │   ├── 1m.csv
│   │   │   └── ...
│   │   │
│   │   ├── _NSEBANK/            # ^NSEBANK sanitized
│   │   │   └── ...
│   │   └── ...
│   │
│   └── metadata/
│       ├── RELIANCE.NS_1m.json
│       ├── RELIANCE.NS_1d.json
│       ├── _NSEI_1d.json
│       └── ...
│
└── logs/
    ├── initial_download_20240115_203000.log
    ├── daily_update_20240115_203000.log
    ├── validation_20240115_203000.log
    ├── gap_fixing_20240115_203000.log
    ├── update_failures.txt
    └── validation_report_20240115_203000.json
```

## Data Type Specifications

### Python Pandas Types

```python
dtypes = {
    'Datetime': 'datetime64[ns, Asia/Kolkata]',  # Index
    'Open': 'float64',
    'High': 'float64',
    'Low': 'float64',
    'Close': 'float64',
    'Volume': 'int64'
}
```

### Loading Data Example

```python
import pandas as pd

# Load with proper types
df = pd.read_csv(
    'data/stocks/RELIANCE.NS/1d.csv',
    index_col='Datetime',
    parse_dates=True
)

# Verify types
assert df.index.tz.zone == 'Asia/Kolkata'
assert df['Open'].dtype == 'float64'
assert df['Volume'].dtype == 'int64'
```

## Data Validation Rules

### OHLC Validation

```python
# High must be >= Open and >= Close
assert (df['High'] >= df['Open']).all()
assert (df['High'] >= df['Close']).all()

# Low must be <= Open and <= Close
assert (df['Low'] <= df['Open']).all()
assert (df['Low'] <= df['Close']).all()

# All prices must be positive
assert (df[['Open', 'High', 'Low', 'Close']] > 0).all().all()

# Volume must be non-negative
assert (df['Volume'] >= 0).all()
```

### Timestamp Validation

```python
# No duplicates
assert not df.index.duplicated().any()

# Sorted chronologically
assert df.index.is_monotonic_increasing

# Timezone aware
assert df.index.tz is not None
```

## File Naming Conventions

### CSV Files

**Pattern**: `{interval}.csv`  
**Examples**:
- `1m.csv` - 1-minute data
- `5m.csv` - 5-minute data  
- `1d.csv` - Daily data

### Metadata Files

**Pattern**: `{SYMBOL}_{INTERVAL}.json`  
**Examples**:
- `RELIANCE.NS_1d.json`
- `TCS.NS_5m.json`
- `_NSEI_1d.json`

### Log Files

**Pattern**: `{script}_{YYYYMMDD_HHMMSS}.log`  
**Examples**:
- `initial_download_20240115_203000.log`
- `daily_update_20240115_203000.log`

## Symbol Sanitization

Special characters in symbols are sanitized for filenames:

| Original | Sanitized | Reason |
|----------|-----------|--------|
| `^NSEI` | `_NSEI` | `^` not allowed in filenames |
| `A/B` | `A_B` | `/` is path separator |
| `A:B` | `A_B` | `:` reserved on Windows |

## Data Size Estimates

### Per Symbol Storage

| Interval | 1 Month | 1 Year | 5 Years |
|----------|---------|--------|---------|
| **1m** | ~20 MB | N/A (7-day limit) | N/A |
| **5m** | ~4 MB | ~48 MB | N/A (60-day limit) |
| **1d** | ~20 KB | ~250 KB | ~1.2 MB |

### Full Pipeline Storage

For **200 stocks** + **25 indices** across **6 intervals**:
- Initial download: ~2-5 GB
- Daily growth: ~50-100 MB
- Annual growth: ~18-36 GB

## Best Practices

### Data Integrity

1. **Always validate** after download
2. **Keep backups** before merging
3. **Check metadata** consistency
4. **Monitor gaps** regularly
5. **Audit logs** for failures

### Performance

1. **Use appropriate dtypes** (int64 for volume, not float)
2. **Index on Datetime** for fast queries
3. **Compress old data** (gzip) if needed
4. **Archive or delete** very old intraday data

### Querying Data

```python
# Load specific date range
df = pd.read_csv('RELIANCE.NS/1d.csv', index_col='Datetime', parse_dates=True)
df_range = df.loc['2024-01-01':'2024-01-31']

# Filter by time
df_morning = df.between_time('09:15', '12:00')

# Resample to different interval
df_hourly = df.resample('1H').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last',
    'Volume': 'sum'
})
```

---

**Last Updated**: 2024-01-15
