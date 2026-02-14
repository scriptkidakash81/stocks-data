# Yahoo Finance API Limits and Best Practices

This document outlines the rate limits, constraints, and best practices when using Yahoo Finance API via the yfinance library.

## API Overview

**Source**: Yahoo Finance (via yfinance Python library)  
**Cost**: Free (public data)  
**Authentication**: Not required  
**Rate Limits**: Undocumented but enforced

## Known Rate Limits

### Request Limits

While Yahoo Finance doesn't publish official limits, observed constraints:

| Limit Type | Estimated Value | Notes |
|-----------|----------------|-------|
| Requests/Hour | ~2,000 | Soft limit, may vary |
| Requests/Second | 5-10 | Avoid rapid-fire requests |
| Concurrent Connections | 1-2 | Sequential is safer |
| Daily Limit | High | Generally not an issue |

**⚠️ Warning**: These are observed limits and may change without notice.

### Data Period Limits by Interval

Yahoo Finance restricts how far back you can fetch data depending on the interval:

| Interval | Max Period | Max Days | Example Usage |
|----------|-----------|----------|---------------|
| **1m** | 7d | 7 | Last 7 days only |
| **2m** | 60d | 60 | Last 2 months |
| **5m** | 60d | 60 | Last 2 months |
| **15m** | 60d | 60 | Last 2 months |
| **30m** | 60d | 60 | Last 2 months |
| **60m** | 730d | 730 | Last 2 years |
| **90m** | 60d | 60 | Last 2 months |
| **1h** | 730d | 730 | Last 2 years |
| **1d** | max | Unlimited | Full history available |
| **1wk** | max | Unlimited | Full history available |
| **1mo** | max | Unlimited | Full history available |

**Implementation in Pipeline**:
```python
# data_fetcher.py
INTERVAL_LIMITS = {
    '1m': 7,
    '2m': 60,
    '5m': 60,
    '15m': 60,
    '60m': 730,
    '1d': None  # No limit
}
```

### Data Retention

- **Intraday data**: Limited historical availability
- **Daily data**: Often goes back 20+ years
- **Real-time data**: 15-20 minute delay for free tier
- **Pre-market/After-hours**: Limited for some symbols

## Best Practices

### 1. Rate Limiting

**Implement Delays**:
```python
import time

for symbol in symbols:
    fetch_data(symbol)
    time.sleep(0.5)  # 500ms delay
```

**Our Implementation**:
```python
# DataFetcher class
self.rate_limit_delay = 0.5  # seconds
```

### 2. Batch Processing

**Bad** ❌:
```python
for symbol in all_200_symbols:
    for interval in all_6_intervals:
        fetch_data(symbol, interval)  # 1200 sequential requests
```

**Good** ✅:
```python
for symbol in symbols_batch:  # Process in chunks
    for interval in intervals:
        fetch_data(symbol, interval)
        time.sleep(0.5)
```

### 3. Retry Strategy

**Use exponential backoff**:
```python
def fetch_with_retry(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            return yfinance.download(symbol)
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                time.sleep(wait)
            else:
                raise
```

**Our Implementation**: `RetryManager` class with exponential backoff

### 4. Error Handling

**Common Errors**:

| Error | Cause | Solution |
|-------|-------|----------|
| `HTTPError 429` | Too many requests | Increase delay, reduce batch size |
| `HTTPError 404` | Invalid symbol | Verify symbol format (.NS for NSE) |
| `Empty DataFrame` | No data available | Check market hours, symbol validity |
| `Timeout` | Network/API slow | Retry with backoff |
| `KeyError` | Missing data columns | Validate response before processing |

**Error Handling Pattern**:
```python
try:
    df = yf.download(symbol, interval=interval, period=period)
    if df.empty:
        log.warning(f"No data for {symbol}")
        return None
    return df
except HTTPError as e:
    if e.code == 429:  # Rate limit
        time.sleep(60)  # Wait 1 minute
        return fetch_with_retry(symbol)
    else:
        raise
```

### 5. Caching Strategy

**Cache validated data**:
- Save to CSV immediately after validation
- Use metadata to track last update
- Avoid re-downloading recent data

**Example**:
```python
if last_update < 24_hours_ago:
    fetch_new_data()
else:
    use_cached_data()
```

### 6. Symbol Format

**NSE (India)**:
```python
correct = "RELIANCE.NS"   # ✅
wrong = "RELIANCE"        # ❌
```

**BSE (India)**:
```python
correct = "RELIANCE.BO"   # ✅
```

**Indices**:
```python
nifty = "^NSEI"          # ✅
bank_nifty = "^NSEBANK"  # ✅
```

## Data Quality Considerations

### 1. Market Hours

**NSE Trading Hours** (IST):
- Pre-open: 9:00 AM - 9:15 AM
- Regular: 9:15 AM - 3:30 PM
- Post-close: 3:30 PM - 4:00 PM

**Data Availability**:
- Intraday data only available during market hours
- 15-20 minute delay on free data
- No data on weekends/holidays

### 2. Holidays

**NSE observes**:
- National holidays (Republic Day, Independence Day, etc.)
- Religious festivals (Diwali, Holi, etc.)
- Special trading holidays

**Pipeline Handling**:
```python
# fix_gaps.py includes NSE holiday calendar
MARKET_HOLIDAYS = [
    '2024-01-26',  # Republic Day
    '2024-03-25',  # Holi
    # ... etc
]
```

### 3. Corporate Actions

**Split/Bonus adjusted**:
- Yahoo Finance provides adjusted prices
- OHLCV all adjusted for splits
- Volume not split-adjusted

**Dividend**:
- Prices adjusted for dividends
- Ex-dividend dates marked

### 4. Data Gaps

**Common causes**:
- Market holidays
- Trading halts
- Delisted stocks
- API issues
- Symbol changes

**Detection**: Use `validate_all.py` and `fix_gaps.py`

## Performance Optimization

### 1. Parallel Processing

**Not Recommended** for Yahoo Finance:
```python
# ❌ May trigger rate limits
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(fetch_data, symbols)
```

**Recommended** - Sequential with delays:
```python
# ✅ Respects rate limits
for symbol in symbols:
    fetch_data(symbol)
    time.sleep(0.5)
```

### 2. Minimize Requests

**Efficient initial download**:
```python
# Fetch once with period="max"
df = yf.download(symbol, period="max", interval="1d")
```

**Efficient updates**:
```python
# Only fetch since last download
df = yf.download(symbol, start=last_date, end=today, interval="1d")
```

### 3. Interval Selection

**Choose appropriate intervals**:
- **Trading**: 1m, 5m, 15m
- **Daily analysis**: 1d
- **Long-term**: 1wk, 1mo

**Storage impact** (approx. per symbol):
| Interval | 1 Year Data | 5 Years Data |
|----------|-------------|--------------|
| 1m | ~160 MB | Not available |
| 5m | ~32 MB | Not available |
| 1d | ~200 KB | ~1 MB |

## Monitoring and Alerts

### Track Request Counts

```python
request_count = 0
start_time = time.time()

for symbol in symbols:
    fetch_data(symbol)
    request_count += 1
    
    # Check rate
    elapsed = time.time() - start_time
    rate = request_count / elapsed * 3600  # requests/hour
    
    if rate > 1800:  # Approaching limit
        time.sleep(10)  # Slow down
```

### Failure Tracking

Use `RetryManager` to track failures:
```python
if failures > 10:
    send_alert("High failure rate detected")
    increase_delay()
```

## Troubleshooting

### Issue: Empty DataFrames

**Symptoms**: `df.empty == True` after download

**Causes**:
1. Invalid symbol
2. No data for requested period
3. Market closed
4. Delisted stock

**Solution**:
```python
df = yf.download(symbol, period="1d")
if df.empty:
    # Try with different period
    df = yf.download(symbol, period="5d")
    if df.empty:
        log_failure(symbol, "No data available")
```

### Issue: Rate Limit Errors (429)

**Symptoms**: `HTTPError: 429 Too Many Requests`

**Solutions**:
1. Increase delay between requests
2. Reduce batch size
3. Wait and retry
4. Implement exponential backoff

**Code**:
```python
try:
    df = yf.download(symbol)
except HTTPError as e:
    if e.code == 429:
        time.sleep(60)  # Wait 1 minute
        df = yf.download(symbol)  # Retry
```

### Issue: Connection Timeouts

**Symptoms**: `ReadTimeout`, `ConnectTimeout`

**Solutions**:
```python
import yfinance as yf

# Increase timeout
yf.download(symbol, timeout=30)

# Or use retry logic
for attempt in range(3):
    try:
        return yf.download(symbol)
    except Timeout:
        if attempt == 2:
            raise
        time.sleep(5)
```

## Compliance

### Terms of Service

**Yahoo Finance TOS**:
- Data for personal use
- No redistribution of data
- No automated trading systems (check TOS)
- Respect rate limits

**yfinance Library**:
- Open source (Apache 2.0)
- Community maintained
- Not officially supported by Yahoo

### Data Usage

**Allowed** ✅:
- Research and analysis
- Educational purposes
- Personal portfolio tracking
- Backtesting strategies

**Restricted** ⚠️:
- Commercial redistribution
- High-frequency trading
- Real-time trading signals (use licensed data)

## Resources

- **yfinance Documentation**: https://github.com/ranaroussi/yfinance
- **Yahoo Finance**: https://finance.yahoo.com
- **NSE India**: https://www.nseindia.com
- **BSE India**: https://www.bseindia.com

## Summary

| Aspect | Recommendation |
|--------|----------------|
| **Request Rate** | 0.5-1 second delay between requests |
| **Batch Size** | 10-20 symbols per batch |
| **Retry Logic** | 3 attempts with exponential backoff |
| **Error Handling** | Log all failures, continue processing |
| **Caching** | Always cache to avoid re-downloads |
| **Monitoring** | Track request rates and failures |

---

**Last Updated**: 2024-01-15
