# GitHub Actions Setup Guide

This guide explains how to configure the automated daily data update workflow.

## Overview

The workflow (`.github/workflows/daily_update.yml`) automatically runs daily at 8:00 PM IST to update market data.

## Features

✅ Scheduled daily runs at 8:00 PM IST (14:30 UTC)  
✅ Python 3.11 environment  
✅ Automatic dependency installation  
✅ Data commit with date-based messages  
✅ Log artifact uploads (30-day retention)  
✅ Multiple notification options on failure  
✅ Auto-creates GitHub issues on failure  
✅ Manual trigger support with parameters  

## Setup Instructions

### 1. Enable GitHub Actions

1. Go to your repository settings
2. Navigate to **Actions** → **General**
3. Ensure "Allow all actions and reusable workflows" is selected
4. Save changes

### 2. Configure GitHub Secrets (Optional)

For notifications on failure, configure these secrets:

**Go to: Settings → Secrets and variables → Actions → New repository secret**

#### Option 1: Slack Notifications

```
Secret Name: SLACK_WEBHOOK_URL
Value: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

**How to get Slack Webhook:**
1. Go to https://api.slack.com/apps
2. Create a new app or select existing
3. Enable "Incoming Webhooks"
4. Add webhook to your channel
5. Copy the webhook URL

#### Option 2: Email Notifications

```
Secret Name: EMAIL_USERNAME
Value: your-gmail@gmail.com

Secret Name: EMAIL_PASSWORD
Value: your-app-specific-password

Secret Name: NOTIFICATION_EMAIL
Value: recipient@example.com
```

**How to get Gmail App Password:**
1. Enable 2-factor authentication on your Google account
2. Go to https://myaccount.google.com/apppasswords
3. Generate a new app password
4. Use this password (not your regular password)

### 3. Update Branch Protection (Optional)

If you have branch protection rules:

1. Go to **Settings** → **Branches**
2. Edit the rule for `main` branch
3. Under "Allow force pushes" → Check "Allow force pushes"
4. Or add `github-actions[bot]` to bypass list

### 4. Manual Trigger

To manually run the workflow:

1. Go to **Actions** tab
2. Select "Daily Data Update" workflow
3. Click "Run workflow"
4. Optional parameters:
   - **symbols**: Space-separated symbols (e.g., `RELIANCE.NS TCS.NS`)
   - **intervals**: Space-separated intervals (e.g., `1d 5m`)
   - **dry_run**: Check to preview without downloading

## Workflow Details

### Schedule
```yaml
cron: '30 14 * * *'  # 14:30 UTC = 20:00 IST (8:00 PM)
```

### Commit Message Format
```
Daily data update 2024-01-15
```

### Artifacts Generated

All workflow runs generate artifacts (kept for 30 days):

1. **update-logs-{run_number}**: Full execution logs
2. **update-failures-{run_number}**: Failure report (if any failures)

### On Failure

The workflow automatically:
1. Creates a GitHub issue with failure details
2. Sends Slack notification (if configured)
3. Sends email notification (if configured)
4. Uploads logs and failure reports as artifacts

## Testing the Workflow

### Test Dry Run
```bash
# Manual trigger with dry-run enabled
# This will preview without actually downloading or committing data
```

### Test Specific Symbols
```bash
# Manual trigger with symbols parameter
# Example: RELIANCE.NS TCS.NS
```

## Monitoring

### View Workflow Status
- Go to **Actions** tab
- See all workflow runs with status indicators
- Green ✓ = Success
- Red ✗ = Failed
- Yellow ○ = Running

### Check Data Updates
- Navigate to repository commits
- Look for commits by `github-actions[bot]`
- Message format: "Daily data update YYYY-MM-DD"

### Download Logs
1. Go to failed workflow run
2. Scroll to bottom - "Artifacts" section
3. Download logs or failure reports

## Troubleshooting

### Workflow Not Running
- Check if scheduled workflows are enabled in repository settings
- Verify the cron schedule is correct
- Ensure repository has activity (GitHub may disable after 60 days of inactivity)

### Permission Errors
```yaml
# Add to workflow file if needed:
permissions:
  contents: write
  issues: write
```

### Git Push Failures
- Check branch protection rules
- Verify `github-actions[bot]` has write permissions
- Ensure no conflicts with manual commits

### API Rate Limits
- yfinance has rate limits
- If hitting limits, reduce frequency or symbols
- Check logs for rate limit errors

## Advanced Configuration

### Change Schedule Time

Edit `.github/workflows/daily_update.yml`:
```yaml
# For 6:00 PM IST (12:30 UTC)
cron: '30 12 * * *'

# For 10:00 PM IST (16:30 UTC)
cron: '30 16 * * *'
```

### Run Multiple Times Per Day
```yaml
schedule:
  - cron: '30 6 * * *'   # 12:00 PM IST
  - cron: '30 14 * * *'  # 8:00 PM IST
```

### Skip Weekends
```yaml
schedule:
  - cron: '30 14 * * 1-5'  # Monday-Friday only
```

## Local Testing

Before relying on GitHub Actions, test locally:

```bash
# Test dry run
python scripts/daily_update.py --dry-run

# Test with specific symbols
python scripts/daily_update.py --symbols RELIANCE.NS

# Test full update
python scripts/daily_update.py
```

## Cost Considerations

GitHub Actions is **FREE** for public repositories with unlimited minutes.

For private repositories:
- Free tier: 2,000 minutes/month
- This workflow uses ~5-15 minutes per run
- Daily runs: ~30 runs/month = 150-450 minutes/month
- Well within free tier limits

## Next Steps

1. ✅ Configure secrets (optional but recommended)
2. ✅ Test with manual trigger first
3. ✅ Monitor first scheduled run
4. ✅ Review artifacts and logs
5. ✅ Adjust configuration as needed

## Support

For issues:
1. Check workflow logs in Actions tab
2. Review failure artifacts
3. Test scripts locally first
4. Check GitHub Actions status page

---

**Note:** The workflow automatically handles market holidays and weekends - no data will be available on these days, which is expected behavior.
