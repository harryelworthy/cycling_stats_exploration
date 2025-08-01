# Cycling Data Scraper Testing Framework

## Overview

This comprehensive testing framework validates the cycling data scraper before processing full years of data. It detects website format changes early and provides detailed diagnostic information to help fix issues.

## Quick Start

### Run Tests Only
```bash
# Simple test runner
python run_tests.py

# Or run tests through main script
cd src
python main.py --test-only 2024
```

### Run Tests Before Scraping
```bash
cd src
# Tests run automatically before scraping
python main.py 2023 2024

# Skip tests (not recommended)
python main.py --skip-tests 2023 2024
```

## Test Framework Components

### 1. Test Cases
The framework tests various scenarios:

- **Basic Connectivity**: Verifies the scraper can fetch race listings
- **Individual Race Tests**: Tests different race types across multiple years:
  - Recent one-day classics (Paris-Roubaix 2024)
  - Multi-stage races (Tour de France 2024)
  - National championships (Italy 2024)
  - Historical races (Milano-Sanremo 2019, Giro 2020)
- **Data Validation**: Checks result consistency, rank sequences, duplicate detection
- **Format Consistency**: Compares same race across different years

### 2. Error Detection
The framework catches:
- Website format changes
- Missing HTML elements
- Insufficient race/result counts
- Data structure inconsistencies
- Network connectivity issues

### 3. Diagnostic Logging
When errors occur, detailed diagnostics are generated including:
- HTML snippets showing page structure
- CSS selector test results
- Expected vs actual element counts
- Actionable suggestions for fixes

## Output Files

### Test Reports
- `reports/scraper_test_report_YYYYMMDD_HHMMSS.json` - Detailed JSON report
- `reports/scraper_test_summary_YYYYMMDD_HHMMSS.txt` - Human-readable summary

### Diagnostic Logs
- `logs/diagnostic_STAGE_TIMESTAMP.json` - Individual error diagnostics
- `logs/scraper.log` - General logging

### Databases
- `test_cycling_data.db` - Test database (separate from production)
- `data/cycling_data.db` - Production database

## Interpreting Results

### ✅ All Tests Pass
```
✅ ALL TESTS PASSED!
🚀 Your scraper is ready for production use.
```
The scraper is working correctly and safe to use for full year processing.

### ❌ Tests Fail
```
❌ SOME TESTS FAILED!
🔍 Check the test reports in the 'reports/' directory.
```

1. **Check Test Summary**: Look at the `.txt` report for an overview
2. **Review Diagnostics**: Check JSON reports for detailed error information
3. **Follow Suggestions**: Each error includes actionable suggestions

## Common Issues and Solutions

### Website Format Changes

**Error**: "No results table found"
**Solution**: Website changed from `table.results` to different layout
- Check diagnostic HTML preview
- Update CSS selectors in `async_scraper.py`

**Error**: "Race name mismatch" 
**Solution**: Race name extraction logic needs updating
- Check if `h1` element still contains race name
- Update `clean_race_name()` function if needed

### Network Issues

**Error**: "Connection failure"
**Solutions**:
- Check internet connectivity
- Verify procyclingstats.com is accessible
- Reduce concurrent requests with `--max-concurrent`
- Increase delays with `--request-delay`

### Historical Data Issues

**Error**: Tests fail for older years (2019-2021)
**Solution**: Historical data may have different formats
- This is expected for some older races
- Focus on fixing current year formats first
- Consider excluding problematic historical races

## Advanced Usage

### Custom Test Configuration
```python
from test_scraper import ScraperTestFramework
from async_scraper import ScrapingConfig

# Conservative settings for testing
config = ScrapingConfig(
    max_concurrent_requests=3,
    request_delay=0.5,
    timeout=60
)

framework = ScraperTestFramework(config)
success = await framework.run_full_test_suite()
```

### Adding New Test Cases
Edit `src/test_scraper.py` and add to `self.test_races`:

```python
TestRace(
    year=2024,
    race_url="race/vuelta-a-espana/2024",
    expected_name="Vuelta a España",
    race_type="stage_race",
    min_results=150,
    description="2024 Vuelta a España - Another Grand Tour"
)
```

### Debugging Failed Tests

1. **Check HTML Preview**: See what the page actually contains
2. **Review CSS Selectors**: Check which selectors work/don't work
3. **Examine Suggestions**: Follow the automated suggestions
4. **Compare Years**: See if the same race works in different years

## Error Types Reference

| Error Type | Meaning | Common Cause |
|------------|---------|--------------|
| `insufficient_results` | Too few races/results found | Website format change |
| `name_mismatch` | Race name extraction failed | HTML structure change |
| `missing_fields` | Required data fields missing | Table format change |
| `connection_failure` | Network request failed | Connectivity or rate limiting |
| `rank_sequence_error` | Results ranking inconsistent | Results parsing issue |
| `duplicate_riders` | Same rider appears multiple times | Parsing logic error |

## Best Practices

1. **Always Run Tests First**: Don't skip validation tests
2. **Check Recent Reports**: Review test reports before major scraping runs
3. **Monitor Error Patterns**: Look for recurring issues across test runs
4. **Update Test Cases**: Add new races when website formats change
5. **Gradual Deployment**: Test with single years before processing multiple years

## Integration with Main Scraper

The testing framework is automatically integrated:

- Tests run before every scraping session (unless `--skip-tests`)
- Failed tests prevent scraping to avoid processing bad data
- Same configuration ensures consistent behavior
- Detailed error logs help debug production issues

This ensures you never accidentally process a full year with a broken scraper! 