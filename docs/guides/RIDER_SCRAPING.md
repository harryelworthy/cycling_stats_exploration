# Rider Profile Scraping

This document explains how to use the rider profile scraping functionality that has been added to the cycling data scraper.

## Overview

The rider scraping functionality extracts detailed profile information from individual rider pages on ProCyclingStats, such as:
- Personal information (date of birth, nationality, weight, height, place of birth)
- Career statistics (total wins, grand tours, classics)
- Specialty scores (climber, GC, time trial, sprint, one-day races, hills)
- Current rankings (UCI World, PCS)
- Team history
- Major achievements and wins

## Database Schema

The rider scraping adds three new tables to your database:

### `riders` table
Stores basic rider profile information:
- `rider_name`, `rider_url`, `date_of_birth`, `nationality`
- `weight_kg`, `height_cm`, `place_of_birth`
- `uci_ranking`, `pcs_ranking`
- `profile_score_*` columns for different specialties
- `total_wins`, `total_grand_tours`, `total_classics`
- `active_years_start`, `active_years_end`

### `rider_teams` table
Stores team history:
- `rider_url`, `team_name`, `year_start`, `year_end`, `team_level`

### `rider_achievements` table
Stores major achievements:
- `rider_url`, `achievement_type`, `race_name`, `year`, `count`, `description`

## Usage Methods

### 1. Integrated with Main Scraper

Scrape races and rider profiles together:

```bash
# Scrape 2023-2024 races and then rider profiles
python src/main.py 2023 2024 --enable-rider-scraping

# This will:
# 1. Scrape all race data for 2023-2024
# 2. Find all riders who competed in those years
# 3. Scrape profile data for riders missing from the database
```

### 2. Rider Profiles Only

Scrape only rider profiles (skip race scraping):

```bash
# Scrape all riders missing profile data
python src/main.py 2023 2024 --riders-only

# Update rider data for specific years
python src/main.py 2023 2024 --update-riders
```

### 3. Standalone Rider Update Script

Use the dedicated rider update script:

```bash
# Scrape all missing rider profiles
python src/update_riders.py --all-missing

# Update riders for specific years
python src/update_riders.py 2023 2024

# Update riders for a year range
python src/update_riders.py 2020-2024

# With custom settings
python src/update_riders.py 2023 --max-concurrent 3 --request-delay 0.5
```

## Configuration Options

### Main Scraper Options

- `--enable-rider-scraping`: Enable rider profile scraping after race data
- `--riders-only`: Only scrape rider profiles (skip races)
- `--update-riders`: Update rider data for specified years

### Standalone Script Options

- `--all-missing`: Scrape all riders missing profile data
- `--refresh`: Re-scrape existing rider profiles (future feature)
- `--max-concurrent`: Max concurrent requests (default: 5)
- `--request-delay`: Delay between requests in seconds (default: 0.2)
- `--verbose`: Enable detailed logging

## Performance Considerations

### Conservative Settings
Rider scraping uses conservative settings to be respectful to the website:
- Default max concurrent requests: 5 (much lower than race scraping)
- Default delay between requests: 0.2 seconds
- Processes riders in batches of 20
- Additional delay between batches

### Recommended Usage

1. **For large datasets**: Run rider scraping separately after race scraping
   ```bash
   # First scrape races
   python src/main.py 2020-2024
   
   # Then scrape riders
   python src/update_riders.py --all-missing
   ```

2. **For small datasets**: Use integrated scraping
   ```bash
   python src/main.py 2024 --enable-rider-scraping
   ```

3. **For updates**: Use the standalone script for specific years
   ```bash
   python src/update_riders.py 2024
   ```

## Example Workflow

### Complete Setup for New Data
```bash
# 1. Scrape race data for recent years
python src/main.py 2020-2024

# 2. Scrape all missing rider profiles
python src/update_riders.py --all-missing

# 3. Check results
python src/scraper_cli.py status
```

### Update for New Year
```bash
# 1. Scrape new year with rider profiles
python src/main.py 2025 --enable-rider-scraping

# Or separately:
python src/main.py 2025
python src/update_riders.py 2025
```

### Update Existing Rider Data
```bash
# Update rider profiles for specific years
python src/update_riders.py 2023 2024
```

## Monitoring and Logging

Rider scraping creates detailed logs:
- Main scraper: logs to `logs/scraper.log`
- Standalone script: logs to `logs/rider_update.log`
- Progress tracking with success/failure counts
- Detailed error reporting for failed profiles

## Error Handling

The rider scraper includes robust error handling:
- Failed individual rider scrapes don't stop the batch
- Network errors are logged and continue with next rider
- Parse errors are logged with details for debugging
- Progress is tracked so you can resume if interrupted

## Data Quality

### What Gets Scraped Successfully
- Most active professional riders have complete profiles
- Historical data back to early 2000s is generally available
- Major achievements and team history are well-documented

### Potential Issues
- Very old riders (pre-2000) may have incomplete data
- Amateur or lower-level riders may have minimal profiles
- Some international characters in names may need special handling
- Website format changes could break parsing (monitored by tests)

## Troubleshooting

### Common Issues

1. **No riders found**: Check that race data exists in database first
2. **Parse errors**: Website format may have changed, check logs
3. **Network timeouts**: Reduce concurrent requests or increase delays
4. **Database errors**: Check database permissions and disk space

### Debug Mode
Enable verbose logging for troubleshooting:
```bash
python src/update_riders.py 2024 --verbose
```

### Test Single Rider
For debugging, you can test with a small sample:
```bash
# Update just a few recent riders to test functionality
python src/update_riders.py 2024 --max-concurrent 1 --verbose
```

## Integration with Analysis

The rider profile data enhances your cycling analysis by providing:
- Physical characteristics for performance modeling
- Career trajectories and team changes
- Specialty scores for race prediction
- Historical context for statistical analysis

Example query to get rider info for analysis:
```sql
SELECT 
    r.rider_name,
    r.nationality,
    r.weight_kg,
    r.height_cm,
    r.profile_score_climber,
    r.profile_score_gc,
    r.total_wins
FROM riders r
JOIN results res ON r.rider_url = res.rider_url
WHERE res.rank = 1  -- Only winners
ORDER BY r.total_wins DESC;
``` 