# Cycling Stats Exploration

A comprehensive data scraping and analysis toolkit for professional cycling data from ProCyclingStats.

## Overview

This project provides tools to scrape, store, and analyze professional cycling race data, including:
- Race results and stage information
- Detailed rider profiles and career statistics
- Team histories and achievements
- Performance analytics and insights

## Features

### 🏁 Race Data Scraping
- Comprehensive race results from ProCyclingStats
- Stage-by-stage data for multi-day races
- Secondary classifications (GC, Points, KOM, Youth)
- Race metadata (distance, profile, weather, etc.)

### 🚴 Rider Profile Data
- Personal information (nationality, physical stats)
- Career statistics and achievements
- Specialty scores (climbing, GC, time trial, etc.)
- Team history and transfers
- UCI and PCS rankings

### 📊 Data Management
- SQLite database storage
- Robust error handling and retry logic
- Progress tracking and resume capability
- Data validation and integrity checks

## Quick Start

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd cycling_stats_exploration
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Basic Usage

```bash
# Scrape race data for recent years
python src/main.py 2023 2024

# Include rider profile scraping
python src/main.py 2023 2024 --enable-rider-scraping

# Update rider profiles only
python src/update_riders.py --all-missing

# Check scraping status
python src/scraper_cli.py status
```

## Documentation

- **[TESTING.md](TESTING.md)** - Testing framework and validation
- **[docs/RIDER_SCRAPING.md](docs/RIDER_SCRAPING.md)** - Complete guide to rider profile scraping

## Project Structure

```
cycling_stats_exploration/
├── src/                    # Source code
│   ├── main.py            # CLI entry point
│   ├── async_scraper.py   # Main scraping engine
│   ├── rider_scraper.py   # Rider profile scraper
│   ├── update_riders.py   # Standalone rider updater
│   ├── test_scraper.py    # Testing framework
│   ├── progress_tracker.py # Progress tracking
│   ├── utils.py           # Utility functions
│   └── models.py          # Data models
├── docs/                  # Additional documentation
│   ├── RIDER_SCRAPING.md  # Rider profile scraping guide
│   └── CLEANUP_NOTES.md   # Repository cleanup notes
├── data/                  # Database and data files
├── logs/                  # Log files
├── reports/               # Test and scraping reports
├── TESTING.md             # Testing framework guide
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Database Schema

### Core Tables
- **races** - Race metadata and information
- **stages** - Individual stage details
- **results** - Rider results for each stage

### Rider Tables
- **riders** - Rider profiles and statistics
- **rider_teams** - Team history
- **rider_achievements** - Major wins and achievements

## Usage Examples

### Complete Data Collection
```bash
# Scrape everything for recent years
python src/main.py 2020-2024 --enable-rider-scraping
```

### Targeted Updates
```bash
# Update specific years
python src/main.py 2024
python src/update_riders.py 2024

# Scrape missing rider profiles
python src/update_riders.py --all-missing
```

### Analysis Ready Data
```bash
# Export data for analysis
python -c "
from src.utils import export_data_to_json
import asyncio
asyncio.run(export_data_to_json('data/cycling_data.db', 'export.json', 2024))
"
```

## Configuration

The scraper uses conservative settings by default:
- 30 concurrent requests for race data
- 5 concurrent requests for rider data
- 0.1s delay between race requests
- 0.2s delay between rider requests

Adjust these in the command line:
```bash
python src/main.py 2024 --max-concurrent 10 --request-delay 0.2
```

## Data Quality & Ethics

- **Respectful scraping**: Conservative request rates with delays
- **Error handling**: Robust retry logic and failure recovery
- **Data validation**: Comprehensive testing framework
- **Progress tracking**: Resume capability for long-running jobs

## Analysis Integration

The scraped data is perfect for:
- Performance analysis and modeling
- Team strategy research
- Historical comparisons
- Predictive analytics
- Career trajectory analysis

Example SQL query:
```sql
SELECT 
    r.rider_name,
    r.nationality,
    r.profile_score_climber,
    COUNT(res.rank) as races,
    AVG(res.rank) as avg_rank
FROM riders r
JOIN results res ON r.rider_url = res.rider_url
WHERE res.rank <= 10
GROUP BY r.rider_name
ORDER BY avg_rank;
```

## Contributing

1. Run tests before making changes:
```bash
python src/run_tests.py
```

2. Follow the existing code style and patterns
3. Add tests for new functionality
4. Update documentation as needed

## License

This project is for educational and research purposes. Please respect ProCyclingStats' terms of service and use reasonable request rates.

## Troubleshooting

### Common Issues

- **Import errors**: Ensure virtual environment is activated
- **Database locked**: Check for running scraper processes
- **Network timeouts**: Reduce concurrency or increase delays
- **Parse errors**: Check logs for website format changes

### Getting Help

1. Check the logs in `logs/` directory
2. Run with `--verbose` flag for detailed output
3. Use the testing framework to validate functionality
4. Review the documentation files

## Changelog

### Latest Version
- ✅ Added comprehensive rider profile scraping
- ✅ Improved error handling and progress tracking
- ✅ Enhanced testing framework
- ✅ Better documentation and CLI interface
