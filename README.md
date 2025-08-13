# PC Puller - Cycling Data Scraper

A tool to scrape professional cycling race data from ProCyclingStats.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Scrape race data
python src/main.py 2024

# Run tests
python tests/fixtures_test.py

# Update rider profiles
python src/update_riders.py
```

## Database Schema

**Main database location**: `data/cycling_data.db`

**races**: race_name, race_url, date, distance, profile_score, departure, arrival  
**stages**: race_id, stage_url, distance, stage_type, date, won_how, avg_speed_winner  
**results**: stage_id, rider_name, team, position, time, uci_points, pcs_points, age  
**classifications**: stage_id, rider_name, classification_type (gc/points/kom/youth), rank, time_gap, points_total, uci_points, pcs_points  
**riders**: rider_url, nationality, date_of_birth, weight, height, specialties  
**rider_teams**: rider_url, team_name, year_start, year_end  
**rider_achievements**: rider_url, achievement_type, race_name, year, position

*Note: GC rankings moved from results to classifications table for proper data normalization*

## Commands

**Scrape specific year**: `python src/main.py YEAR`  
**Scrape with automatic rider updates**: `python src/main.py YEAR --overwrite-riders`  
**Scrape with rider profiles**: `python src/main.py YEAR --enable-rider-scraping`  
**Overwrite existing data**: `python src/main.py YEAR --overwrite-data`  
**Test scraper accuracy**: `python tests/fixtures_test.py`  
**Update riders only**: `python src/update_riders.py --all-missing`

## Configuration

Default settings: 30 concurrent requests, 0.1s delay, 3 retries, SQLite database at `data/cycling_data.db`.  
Adjust: `python src/main.py YEAR --max-concurrent 10 --request-delay 0.2`

## Core Files

- `src/main.py` - CLI entry point
- `src/async_scraper.py` - Main scraper engine  
- `src/rider_scraper.py` - Rider profile scraper
- `src/progress_tracker.py` - Progress management
- `tests/fixtures_test.py` - Accuracy testing

## Features

- **High-performance async scraping** with configurable concurrency controls
- **Comprehensive data extraction**: races, stages, results, classifications, rider profiles  
- **Automatic rider profile updates** after scraping race data
- **Progress tracking and resumable sessions** for large multi-year scrapes
- **Database integrity** with proper foreign key relationships and data validation
- **Flexible overwrite options** for data updates and corrections
- **Historical race support** back to early 1900s
- **Detailed test coverage** with accuracy validation against known fixtures