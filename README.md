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

**races**: race_name, race_url, date, distance, profile_score, departure, arrival  
**stages**: race_id, stage_url, distance, stage_type, date, won_how, avg_speed_winner  
**results**: stage_id, rider_name, team, position, time, uci_points, pcs_points, rank  
**classifications**: stage_id, rider_name, classification_type (gc/points/kom/youth), rank, time_gap, points_total, uci_points, pcs_points  
**riders**: rider_url, nationality, date_of_birth, weight, height, specialties  
**rider_teams**: rider_url, team_name, year_start, year_end  
**rider_achievements**: rider_url, achievement_type, race_name, year, position

## Commands

**Scrape specific year**: `python src/main.py YEAR`  
**Scrape with rider profiles**: `python src/main.py YEAR --enable-rider-scraping`  
**Test scraper accuracy**: `python tests/fixtures_test.py`  
**Update riders only**: `python src/update_riders.py --all-missing`  
**Check status**: `python src/scraper_cli.py status`

## Configuration

Default settings: 50 concurrent requests, 0.1s delay, 3 retries, SQLite database.  
Adjust: `python src/main.py YEAR --max-concurrent 10 --request-delay 0.2`

## Core Files

- `src/main.py` - CLI entry point
- `src/async_scraper.py` - Main scraper engine  
- `src/rider_scraper.py` - Rider profile scraper
- `src/progress_tracker.py` - Progress management
- `tests/fixtures_test.py` - Accuracy testing

## TODOS

### Critical Scraper Accuracy Issues (Current: 36.1% accuracy)
- **Priority 1**: Fix missing race metadata (race_name, race_url, race_type, winner) - affects all 8/8 test fixtures
- **Priority 2**: Fix missing demographic data (age, specialty, bib) - affects 8/8 test fixtures
- **Priority 3**: Fix incorrect time formatting (missing seconds: "5:25" vs "5:25:58") - affects 6/8 test fixtures
- **Priority 4**: Fix incorrect rider name formatting ("van der PoelMathieu" vs "Mathieu van der Poel") - affects 4/8 test fixtures
- **Priority 5**: Fix GC/classification URL discovery in `get_race_info()` - currently only finds `/stage-*` and `/result` URLs
  - Missing final GC URLs: `/race/giro-d-italia/2024/gc`
  - Missing mid-stage GC URLs: `/race/giro-d-italia/2024/stage-8-gc`  
  - Missing Points/KOM URLs: `/race/giro-d-italia/2024/points`, `/race/giro-d-italia/2024/kom`
  - Root cause: URL discovery logic doesn't include classification patterns
  - Impact: Classifications table may be underpopulated despite proper 404 handling