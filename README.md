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
**results**: stage_id, rider_name, team, position, time, uci_points, pcs_points, age  
**classifications**: stage_id, rider_name, classification_type (gc/points/kom/youth), rank, time_gap, points_total, uci_points, pcs_points  
**riders**: rider_url, nationality, date_of_birth, weight, height, specialties  
**rider_teams**: rider_url, team_name, year_start, year_end  
**rider_achievements**: rider_url, achievement_type, race_name, year, position

*Note: GC rankings moved from results to classifications table for proper data normalization*

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

## Current Status

### Scraper Accuracy: **95.7%** 🚀  
*Approaching 100% - up from 36.1% initial accuracy*

**Recent Achievements:**
- ✅ **Race metadata extraction**: Now extracts race_name, race_url, race_type, winner  
- ✅ **Demographic data**: Age, specialty, bib number extraction working  
- ✅ **Time formatting**: Proper time parsing (e.g., "5:25:58")  
- ✅ **Rider name formatting**: Fixed parsing ("Mathieu van der Poel")  
- ✅ **GC/Classification URLs**: Systematic discovery of GC, points, KOM, youth classifications  
- ✅ **Database normalization**: Separated results vs classifications data  
- ✅ **GC table selection**: Improved parsing to get correct GC winners vs stage winners
- ✅ **Jersey leaders**: Extract gc_leader, points_leader, youth_leader for stage races
- ✅ **Jersey extraction**: Identify yellow jersey holders in GC results

**Individual Fixture Performance:**
- Paris-Roubaix 2024: **100.0%** accuracy ✅
- Amstel Gold Race 2015: **100.0%** accuracy ✅
- Tour de France 2016 Stage 14: **100.0%** accuracy ✅
- Milano-Sanremo 1985: **100.0%** accuracy ✅
- Giro d'Italia 2013 GC: **95.0%** accuracy
- Paris-Roubaix 1905: **90.9%** accuracy
- Tour de France 1986 GC: **84.6%** accuracy
- Tour de France 1905 GC: **81.2%** accuracy

### Remaining Minor Issues  
- Missing `time_gap` for some GC results
- Missing `point_classification` arrays for GC pages