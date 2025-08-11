# PC Puller - Cycling Data Scraper

## Current Status
- **Scraper Accuracy**: 28.5% (needs improvement)
- **Test Command**: `python tests/fixtures_test.py`
- **Main Issues**: Missing race metadata, position mapping, time formatting

## Database Tables
- **races**: race_name, race_url, date, distance, profile_score
- **stages**: stage_url, distance, stage_type, date, results
- **results**: rider_name, team, position, time, uci_points, pcs_points
- **riders**: rider profiles, nationality, specialties, rankings

## Key Commands
- **Scrape data**: `python src/main.py YEAR`
- **Run tests**: `python tests/fixtures_test.py`
- **Update riders**: `python src/update_riders.py`

## CRITICAL RULE
🚨 **NO NEW FILES** without explicit approval. Consolidate into existing files instead of proliferating more debug/temp scripts.

## Details
See README.md for installation, database schema, and configuration details.