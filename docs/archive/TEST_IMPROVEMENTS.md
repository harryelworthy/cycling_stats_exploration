# Enhanced Testing Framework - Key Improvements

## Fixed UCI Points Values
- **Tour de France Winner**: Corrected from 1000 to 1300 UCI points
- **Grand Tour Winners**: All set to 1300 UCI points (TdF, Giro, Vuelta)
- **Monument Winners**: 500 UCI points (Paris-Roubaix, Milano-Sanremo, etc.)
- **Stage Wins**: 120 UCI points for Grand Tour stage victories

## Distinguishes Between GC and Stage Results
- **GC Results**: `/gc` URLs test overall classification UCI points (higher values)
- **Stage Results**: `/stage-X` or `/result` URLs test stage-specific UCI points (lower values)
- **Test Type Labels**: Each test marked as 'gc', 'stage', 'general', or 'historical'

## Historical Coverage (One Per Decade)
- **2010s**: Egan Bernal TDF 2019 (Team INEOS, age 22)
- **2000s**: Lance Armstrong TDF 2005 (Discovery Channel, age 33)
- **1990s**: Lance Armstrong TDF 1999 (US Postal Service, age 27)  
- **1980s**: Greg LeMond TDF 1986 (La Vie Claire, age 25)

## Multi-Variable Validation
Beyond just UCI points, now tests:
- **Team Names**: Flexible matching for team name variations
- **Rider Ages**: ±1 year tolerance for birthday timing
- **Rankings**: Exact position validation
- **Race Types**: GC vs stage vs one-day classics

## Enhanced Test Cases
- **14 known results** spanning different race types and eras
- **Monument classics** (Paris-Roubaix, Milano-Sanremo)
- **Grand Tour GC winners** and podium finishers
- **Stage victories** with appropriate UCI points
- **Women's racing** (Tour de France Femmes)

## Improved Error Reporting
- **Test Type Context**: Shows whether it's testing GC, stage, or general result
- **Multiple Validation Errors**: Separate errors for UCI points, team, age, rank
- **Detailed Comparisons**: Expected vs actual for all validated fields

## Quick Test Runner
- **`quick_accuracy_test.py`**: Runs only known results validation (fast)
- **Detailed Failure Output**: Shows exactly what's wrong (UCI points, team, age)
- **Test Type Labels**: Helps identify which type of result failed

## Usage Examples

```bash
# Quick accuracy check (validates known results only)
cd src && python quick_accuracy_test.py

# Full test suite (includes structure, format, and known results)
cd src && python run_tests.py

# Test through main scraper
cd src && python main.py --test-only 2024
```

## Key Validation Improvements
1. **UCI Points Range**: Now allows up to 1300 (was 1000)
2. **Winner Logic**: Validates that race winners have highest UCI points
3. **Flexible Team Matching**: Handles "Team Jumbo-Visma" vs "Jumbo-Visma" variations
4. **Age Tolerance**: Accounts for races at different times of year
5. **Historical Context**: Tests older races with different data availability

This enhanced framework will catch UCI points errors immediately and validate multiple data fields, giving you confidence in your scraper's accuracy before processing full years of data.