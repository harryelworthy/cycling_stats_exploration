#!/usr/bin/env python3
"""
Historical data handler for very early cycling data (1903-1980)
Handles special cases and format differences in historical cycling data
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

class HistoricalDataHandler:
    """Handler for historical cycling data with different formats"""
    
    @staticmethod
    def is_historical_year(year: int) -> bool:
        """Check if a year is considered historical (different data format)"""
        return year < 1980
    
    @staticmethod
    def is_very_early_year(year: int) -> bool:
        """Check if a year is very early (may have very limited data)"""
        return year < 1950
    
    @staticmethod
    def adjust_expectations_for_year(year: int) -> Dict[str, Any]:
        """Adjust data expectations based on the year"""
        if year < 1920:
            return {
                'min_races_expected': 1,  # Very few races existed
                'min_results_per_race': 5,  # Small fields
                'expect_team_data': False,  # Teams weren't organized yet
                'expect_time_data': False,  # Timing was basic
                'expect_uci_points': False,  # UCI didn't exist
                'expect_multiple_classifications': False  # Only general classification
            }
        elif year < 1950:
            return {
                'min_races_expected': 3,
                'min_results_per_race': 15,
                'expect_team_data': True,
                'expect_time_data': True,
                'expect_uci_points': False,  # UCI founded 1900 but points system much later
                'expect_multiple_classifications': False
            }
        elif year < 1980:
            return {
                'min_races_expected': 8,
                'min_results_per_race': 30,
                'expect_team_data': True,
                'expect_time_data': True,
                'expect_uci_points': False,  # Modern UCI points system started later
                'expect_multiple_classifications': True
            }
        else:
            return {
                'min_races_expected': 15,
                'min_results_per_race': 50,
                'expect_team_data': True,
                'expect_time_data': True,
                'expect_uci_points': True,
                'expect_multiple_classifications': True
            }
    
    @staticmethod
    def get_historical_context(year: int) -> str:
        """Get historical context for a given year"""
        if year == 1903:
            return "First Tour de France - only 6 stages, 60 starters, 21 finishers"
        elif year < 1910:
            return "Very early cycling era - few organized races, primitive timing"
        elif year < 1920:
            return "Pre-WWI cycling - limited international racing"
        elif year < 1940:
            return "Inter-war period - growing race calendar but still limited"
        elif year < 1950:
            return "Post-WWII recovery period - racing resuming"
        elif year < 1970:
            return "Classic era - modern race formats developing"
        elif year < 1980:
            return "Professional era - organized teams and sponsorship"
        else:
            return "Modern cycling era"
    
    @staticmethod
    def validate_historical_data(year: int, race_data: Dict[str, Any]) -> List[str]:
        """Validate data against historical expectations"""
        expectations = HistoricalDataHandler.adjust_expectations_for_year(year)
        issues = []
        
        # Check stage data expectations
        for stage in race_data.get('stages', []):
            results = stage.get('results', [])
            
            if len(results) < expectations['min_results_per_race']:
                # This might be expected for very early years
                if year < 1920:
                    logger.info(f"Small field ({len(results)} riders) expected for {year}")
                else:
                    issues.append(f"Unexpectedly small field: {len(results)} results")
            
            # Check data completeness based on era
            if results:
                first_result = results[0]
                
                if expectations['expect_team_data'] and not first_result.get('team_name'):
                    issues.append(f"Missing team data for {year} (expected for this era)")
                
                if expectations['expect_time_data'] and not first_result.get('time'):
                    issues.append(f"Missing time data for {year} (expected for this era)")
                
                if not expectations['expect_uci_points'] and first_result.get('uci_points', 0) > 0:
                    issues.append(f"Unexpected UCI points for {year} (system didn't exist)")
        
        return issues
    
    @staticmethod
    def enhance_historical_race_info(year: int, race_info: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance race info with historical context"""
        if not race_info:
            return race_info
            
        # Add historical context
        race_info['historical_context'] = HistoricalDataHandler.get_historical_context(year)
        race_info['is_historical'] = HistoricalDataHandler.is_historical_year(year)
        race_info['data_expectations'] = HistoricalDataHandler.adjust_expectations_for_year(year)
        
        # Adjust race category for very early years
        if year < 1950 and race_info.get('race_category') == 'Unknown':
            race_info['race_category'] = 'Historical'
        
        return race_info
    
    @staticmethod
    def get_known_historical_races(year: int) -> List[Dict[str, Any]]:
        """Get list of known races for historical years"""
        known_races = []
        
        # Tour de France (started 1903)
        if year >= 1903:
            # Tour was cancelled during WWI (1915-1918) and WWII (1940-1946)
            if not (1915 <= year <= 1918 or 1940 <= year <= 1946):
                known_races.append({
                    'name': 'Tour de France',
                    'url_pattern': f'race/tour-de-france/{year}',
                    'type': 'stage_race',
                    'established': 1903
                })
        
        # Giro d'Italia (started 1909)
        if year >= 1909:
            # Giro was cancelled during WWI (1915-1918) and WWII (1941-1945)
            if not (1915 <= year <= 1918 or 1941 <= year <= 1945):
                known_races.append({
                    'name': "Giro d'Italia",
                    'url_pattern': f'race/giro-d-italia/{year}',
                    'type': 'stage_race',
                    'established': 1909
                })
        
        # Paris-Roubaix (started 1896)
        if year >= 1896:
            # Cancelled during wars
            if not (1915 <= year <= 1918 or 1940 <= year <= 1942):
                known_races.append({
                    'name': 'Paris-Roubaix',
                    'url_pattern': f'race/paris-roubaix/{year}',
                    'type': 'one_day',
                    'established': 1896
                })
        
        # Milano-Sanremo (started 1907)
        if year >= 1907:
            # Cancelled during wars
            if not (1916 <= year <= 1918 or 1944 <= year <= 1945):
                known_races.append({
                    'name': 'Milano-Sanremo',
                    'url_pattern': f'race/milano-sanremo/{year}',
                    'type': 'one_day',
                    'established': 1907
                })
        
        return known_races 