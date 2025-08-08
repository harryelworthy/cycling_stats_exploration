#!/usr/bin/env python3
"""
Comprehensive test framework for cycling data scraper
Tests the scraping pipeline end-to-end with specific races to validate format compatibility
"""

import asyncio
import logging
import json
import sys
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
import aiosqlite

from improved_async_scraper import ImprovedAsyncCyclingDataScraper as AsyncCyclingDataScraper, ScrapingConfig

# Configure detailed test logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestRace:
    """Represents a test race for validation"""
    year: int
    race_url: str
    expected_name: str
    race_type: str  # 'one_day', 'stage_race', 'national_championship'
    min_results: int  # Minimum expected results
    description: str

@dataclass
class KnownResult:
    """Known result for accuracy validation"""
    race_url: str
    rider_name: str
    expected_rank: int
    expected_uci_points: Optional[int] = None
    expected_pcs_points: Optional[int] = None
    expected_time: Optional[str] = None
    expected_team: Optional[str] = None
    expected_age: Optional[int] = None
    expected_time_gap: Optional[str] = None  # e.g., "4:43" for time behind
    expected_avg_speed: Optional[float] = None  # e.g., 43.74 kph
    expected_won_how: Optional[str] = None  # e.g., "Spring of small group"
    expected_startlist_quality: Optional[int] = None  # e.g., 1933
    expected_distance: Optional[float] = None  # e.g., 161.0 km
    expected_elevation: Optional[int] = None  # e.g., 1679 m
    expected_profile_score: Optional[int] = None  # e.g., 29
    jersey_type: Optional[str] = None  # 'gc', 'points', 'kom', 'youth' for jersey standings
    expected_gc_rank: Optional[int] = None  # Expected GC rank for stage results
    expected_points_rank: Optional[int] = None  # Expected points classification rank
    expected_kom_rank: Optional[int] = None  # Expected KOM classification rank
    expected_youth_rank: Optional[int] = None  # Expected youth classification rank
    description: str = ""
    test_type: str = "general"  # 'gc', 'stage', 'general', 'historical', 'jersey', 'stage_with_gc'

@dataclass
class TestResult:
    """Result of a test case"""
    test_name: str
    passed: bool
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    execution_time: float = 0.0

@dataclass
class ScrapingValidationError:
    """Detailed error information for debugging"""
    stage: str  # 'get_races', 'get_race_info', 'get_stage_info', 'parse_results'
    url: str
    error_type: str
    error_message: str
    html_snippet: Optional[str] = None
    expected_vs_actual: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.now)

class ScraperTestFramework:
    """Comprehensive test framework for the cycling data scraper"""
    
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig(
            max_concurrent_requests=5,  # Conservative for testing
            request_delay=0.2,
            database_path="test_cycling_data.db"
        )
        self.validation_errors: List[ScrapingValidationError] = []
        self.test_results: List[TestResult] = []
        
        # Known results for accuracy validation - VERIFIED DATA FROM USER
        self.known_results = [
            # === VERIFIED GRAND TOUR GC RESULTS ===
            # 2013 Giro d'Italia Winner - Vincenzo Nibali
            KnownResult(
                race_url="race/giro-d-italia/2013/gc",
                rider_name="Vincenzo Nibali",
                expected_rank=1,
                expected_pcs_points=400,  # User verified: Nibali won 400 PCS points for GC
                test_type="gc",
                description="Vincenzo Nibali Giro 2013 GC Winner - 400 PCS points (VERIFIED)"
            ),
            
            # 2013 Giro d'Italia 2nd place - Rigoberto Uran
            KnownResult(
                race_url="race/giro-d-italia/2013/gc",
                rider_name="Rigoberto Uran",
                expected_rank=2,
                expected_pcs_points=290,  # User verified: Uran got 290 PCS points for 2nd
                expected_time_gap="4:43",  # User verified: 4:43 behind
                test_type="gc",
                description="Rigoberto Uran Giro 2013 GC 2nd - 290 PCS points, +4:43 (VERIFIED)"
            ),
            
            # 1930 Tour de France Winner - André Leducq  
            KnownResult(
                race_url="race/tour-de-france/1930/gc",
                rider_name="André Leducq",
                expected_rank=1,
                expected_pcs_points=500,  # User verified: 500 PCS points for GC win
                test_type="historical",
                description="André Leducq TDF 1930 GC Winner - 500 PCS points (VERIFIED)"
            ),
            
            # === VERIFIED CLASSICS ===
            # 1980 Paris-Roubaix Winner - Francesco Moser
            KnownResult(
                race_url="race/paris-roubaix/1980/result",
                rider_name="Francesco Moser",
                expected_rank=1,
                expected_pcs_points=275,  # User verified: 275 PCS points
                expected_team="Sanson",  # User verified team
                test_type="historical",
                description="Francesco Moser Paris-Roubaix 1980 Winner - 275 PCS points (VERIFIED)"
            ),
            
            # === VERIFIED STAGE WINS ===
            # 1991 Tour de France Stage 9 - Mauro Ribeiro (FULL DATA)
            KnownResult(
                race_url="race/tour-de-france/1991/stage-9",
                rider_name="Mauro Ribeiro",
                expected_rank=1,
                expected_pcs_points=100,  # User verified: 100 PCS points for stage
                expected_team="R.M.O.",  # User verified team
                expected_avg_speed=43.74,  # User verified: 43.74 kph
                expected_won_how="Spring of small group",  # User verified: Won in a Spring of small group
                expected_startlist_quality=1933,  # User verified: startlist quality score 1933
                expected_distance=161.0,  # User verified: 161km distance
                expected_elevation=1679,  # User verified: 1679m vertical
                expected_profile_score=29,  # User verified: profile score 29
                test_type="stage",
                description="Mauro Ribeiro TDF 1991 Stage 9 Winner - FULL DATA (VERIFIED)"
            ),
            
            # 2016 Tour de France Stage 14 - Mark Cavendish (FULL DATA)
            KnownResult(
                race_url="race/tour-de-france/2016/stage-14",
                rider_name="Mark Cavendish",
                expected_rank=1,
                expected_uci_points=120,  # User verified: 120 UCI points
                expected_pcs_points=100,  # User verified: 100 PCS points
                expected_avg_speed=36.39,  # User verified: 36.39 kph
                expected_distance=208.5,  # User verified: 208.5km distance
                expected_elevation=1958,  # User verified: 1958m vertical
                expected_profile_score=30,  # User verified: profile score 30
                test_type="stage",
                description="Mark Cavendish TDF 2016 Stage 14 Winner - FULL DATA (VERIFIED)"
            ),
            
            # 2022 Tour de France Stage 12 - Tom Pidcock (MOUNTAIN STAGE)
            KnownResult(
                race_url="race/tour-de-france/2022/stage-12",
                rider_name="Tom Pidcock",
                expected_rank=1,
                expected_distance=165.1,  # User verified: 165.1km distance
                expected_avg_speed=33.534,  # User verified: 33.534 kph
                expected_elevation=4660,  # User verified: 4660m vertical
                expected_profile_score=389,  # User verified: profile score 389
                expected_won_how="11 km solo",  # User verified: won how is 11 km solo
                test_type="stage",
                description="Tom Pidcock TDF 2022 Stage 12 Winner - Mountain stage, 11km solo (VERIFIED)"
            ),
            
            # === VERIFIED CLASSICS ===
            # 2015 Amstel Gold Race - Michał Kwiatkowski
            KnownResult(
                race_url="race/amstel-gold-race/2015/result",
                rider_name="Michał Kwiatkowski",
                expected_rank=1,
                expected_uci_points=80,  # User verified: 80 UCI points
                expected_pcs_points=225,  # User verified: 225 PCS points
                expected_won_how="Sprint of a small group",  # User verified: Sprint of a small group
                expected_distance=258.0,  # User verified: 258km distance
                expected_elevation=3558,  # User verified: 3558m vertical
                test_type="general",
                description="Michał Kwiatkowski Amstel Gold 2015 Winner - Sprint of small group (VERIFIED)"
            ),
            
            # === VERIFIED JERSEY STANDINGS ===
            # TDF 2018 Stage 4 - GC Leader Greg van Avermaet
            KnownResult(
                race_url="race/tour-de-france/2018/stage-4/gc",
                rider_name="Greg van Avermaet",
                expected_rank=1,
                jersey_type="gc",
                test_type="jersey",
                description="Greg van Avermaet TDF 2018 Stage 4 - GC Leader (Yellow Jersey) (VERIFIED)"
            ),
            
            # TDF 2018 Stage 4 - Points Leader Peter Sagan
            KnownResult(
                race_url="race/tour-de-france/2018/stage-4/points",
                rider_name="Peter Sagan",
                expected_rank=1,
                jersey_type="points",
                test_type="jersey",
                description="Peter Sagan TDF 2018 Stage 4 - Points Leader (Green Jersey) (VERIFIED)"
            ),
            
            # TDF 2018 Stage 4 - KOM Leader Van Keirsbulck (corrected from Dion Smith)
            KnownResult(
                race_url="race/tour-de-france/2018/stage-4/kom",
                rider_name="Van Keirsbulck",
                expected_rank=1,
                jersey_type="kom",
                test_type="jersey",
                description="Van Keirsbulck TDF 2018 Stage 4 - KOM Leader (Polka Dot Jersey) (CORRECTED)"
            ),
            
            # Tour de Suisse 2012 - GC Leader Rui Costa
            KnownResult(
                race_url="race/tour-de-suisse/2012/gc",
                rider_name="Rui Costa",
                expected_rank=1,
                jersey_type="gc",
                test_type="jersey",
                description="Rui Costa Tour de Suisse 2012 - GC Leader (VERIFIED)"
            ),
            
            # Tour de Suisse 2012 - KOM Leader Montaguti (corrected from Frank Schleck)
            KnownResult(
                race_url="race/tour-de-suisse/2012/kom",
                rider_name="Montaguti",
                expected_rank=1,
                jersey_type="kom",
                test_type="jersey",
                description="Montaguti Tour de Suisse 2012 - KOM Leader (CORRECTED)"
            ),
            
            # === STAGE-LEVEL GC TESTS (Should include GC data for stage results) ===
            # Tour de France 2024 Stage 21 - Final stage should have complete GC standings
            KnownResult(
                race_url="race/tour-de-france/2024/stage-21",
                rider_name="Tadej Pogačar",
                expected_rank=1,  # Stage winner
                expected_gc_rank=1,  # GC winner
                test_type="stage_with_gc",
                description="TDF 2024 Stage 21 - Pogacar stage win + GC win (STAGE GC TEST)"
            ),
            
            # Tour de France 2024 Stage 21 - Vingegaard should be 2nd in GC
            KnownResult(
                race_url="race/tour-de-france/2024/stage-21",
                rider_name="Jonas Vingegaard", 
                expected_rank=999,  # We don't care about stage rank, just GC
                expected_gc_rank=2,  # 2nd in GC
                test_type="stage_with_gc",
                description="TDF 2024 Stage 21 - Vingegaard 2nd in GC (STAGE GC TEST)"
            ),
            
            # Mid-race stage test - Tour de France 2024 Stage 15
            KnownResult(
                race_url="race/tour-de-france/2024/stage-15",
                rider_name="Tadej Pogačar",
                expected_rank=999,  # We don't care about stage rank, just GC
                expected_gc_rank=1,  # Should be GC leader after stage 15
                test_type="stage_with_gc", 
                description="TDF 2024 Stage 15 - Pogacar GC leader mid-race (STAGE GC TEST)"
            ),
            
            # Giro d'Italia 2024 final stage GC test
            KnownResult(
                race_url="race/giro-d-italia/2024/stage-21",
                rider_name="Tadej Pogačar",
                expected_rank=999,  # We don't care about stage rank, just GC
                expected_gc_rank=1,  # GC winner
                test_type="stage_with_gc",
                description="Giro 2024 Stage 21 - Pogacar GC winner (STAGE GC TEST)"
            )
        ]
        
        # Define comprehensive test races covering different scenarios
        self.test_races = [
            # Recent one-day classics (format should be stable)
            TestRace(
                year=2024,
                race_url="race/paris-roubaix/2024",
                expected_name="Paris-Roubaix",
                race_type="one_day",
                min_results=50,
                description="2024 Paris-Roubaix - Major one-day classic"
            ),
            
            # Multi-stage race
            TestRace(
                year=2024,
                race_url="race/tour-de-france/2024",
                expected_name="Tour de France",
                race_type="stage_race",
                min_results=150,
                description="2024 Tour de France - Major stage race"
            ),
            
            # Another classic race (reliable format)
            TestRace(
                year=2024,
                race_url="race/liege-bastogne-liege/2024",
                expected_name="Liège-Bastogne-Liège",
                race_type="one_day_classic",
                min_results=80,
                description="2024 Liège-Bastogne-Liège - Monument classic"
            ),
            
            # Older race to test format compatibility
            TestRace(
                year=2019,
                race_url="race/milano-sanremo/2019",
                expected_name="Milano-Sanremo",
                race_type="one_day",
                min_results=100,
                description="2019 Milano-Sanremo - Test older format"
            ),
            
            # Another older multi-stage race (COVID-affected year)
            TestRace(
                year=2020,
                race_url="race/giro-d-italia/2020",
                expected_name="Giro d'Italia",
                race_type="stage_race",
                min_results=120,  # Lower expectation for COVID year (smaller fields)
                description="2020 Giro d'Italia - Test COVID-affected year"
            ),
            
            # Historical test - early Tour de France
            TestRace(
                year=1903,
                race_url="race/tour-de-france/1903",
                expected_name="Tour de France",
                race_type="historical_stage_race",
                min_results=10,  # Much smaller field in 1903
                description="1903 Tour de France - First ever Tour (historical test)"
            )
        ]
    
    async def run_full_test_suite(self) -> bool:
        """Run the complete test suite and return True if all tests pass"""
        logger.info("🚀 Starting comprehensive scraper test suite")
        
        start_time = datetime.now()
        
        try:
            async with AsyncCyclingDataScraper(self.config) as scraper:
                # Test 1: Basic connectivity and race listing
                await self._test_basic_connectivity(scraper)
                
                # Test 2: Individual race tests
                for test_race in self.test_races:
                    await self._test_individual_race(scraper, test_race)
                
                # Test 3: Data validation tests
                await self._test_data_validation(scraper)
                
                # Test 4: Known result accuracy tests
                await self._test_known_results(scraper)
                
                # Test 5: Format consistency tests
                await self._test_format_consistency(scraper)
                
                # Test 6: Database GC data verification tests
                await self._test_database_gc_data()
                
        except Exception as e:
            logger.error(f"❌ Test suite failed with exception: {e}")
            self.validation_errors.append(
                ScrapingValidationError(
                    stage="test_suite",
                    url="N/A",
                    error_type="critical_failure",
                    error_message=str(e)
                )
            )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Generate comprehensive test report
        passed_tests = len([r for r in self.test_results if r.passed])
        total_tests = len(self.test_results)
        
        await self._generate_test_report(execution_time, passed_tests, total_tests)
        
        # Return True if all tests passed and no validation errors
        all_passed = total_tests > 0 and passed_tests == total_tests and len(self.validation_errors) == 0
        
        if all_passed:
            logger.info("✅ All tests passed! Scraper is ready for full year processing.")
        else:
            logger.error("❌ Some tests failed. Review the test report before processing full years.")
        
        return all_passed
    
    async def _test_basic_connectivity(self, scraper: AsyncCyclingDataScraper):
        """Test basic connectivity and race listing functionality"""
        logger.info("🔍 Testing basic connectivity...")
        
        test_start = datetime.now()
        
        try:
            # Test getting races for a recent year
            race_urls = await scraper.get_races(2024)
            
            execution_time = (datetime.now() - test_start).total_seconds()
            
            if len(race_urls) < 50:  # Expect at least 50 races
                self.validation_errors.append(
                    ScrapingValidationError(
                        stage="get_races",
                        url="races.php?season=2024",
                        error_type="insufficient_results",
                        error_message=f"Only found {len(race_urls)} races, expected at least 50",
                        expected_vs_actual={"expected_min": 50, "actual": len(race_urls)}
                    )
                )
                self.test_results.append(TestResult(
                    test_name="basic_connectivity",
                    passed=False,
                    error=f"Insufficient race URLs found: {len(race_urls)}",
                    execution_time=execution_time
                ))
            else:
                self.test_results.append(TestResult(
                    test_name="basic_connectivity",
                    passed=True,
                    details={"race_count": len(race_urls)},
                    execution_time=execution_time
                ))
                logger.info(f"✅ Basic connectivity test passed - found {len(race_urls)} races")
                
        except Exception as e:
            execution_time = (datetime.now() - test_start).total_seconds()
            self.validation_errors.append(
                ScrapingValidationError(
                    stage="get_races",
                    url="races.php?season=2024",
                    error_type="connection_failure",
                    error_message=str(e)
                )
            )
            self.test_results.append(TestResult(
                test_name="basic_connectivity",
                passed=False,
                error=str(e),
                execution_time=execution_time
            ))
    
    async def _test_individual_race(self, scraper: AsyncCyclingDataScraper, test_race: TestRace):
        """Test scraping a specific race end-to-end"""
        logger.info(f"🏁 Testing race: {test_race.description}")
        
        test_start = datetime.now()
        
        try:
            # Step 1: Get race info
            race_info = await scraper.get_race_info(test_race.race_url)
            
            if not race_info:
                raise Exception(f"Failed to get race info for {test_race.race_url}")
            
            # Validate race name
            if test_race.expected_name.lower() not in race_info['race_name'].lower():
                self.validation_errors.append(
                    ScrapingValidationError(
                        stage="get_race_info",
                        url=test_race.race_url,
                        error_type="name_mismatch",
                        error_message=f"Race name mismatch",
                        expected_vs_actual={
                            "expected": test_race.expected_name,
                            "actual": race_info['race_name']
                        }
                    )
                )
            
            # Step 2: Test stage URLs
            stage_urls = race_info.get('stage_urls', [])
            if not stage_urls:
                raise Exception(f"No stage URLs found for {test_race.race_url}")
            
            # Step 3: Test first stage in detail
            first_stage_url = stage_urls[0]
            stage_info = await scraper.get_stage_info(first_stage_url)
            
            if not stage_info:
                raise Exception(f"Failed to get stage info for {first_stage_url}")
            
            # Validate results
            results = stage_info.get('results', [])
            if len(results) < test_race.min_results:
                self.validation_errors.append(
                    ScrapingValidationError(
                        stage="get_stage_info",
                        url=first_stage_url,
                        error_type="insufficient_results",
                        error_message=f"Only found {len(results)} results, expected at least {test_race.min_results}",
                        expected_vs_actual={
                            "expected_min": test_race.min_results,
                            "actual": len(results)
                        }
                    )
                )
            
            # Validate result structure
            if results:
                first_result = results[0]
                required_fields = ['rider_name', 'rank']
                missing_fields = [field for field in required_fields if field not in first_result or not first_result[field]]
                
                if missing_fields:
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="parse_results",
                            url=first_stage_url,
                            error_type="missing_fields",
                            error_message=f"Missing required fields: {missing_fields}",
                            expected_vs_actual={
                                "missing_fields": missing_fields,
                                "sample_result": first_result
                            }
                        )
                    )
            
            execution_time = (datetime.now() - test_start).total_seconds()
            
            # Determine if test passed
            race_errors = [e for e in self.validation_errors if test_race.race_url in e.url or first_stage_url in e.url]
            test_passed = len(race_errors) == 0
            
            self.test_results.append(TestResult(
                test_name=f"race_{test_race.race_type}_{test_race.year}",
                passed=test_passed,
                details={
                    "race_name": race_info['race_name'],
                    "stage_count": len(stage_urls),
                    "result_count": len(results),
                    "race_category": race_info.get('race_category'),
                    "first_stage_url": first_stage_url
                },
                execution_time=execution_time
            ))
            
            if test_passed:
                logger.info(f"✅ Race test passed: {test_race.description}")
            else:
                logger.warning(f"⚠️ Race test had issues: {test_race.description}")
                
        except Exception as e:
            execution_time = (datetime.now() - test_start).total_seconds()
            self.validation_errors.append(
                ScrapingValidationError(
                    stage="test_individual_race",
                    url=test_race.race_url,
                    error_type="test_failure",
                    error_message=str(e)
                )
            )
            self.test_results.append(TestResult(
                test_name=f"race_{test_race.race_type}_{test_race.year}",
                passed=False,
                error=str(e),
                execution_time=execution_time
            ))
            logger.error(f"❌ Race test failed: {test_race.description} - {e}")
    
    async def _test_data_validation(self, scraper: AsyncCyclingDataScraper):
        """Test data validation and consistency"""
        logger.info("🔍 Testing data validation...")
        
        test_start = datetime.now()
        
        try:
            # Test with a known race
            test_url = "race/paris-roubaix/2024"
            stage_info = await scraper.get_stage_info(f"{test_url}/result")
            
            if stage_info and stage_info.get('results'):
                results = stage_info['results']
                
                # Check rank sequence
                ranks = [r.get('rank') for r in results if r.get('rank') is not None]
                if ranks:
                    expected_ranks = list(range(1, len(ranks) + 1))
                    if ranks[:len(expected_ranks)] != expected_ranks:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="data_validation",
                                url=test_url,
                                error_type="rank_sequence_error",
                                error_message="Rank sequence is not consecutive starting from 1",
                                expected_vs_actual={
                                    "expected_start": expected_ranks[:10],
                                    "actual_start": ranks[:10]
                                }
                            )
                        )
                
                # Check for duplicate riders
                rider_names = [r.get('rider_name') for r in results if r.get('rider_name')]
                duplicates = [name for name in set(rider_names) if rider_names.count(name) > 1]
                if duplicates:
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="data_validation",
                            url=test_url,
                            error_type="duplicate_riders",
                            error_message=f"Found duplicate rider names: {duplicates[:5]}",
                            expected_vs_actual={"duplicates": duplicates}
                        )
                    )
                
                # Enhanced UCI points validation
                uci_points_with_values = [r.get('uci_points', 0) for r in results if r.get('uci_points', 0) > 0]
                if len(uci_points_with_values) > 0:
                    # Check if UCI points follow expected patterns
                    winner_points = results[0].get('uci_points', 0) if results else 0
                    if winner_points > 0:
                        # Winner should have the highest UCI points (or tied for highest)
                        max_points = max(uci_points_with_values)
                        if winner_points != max_points:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="data_validation",
                                    url=test_url,
                                    error_type="uci_points_logic_error",
                                    error_message="Winner doesn't have highest UCI points",
                                    expected_vs_actual={
                                        "winner_points": winner_points,
                                        "max_points": max_points,
                                        "winner_name": results[0].get('rider_name', 'Unknown')
                                    }
                                )
                            )
                
                # Check for reasonable UCI points ranges
                if uci_points_with_values:
                    max_points = max(uci_points_with_values)
                    # Grand Tour winners get 1300, Monument winners get 500, Stage wins get 120
                    # Anything above 1300 is unrealistic
                    if max_points > 1300:  # No race should give more than 1300 UCI points
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="data_validation",
                                url=test_url,
                                error_type="unrealistic_uci_points",
                                error_message=f"Unrealistically high UCI points detected: {max_points}",
                                expected_vs_actual={"max_points": max_points, "reasonable_max": 1300}
                            )
                        )
            
            execution_time = (datetime.now() - test_start).total_seconds()
            validation_errors_count = len([e for e in self.validation_errors if e.stage == "data_validation"])
            
            self.test_results.append(TestResult(
                test_name="data_validation",
                passed=validation_errors_count == 0,
                details={"validation_errors": validation_errors_count},
                execution_time=execution_time
            ))
            
        except Exception as e:
            execution_time = (datetime.now() - test_start).total_seconds()
            self.test_results.append(TestResult(
                test_name="data_validation",
                passed=False,
                error=str(e),
                execution_time=execution_time
            ))
    
    async def _test_known_results(self, scraper: AsyncCyclingDataScraper):
        """Test specific known results for data accuracy"""
        logger.info("🎯 Testing known result accuracy...")
        
        for known_result in self.known_results:
            test_start = datetime.now()
            test_name = f"known_result_{known_result.rider_name.replace(' ', '_').lower()}"
            
            try:
                logger.info(f"   Testing: {known_result.description}")
                
                # Get stage info for the known result
                stage_info = await scraper.get_stage_info(known_result.race_url)
                
                if not stage_info or not stage_info.get('results'):
                    raise Exception(f"Failed to get results for {known_result.race_url}")
                
                # For GC tests that have parsed GC data, save to database for verification
                if (hasattr(known_result, 'expected_gc_rank') and known_result.expected_gc_rank is not None and 
                    stage_info.get('gc')):
                    try:
                        # Create a test race and stage using the scraper's own methods
                        url_parts = known_result.race_url.split('/')
                        race_name = url_parts[1].replace('-', ' ').title()
                        year = int(url_parts[2])
                        
                        # Create a fake race record
                        race_data = {
                            'race_key': f"test_{url_parts[1]}_{year}",
                            'race_name': race_name,
                            'year': year,
                            'stage_url': f"race/{url_parts[1]}/{year}",
                            'race_category': 'test',
                            'uci_tour': 'test',
                            'stage_urls': [known_result.race_url]
                        }
                        
                        # Save race and get race_id
                        race_id = await scraper.save_race_data(year, race_data)
                        if race_id:
                            # Save stage and get stage_id  
                            stage_data = {
                                'stage_url': known_result.race_url,
                                'stage_type': 'test',
                                'is_one_day_race': False,
                                'distance': 100.0,
                                'date': '2024-01-01',
                                'winning_attack_length': 0.0,
                                'won_how': 'test',
                                'avg_speed_winner': 40.0,
                                'avg_temperature': 20.0,
                                'vertical_meters': 1000,
                                'profile_icon': 'flat',
                                'profile_score': 1,
                                'race_startlist_quality_score': 500
                            }
                            stage_id = await scraper.save_stage_data(race_id, stage_data)
                            if stage_id:
                                # Save results with GC data
                                await scraper.save_results_data(stage_id, stage_info)
                                logger.debug(f"Saved GC test data for {known_result.description}")
                    except Exception as e:
                        logger.debug(f"Could not save GC test data for {known_result.description}: {e}")
                
                results = stage_info['results']
                
                # Find the rider in results
                rider_result = None
                for result in results:
                    if result.get('rider_name', '').lower() == known_result.rider_name.lower():
                        rider_result = result
                        break
                
                if not rider_result:
                    # Try enhanced name matching for different name formats
                    for result in results:
                        rider_name = result.get('rider_name', '').lower()
                        known_name = known_result.rider_name.lower()
                        
                        # Direct match
                        if rider_name == known_name:
                            rider_result = result
                            break
                        
                        # Split name matching (handles "SaganPeter" vs "Peter Sagan")
                        known_parts = known_name.split()
                        rider_parts = rider_name.split()
                        
                        # Check if all parts of known name are in rider name
                        if len(known_parts) >= 2:
                            # For "Peter Sagan" vs "SaganPeter", check if both "peter" and "sagan" are in the rider name
                            if all(part in rider_name for part in known_parts):
                                rider_result = result
                                break
                        
                        # Reverse name matching (handles "SchleckFränk" vs "Frank Schleck")
                        if len(known_parts) >= 2:
                            # Try reversed order
                            reversed_known = ' '.join(reversed(known_parts))
                            if reversed_known in rider_name:
                                rider_result = result
                                break
                        
                        # Partial matching as fallback
                        if any(part in rider_name for part in known_parts):
                            rider_result = result
                            break
                
                execution_time = (datetime.now() - test_start).total_seconds()
                
                if not rider_result:
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="known_results",
                            url=known_result.race_url,
                            error_type="rider_not_found",
                            error_message=f"Rider '{known_result.rider_name}' not found in results",
                            expected_vs_actual={
                                "expected_rider": known_result.rider_name,
                                "available_riders": [r.get('rider_name') for r in results[:10]]
                            }
                        )
                    )
                    self.test_results.append(TestResult(
                        test_name=test_name,
                        passed=False,
                        error=f"Rider '{known_result.rider_name}' not found",
                        execution_time=execution_time
                    ))
                    continue
                
                # Validate rank
                actual_rank = rider_result.get('rank')
                rank_correct = actual_rank == known_result.expected_rank
                
                # Validate UCI points if expected
                uci_points_correct = True
                if known_result.expected_uci_points is not None:
                    actual_uci_points = rider_result.get('uci_points', 0)
                    uci_points_correct = actual_uci_points == known_result.expected_uci_points
                    
                    if not uci_points_correct:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="known_results",
                                url=known_result.race_url,
                                error_type="uci_points_mismatch",
                                error_message=f"UCI points mismatch for {known_result.rider_name} ({known_result.test_type})",
                                expected_vs_actual={
                                    "test_type": known_result.test_type,
                                    "expected_uci_points": known_result.expected_uci_points,
                                    "actual_uci_points": actual_uci_points,
                                    "expected_rank": known_result.expected_rank,
                                    "actual_rank": actual_rank
                                }
                            )
                        )
                
                # Validate PCS points if expected
                pcs_points_correct = True
                if known_result.expected_pcs_points is not None:
                    actual_pcs_points = rider_result.get('pcs_points', 0)
                    pcs_points_correct = actual_pcs_points == known_result.expected_pcs_points
                    
                    if not pcs_points_correct:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="known_results",
                                url=known_result.race_url,
                                error_type="pcs_points_mismatch",
                                error_message=f"PCS points mismatch for {known_result.rider_name} ({known_result.test_type})",
                                expected_vs_actual={
                                    "test_type": known_result.test_type,
                                    "expected_pcs_points": known_result.expected_pcs_points,
                                    "actual_pcs_points": actual_pcs_points,
                                    "expected_rank": known_result.expected_rank,
                                    "actual_rank": actual_rank
                                }
                            )
                        )
                
                # Validate team if expected
                team_correct = True
                if known_result.expected_team is not None:
                    actual_team = rider_result.get('team_name', '')
                    # Flexible team matching (handles slight name variations)
                    if actual_team and known_result.expected_team:
                        team_correct = (known_result.expected_team.lower() in actual_team.lower() or 
                                      actual_team.lower() in known_result.expected_team.lower())
                    else:
                        team_correct = False
                    
                    if not team_correct:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="known_results",
                                url=known_result.race_url,
                                error_type="team_mismatch",
                                error_message=f"Team mismatch for {known_result.rider_name}",
                                expected_vs_actual={
                                    "expected_team": known_result.expected_team,
                                    "actual_team": actual_team,
                                    "rider_name": known_result.rider_name
                                }
                            )
                        )
                
                # Validate age if expected
                age_correct = True
                if known_result.expected_age is not None:
                    actual_age = rider_result.get('age')
                    if actual_age is not None:
                        # Allow +/- 1 year difference (races can be at different times of year)
                        age_correct = abs(actual_age - known_result.expected_age) <= 1
                        
                        if not age_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="age_mismatch",
                                    error_message=f"Age mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_age": known_result.expected_age,
                                        "actual_age": actual_age,
                                        "rider_name": known_result.rider_name
                                    }
                                )
                            )
                
                # Validate average speed if expected
                avg_speed_correct = True
                if known_result.expected_avg_speed is not None:
                    actual_avg_speed = rider_result.get('avg_speed') or stage_info.get('avg_speed_winner')
                    if actual_avg_speed is not None:
                        # Allow small tolerance for speed calculations
                        speed_diff = abs(float(actual_avg_speed) - known_result.expected_avg_speed)
                        avg_speed_correct = speed_diff <= 0.1  # ±0.1 kph tolerance
                        
                        if not avg_speed_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="avg_speed_mismatch",
                                    error_message=f"Average speed mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_avg_speed": known_result.expected_avg_speed,
                                        "actual_avg_speed": actual_avg_speed,
                                        "difference": speed_diff
                                    }
                                )
                            )
                
                # Validate "won how" if expected
                won_how_correct = True
                if known_result.expected_won_how is not None:
                    actual_won_how = rider_result.get('won_how') or stage_info.get('won_how', '')
                    # Flexible matching for "won how" descriptions
                    if actual_won_how and known_result.expected_won_how:
                        # Normalize both strings for comparison
                        expected_normalized = known_result.expected_won_how.lower().replace('spring', 'sprint').replace(' a ', ' ')
                        actual_normalized = actual_won_how.lower().replace('spring', 'sprint').replace(' a ', ' ')
                        won_how_correct = (expected_normalized in actual_normalized or
                                         actual_normalized in expected_normalized)
                    else:
                        won_how_correct = False
                    
                    if not won_how_correct:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="known_results",
                                url=known_result.race_url,
                                error_type="won_how_mismatch",
                                error_message=f"'Won how' mismatch for {known_result.rider_name}",
                                expected_vs_actual={
                                    "expected_won_how": known_result.expected_won_how,
                                    "actual_won_how": actual_won_how
                                }
                            )
                        )
                
                # Validate startlist quality if expected
                startlist_quality_correct = True
                if known_result.expected_startlist_quality is not None:
                    actual_startlist_quality = stage_info.get('race_startlist_quality_score')
                    if actual_startlist_quality is not None:
                        startlist_quality_correct = int(actual_startlist_quality) == known_result.expected_startlist_quality
                        
                        if not startlist_quality_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="startlist_quality_mismatch",
                                    error_message=f"Startlist quality mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_startlist_quality": known_result.expected_startlist_quality,
                                        "actual_startlist_quality": actual_startlist_quality
                                    }
                                )
                            )
                
                # Validate distance if expected
                distance_correct = True
                if known_result.expected_distance is not None:
                    actual_distance = stage_info.get('distance')
                    if actual_distance is not None:
                        # Allow small tolerance for distance
                        distance_diff = abs(float(actual_distance) - known_result.expected_distance)
                        distance_correct = distance_diff <= 0.5  # ±0.5 km tolerance
                        
                        if not distance_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="distance_mismatch",
                                    error_message=f"Distance mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_distance": known_result.expected_distance,
                                        "actual_distance": actual_distance,
                                        "difference": distance_diff
                                    }
                                )
                            )
                
                # Validate elevation if expected
                elevation_correct = True
                if known_result.expected_elevation is not None:
                    actual_elevation = stage_info.get('elevation') or stage_info.get('vertical_meters')
                    if actual_elevation is not None:
                        elevation_correct = int(actual_elevation) == known_result.expected_elevation
                        
                        if not elevation_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="elevation_mismatch",
                                    error_message=f"Elevation mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_elevation": known_result.expected_elevation,
                                        "actual_elevation": actual_elevation
                                    }
                                )
                            )
                
                # Validate profile score if expected
                profile_score_correct = True
                if known_result.expected_profile_score is not None:
                    actual_profile_score = stage_info.get('profile_score')
                    if actual_profile_score is not None:
                        profile_score_correct = int(actual_profile_score) == known_result.expected_profile_score
                        
                        if not profile_score_correct:
                            self.validation_errors.append(
                                ScrapingValidationError(
                                    stage="known_results",
                                    url=known_result.race_url,
                                    error_type="profile_score_mismatch",
                                    error_message=f"Profile score mismatch for {known_result.rider_name}",
                                    expected_vs_actual={
                                        "expected_profile_score": known_result.expected_profile_score,
                                        "actual_profile_score": actual_profile_score
                                    }
                                )
                            )
                
                # Validate GC rank if expected (for stage_with_gc test type)
                gc_rank_correct = True
                if known_result.expected_gc_rank is not None:
                    actual_gc_rank = rider_result.get('gc_rank')
                    gc_rank_correct = actual_gc_rank == known_result.expected_gc_rank
                    
                    if not gc_rank_correct:
                        self.validation_errors.append(
                            ScrapingValidationError(
                                stage="known_results",
                                url=known_result.race_url,
                                error_type="gc_rank_mismatch",
                                error_message=f"GC rank mismatch for {known_result.rider_name} (STAGE GC TEST FAILED)",
                                expected_vs_actual={
                                    "expected_gc_rank": known_result.expected_gc_rank,
                                    "actual_gc_rank": actual_gc_rank,
                                    "test_type": known_result.test_type,
                                    "stage_url": known_result.race_url,
                                    "rider_name": known_result.rider_name,
                                    "stage_rank": rider_result.get('rank'),
                                    "all_gc_data": {
                                        "gc_rank": actual_gc_rank,
                                        "points_rank": rider_result.get('points_rank'),
                                        "kom_rank": rider_result.get('kom_rank'),
                                        "youth_rank": rider_result.get('youth_rank')
                                    }
                                }
                            )
                        )
                
                if not rank_correct:
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="known_results",
                            url=known_result.race_url,
                            error_type="rank_mismatch",
                            error_message=f"Rank mismatch for {known_result.rider_name}",
                            expected_vs_actual={
                                "expected_rank": known_result.expected_rank,
                                "actual_rank": actual_rank,
                                "rider_data": rider_result
                            }
                        )
                    )
                
                test_passed = (rank_correct and uci_points_correct and pcs_points_correct and 
                             team_correct and age_correct and avg_speed_correct and won_how_correct and
                             startlist_quality_correct and distance_correct and elevation_correct and
                             profile_score_correct and gc_rank_correct)
                
                self.test_results.append(TestResult(
                    test_name=test_name,
                    passed=test_passed,
                    details={
                        "rider_name": known_result.rider_name,
                        "test_type": known_result.test_type,
                        "expected_rank": known_result.expected_rank,
                        "actual_rank": actual_rank,
                        "expected_uci_points": known_result.expected_uci_points,
                        "actual_uci_points": rider_result.get('uci_points', 0),
                        "expected_pcs_points": known_result.expected_pcs_points,
                        "actual_pcs_points": rider_result.get('pcs_points', 0),
                        "expected_team": known_result.expected_team,
                        "actual_team": rider_result.get('team_name', ''),
                        "expected_age": known_result.expected_age,
                        "actual_age": rider_result.get('age'),
                        "expected_time_gap": known_result.expected_time_gap,
                        "actual_time_gap": rider_result.get('time', ''),
                        "expected_avg_speed": known_result.expected_avg_speed,
                        "actual_avg_speed": rider_result.get('avg_speed') or stage_info.get('avg_speed_winner'),
                        "expected_won_how": known_result.expected_won_how,
                        "actual_won_how": rider_result.get('won_how') or stage_info.get('won_how'),
                        "expected_startlist_quality": known_result.expected_startlist_quality,
                        "actual_startlist_quality": stage_info.get('race_startlist_quality_score'),
                        "expected_distance": known_result.expected_distance,
                        "actual_distance": stage_info.get('distance'),
                        "expected_elevation": known_result.expected_elevation,
                        "actual_elevation": stage_info.get('elevation') or stage_info.get('vertical_meters'),
                        "expected_profile_score": known_result.expected_profile_score,
                        "actual_profile_score": stage_info.get('profile_score'),
                        "jersey_type": known_result.jersey_type,
                        "race_url": known_result.race_url
                    },
                    execution_time=execution_time
                ))
                
                if test_passed:
                    logger.info(f"     ✅ {known_result.description} - CORRECT")
                else:
                    logger.warning(f"     ❌ {known_result.description} - INCORRECT")
                    
            except Exception as e:
                execution_time = (datetime.now() - test_start).total_seconds()
                self.validation_errors.append(
                    ScrapingValidationError(
                        stage="known_results",
                        url=known_result.race_url,
                        error_type="test_failure",
                        error_message=str(e)
                    )
                )
                self.test_results.append(TestResult(
                    test_name=test_name,
                    passed=False,
                    error=str(e),
                    execution_time=execution_time
                ))
                logger.error(f"     💥 {known_result.description} - ERROR: {e}")
    
    async def _test_format_consistency(self, scraper: AsyncCyclingDataScraper):
        """Test format consistency across different years"""
        logger.info("🔍 Testing format consistency across years...")
        
        test_start = datetime.now()
        
        try:
            # Test same race across different years
            test_years = [2019, 2022, 2024]
            base_race = "race/milano-sanremo"
            
            year_results = {}
            
            for year in test_years:
                try:
                    race_url = f"{base_race}/{year}"
                    race_info = await scraper.get_race_info(race_url)
                    
                    if race_info and race_info.get('stage_urls'):
                        stage_info = await scraper.get_stage_info(race_info['stage_urls'][0])
                        if stage_info:
                            year_results[year] = {
                                'race_name': race_info['race_name'],
                                'result_count': len(stage_info.get('results', [])),
                                'has_rider_names': any(r.get('rider_name') for r in stage_info.get('results', [])),
                                'has_teams': any(r.get('team_name') for r in stage_info.get('results', []))
                            }
                except Exception as e:
                    logger.warning(f"Could not test year {year}: {e}")
                    continue
            
            # Check consistency
            if len(year_results) >= 2:
                result_counts = [data['result_count'] for data in year_results.values()]
                if max(result_counts) - min(result_counts) > 50:  # Allow some variation
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="format_consistency",
                            url=base_race,
                            error_type="inconsistent_result_counts",
                            error_message="Large variation in result counts across years",
                            expected_vs_actual={"year_results": year_results}
                        )
                    )
            
            execution_time = (datetime.now() - test_start).total_seconds()
            consistency_errors = len([e for e in self.validation_errors if e.stage == "format_consistency"])
            
            self.test_results.append(TestResult(
                test_name="format_consistency",
                passed=consistency_errors == 0,
                details={"tested_years": list(year_results.keys()), "year_results": year_results},
                execution_time=execution_time
            ))
            
        except Exception as e:
            execution_time = (datetime.now() - test_start).total_seconds()
            self.test_results.append(TestResult(
                test_name="format_consistency",
                passed=False,
                error=str(e),
                execution_time=execution_time
            ))
    
    async def _test_database_gc_data(self):
        """Test that the database actually contains GC and classification data"""
        logger.info("🗃️ Testing database GC data integrity...")
        
        test_start = datetime.now()
        
        try:
            import sqlite3
            
            # Connect to the test database where we saved the GC test data
            db_path = self.config.database_path
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Test cases for database verification
            database_tests = [
                {
                    "name": "TDF_2024_Final_GC_Pogacar",
                    "description": "Tour de France 2024 Final Stage - Pogacar GC Winner in Database",
                    "query": """
                        SELECT r.gc_rank, r.rider_name, s.stage_url, race.race_name, race.year
                        FROM results r 
                        JOIN stages s ON r.stage_id = s.id 
                        JOIN races race ON s.race_id = race.id
                        WHERE race.race_name LIKE '%Tour%France%' 
                        AND race.year = 2024 
                        AND s.stage_url LIKE '%stage-21%'
                        AND r.rider_name LIKE '%Pog%'
                        AND r.gc_rank = 1
                    """,
                    "expected_rows": 1,
                    "test_type": "database_gc"
                },
                {
                    "name": "TDF_2024_Final_GC_Vingegaard", 
                    "description": "Tour de France 2024 Final Stage - Vingegaard 2nd GC in Database",
                    "query": """
                        SELECT r.gc_rank, r.rider_name, s.stage_url, race.race_name, race.year
                        FROM results r 
                        JOIN stages s ON r.stage_id = s.id 
                        JOIN races race ON s.race_id = race.id
                        WHERE race.race_name LIKE '%Tour%France%' 
                        AND race.year = 2024 
                        AND s.stage_url LIKE '%stage-21%'
                        AND r.rider_name LIKE '%Vingegaard%'
                        AND r.gc_rank = 2
                    """,
                    "expected_rows": 1,
                    "test_type": "database_gc"
                },
                {
                    "name": "Any_Stage_Race_GC_Data",
                    "description": "Any stage race should have some GC data in database",
                    "query": """
                        SELECT COUNT(*) as gc_results_count
                        FROM results r 
                        WHERE r.gc_rank IS NOT NULL AND r.gc_rank > 0
                    """,
                    "expected_minimum": 100,  # Should have hundreds of GC results
                    "test_type": "database_gc_count"
                },
                {
                    "name": "Points_Classification_Data",
                    "description": "Should have points classification data in database", 
                    "query": """
                        SELECT COUNT(*) as points_results_count
                        FROM results r 
                        WHERE r.points_rank IS NOT NULL AND r.points_rank > 0
                    """,
                    "expected_minimum": 50,  # Should have some points classification results
                    "test_type": "database_points_count"
                }
            ]
            
            for test_case in database_tests:
                logger.info(f"   Testing: {test_case['description']}")
                
                cursor.execute(test_case['query'])
                results = cursor.fetchall()
                
                test_passed = False
                error_details = {}
                
                if test_case['test_type'] in ['database_gc_count', 'database_points_count']:
                    # Count-based test
                    actual_count = results[0][0] if results else 0
                    expected_min = test_case['expected_minimum']
                    test_passed = actual_count >= expected_min
                    
                    if not test_passed:
                        error_details = {
                            "expected_minimum": expected_min,
                            "actual_count": actual_count,
                            "query": test_case['query']
                        }
                        logger.warning(f"      ❌ {test_case['description']} - FAILED")
                        logger.warning(f"         Expected >= {expected_min}, got {actual_count}")
                else:
                    # Row-based test
                    expected_rows = test_case['expected_rows']
                    actual_rows = len(results)
                    test_passed = actual_rows == expected_rows
                    
                    if not test_passed:
                        error_details = {
                            "expected_rows": expected_rows,
                            "actual_rows": actual_rows,
                            "query": test_case['query'],
                            "sample_results": results[:3] if results else []
                        }
                        logger.warning(f"      ❌ {test_case['description']} - FAILED")
                        logger.warning(f"         Expected {expected_rows} rows, got {actual_rows}")
                
                if not test_passed:
                    self.validation_errors.append(
                        ScrapingValidationError(
                            stage="database_gc_verification",
                            url="database",
                            error_type="database_gc_missing",
                            error_message=f"Database GC test failed: {test_case['description']}",
                            expected_vs_actual=error_details
                        )
                    )
                else:
                    logger.info(f"      ✅ {test_case['description']} - PASSED")
                
                # Add individual test result
                self.test_results.append(TestResult(
                    test_name=f"database_{test_case['name']}",
                    passed=test_passed,
                    error=None if test_passed else f"Database test failed: {test_case['description']}",
                    execution_time=0.1  # Database queries are fast
                ))
            
            conn.close()
            
        except Exception as e:
            logger.error(f"💥 Database GC test failed with exception: {e}")
            self.validation_errors.append(
                ScrapingValidationError(
                    stage="database_gc_verification",
                    url="database",
                    error_type="database_connection_failed",
                    error_message=f"Could not test database GC data: {str(e)}"
                )
            )
            self.test_results.append(TestResult(
                test_name="database_gc_connection",
                passed=False,
                error=str(e),
                execution_time=(datetime.now() - test_start).total_seconds()
            ))
    
    async def _generate_test_report(self, execution_time: float, passed_tests: int, total_tests: int):
        """Generate comprehensive test report"""
        
        # Create reports directory
        Path("reports").mkdir(exist_ok=True)
        
        # Generate JSON report
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "execution_time": execution_time,
            "summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": total_tests - passed_tests,
                "success_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                "validation_errors": len(self.validation_errors)
            },
            "test_results": [
                {
                    "test_name": r.test_name,
                    "passed": r.passed,
                    "error": r.error,
                    "details": r.details,
                    "execution_time": r.execution_time
                }
                for r in self.test_results
            ],
            "validation_errors": [
                {
                    "stage": e.stage,
                    "url": e.url,
                    "error_type": e.error_type,
                    "error_message": e.error_message,
                    "expected_vs_actual": e.expected_vs_actual,
                    "timestamp": e.timestamp.isoformat()
                }
                for e in self.validation_errors
            ]
        }
        
        report_file = f"reports/scraper_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        # Generate human-readable summary
        summary_file = f"reports/scraper_test_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(summary_file, 'w') as f:
            f.write("CYCLING DATA SCRAPER TEST REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Execution Time: {execution_time:.2f} seconds\n")
            f.write(f"Tests Passed: {passed_tests}/{total_tests} ({passed_tests/total_tests*100:.1f}%)\n")
            f.write(f"Validation Errors: {len(self.validation_errors)}\n\n")
            
            if self.validation_errors:
                f.write("VALIDATION ERRORS:\n")
                f.write("-" * 20 + "\n")
                for error in self.validation_errors:
                    f.write(f"Stage: {error.stage}\n")
                    f.write(f"URL: {error.url}\n")
                    f.write(f"Error: {error.error_type} - {error.error_message}\n")
                    if error.expected_vs_actual:
                        f.write(f"Details: {error.expected_vs_actual}\n")
                    f.write("\n")
            
            f.write("INDIVIDUAL TEST RESULTS:\n")
            f.write("-" * 25 + "\n")
            for result in self.test_results:
                status = "✅ PASS" if result.passed else "❌ FAIL"
                f.write(f"{status} {result.test_name} ({result.execution_time:.2f}s)\n")
                if result.error:
                    f.write(f"  Error: {result.error}\n")
                if result.details:
                    f.write(f"  Details: {result.details}\n")
                f.write("\n")
        
        logger.info(f"📄 Test report saved to: {report_file}")
        logger.info(f"📄 Test summary saved to: {summary_file}")
        
        # Log critical issues for immediate attention
        critical_errors = [e for e in self.validation_errors if e.error_type in ['connection_failure', 'critical_failure']]
        if critical_errors:
            logger.error("🚨 CRITICAL ISSUES DETECTED:")
            for error in critical_errors:
                logger.error(f"  - {error.stage}: {error.error_message}")


async def main():
    """Run the test framework"""
    test_framework = ScraperTestFramework()
    success = await test_framework.run_full_test_suite()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 