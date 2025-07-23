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

from async_scraper import AsyncCyclingDataScraper, ScrapingConfig

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
                
                # Test 4: Format consistency tests
                await self._test_format_consistency(scraper)
                
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