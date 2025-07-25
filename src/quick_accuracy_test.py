#!/usr/bin/env python3
"""
Quick accuracy test script for validating known cycling results
This runs ONLY the known results validation tests for rapid feedback

Usage: python quick_accuracy_test.py
"""

import asyncio
import sys
import logging
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent))

from test_scraper import ScraperTestFramework, KnownResult
from async_scraper import ScrapingConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

async def run_accuracy_tests():
    """Run only the known results accuracy tests"""
    print("🎯 Quick Accuracy Test - Known Results Only")
    print("=" * 50)
    
    # Conservative config for testing
    config = ScrapingConfig(
        max_concurrent_requests=2,
        request_delay=0.5,
        timeout=30,
        database_path="test_cycling_data.db"
    )
    
    framework = ScraperTestFramework(config)
    
    try:
        from async_scraper import AsyncCyclingDataScraper
        async with AsyncCyclingDataScraper(config) as scraper:
            await framework._test_known_results(scraper)
        
        # Report results
        known_result_tests = [t for t in framework.test_results if t.test_name.startswith('known_result_')]
        passed = len([t for t in known_result_tests if t.passed])
        total = len(known_result_tests)
        
        print(f"\n📊 Results: {passed}/{total} tests passed")
        
        if passed == total:
            print("✅ All known results are accurate!")
        else:
            print("❌ Some known results failed - check UCI points accuracy")
            
            # Print specific failures
            for test in known_result_tests:
                if not test.passed:
                    print(f"  ❌ {test.test_name}: {test.error}")
                    if test.details:
                        print(f"     Test type: {test.details.get('test_type', 'unknown')}")
                        expected_uci = test.details.get('expected_uci_points')
                        actual_uci = test.details.get('actual_uci_points')
                        if expected_uci is not None:
                            print(f"     Expected UCI points: {expected_uci}, Got: {actual_uci}")
                        expected_pcs = test.details.get('expected_pcs_points')
                        actual_pcs = test.details.get('actual_pcs_points')
                        if expected_pcs is not None:
                            print(f"     Expected PCS points: {expected_pcs}, Got: {actual_pcs}")
                        expected_team = test.details.get('expected_team')
                        actual_team = test.details.get('actual_team')
                        if expected_team:
                            print(f"     Expected team: {expected_team}, Got: {actual_team}")
                        expected_gap = test.details.get('expected_time_gap')
                        actual_gap = test.details.get('actual_time_gap')
                        if expected_gap:
                            print(f"     Expected time gap: {expected_gap}, Got: {actual_gap}")
                        expected_age = test.details.get('expected_age')
                        actual_age = test.details.get('actual_age')
                        if expected_age:
                            print(f"     Expected age: {expected_age}, Got: {actual_age}")
                        
                        # Additional fields
                        expected_speed = test.details.get('expected_avg_speed')
                        actual_speed = test.details.get('actual_avg_speed')
                        if expected_speed:
                            print(f"     Expected avg speed: {expected_speed} kph, Got: {actual_speed}")
                        
                        expected_won_how = test.details.get('expected_won_how')
                        actual_won_how = test.details.get('actual_won_how')
                        if expected_won_how:
                            print(f"     Expected won how: {expected_won_how}, Got: {actual_won_how}")
                        
                        expected_startlist = test.details.get('expected_startlist_quality')
                        actual_startlist = test.details.get('actual_startlist_quality')
                        if expected_startlist:
                            print(f"     Expected startlist quality: {expected_startlist}, Got: {actual_startlist}")
                        
                        expected_distance = test.details.get('expected_distance')
                        actual_distance = test.details.get('actual_distance')
                        if expected_distance:
                            print(f"     Expected distance: {expected_distance} km, Got: {actual_distance}")
                        
                        expected_elevation = test.details.get('expected_elevation')
                        actual_elevation = test.details.get('actual_elevation')
                        if expected_elevation:
                            print(f"     Expected elevation: {expected_elevation} m, Got: {actual_elevation}")
                        
                        expected_profile = test.details.get('expected_profile_score')
                        actual_profile = test.details.get('actual_profile_score')
                        if expected_profile:
                            print(f"     Expected profile score: {expected_profile}, Got: {actual_profile}")
        
        # Print validation errors for known results
        known_errors = [e for e in framework.validation_errors if e.stage == "known_results"]
        if known_errors:
            print(f"\n🔍 Validation Issues ({len(known_errors)}):")
            for error in known_errors[:5]:  # Show first 5
                print(f"  • {error.error_type}: {error.error_message}")
        
        return passed == total
        
    except Exception as e:
        print(f"💥 Test failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(run_accuracy_tests())
    sys.exit(0 if success else 1)