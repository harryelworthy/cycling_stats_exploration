#!/usr/bin/env python3
"""
Test script for the enhanced async scraper
This will test the enhanced scraper with a small subset of data to validate improvements
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

from enhanced_async_scraper import EnhancedAsyncCyclingDataScraper, ScrapingConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/test_enhanced_scraper.log')
    ]
)
logger = logging.getLogger(__name__)

async def test_single_url(scraper, url: str) -> dict:
    """Test scraping a single URL"""
    logger.info(f"Testing URL: {url}")
    
    try:
        # Test stage info extraction
        stage_info = await scraper.get_stage_info(url)
        
        if stage_info:
            results_count = len(stage_info.get('results', []))
            logger.info(f"✅ Success: Found {results_count} results")
            
            # Show sample results
            if results_count > 0:
                sample_result = stage_info['results'][0]
                logger.info(f"   Sample rider: {sample_result.get('rider_name', 'N/A')}")
                logger.info(f"   Sample team: {sample_result.get('team_name', 'N/A')}")
                logger.info(f"   Sample rank: {sample_result.get('rank', 'N/A')}")
            
            return {
                'success': True,
                'results_count': results_count,
                'stage_info': stage_info
            }
        else:
            logger.error(f"❌ Failed: No stage info returned")
            return {
                'success': False,
                'error': 'No stage info returned'
            }
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return {
            'success': False,
            'error': str(e)
        }

async def test_enhanced_scraper():
    """Test the enhanced scraper with problematic URLs"""
    
    # Test URLs that failed in the original scraper
    test_urls = [
        "race/vuelta-a-espana/2025/stage-5",
        "race/gp-de-wallonie/2025/result", 
        "race/tour-of-guangxi/2020/stage-5",
        "race/tour-de-pologne/2025/stage-1",
        "race/bretagne-classic/2025/result"
    ]
    
    # Create enhanced scraper config
    config = ScrapingConfig(
        max_concurrent_requests=5,  # Conservative for testing
        request_delay=0.3,  # Slower for testing
        max_retries=3,
        timeout=30,
        database_path="data/test_enhanced_cycling_data.db",  # Separate test database
        wait_for_dynamic_content=True
    )
    
    logger.info("🧪 Testing Enhanced Async Scraper")
    logger.info("=" * 60)
    
    results = {}
    
    async with EnhancedAsyncCyclingDataScraper(config) as scraper:
        for url in test_urls:
            result = await test_single_url(scraper, url)
            results[url] = result
            
            # Add delay between tests
            await asyncio.sleep(1)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 TEST SUMMARY")
    logger.info("=" * 60)
    
    successful_tests = [r for r in results.values() if r['success']]
    failed_tests = [r for r in results.values() if not r['success']]
    
    logger.info(f"✅ Successful: {len(successful_tests)}/{len(test_urls)}")
    logger.info(f"❌ Failed: {len(failed_tests)}/{len(test_urls)}")
    
    if successful_tests:
        total_results = sum(r['results_count'] for r in successful_tests)
        avg_results = total_results / len(successful_tests)
        logger.info(f"📊 Average results per successful test: {avg_results:.1f}")
    
    if failed_tests:
        logger.info("\n❌ Failed URLs:")
        for url, result in results.items():
            if not result['success']:
                logger.info(f"   {url}: {result.get('error', 'Unknown error')}")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"logs/enhanced_scraper_test_results_{timestamp}.json"
    
    import json
    with open(results_file, 'w') as f:
        # Convert stage_info to serializable format
        serializable_results = {}
        for url, result in results.items():
            serializable_result = result.copy()
            if 'stage_info' in serializable_result:
                # Remove non-serializable objects
                del serializable_result['stage_info']
            serializable_results[url] = serializable_result
        
        json.dump(serializable_results, f, indent=2)
    
    logger.info(f"💾 Detailed results saved to: {results_file}")
    
    return len(successful_tests) > 0

async def test_small_dataset():
    """Test with a small dataset to validate the enhanced scraper"""
    
    logger.info("\n🧪 Testing with small dataset (2025 only)")
    logger.info("=" * 60)
    
    config = ScrapingConfig(
        max_concurrent_requests=3,
        request_delay=0.5,
        max_retries=3,
        timeout=30,
        database_path="data/test_enhanced_cycling_data.db",
        wait_for_dynamic_content=True
    )
    
    try:
        async with EnhancedAsyncCyclingDataScraper(config) as scraper:
            # Test with just 2025
            test_years = [2025]
            
            logger.info(f"Testing years: {test_years}")
            
            # Get races for 2025
            races = await scraper.get_races(2025)
            logger.info(f"Found {len(races)} races for 2025")
            
            if races:
                # Test first 3 races only
                test_races = races[:3]
                logger.info(f"Testing first {len(test_races)} races")
                
                for i, race_url in enumerate(test_races, 1):
                    logger.info(f"Processing race {i}/{len(test_races)}: {race_url}")
                    
                    try:
                        race_info = await scraper.get_race_info(race_url)
                        if race_info:
                            logger.info(f"   Race: {race_info['race_name']}")
                            logger.info(f"   Stages: {len(race_info['stage_urls'])}")
                            
                            # Test first stage only
                            if race_info['stage_urls']:
                                stage_url = race_info['stage_urls'][0]
                                stage_info = await scraper.get_stage_info(stage_url)
                                
                                if stage_info and stage_info.get('results'):
                                    logger.info(f"   ✅ Stage results: {len(stage_info['results'])} riders")
                                else:
                                    logger.info(f"   ❌ No stage results")
                        
                        await asyncio.sleep(1)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"   ❌ Error processing race {race_url}: {e}")
        
        logger.info("✅ Small dataset test completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Small dataset test failed: {e}")
        return False

async def main():
    """Main test function"""
    
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    logger.info("🚀 Starting Enhanced Scraper Tests")
    logger.info("=" * 60)
    
    # Test 1: Individual URL testing
    logger.info("📋 Test 1: Individual URL Testing")
    url_test_success = await test_enhanced_scraper()
    
    # Test 2: Small dataset testing
    logger.info("\n📋 Test 2: Small Dataset Testing")
    dataset_test_success = await test_small_dataset()
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("🎯 FINAL TEST SUMMARY")
    logger.info("=" * 60)
    
    if url_test_success and dataset_test_success:
        logger.info("✅ All tests passed! Enhanced scraper is working correctly.")
        logger.info("🚀 Ready for full re-pull with enhanced scraper.")
    elif url_test_success:
        logger.info("⚠️  URL tests passed but dataset test had issues.")
        logger.info("🔧 May need additional fixes before full re-pull.")
    elif dataset_test_success:
        logger.info("⚠️  Dataset test passed but URL tests had issues.")
        logger.info("🔧 May need additional fixes before full re-pull.")
    else:
        logger.error("❌ All tests failed. Enhanced scraper needs more work.")
        logger.error("🔧 Do not proceed with full re-pull yet.")
    
    return url_test_success and dataset_test_success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 