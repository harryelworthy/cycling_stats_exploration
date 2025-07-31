#!/usr/bin/env python3
"""
Test script for the improved async scraper
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent))

from improved_async_scraper import ImprovedAsyncCyclingDataScraper, ScrapingConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_improved_scraper():
    """Test the improved scraper with a small sample"""
    logger.info("🧪 Testing Improved Async Scraper")
    
    # Create a test configuration
    config = ScrapingConfig(
        max_concurrent_requests=5,
        request_delay=0.5,
        timeout=30,
        database_path="data/test_improved_cycling_data.db"
    )
    
    try:
        async with ImprovedAsyncCyclingDataScraper(config) as scraper:
            # Test with a small sample of races from 2024
            test_race_urls = [
                "race/paris-roubaix/2024",
                "race/milano-sanremo/2024",
                "race/tour-de-france/2024"
            ]
            
            logger.info(f"Testing with {len(test_race_urls)} sample races")
            
            # Test race info extraction
            for race_url in test_race_urls:
                logger.info(f"Testing race info extraction: {race_url}")
                race_info = await scraper.get_race_info(race_url)
                
                if race_info:
                    logger.info(f"✅ Successfully extracted race info:")
                    logger.info(f"   Name: {race_info['race_name']}")
                    logger.info(f"   Category: {race_info['race_category']}")
                    logger.info(f"   Stages: {len(race_info['stage_urls'])}")
                    
                    # Test stage info extraction for first stage
                    if race_info['stage_urls']:
                        first_stage = race_info['stage_urls'][0]
                        logger.info(f"Testing stage info extraction: {first_stage}")
                        stage_info = await scraper.get_stage_info(first_stage)
                        
                        if stage_info:
                            logger.info(f"✅ Successfully extracted stage info:")
                            logger.info(f"   Results: {len(stage_info.get('results', []))}")
                            logger.info(f"   Distance: {stage_info.get('distance')}")
                            logger.info(f"   Stage type: {stage_info.get('stage_type')}")
                        else:
                            logger.warning(f"❌ Failed to extract stage info: {first_stage}")
                else:
                    logger.warning(f"❌ Failed to extract race info: {race_url}")
            
            logger.info("🎉 Improved scraper test completed successfully!")
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise

async def test_duplicate_prevention():
    """Test that the improved scraper prevents duplicates"""
    logger.info("🧪 Testing Duplicate Prevention")
    
    config = ScrapingConfig(
        max_concurrent_requests=2,
        request_delay=0.5,
        timeout=30,
        database_path="data/test_duplicate_prevention.db"
    )
    
    try:
        async with ImprovedAsyncCyclingDataScraper(config) as scraper:
            # Test race: Paris-Roubaix 2024
            race_url = "race/paris-roubaix/2024"
            
            # Get race info
            race_info = await scraper.get_race_info(race_url)
            if not race_info:
                logger.error("Failed to get race info")
                return
            
            # Save race data multiple times
            logger.info("Saving race data multiple times to test duplicate prevention...")
            
            race_id_1 = await scraper.save_race_data(2024, race_info)
            logger.info(f"First save: race_id = {race_id_1}")
            
            race_id_2 = await scraper.save_race_data(2024, race_info)
            logger.info(f"Second save: race_id = {race_id_2}")
            
            race_id_3 = await scraper.save_race_data(2024, race_info)
            logger.info(f"Third save: race_id = {race_id_3}")
            
            # Check if all IDs are the same (duplicate prevention working)
            if race_id_1 == race_id_2 == race_id_3:
                logger.info("✅ Duplicate prevention working correctly!")
            else:
                logger.error("❌ Duplicate prevention failed!")
            
            # Verify only one race exists in database
            import aiosqlite
            async with aiosqlite.connect(config.database_path) as db:
                cursor = await db.execute("SELECT COUNT(*) FROM races WHERE race_name = ?", (race_info['race_name'],))
                count = await cursor.fetchone()
                logger.info(f"Races in database with name '{race_info['race_name']}': {count[0]}")
                
                if count[0] == 1:
                    logger.info("✅ Only one race entry exists - duplicate prevention successful!")
                else:
                    logger.error(f"❌ Found {count[0]} race entries - duplicate prevention failed!")
            
    except Exception as e:
        logger.error(f"❌ Duplicate prevention test failed: {e}")
        raise

async def main():
    """Run all tests"""
    logger.info("🚀 Starting Improved Scraper Tests")
    
    try:
        # Test basic functionality
        await test_improved_scraper()
        
        # Test duplicate prevention
        await test_duplicate_prevention()
        
        logger.info("🎉 All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Tests failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 