#!/usr/bin/env python3
"""
Debug script to test the async scraper with a single race
"""

import asyncio
from async_scraper import AsyncCyclingDataScraper, ScrapingConfig
import json

async def debug_single_race():
    """Debug a single race to see what data is extracted"""
    config = ScrapingConfig(
        max_concurrent_requests=1,
        request_delay=0.5,
        database_path="debug.db"
    )
    
    async with AsyncCyclingDataScraper(config) as scraper:
        # Test getting races for 2019
        print("=== Testing get_races() ===")
        race_urls = await scraper.get_races(2019)
        print(f"Found {len(race_urls)} races")
        print("First 5 races:")
        for i, url in enumerate(race_urls[:5]):
            print(f"  {i+1}. {url}")
        
        if race_urls:
            # Test getting race info for first race
            print(f"\n=== Testing get_race_info() for: {race_urls[0]} ===")
            race_info = await scraper.get_race_info(race_urls[0])
            print("Race info:")
            print(json.dumps(race_info, indent=2, default=str))
            
            if race_info and race_info.get('stage_urls'):
                # Test getting stage info for first stage
                stage_url = race_info['stage_urls'][0]
                print(f"\n=== Testing get_stage_info() for: {stage_url} ===")
                stage_info = await scraper.get_stage_info(stage_url)
                if stage_info:
                    print("Stage info (first 10 results):")
                    stage_info_copy = stage_info.copy()
                    if stage_info_copy.get('results'):
                        stage_info_copy['results'] = stage_info_copy['results'][:10]
                    print(json.dumps(stage_info_copy, indent=2, default=str))
                else:
                    print("No stage info returned")

if __name__ == "__main__":
    asyncio.run(debug_single_race()) 