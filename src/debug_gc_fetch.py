#!/usr/bin/env python3

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the src directory to the path so we can import the scraper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from improved_async_scraper import ImprovedAsyncCyclingDataScraper

async def test_gc_fetch():
    """Test fetching GC data for 1994 Tour de France"""
    
    async with ImprovedAsyncCyclingDataScraper() as scraper:
        # Test fetching the final GC classification for 1994
        gc_url = "race/tour-de-france/1994/gc"
        print(f"Testing GC URL: {gc_url}")
        
        gc_data = await scraper._fetch_classification_data(gc_url)
        
        if gc_data:
            print(f"Successfully fetched {len(gc_data)} GC results")
            print("Top 5 GC riders:")
            for i, rider in enumerate(gc_data[:5]):
                print(f"  {i+1}. {rider.get('rider_name', 'Unknown')} (rank: {rider.get('rank', 'Unknown')})")
        else:
            print("No GC data fetched!")
        
        # Now test a stage and see what happens when we merge
        print("\nTesting stage 21 data merge...")
        stage_url = "race/tour-de-france/1994/stage-21"
        stage_info = await scraper.get_stage_info(stage_url)
        
        if stage_info:
            print(f"Stage results count: {len(stage_info.get('results', []))}")
            print(f"GC data count: {len(stage_info.get('gc', []))}")
            
            # Check the merged results
            results = stage_info.get('results', [])
            gc_riders = [r for r in results if r.get('gc_rank') == 1]
            
            if gc_riders:
                print("Riders with GC rank 1:")
                for rider in gc_riders:
                    print(f"  - {rider.get('rider_name')} (stage rank: {rider.get('rank')}, gc_rank: {rider.get('gc_rank')})")
            else:
                print("No riders found with GC rank 1!")
        else:
            print("Failed to get stage info!")
            
        # Also test what the expected winner should be
        print(f"\n1994 Tour de France should have been won by Miguel Indurain")
        print(f"Let's verify this matches historical records...")

if __name__ == "__main__":
    asyncio.run(test_gc_fetch())