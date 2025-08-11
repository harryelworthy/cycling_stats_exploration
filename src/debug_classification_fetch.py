#!/usr/bin/env python3

import asyncio
import sys
import os
import logging

# Set up verbose logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the src directory to the path so we can import the scraper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from async_scraper import AsyncCyclingDataScraper

async def debug_classification_fetch():
    """Debug the classification fetching process in detail"""
    
    async with AsyncCyclingDataScraper() as scraper:
        # Test the specific classification URLs
        test_urls = [
            "race/tour-de-france/2024/gc",  # Final GC
            "race/tour-de-france/2024/stage-5-gc",  # Stage 5 GC
        ]
        
        for url in test_urls:
            print(f"\n=== Testing URL: {url} ===")
            classification_data = await scraper._fetch_classification_data(url)
            
            if classification_data:
                print(f"✅ Successfully fetched {len(classification_data)} results")
                print("Top 3 riders:")
                for i, rider in enumerate(classification_data[:3]):
                    print(f"  {i+1}. {rider.get('rider_name')} (rank: {rider.get('rank')})")
            else:
                print("❌ No classification data fetched!")
        
        # Test the full stage processing
        print(f"\n=== Testing full stage processing ===")
        stage_url = "race/tour-de-france/2024/stage-5"
        
        # Manually call the classification fetching method
        await scraper._fetch_stage_classifications(stage_url, {})

if __name__ == "__main__":
    asyncio.run(debug_classification_fetch())