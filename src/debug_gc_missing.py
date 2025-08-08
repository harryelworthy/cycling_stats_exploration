#!/usr/bin/env python3

import asyncio
import sqlite3
import sys
import os

# Add the src directory to the path so we can import the scraper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from improved_async_scraper import ImprovedAsyncCyclingDataScraper

async def debug_gc_missing():
    """Debug why GC data is missing from most stages"""
    
    async with ImprovedAsyncCyclingDataScraper() as scraper:
        # Test a stage that should have GC data but doesn't
        stage_url = "race/tour-de-france/2024/stage-5"
        print(f"Testing stage: {stage_url}")
        
        stage_info = await scraper.get_stage_info(stage_url)
        
        if stage_info:
            print(f"Stage results count: {len(stage_info.get('results', []))}")
            print(f"GC data count: {len(stage_info.get('gc', []))}")
            print(f"Points data count: {len(stage_info.get('points', []))}")
            print(f"KOM data count: {len(stage_info.get('kom', []))}")
            print(f"Youth data count: {len(stage_info.get('youth', []))}")
            
            # Check if GC data was merged
            results = stage_info.get('results', [])
            gc_riders = [r for r in results if r.get('gc_rank') is not None]
            print(f"Results with GC rank after merge: {len(gc_riders)}")
            
            if gc_riders:
                print("Sample riders with GC ranks:")
                for rider in gc_riders[:3]:
                    print(f"  - {rider.get('rider_name')} (gc_rank: {rider.get('gc_rank')})")
            else:
                print("No riders have GC ranks after merge!")
                
            # Check raw classification data
            gc_data = stage_info.get('gc', [])
            if gc_data:
                print("Raw GC classification data (first 3):")
                for rider in gc_data[:3]:
                    print(f"  - {rider}")
            else:
                print("No raw GC classification data found!")
        else:
            print("Failed to get stage info!")

if __name__ == "__main__":
    asyncio.run(debug_gc_missing())