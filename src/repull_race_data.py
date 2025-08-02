#!/usr/bin/env python3
"""
Script to re-pull races, stages, and results data while preserving riders table
This script will:
1. Create a backup of the current database
2. Drop and recreate the races, stages, and results tables
3. Re-pull the data for these tables
4. Preserve all rider-related data
"""

import asyncio
import aiosqlite
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List

from improved_async_scraper import ImprovedAsyncCyclingDataScraper as AsyncCyclingDataScraper, ScrapingConfig
from progress_tracker import progress_tracker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/repull_race_data.log')
    ]
)
logger = logging.getLogger(__name__)

async def backup_database(database_path: str) -> str:
    """Create a backup of the current database"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"data/backups/cycling_data_backup_{timestamp}.db"
    
    logger.info(f"Creating backup: {backup_path}")
    
    # Create backup directory if it doesn't exist
    Path("data/backups").mkdir(parents=True, exist_ok=True)
    
    # Copy the database file
    import shutil
    shutil.copy2(database_path, backup_path)
    
    logger.info(f"✅ Backup created successfully: {backup_path}")
    return backup_path

async def drop_race_tables(database_path: str):
    """Drop races, stages, and results tables while preserving riders tables"""
    logger.info("🗑️ Dropping races, stages, and results tables...")
    
    async with aiosqlite.connect(database_path) as db:
        # Drop tables in correct order (respecting foreign key constraints)
        tables_to_drop = ['results', 'stages', 'races']
        
        for table in tables_to_drop:
            try:
                await db.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"   ✅ Dropped table: {table}")
            except Exception as e:
                logger.error(f"   ❌ Failed to drop table {table}: {e}")
        
        await db.commit()
    
    logger.info("✅ Race tables dropped successfully")

async def get_years_from_riders(database_path: str) -> List[int]:
    """Extract years from existing rider data to determine what years to re-scrape"""
    logger.info("🔍 Analyzing existing rider data to determine years to re-scrape...")
    
    async with aiosqlite.connect(database_path) as db:
        # Get years from rider_teams table if it exists
        try:
            cursor = await db.execute("""
                SELECT DISTINCT year_start, year_end 
                FROM rider_teams 
                ORDER BY year_start
            """)
            rows = await cursor.fetchall()
            
            if rows:
                years = set()
                for row in rows:
                    year_start, year_end = row
                    if year_start and year_end:
                        years.update(range(year_start, year_end + 1))
                    elif year_start:
                        years.add(year_start)
                
                years_list = sorted(list(years))
                logger.info(f"📅 Found {len(years_list)} years from rider team data: {min(years_list)}-{max(years_list)}")
                return years_list
        except Exception as e:
            logger.warning(f"Could not extract years from rider_teams: {e}")
        
        # Fallback: try to get years from rider_achievements table
        try:
            cursor = await db.execute("""
                SELECT DISTINCT year 
                FROM rider_achievements 
                ORDER BY year
            """)
            rows = await cursor.fetchall()
            
            if rows:
                years_list = [row[0] for row in rows if row[0]]
                logger.info(f"📅 Found {len(years_list)} years from rider achievements: {min(years_list)}-{max(years_list)}")
                return years_list
        except Exception as e:
            logger.warning(f"Could not extract years from rider_achievements: {e}")
        
        # Final fallback: default to recent years
        current_year = datetime.now().year
        default_years = list(range(current_year - 5, current_year + 1))
        logger.info(f"📅 Using default years: {default_years}")
        return default_years

async def repull_race_data(years: List[int], database_path: str = "data/cycling_data.db"):
    """Re-pull race data for the specified years"""
    logger.info(f"🔄 Starting race data re-pull for {len(years)} years...")
    
    # Create scraping configuration
    config = ScrapingConfig(
        max_concurrent_requests=30,
        request_delay=0.1,
        max_retries=3,
        timeout=30,
        database_path=database_path
    )
    
    try:
        async with AsyncCyclingDataScraper(config) as scraper:
            # Set up progress tracking
            scraper.progress_tracker = progress_tracker
            scraper.checkpoint_interval = 300  # 5 minutes
            
            # Initialize progress tracking for these years
            session_id = await progress_tracker.start_session(years)
            
            # Scrape only race data (no rider profiles)
            await scraper.scrape_years_with_progress(years)
        
        logger.info("🎉 Race data re-pull completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Race data re-pull failed: {e}")
        raise

async def main():
    """Main function to orchestrate the re-pull process"""
    database_path = "data/cycling_data.db"
    
    # Ensure we're in the right directory
    if not Path(database_path).exists():
        logger.error(f"Database not found: {database_path}")
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    logger.info("🚀 Starting race data re-pull process...")
    logger.info(f"📁 Database: {database_path}")
    
    try:
        # Step 1: Create backup
        backup_path = await backup_database(database_path)
        
        # Step 2: Drop race tables
        await drop_race_tables(database_path)
        
        # Step 3: Reset progress tracking session
        logger.info("🔄 Resetting progress tracking session...")
        await progress_tracker.reset_session()
        
        # Step 4: Determine years to re-scrape
        years = await get_years_from_riders(database_path)
        
        if not years:
            logger.error("❌ Could not determine years to re-scrape")
            sys.exit(1)
        
        # Step 5: Re-pull race data
        await repull_race_data(years, database_path)
        
        logger.info("✅ Race data re-pull process completed successfully!")
        logger.info(f"💾 Backup saved at: {backup_path}")
        logger.info(f"📊 Re-pulled data for years: {years}")
        
    except Exception as e:
        logger.error(f"💥 Process failed: {e}")
        logger.error("💾 Your original data is safe in the backup")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 