#!/usr/bin/env python3
"""
Script to clean up duplicate races in the existing database
"""

import asyncio
import aiosqlite
import logging
from typing import List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def analyze_duplicates(database_path: str) -> List[Tuple[str, int]]:
    """Analyze duplicate races in the database"""
    async with aiosqlite.connect(database_path) as db:
        # Find races with the same name and year
        cursor = await db.execute('''
            SELECT race_name, year, COUNT(*) as count
            FROM races 
            GROUP BY race_name, year 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        ''')
        
        duplicates = await cursor.fetchall()
        return duplicates

async def cleanup_duplicates(database_path: str):
    """Clean up duplicate races, keeping the one with the most data"""
    async with aiosqlite.connect(database_path) as db:
        # Get all duplicate races
        cursor = await db.execute('''
            SELECT id, race_name, year, stage_url,
                   (SELECT COUNT(*) FROM stages WHERE race_id = races.id) as stage_count,
                   (SELECT COUNT(*) FROM results r JOIN stages s ON r.stage_id = s.id WHERE s.race_id = races.id) as result_count
            FROM races 
            WHERE (race_name, year) IN (
                SELECT race_name, year 
                FROM races 
                GROUP BY race_name, year 
                HAVING COUNT(*) > 1
            )
            ORDER BY race_name, year, result_count DESC, stage_count DESC
        ''')
        
        races = await cursor.fetchall()
        
        # Group by race_name and year
        race_groups = {}
        for race in races:
            race_id, race_name, year, stage_url, stage_count, result_count = race
            key = (race_name, year)
            if key not in race_groups:
                race_groups[key] = []
            race_groups[key].append({
                'id': race_id,
                'stage_url': stage_url,
                'stage_count': stage_count,
                'result_count': result_count
            })
        
        # Keep the best race from each group and delete the rest
        total_deleted = 0
        for (race_name, year), group in race_groups.items():
            if len(group) > 1:
                # Sort by result_count, then stage_count (descending)
                group.sort(key=lambda x: (x['result_count'], x['stage_count']), reverse=True)
                
                # Keep the first one (best), delete the rest
                to_delete = group[1:]
                
                logger.info(f"Race: {race_name} ({year})")
                logger.info(f"  Keeping: ID {group[0]['id']} with {group[0]['result_count']} results, {group[0]['stage_count']} stages")
                
                for race_to_delete in to_delete:
                    logger.info(f"  Deleting: ID {race_to_delete['id']} with {race_to_delete['result_count']} results, {race_to_delete['stage_count']} stages")
                    
                    # Delete associated stages and results first
                    await db.execute('DELETE FROM results WHERE stage_id IN (SELECT id FROM stages WHERE race_id = ?)', (race_to_delete['id'],))
                    await db.execute('DELETE FROM stages WHERE race_id = ?', (race_to_delete['id'],))
                    
                    # Delete the race
                    await db.execute('DELETE FROM races WHERE id = ?', (race_to_delete['id'],))
                    total_deleted += 1
                
                logger.info(f"  Total deleted for this race: {len(to_delete)}")
        
        await db.commit()
        logger.info(f"Cleanup completed. Total races deleted: {total_deleted}")

async def add_race_key_column(database_path: str):
    """Add race_key column to races table for the improved scraper"""
    async with aiosqlite.connect(database_path) as db:
        # Check if race_key column already exists
        cursor = await db.execute("PRAGMA table_info(races)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'race_key' not in column_names:
            logger.info("Adding race_key column to races table...")
            
            # Add the race_key column
            await db.execute('ALTER TABLE races ADD COLUMN race_key TEXT')
            
            # Generate race keys for existing races
            cursor = await db.execute('SELECT id, year, race_name FROM races')
            races = await cursor.fetchall()
            
            for race_id, year, race_name in races:
                # Clean race name and generate key
                clean_name = race_name.strip()
                race_key = f"{year}_{clean_name}"
                
                await db.execute('UPDATE races SET race_key = ? WHERE id = ?', (race_key, race_id))
            
            # Add unique constraint on race_key
            await db.execute('CREATE UNIQUE INDEX idx_races_key ON races(race_key)')
            
            await db.commit()
            logger.info("race_key column added successfully")
        else:
            logger.info("race_key column already exists")

async def main():
    """Main function to clean up the database"""
    database_path = "data/cycling_data.db"
    
    logger.info("Starting database cleanup...")
    
    # Analyze duplicates
    logger.info("Analyzing duplicates...")
    duplicates = await analyze_duplicates(database_path)
    
    if duplicates:
        logger.info(f"Found {len(duplicates)} duplicate race groups:")
        for race_name, year, count in duplicates:
            logger.info(f"  {race_name} ({year}): {count} duplicates")
        
        # Clean up duplicates
        logger.info("Cleaning up duplicates...")
        await cleanup_duplicates(database_path)
    else:
        logger.info("No duplicates found")
    
    # Add race_key column for improved scraper
    logger.info("Preparing database for improved scraper...")
    await add_race_key_column(database_path)
    
    logger.info("Database cleanup completed!")

if __name__ == "__main__":
    asyncio.run(main()) 