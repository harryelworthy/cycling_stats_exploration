"""
Utility functions for cycling data scraper
"""

import aiosqlite
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import json
from datetime import datetime

logger = logging.getLogger(__name__)

async def export_data_to_json(database_path: str, output_path: str, year: Optional[int] = None):
    """Export data from SQLite to JSON format"""
    async with aiosqlite.connect(database_path) as db:
        # Build query based on year filter
        where_clause = f"WHERE r.year = {year}" if year else ""
        
        query = f"""
        SELECT 
            r.year,
            r.race_name,
            r.race_category,
            r.uci_tour,
            s.stage_url,
            s.is_one_day_race,
            s.distance,
            s.stage_type,
            s.date,
            res.rider_name,
            res.rider_url,
            res.team_name,
            res.team_url,
            res.rank,
            res.status,
            res.time,
            res.uci_points,
            res.pcs_points,
            res.age,
            res.gc_rank,
            res.gc_uci_points,
            res.points_rank,
            res.points_uci_points,
            res.kom_rank,
            res.kom_uci_points,
            res.youth_rank,
            res.youth_uci_points
        FROM races r
        JOIN stages s ON r.id = s.race_id
        JOIN results res ON s.id = res.stage_id
        {where_clause}
        ORDER BY r.year, r.race_name, s.stage_url, res.rank
        """
        
        cursor = await db.execute(query)
        rows = await cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        # Convert to list of dictionaries
        data = [dict(zip(columns, row)) for row in rows]
        
        # Write to JSON file
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Exported {len(data)} records to {output_path}")

async def get_database_stats(database_path: str) -> Dict[str, int]:
    """Get statistics about the database contents"""
    async with aiosqlite.connect(database_path) as db:
        stats = {}
        
        # Count races
        cursor = await db.execute("SELECT COUNT(*) FROM races")
        stats['total_races'] = (await cursor.fetchone())[0]
        
        # Count stages
        cursor = await db.execute("SELECT COUNT(*) FROM stages")
        stats['total_stages'] = (await cursor.fetchone())[0]
        
        # Count results
        cursor = await db.execute("SELECT COUNT(*) FROM results")
        stats['total_results'] = (await cursor.fetchone())[0]
        
        # Count by year
        cursor = await db.execute("SELECT year, COUNT(*) FROM races GROUP BY year ORDER BY year")
        races_by_year = await cursor.fetchall()
        stats['races_by_year'] = dict(races_by_year)
        
        # Count unique riders
        cursor = await db.execute("SELECT COUNT(DISTINCT rider_url) FROM results WHERE rider_url IS NOT NULL")
        stats['unique_riders'] = (await cursor.fetchone())[0]
        
        # Count unique teams
        cursor = await db.execute("SELECT COUNT(DISTINCT team_url) FROM results WHERE team_url IS NOT NULL")
        stats['unique_teams'] = (await cursor.fetchone())[0]
        
        return stats

async def clean_database(database_path: str, dry_run: bool = True) -> Dict[str, int]:
    """Clean up database by removing orphaned records and duplicates"""
    async with aiosqlite.connect(database_path) as db:
        cleanup_stats = {
            'orphaned_stages': 0,
            'orphaned_results': 0,
            'duplicate_races': 0,
            'duplicate_stages': 0
        }
        
        if not dry_run:
            # Remove orphaned stages (stages without races)
            cursor = await db.execute("""
                DELETE FROM stages 
                WHERE race_id NOT IN (SELECT id FROM races)
            """)
            cleanup_stats['orphaned_stages'] = cursor.rowcount
            
            # Remove orphaned results (results without stages)
            cursor = await db.execute("""
                DELETE FROM results 
                WHERE stage_id NOT IN (SELECT id FROM stages)
            """)
            cleanup_stats['orphaned_results'] = cursor.rowcount
            
            await db.commit()
        else:
            # Just count what would be cleaned
            cursor = await db.execute("""
                SELECT COUNT(*) FROM stages 
                WHERE race_id NOT IN (SELECT id FROM races)
            """)
            cleanup_stats['orphaned_stages'] = (await cursor.fetchone())[0]
            
            cursor = await db.execute("""
                SELECT COUNT(*) FROM results 
                WHERE stage_id NOT IN (SELECT id FROM stages)
            """)
            cleanup_stats['orphaned_results'] = (await cursor.fetchone())[0]
        
        return cleanup_stats

def parse_time_string(time_str: str) -> Optional[float]:
    """Parse time string (e.g., '4:32:15' or '1:23') to seconds"""
    if not time_str or time_str == '-':
        return None
    
    try:
        parts = time_str.split(':')
        if len(parts) == 2:  # MM:SS
            minutes, seconds = map(float, parts)
            return minutes * 60 + seconds
        elif len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(float, parts)
            return hours * 3600 + minutes * 60 + seconds
        else:
            return float(time_str)  # Just seconds
    except (ValueError, AttributeError):
        return None

def parse_distance_string(distance_str: str) -> Optional[float]:
    """Parse distance string (e.g., '180 km' or '45.5km') to kilometers"""
    if not distance_str:
        return None
    
    try:
        # Remove 'km' and any other non-numeric characters except decimal point
        cleaned = ''.join(c for c in distance_str.lower().replace('km', '') if c.isdigit() or c == '.')
        return float(cleaned) if cleaned else None
    except (ValueError, AttributeError):
        return None

def create_database_backup(database_path: str, backup_dir: str = "backups") -> str:
    """Create a timestamped backup of the database"""
    Path(backup_dir).mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"cycling_data_backup_{timestamp}.db"
    backup_path = Path(backup_dir) / backup_filename
    
    # Copy database file
    import shutil
    shutil.copy2(database_path, backup_path)
    
    logger.info(f"Database backed up to {backup_path}")
    return str(backup_path)

async def validate_data_integrity(database_path: str) -> Dict[str, List[str]]:
    """Validate data integrity and return list of issues"""
    issues = {
        'missing_data': [],
        'invalid_data': [],
        'inconsistencies': []
    }
    
    async with aiosqlite.connect(database_path) as db:
        # Check for races without stages
        cursor = await db.execute("""
            SELECT race_name, year FROM races 
            WHERE id NOT IN (SELECT DISTINCT race_id FROM stages WHERE race_id IS NOT NULL)
        """)
        races_without_stages = await cursor.fetchall()
        for race_name, year in races_without_stages:
            issues['missing_data'].append(f"Race '{race_name}' ({year}) has no stages")
        
        # Check for stages without results
        cursor = await db.execute("""
            SELECT s.stage_url, r.race_name FROM stages s
            JOIN races r ON s.race_id = r.id
            WHERE s.id NOT IN (SELECT DISTINCT stage_id FROM results WHERE stage_id IS NOT NULL)
        """)
        stages_without_results = await cursor.fetchall()
        for stage_url, race_name in stages_without_results:
            issues['missing_data'].append(f"Stage '{stage_url}' in race '{race_name}' has no results")
        
        # Check for invalid ranks (should be positive integers)
        cursor = await db.execute("""
            SELECT COUNT(*) FROM results WHERE rank IS NOT NULL AND rank <= 0
        """)
        invalid_ranks = (await cursor.fetchone())[0]
        if invalid_ranks > 0:
            issues['invalid_data'].append(f"{invalid_ranks} results have invalid ranks (≤ 0)")
        
        # Check for riders with missing names
        cursor = await db.execute("""
            SELECT COUNT(*) FROM results WHERE rider_name IS NULL OR rider_name = ''
        """)
        missing_rider_names = (await cursor.fetchone())[0]
        if missing_rider_names > 0:
            issues['missing_data'].append(f"{missing_rider_names} results have missing rider names")
    
    return issues

async def get_top_riders_by_points(database_path: str, year: Optional[int] = None, limit: int = 20) -> List[Tuple[str, int, int]]:
    """Get top riders by total UCI points"""
    async with aiosqlite.connect(database_path) as db:
        where_clause = f"JOIN races r ON s.race_id = r.id WHERE r.year = {year}" if year else ""
        
        query = f"""
        SELECT 
            res.rider_name,
            SUM(res.uci_points) as total_uci_points,
            COUNT(*) as race_count
        FROM results res
        JOIN stages s ON res.stage_id = s.id
        {where_clause}
        WHERE res.rider_name IS NOT NULL
        GROUP BY res.rider_name, res.rider_url
        ORDER BY total_uci_points DESC
        LIMIT {limit}
        """
        
        cursor = await db.execute(query)
        return await cursor.fetchall()

async def get_race_winners(database_path: str, year: Optional[int] = None) -> List[Tuple[str, str, str]]:
    """Get race winners (riders who won at least one stage)"""
    async with aiosqlite.connect(database_path) as db:
        where_clause = f"AND r.year = {year}" if year else ""
        
        query = f"""
        SELECT DISTINCT
            res.rider_name,
            r.race_name,
            s.stage_url
        FROM results res
        JOIN stages s ON res.stage_id = s.id
        JOIN races r ON s.race_id = r.id
        WHERE res.rank = 1 {where_clause}
        ORDER BY r.race_name, s.stage_url
        """
        
        cursor = await db.execute(query)
        return await cursor.fetchall()
