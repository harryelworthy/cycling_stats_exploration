#!/usr/bin/env python3
"""
Progress tracking and checkpointing system for the cycling data scraper
Provides resume functionality and prevents data loss during long scraping sessions
"""

import asyncio
import aiosqlite
import json
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class ScrapingProgress:
    """Track scraping progress across years and races"""
    session_id: str
    start_time: datetime
    completed_years: Set[int] = field(default_factory=set)
    failed_years: Set[int] = field(default_factory=set)
    completed_races: Set[str] = field(default_factory=set)  # race URLs
    failed_races: Set[str] = field(default_factory=set)
    total_races_processed: int = 0
    total_stages_processed: int = 0
    total_results_processed: int = 0
    last_checkpoint: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None

class ProgressTracker:
    """Comprehensive progress tracking with checkpointing and recovery"""
    
    def __init__(self, database_path: str = "data/cycling_data.db", 
                 progress_file: str = "data/scraping_progress.json",
                 backup_dir: str = "data/backups"):
        self.database_path = Path(database_path)
        self.progress_file = Path(progress_file)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        
        self.current_progress: Optional[ScrapingProgress] = None
        self.checkpoint_interval = 300  # 5 minutes
        self.backup_frequency = 1  # Backup after each year
        
    async def start_session(self, target_years: List[int]) -> str:
        """Start a new scraping session or resume existing one"""
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Check for existing session to resume
        existing_progress = await self.load_progress()
        if existing_progress:
            logger.info(f"📋 Found existing session: {existing_progress.session_id}")
            logger.info(f"⏱️  Started: {existing_progress.start_time}")
            logger.info(f"✅ Completed years: {sorted(existing_progress.completed_years)}")
            logger.info(f"❌ Failed years: {sorted(existing_progress.failed_years)}")
            
            resume_choice = input("Resume existing session? (y/n): ").lower().strip()
            if resume_choice == 'y':
                self.current_progress = existing_progress
                logger.info("🔄 Resuming existing session")
                return existing_progress.session_id
        
        # Start new session
        self.current_progress = ScrapingProgress(
            session_id=session_id,
            start_time=datetime.now()
        )
        
        await self.save_progress()
        logger.info(f"🆕 Started new scraping session: {session_id}")
        return session_id
    
    async def load_progress(self) -> Optional[ScrapingProgress]:
        """Load existing progress from file"""
        if not self.progress_file.exists():
            return None
            
        try:
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
            
            progress = ScrapingProgress(
                session_id=data['session_id'],
                start_time=datetime.fromisoformat(data['start_time']),
                completed_years=set(data.get('completed_years', [])),
                failed_years=set(data.get('failed_years', [])),
                completed_races=set(data.get('completed_races', [])),
                failed_races=set(data.get('failed_races', [])),
                total_races_processed=data.get('total_races_processed', 0),
                total_stages_processed=data.get('total_stages_processed', 0),
                total_results_processed=data.get('total_results_processed', 0),
                last_checkpoint=datetime.fromisoformat(data['last_checkpoint']) if data.get('last_checkpoint') else None,
                estimated_completion=datetime.fromisoformat(data['estimated_completion']) if data.get('estimated_completion') else None
            )
            
            return progress
            
        except Exception as e:
            logger.error(f"Failed to load progress: {e}")
            return None
    
    async def save_progress(self):
        """Save current progress to file"""
        if not self.current_progress:
            return
            
        try:
            data = {
                'session_id': self.current_progress.session_id,
                'start_time': self.current_progress.start_time.isoformat(),
                'completed_years': list(self.current_progress.completed_years),
                'failed_years': list(self.current_progress.failed_years),
                'completed_races': list(self.current_progress.completed_races),
                'failed_races': list(self.current_progress.failed_races),
                'total_races_processed': self.current_progress.total_races_processed,
                'total_stages_processed': self.current_progress.total_stages_processed,
                'total_results_processed': self.current_progress.total_results_processed,
                'last_checkpoint': self.current_progress.last_checkpoint.isoformat() if self.current_progress.last_checkpoint else None,
                'estimated_completion': self.current_progress.estimated_completion.isoformat() if self.current_progress.estimated_completion else None,
                'last_updated': datetime.now().isoformat()
            }
            
            # Atomic write
            temp_file = self.progress_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            temp_file.replace(self.progress_file)
            
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    async def should_skip_year(self, year: int) -> bool:
        """Check if a year should be skipped (already completed)"""
        if not self.current_progress:
            return False
        return year in self.current_progress.completed_years
    
    async def should_skip_race(self, race_url: str) -> bool:
        """Check if a race should be skipped (already completed)"""
        if not self.current_progress:
            return False
        return race_url in self.current_progress.completed_races
    
    async def mark_year_completed(self, year: int):
        """Mark a year as completed"""
        if self.current_progress:
            self.current_progress.completed_years.add(year)
            self.current_progress.failed_years.discard(year)  # Remove from failed if it was there
            await self.save_progress()
            await self.create_checkpoint(f"Completed year {year}")
    
    async def mark_year_failed(self, year: int, error: str):
        """Mark a year as failed"""
        if self.current_progress:
            self.current_progress.failed_years.add(year)
            await self.save_progress()
            logger.error(f"Year {year} marked as failed: {error}")
    
    async def mark_race_completed(self, race_url: str, stages_count: int, results_count: int):
        """Mark a race as completed"""
        if self.current_progress:
            self.current_progress.completed_races.add(race_url)
            self.current_progress.failed_races.discard(race_url)
            self.current_progress.total_races_processed += 1
            self.current_progress.total_stages_processed += stages_count
            self.current_progress.total_results_processed += results_count
            await self.save_progress()
    
    async def mark_race_failed(self, race_url: str, error: str):
        """Mark a race as failed"""
        if self.current_progress:
            self.current_progress.failed_races.add(race_url)
            await self.save_progress()
            logger.warning(f"Race {race_url} marked as failed: {error}")
    
    async def create_checkpoint(self, description: str = "Manual checkpoint"):
        """Create a database backup checkpoint"""
        if not self.database_path.exists():
            return
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = self.backup_dir / f"cycling_data_backup_{timestamp}.db"
            
            # Create backup
            shutil.copy2(self.database_path, backup_file)
            
            # Update progress
            if self.current_progress:
                self.current_progress.last_checkpoint = datetime.now()
                await self.save_progress()
            
            logger.info(f"💾 Database backup created: {backup_file}")
            logger.info(f"📝 Checkpoint: {description}")
            
            # Clean up old backups (keep last 10)
            await self._cleanup_old_backups()
            
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
    
    async def _cleanup_old_backups(self):
        """Keep only the most recent 10 backups"""
        try:
            backup_files = list(self.backup_dir.glob("cycling_data_backup_*.db"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            for old_backup in backup_files[10:]:  # Keep only 10 most recent
                old_backup.unlink()
                logger.debug(f"Cleaned up old backup: {old_backup}")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")
    
    async def estimate_completion(self, target_years: List[int]) -> Optional[datetime]:
        """Estimate completion time based on current progress"""
        if not self.current_progress:
            return None
            
        completed_count = len(self.current_progress.completed_years)
        total_count = len(target_years)
        
        if completed_count == 0:
            return None
            
        elapsed_time = datetime.now() - self.current_progress.start_time
        time_per_year = elapsed_time / completed_count
        remaining_years = total_count - completed_count
        
        estimated_completion = datetime.now() + (time_per_year * remaining_years)
        
        if self.current_progress:
            self.current_progress.estimated_completion = estimated_completion
            await self.save_progress()
        
        return estimated_completion
    
    async def get_status_report(self, target_years: List[int]) -> str:
        """Generate a comprehensive status report"""
        if not self.current_progress:
            return "No active session"
        
        elapsed = datetime.now() - self.current_progress.start_time
        completed_years = len(self.current_progress.completed_years)
        total_years = len(target_years)
        failed_years = len(self.current_progress.failed_years)
        
        # Calculate progress percentage
        progress_pct = (completed_years / total_years * 100) if total_years > 0 else 0
        
        # Estimate completion
        estimated_completion = await self.estimate_completion(target_years)
        eta_str = estimated_completion.strftime("%Y-%m-%d %H:%M:%S") if estimated_completion else "Unknown"
        
        # Performance metrics
        races_per_hour = (self.current_progress.total_races_processed / elapsed.total_seconds() * 3600) if elapsed.total_seconds() > 0 else 0
        
        report = f"""
🏁 SCRAPING PROGRESS REPORT
{'=' * 50}
📅 Session: {self.current_progress.session_id}
⏱️  Started: {self.current_progress.start_time.strftime('%Y-%m-%d %H:%M:%S')}
🕐 Elapsed: {str(elapsed).split('.')[0]}

📊 YEAR PROGRESS:
   ✅ Completed: {completed_years}/{total_years} years ({progress_pct:.1f}%)
   ❌ Failed: {failed_years} years
   🔄 Remaining: {total_years - completed_years - failed_years} years

📈 DATA COLLECTED:
   🏁 Races: {self.current_progress.total_races_processed:,}
   🚴 Stages: {self.current_progress.total_stages_processed:,}
   📋 Results: {self.current_progress.total_results_processed:,}

⚡ PERFORMANCE:
   🏃 Rate: {races_per_hour:.1f} races/hour
   💾 Last backup: {self.current_progress.last_checkpoint.strftime('%H:%M:%S') if self.current_progress.last_checkpoint else 'Never'}

🎯 PROJECTION:
   🏆 ETA: {eta_str}
"""
        
        if self.current_progress.failed_years:
            report += f"\n⚠️  FAILED YEARS: {sorted(self.current_progress.failed_years)}"
        
        return report
    
    async def get_remaining_years(self, target_years: List[int]) -> List[int]:
        """Get list of years that still need to be processed"""
        if not self.current_progress:
            return target_years
        
        return [year for year in target_years 
                if year not in self.current_progress.completed_years 
                and year not in self.current_progress.failed_years]
    
    async def get_failed_races_report(self) -> str:
        """Generate report of failed races for debugging"""
        if not self.current_progress or not self.current_progress.failed_races:
            return "No failed races to report"
        
        report = f"""
❌ FAILED RACES REPORT
{'=' * 30}
Total failed races: {len(self.current_progress.failed_races)}

Failed race URLs:
"""
        
        for race_url in sorted(self.current_progress.failed_races):
            report += f"   • {race_url}\n"
        
        return report
    
    async def reset_session(self):
        """Reset/clear current session"""
        if self.progress_file.exists():
            backup_file = self.progress_file.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            shutil.move(self.progress_file, backup_file)
            logger.info(f"Previous session backed up to: {backup_file}")
        
        self.current_progress = None
        logger.info("Session reset completed")

# Global progress tracker instance
progress_tracker = ProgressTracker() 