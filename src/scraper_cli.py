#!/usr/bin/env python3
"""
Extended CLI commands for the cycling data scraper
Provides utilities for managing progress, backups, and recovery
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Import progress tracker
sys.path.insert(0, str(Path(__file__).parent))
from progress_tracker import progress_tracker

async def show_status():
    """Show current scraping status"""
    # Dummy years list since we're just showing status
    dummy_years = list(range(1903, 2026))
    report = await progress_tracker.get_status_report(dummy_years)
    print(report)

async def show_failed_races():
    """Show failed races report"""
    report = await progress_tracker.get_failed_races_report()
    print(report)

async def create_manual_backup():
    """Create a manual database backup"""
    await progress_tracker.create_checkpoint("Manual backup via CLI")
    print("✅ Manual backup created successfully")

async def reset_progress():
    """Reset progress tracking"""
    confirm = input("⚠️  Are you sure you want to reset all progress? This cannot be undone. (yes/no): ")
    if confirm.lower() == 'yes':
        await progress_tracker.reset_session()
        print("✅ Progress reset completed")
    else:
        print("❌ Reset cancelled")

async def list_backups():
    """List available database backups"""
    backup_dir = Path("data/backups")
    if not backup_dir.exists():
        print("No backup directory found")
        return
    
    backups = list(backup_dir.glob("cycling_data_backup_*.db"))
    if not backups:
        print("No backups found")
        return
    
    backups.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    print("📦 Available Database Backups:")
    print("=" * 50)
    
    for backup in backups:
        size_mb = backup.stat().st_size / (1024 * 1024)
        mtime = backup.stat().st_mtime
        from datetime import datetime
        date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"📁 {backup.name}")
        print(f"   📅 Created: {date_str}")
        print(f"   📊 Size: {size_mb:.1f} MB")
        print()

async def estimate_time(years_str: str):
    """Estimate completion time for given years"""
    try:
        if '-' in years_str:
            start, end = map(int, years_str.split('-'))
            years = list(range(start, end + 1))
        else:
            years = [int(y) for y in years_str.split(',')]
        
        remaining_years = await progress_tracker.get_remaining_years(years)
        estimated_completion = await progress_tracker.estimate_completion(years)
        
        print(f"📊 Time Estimation for Years {years_str}:")
        print(f"   Total years: {len(years)}")
        print(f"   Remaining: {len(remaining_years)}")
        print(f"   Completed: {len(years) - len(remaining_years)}")
        
        if estimated_completion:
            print(f"   🎯 Estimated completion: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("   ⏳ Not enough data for time estimation")
            
    except ValueError:
        print("❌ Invalid year format. Use: 1903-2025 or 1903,1904,1905")

def main():
    """CLI entry point for scraper utilities"""
    parser = argparse.ArgumentParser(
        description="Cycling Data Scraper Utilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper_cli.py status              # Show current progress
  python scraper_cli.py failed              # Show failed races
  python scraper_cli.py backup              # Create manual backup
  python scraper_cli.py reset               # Reset progress (careful!)
  python scraper_cli.py backups             # List all backups
  python scraper_cli.py estimate 1903-2025  # Estimate completion time
"""
    )
    
    parser.add_argument(
        'command',
        choices=['status', 'failed', 'backup', 'reset', 'backups', 'estimate'],
        help='Command to execute'
    )
    
    parser.add_argument(
        'args',
        nargs='*',
        help='Additional arguments for the command'
    )
    
    args = parser.parse_args()
    
    async def run_command():
        if args.command == 'status':
            await show_status()
        elif args.command == 'failed':
            await show_failed_races()
        elif args.command == 'backup':
            await create_manual_backup()
        elif args.command == 'reset':
            await reset_progress()
        elif args.command == 'backups':
            await list_backups()
        elif args.command == 'estimate':
            if not args.args:
                print("❌ Please provide years to estimate (e.g., 1903-2025)")
                sys.exit(1)
            await estimate_time(args.args[0])
    
    try:
        asyncio.run(run_command())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)

if __name__ == "__main__":
    main() 