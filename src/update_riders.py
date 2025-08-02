#!/usr/bin/env python3
"""
Standalone script for updating rider profile data from ProCyclingStats

This script can be run independently to:
1. Update rider data for specific years
2. Scrape all missing rider profiles
3. Re-scrape existing rider profiles

Usage:
    python update_riders.py 2023 2024                # Update riders for specific years
    python update_riders.py --all-missing            # Scrape all missing riders
    python update_riders.py --refresh 2023           # Re-scrape existing riders for 2023
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import List

from improved_async_scraper import ImprovedAsyncCyclingDataScraper as AsyncCyclingDataScraper, ScrapingConfig

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/rider_update.log')
        ]
    )

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Update rider profile data from ProCyclingStats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python update_riders.py 2023 2024                # Update riders for specific years
  python update_riders.py --all-missing            # Scrape all missing riders
  python update_riders.py --refresh 2023           # Re-scrape existing riders for 2023
  python update_riders.py --years 2020-2024        # Update riders for year range
"""
    )
    
    parser.add_argument(
        'years',
        nargs='*',
        help='Years to update rider data for (e.g., 2023 2024 or 2020-2024)'
    )
    
    parser.add_argument(
        '--all-missing',
        action='store_true',
        help='Scrape all riders missing profile data (ignores year arguments)'
    )
    
    parser.add_argument(
        '--refresh',
        action='store_true',
        help='Re-scrape existing rider profiles (use with years)'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent requests for rider scraping (default: 5)'
    )
    
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.2,
        help='Delay between requests in seconds (default: 0.2)'
    )
    
    parser.add_argument(
        '--database',
        type=str,
        default='data/cycling_data.db',
        help='SQLite database path (default: data/cycling_data.db)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()

async def update_riders_for_years(scraper: AsyncCyclingDataScraper, years: List[int], refresh: bool = False):
    """Update rider data for specific years"""
    logger = logging.getLogger(__name__)
    
    logger.info(f"🔄 Updating rider profiles for years: {years}")
    logger.info(f"🔄 Refresh mode: {'ON' if refresh else 'OFF'}")
    
    if refresh:
        # Re-scrape existing riders by temporarily removing them
        # This is more complex and would require additional logic
        logger.warning("⚠️  Refresh mode not fully implemented yet")
        logger.info("💡 For now, use --all-missing to get missing riders")
    
    # Get riders missing profiles for specified years
    if not scraper.rider_scraper:
        logger.error("❌ Rider scraper not initialized")
        return
    
    missing_riders = await scraper.rider_scraper.get_riders_missing_profiles(years)
    
    if not missing_riders:
        logger.info("✅ All riders for specified years already have profile data")
        return
    
    logger.info(f"📊 Found {len(missing_riders)} riders missing profile data")
    
    # Scrape missing rider profiles
    results = await scraper.rider_scraper.scrape_riders_batch(missing_riders, max_concurrent=5)
    
    logger.info(f"🎉 Rider update completed for years {years}:")
    logger.info(f"   ✅ Success: {results['success']}")
    logger.info(f"   ❌ Failed: {results['failed']}")
    logger.info(f"   ⏭️  Skipped: {results['skipped']}")

async def scrape_all_missing_riders(scraper: AsyncCyclingDataScraper):
    """Scrape all riders missing profile data"""
    logger = logging.getLogger(__name__)
    
    logger.info("🔄 Scraping all missing rider profiles")
    
    if not scraper.rider_scraper:
        logger.error("❌ Rider scraper not initialized")
        return
    
    # Get all riders missing profiles
    missing_riders = await scraper.rider_scraper.get_riders_missing_profiles()
    
    if not missing_riders:
        logger.info("✅ All riders already have profile data")
        return
    
    logger.info(f"📊 Found {len(missing_riders)} riders missing profile data")
    
    # Scrape missing rider profiles
    results = await scraper.rider_scraper.scrape_riders_batch(missing_riders, max_concurrent=5)
    
    logger.info(f"🎉 All missing rider profiles processed:")
    logger.info(f"   ✅ Success: {results['success']}")
    logger.info(f"   ❌ Failed: {results['failed']}")
    logger.info(f"   ⏭️  Skipped: {results['skipped']}")

async def main():
    """Main entry point"""
    args = parse_args()
    
    # Create necessary directories
    Path('data').mkdir(exist_ok=True)
    Path('logs').mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    logger.info("🏃 Starting rider profile update utility")
    
    # Parse years if provided
    years = []
    if args.years and not args.all_missing:
        for year_arg in args.years:
            if '-' in str(year_arg):
                # Handle year ranges like 2020-2024
                try:
                    start_year, end_year = map(int, str(year_arg).split('-'))
                    if start_year > end_year:
                        logger.error(f"❌ Invalid year range: {year_arg}")
                        sys.exit(1)
                    years.extend(range(start_year, end_year + 1))
                except ValueError:
                    logger.error(f"❌ Invalid year range format: {year_arg}")
                    sys.exit(1)
            else:
                # Handle individual years
                try:
                    years.append(int(year_arg))
                except ValueError:
                    logger.error(f"❌ Invalid year: {year_arg}")
                    sys.exit(1)
        
        # Remove duplicates and sort
        years = sorted(list(set(years)))
        logger.info(f"📅 Target years: {years}")
    
    # Validate arguments
    if not args.all_missing and not years:
        logger.error("❌ Please specify years or use --all-missing")
        logger.info("💡 Examples:")
        logger.info("   python update_riders.py 2023 2024")
        logger.info("   python update_riders.py --all-missing")
        sys.exit(1)
    
    # Create scraping configuration
    config = ScrapingConfig(
        max_concurrent_requests=args.max_concurrent,
        request_delay=args.request_delay,
        max_retries=3,
        timeout=30,
        database_path=args.database
    )
    
    logger.info(f"⚙️ Configuration: max_concurrent={args.max_concurrent}, delay={args.request_delay}s")
    
    try:
        async with AsyncCyclingDataScraper(config) as scraper:
            if args.all_missing:
                await scrape_all_missing_riders(scraper)
            else:
                await update_riders_for_years(scraper, years, args.refresh)
                
    except KeyboardInterrupt:
        logger.info("🛑 Update interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Update failed: {e}")
        sys.exit(1)
    
    logger.info("🎉 Rider profile update completed successfully!")

if __name__ == "__main__":
    asyncio.run(main()) 