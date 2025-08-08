#!/usr/bin/env python3
"""
CLI entry point for the cycling data async scraper
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import List

from improved_async_scraper import ImprovedAsyncCyclingDataScraper as AsyncCyclingDataScraper, ScrapingConfig
from test_scraper import ScraperTestFramework
from progress_tracker import progress_tracker

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/scraper.log')
        ]
    )

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Async cycling data scraper for procyclingstats.com"
    )
    
    parser.add_argument(
        'years',
        nargs='+',
        help='Years to scrape (e.g., 2023 2024 or 1903-2025)'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=30,
        help='Maximum concurrent requests (default: 30)'
    )
    
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.1,
        help='Delay between requests in seconds (default: 0.1)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum retries for failed requests (default: 3)'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Request timeout in seconds (default: 30)'
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
    
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip pre-scraping validation tests (not recommended)'
    )
    
    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Run only validation tests without scraping'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from previous session if available'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current progress status and exit'
    )
    
    parser.add_argument(
        '--reset-session',
        action='store_true',
        help='Reset/clear current session progress'
    )
    
    parser.add_argument(
        '--enable-rider-scraping',
        action='store_true',
        help='Enable rider profile scraping after race data scraping'
    )
    
    parser.add_argument(
        '--riders-only',
        action='store_true',
        help='Only scrape rider profiles for existing race data (skip race scraping)'
    )
    
    parser.add_argument(
        '--update-riders',
        action='store_true',
        help='Update rider data for specified years without scraping races'
    )
    
    parser.add_argument(
        '--checkpoint-interval',
        type=int,
        default=300,
        help='Database backup interval in seconds (default: 300 = 5 minutes)'
    )
    
    # Overwrite control options
    parser.add_argument(
        '--overwrite-data',
        action='store_true',
        help='Allow overwriting existing race, stage, and result data'
    )
    
    parser.add_argument(
        '--overwrite-stages',
        action='store_true',
        help='Allow overwriting existing stage data only'
    )
    
    parser.add_argument(
        '--overwrite-results',
        action='store_true',
        help='Allow overwriting existing result data only'
    )
    
    return parser.parse_args()

async def run_validation_tests(config: ScrapingConfig) -> bool:
    """Run comprehensive validation tests before scraping"""
    logger = logging.getLogger(__name__)
    
    logger.info("🧪 Running pre-scraping validation tests...")
    logger.info("This ensures the scraper works correctly before processing full years")
    
    # Create test framework with same config but separate database
    test_config = ScrapingConfig(
        max_concurrent_requests=min(5, config.max_concurrent_requests),  # Conservative for tests
        request_delay=max(0.2, config.request_delay),  # Slower for tests
        max_retries=config.max_retries,
        timeout=config.timeout,
        database_path="test_cycling_data.db"  # Separate test database
    )
    
    test_framework = ScraperTestFramework(test_config)
    
    try:
        success = await test_framework.run_full_test_suite()
        
        if success:
            logger.info("✅ All validation tests passed!")
            logger.info("🚀 Scraper is verified and ready for full year processing")
            return True
        else:
            logger.error("❌ Validation tests failed!")
            logger.error("🛑 Scraping aborted to prevent processing with broken scraper")
            logger.error("📋 Check the test reports in the 'reports/' directory for details")
            logger.error("🔧 Fix the identified issues before running the scraper again")
            return False
            
    except Exception as e:
        logger.error(f"💥 Test framework crashed: {e}")
        logger.error("🛑 Cannot verify scraper functionality - aborting")
        return False

async def main():
    """Main entry point"""
    args = parse_args()
    
    # Create necessary directories
    Path('data').mkdir(exist_ok=True)
    Path('data/backups').mkdir(exist_ok=True)
    Path('logs').mkdir(exist_ok=True)
    Path('reports').mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    # Handle special commands first
    if args.status:
        report = await progress_tracker.get_status_report(args.years)
        print(report)
        return
    
    if args.reset_session:
        await progress_tracker.reset_session()
        print("✅ Session reset completed")
        return
    
    # Parse and validate years (support ranges like 1903-2025)
    parsed_years = []
    current_year = 2025  # Updated to current year
    
    for year_arg in args.years:
        if '-' in str(year_arg):
            # Handle year ranges like 1903-2025
            try:
                start_year, end_year = map(int, str(year_arg).split('-'))
                if start_year > end_year:
                    logger.error(f"Invalid year range: {year_arg}. Start year must be <= end year")
                    sys.exit(1)
                parsed_years.extend(range(start_year, end_year + 1))
            except ValueError:
                logger.error(f"Invalid year range format: {year_arg}. Use format like 1903-2025")
                sys.exit(1)
        else:
            # Handle individual years
            try:
                parsed_years.append(int(year_arg))
            except ValueError:
                logger.error(f"Invalid year: {year_arg}. Must be an integer or range like 1903-2025")
                sys.exit(1)
    
    # Validate parsed years
    for year in parsed_years:
        if year < 1903 or year > current_year:  # 1903 = first Tour de France
            logger.error(f"Invalid year: {year}. Must be between 1903 and {current_year}")
            logger.info("Note: Very early years (1903-1950s) may have limited data availability")
            sys.exit(1)
    
    # Update args.years with parsed years
    args.years = sorted(list(set(parsed_years)))  # Remove duplicates and sort
    logger.info(f"📅 Parsed years: {len(args.years)} years from {min(args.years)} to {max(args.years)}")
    
    # Create scraping configuration
    config = ScrapingConfig(
        max_concurrent_requests=args.max_concurrent,
        request_delay=args.request_delay,
        max_retries=args.max_retries,
        timeout=args.timeout,
        database_path=args.database,
        overwrite_existing_data=args.overwrite_data,
        overwrite_stages=args.overwrite_stages,
        overwrite_results=args.overwrite_results
    )
    
    logger.info(f"🚀 Starting cycling data scraper")
    logger.info(f"📅 Target years: {args.years}")
    logger.info(f"⚙️ Configuration: {config}")
    
    # Step 1: Run validation tests (unless explicitly skipped)
    if not args.skip_tests:
        logger.info("🔍 Step 1: Validation Tests")
        test_success = await run_validation_tests(config)
        
        if not test_success:
            logger.error("❌ Validation failed. Use --skip-tests to bypass (not recommended)")
            sys.exit(1)
        
        if args.test_only:
            logger.info("✅ Test-only mode completed successfully!")
            return
    else:
        logger.warning("⚠️ Skipping validation tests as requested")
        logger.warning("🚨 This is not recommended - scraper may fail on format changes")
    
    # Step 2: Initialize progress tracking
    logger.info("📋 Initializing progress tracking...")
    session_id = await progress_tracker.start_session(args.years)
    
    # Get remaining years to process
    remaining_years = await progress_tracker.get_remaining_years(args.years)
    
    if not remaining_years:
        logger.info("✅ All specified years have already been completed!")
        report = await progress_tracker.get_status_report(args.years)
        print(report)
        return
    
    logger.info(f"🔄 Years to process: {remaining_years}")
    logger.info(f"⏭️  Years already completed: {len(args.years) - len(remaining_years)}")
    
    # Step 3: Handle different scraping modes
    if args.riders_only:
        logger.info("🏃 Step 3: Rider Profile Scraping Only")
        try:
            async with AsyncCyclingDataScraper(config) as scraper:
                results = await scraper.scrape_all_missing_riders()
                logger.info(f"🎉 Rider scraping completed!")
                logger.info(f"   ✅ Success: {results['success']}")
                logger.info(f"   ❌ Failed: {results['failed']}")
                logger.info(f"   ⏭️  Skipped: {results['skipped']}")
        except Exception as e:
            logger.error(f"💥 Rider scraping failed: {e}")
            sys.exit(1)
    elif args.update_riders:
        logger.info("🔄 Step 3: Update Rider Data for Specified Years")
        try:
            async with AsyncCyclingDataScraper(config) as scraper:
                results = await scraper.update_rider_data_for_years(args.years)
                logger.info(f"🎉 Rider data update completed!")
                logger.info(f"   ✅ Success: {results['success']}")
                logger.info(f"   ❌ Failed: {results['failed']}")
                logger.info(f"   ⏭️  Skipped: {results['skipped']}")
        except Exception as e:
            logger.error(f"💥 Rider data update failed: {e}")
            sys.exit(1)
    else:
        # Step 3: Run actual scraping with progress tracking
        logger.info("🔄 Step 3: Data Scraping with Progress Tracking")
        
        try:
            async with AsyncCyclingDataScraper(config) as scraper:
                # Set up progress tracking
                scraper.progress_tracker = progress_tracker
                scraper.checkpoint_interval = getattr(args, 'checkpoint_interval', 300)
                
                if args.enable_rider_scraping:
                    # Scrape races and then riders
                    await scraper.scrape_years_with_riders(remaining_years, enable_rider_scraping=True)
                else:
                    # Scrape only races
                    await scraper.scrape_years_with_progress(remaining_years)
            
            logger.info("🎉 Scraping completed successfully!")
            logger.info(f"📊 Data saved to: {args.database}")
            
            # Final status report
            if hasattr(progress_tracker, 'get_status_report'):
                final_report = await progress_tracker.get_status_report(args.years)
                print(final_report)
        except KeyboardInterrupt:
            logger.info("🛑 Scraping interrupted by user")
            logger.info("💾 Progress has been saved - use --resume to continue")
            report = await progress_tracker.get_status_report(args.years)
            print(report)
            sys.exit(1)
        except Exception as e:
            logger.error(f"💥 Scraping failed: {e}")
            logger.error("💾 Progress has been saved - check logs and use --resume to continue")
            logger.error("💡 Try running with --test-only to diagnose issues")
            
            # Save failure info
            if remaining_years:
                await progress_tracker.mark_year_failed(remaining_years[0], str(e))
            
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 