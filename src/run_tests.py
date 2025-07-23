#!/usr/bin/env python3
"""
Simple test runner for the cycling data scraper validation tests

Prerequisites:
- Activate virtual environment: source venv/bin/activate
- Or run with: python3 run_tests.py (from project root)

Usage: 
  source venv/bin/activate && python run_tests.py
  # OR
  python3 run_tests.py
"""

import asyncio
import sys
from pathlib import Path

# Add src directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from test_scraper import ScraperTestFramework
from async_scraper import ScrapingConfig

async def main():
    """Run the test framework"""
    print("🚀 Running Cycling Data Scraper Validation Tests")
    print("=" * 60)
    
    # Create conservative test configuration
    config = ScrapingConfig(
        max_concurrent_requests=3,  # Very conservative for testing
        request_delay=0.3,  # Slower to be polite
        max_retries=2,
        timeout=45,
        database_path="test_cycling_data.db"
    )
    
    test_framework = ScraperTestFramework(config)
    
    try:
        success = await test_framework.run_full_test_suite()
        
        print("\n" + "=" * 60)
        if success:
            print("✅ ALL TESTS PASSED!")
            print("🚀 Your scraper is ready for production use.")
            print("💡 You can now run the full scraper with confidence.")
        else:
            print("❌ SOME TESTS FAILED!")
            print("🔍 Check the test reports in the 'reports/' directory.")
            print("🛠️  Fix the issues before running the full scraper.")
            
        print(f"\n📊 Test Summary:")
        print(f"   • Reports saved in: reports/")
        print(f"   • Diagnostic logs in: logs/")
        print(f"   • Test database: test_cycling_data.db")
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"💥 Test framework crashed: {e}")
        print("🔧 This indicates a serious issue - check your environment setup.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 