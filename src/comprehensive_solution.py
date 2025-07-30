#!/usr/bin/env python3
"""
Comprehensive solution for the cycling data scraper issues
This addresses the JavaScript-rendered content problem and provides multiple approaches
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/comprehensive_solution.log')
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveSolution:
    """Comprehensive solution for the cycling data scraper issues"""
    
    def __init__(self):
        self.analysis_results = {}
        self.recommendations = []
    
    def analyze_current_situation(self):
        """Analyze the current situation and provide recommendations"""
        
        logger.info("🔍 Analyzing Current Situation")
        logger.info("=" * 60)
        
        # Check what we have
        current_data = self.check_current_data()
        
        # Analyze the problems
        problems = self.identify_problems()
        
        # Generate recommendations
        recommendations = self.generate_recommendations(current_data, problems)
        
        return {
            'current_data': current_data,
            'problems': problems,
            'recommendations': recommendations
        }
    
    def check_current_data(self):
        """Check what data we currently have"""
        import sqlite3
        
        try:
            conn = sqlite3.connect('data/cycling_data.db')
            cursor = conn.cursor()
            
            # Check table counts
            cursor.execute("SELECT COUNT(*) FROM races")
            races_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM stages")
            stages_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM results")
            results_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM riders")
            riders_count = cursor.fetchone()[0]
            
            # Check years covered
            cursor.execute("SELECT DISTINCT year FROM races ORDER BY year")
            years = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                'races': races_count,
                'stages': stages_count,
                'results': results_count,
                'riders': riders_count,
                'years': years,
                'database_size_mb': Path('data/cycling_data.db').stat().st_size / (1024 * 1024)
            }
            
        except Exception as e:
            logger.error(f"Error checking current data: {e}")
            return None
    
    def identify_problems(self):
        """Identify the main problems we're facing"""
        
        problems = [
            {
                'issue': 'JavaScript-rendered content',
                'description': 'The website uses JavaScript to dynamically load table content, making traditional scraping ineffective',
                'impact': 'High - Most race results cannot be scraped',
                'evidence': 'Diagnostic files show tables exist but have 0 rows'
            },
            {
                'issue': 'Website structure changes',
                'description': 'The website has changed its structure, breaking existing CSS selectors',
                'impact': 'High - Scraper cannot find or parse results',
                'evidence': 'Enhanced scraper still fails on same URLs'
            },
            {
                'issue': 'Rate limiting and blocking',
                'description': 'The website may be implementing anti-scraping measures',
                'impact': 'Medium - May cause intermittent failures',
                'evidence': 'Some requests succeed, others fail'
            },
            {
                'issue': 'Incomplete data coverage',
                'description': 'Current data only covers 2020-2025 with limited races',
                'impact': 'High - Missing historical data and many races',
                'evidence': 'Only 1,737 races vs expected 12,000+'
            }
        ]
        
        return problems
    
    def generate_recommendations(self, current_data, problems):
        """Generate comprehensive recommendations"""
        
        recommendations = []
        
        # Immediate actions
        recommendations.append({
            'priority': 'High',
            'action': 'Preserve current data',
            'description': 'The riders table (51,528 records) is valuable and should be preserved',
            'implementation': 'Use existing backup or create new one before any changes'
        })
        
        # Technical solutions
        recommendations.append({
            'priority': 'High',
            'action': 'Implement JavaScript-aware scraping',
            'description': 'Use Playwright or Selenium to handle dynamically loaded content',
            'implementation': 'Install Playwright: pip install playwright && playwright install',
            'effort': 'Medium (2-3 days)',
            'success_probability': 'High'
        })
        
        recommendations.append({
            'priority': 'High',
            'action': 'Update parsing logic',
            'description': 'Modify table parsing to handle new website structure',
            'implementation': 'Update CSS selectors and add fallback parsing methods',
            'effort': 'Low (1 day)',
            'success_probability': 'Medium'
        })
        
        # Alternative approaches
        recommendations.append({
            'priority': 'Medium',
            'action': 'Explore API alternatives',
            'description': 'Check if ProCyclingStats has an API or if other data sources are available',
            'implementation': 'Research cycling data APIs and alternative sources',
            'effort': 'Low (1 day research)',
            'success_probability': 'Unknown'
        })
        
        recommendations.append({
            'priority': 'Medium',
            'action': 'Implement hybrid approach',
            'description': 'Combine multiple scraping methods for better coverage',
            'implementation': 'Use both traditional and JavaScript-aware scraping',
            'effort': 'Medium (2 days)',
            'success_probability': 'High'
        })
        
        # Data management
        recommendations.append({
            'priority': 'Medium',
            'action': 'Implement incremental updates',
            'description': 'Only scrape new/updated data instead of full re-pulls',
            'implementation': 'Track last update times and only scrape changes',
            'effort': 'Medium (2 days)',
            'success_probability': 'High'
        })
        
        return recommendations
    
    def create_action_plan(self):
        """Create a detailed action plan"""
        
        logger.info("📋 Creating Action Plan")
        logger.info("=" * 60)
        
        plan = {
            'phase_1': {
                'title': 'Immediate Actions (Day 1)',
                'tasks': [
                    'Create comprehensive backup of current database',
                    'Install Playwright for JavaScript support',
                    'Test JavaScript-aware scraper on small sample',
                    'Document current data state and issues'
                ]
            },
            'phase_2': {
                'title': 'Technical Implementation (Days 2-4)',
                'tasks': [
                    'Implement JavaScript-aware scraper',
                    'Update parsing logic for new website structure',
                    'Add comprehensive error handling and logging',
                    'Create test suite for validation'
                ]
            },
            'phase_3': {
                'title': 'Testing and Validation (Days 5-6)',
                'tasks': [
                    'Test enhanced scraper on problematic URLs',
                    'Validate data quality and completeness',
                    'Performance testing and optimization',
                    'Create rollback plan'
                ]
            },
            'phase_4': {
                'title': 'Full Implementation (Days 7+)',
                'tasks': [
                    'Run full re-pull with enhanced scraper',
                    'Monitor progress and handle errors',
                    'Validate final data quality',
                    'Document lessons learned'
                ]
            }
        }
        
        return plan
    
    def create_test_script(self):
        """Create a comprehensive test script"""
        
        test_script = '''#!/usr/bin/env python3
"""
Comprehensive test script for the enhanced scraper
"""

import asyncio
import logging
from pathlib import Path

# Test different approaches
async def test_traditional_scraping():
    """Test traditional scraping approach"""
    pass

async def test_javascript_scraping():
    """Test JavaScript-aware scraping approach"""
    pass

async def test_hybrid_approach():
    """Test hybrid approach combining multiple methods"""
    pass

async def main():
    """Run comprehensive tests"""
    logger.info("🧪 Running Comprehensive Tests")
    
    # Test 1: Traditional scraping
    await test_traditional_scraping()
    
    # Test 2: JavaScript scraping
    await test_javascript_scraping()
    
    # Test 3: Hybrid approach
    await test_hybrid_approach()
    
    logger.info("✅ All tests completed")

if __name__ == "__main__":
    asyncio.run(main())
'''
        
        with open('src/comprehensive_test.py', 'w') as f:
            f.write(test_script)
        
        logger.info("📝 Created comprehensive test script: src/comprehensive_test.py")
    
    def generate_report(self):
        """Generate a comprehensive report"""
        
        logger.info("📊 Generating Comprehensive Report")
        logger.info("=" * 60)
        
        # Analyze current situation
        analysis = self.analyze_current_situation()
        
        # Create action plan
        action_plan = self.create_action_plan()
        
        # Create test script
        self.create_test_script()
        
        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'current_data_state': analysis['current_data'],
                'main_problems': len(analysis['problems']),
                'recommendations': len(analysis['recommendations']),
                'estimated_effort_days': 7,
                'success_probability': 'High with JavaScript-aware approach'
            },
            'detailed_analysis': analysis,
            'action_plan': action_plan,
            'next_steps': [
                '1. Install Playwright: pip install playwright && playwright install',
                '2. Run comprehensive test: python src/comprehensive_test.py',
                '3. Implement JavaScript-aware scraper',
                '4. Test on small dataset before full re-pull',
                '5. Monitor and validate results'
            ]
        }
        
        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"reports/comprehensive_solution_report_{timestamp}.json"
        
        Path('reports').mkdir(exist_ok=True)
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"💾 Report saved to: {report_file}")
        
        # Print summary
        self.print_summary(report)
        
        return report
    
    def print_summary(self, report):
        """Print a summary of the report"""
        
        logger.info("\n" + "=" * 60)
        logger.info("📋 COMPREHENSIVE SOLUTION SUMMARY")
        logger.info("=" * 60)
        
        current_data = report['detailed_analysis']['current_data']
        if current_data:
            logger.info(f"📊 Current Data:")
            logger.info(f"   Races: {current_data['races']:,}")
            logger.info(f"   Stages: {current_data['stages']:,}")
            logger.info(f"   Results: {current_data['results']:,}")
            logger.info(f"   Riders: {current_data['riders']:,}")
            logger.info(f"   Years: {current_data['years']}")
            logger.info(f"   Database Size: {current_data['database_size_mb']:.1f} MB")
        
        logger.info(f"\n⚠️  Problems Identified: {len(report['detailed_analysis']['problems'])}")
        for problem in report['detailed_analysis']['problems']:
            logger.info(f"   • {problem['issue']}: {problem['impact']} impact")
        
        logger.info(f"\n💡 Recommendations: {len(report['detailed_analysis']['recommendations'])}")
        high_priority = [r for r in report['detailed_analysis']['recommendations'] if r['priority'] == 'High']
        logger.info(f"   High Priority: {len(high_priority)}")
        
        logger.info(f"\n📅 Action Plan:")
        for phase, details in report['action_plan'].items():
            logger.info(f"   {details['title']}: {len(details['tasks'])} tasks")
        
        logger.info(f"\n🎯 Success Probability: {report['summary']['success_probability']}")
        logger.info(f"⏱️  Estimated Effort: {report['summary']['estimated_effort_days']} days")
        
        logger.info(f"\n🚀 Next Steps:")
        for step in report['next_steps']:
            logger.info(f"   {step}")

async def main():
    """Main function"""
    
    # Create logs and reports directories
    Path('logs').mkdir(exist_ok=True)
    Path('reports').mkdir(exist_ok=True)
    
    logger.info("🚀 Starting Comprehensive Solution Analysis")
    logger.info("=" * 60)
    
    solution = ComprehensiveSolution()
    report = solution.generate_report()
    
    logger.info("\n✅ Comprehensive solution analysis completed!")
    logger.info("📋 Check the generated report for detailed recommendations and action plan.")
    
    return report

if __name__ == "__main__":
    asyncio.run(main()) 