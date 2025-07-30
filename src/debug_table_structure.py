#!/usr/bin/env python3
"""
Debug script to analyze the actual HTML structure of failing pages
This will help us understand what's changed in the website structure
"""

import asyncio
import aiohttp
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
from typing import Dict, Any, List

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableStructureAnalyzer:
    """Analyze table structure on cycling pages"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
    
    async def analyze_url(self, url: str) -> Dict[str, Any]:
        """Analyze the table structure of a specific URL"""
        logger.info(f"Analyzing: {url}")
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        return {"error": f"HTTP {response.status}"}
                    
                    html_content = await response.text()
                    return self.analyze_html(html_content, url)
                    
            except Exception as e:
                return {"error": str(e)}
    
    def analyze_html(self, html_content: str, url: str) -> Dict[str, Any]:
        """Analyze HTML content for table structure"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        analysis = {
            "url": url,
            "page_title": soup.find('title').get_text() if soup.find('title') else None,
            "h1_content": soup.find('h1').get_text() if soup.find('h1') else None,
            "tables_found": [],
            "table_analysis": {},
            "potential_issues": []
        }
        
        # Find all tables
        tables = soup.find_all('table')
        analysis["total_tables"] = len(tables)
        
        for i, table in enumerate(tables):
            table_info = {
                "index": i,
                "classes": table.get('class', []),
                "id": table.get('id'),
                "style": table.get('style'),
                "row_count": 0,
                "column_count": 0,
                "has_thead": bool(table.find('thead')),
                "has_tbody": bool(table.find('tbody')),
                "row_types": {},
                "sample_rows": []
            }
            
            # Analyze rows
            rows = table.find_all('tr')
            table_info["row_count"] = len(rows)
            
            if rows:
                # Analyze first few rows
                for j, row in enumerate(rows[:5]):  # First 5 rows
                    row_info = {
                        "row_index": j,
                        "classes": row.get('class', []),
                        "cells_count": len(row.find_all(['td', 'th'])),
                        "cell_types": [],
                        "has_rider_links": bool(row.find('a', href=lambda x: x and 'rider/' in x)),
                        "has_team_links": bool(row.find('a', href=lambda x: x and 'team/' in x)),
                        "sample_content": row.get_text()[:100] + "..." if len(row.get_text()) > 100 else row.get_text()
                    }
                    
                    # Analyze cells
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        cell_type = 'th' if cell.name == 'th' else 'td'
                        row_info["cell_types"].append(cell_type)
                    
                    table_info["sample_rows"].append(row_info)
                    
                    # Count row types
                    row_type = f"{row_info['cells_count']} cells"
                    table_info["row_types"][row_type] = table_info["row_types"].get(row_type, 0) + 1
                
                # Get column count from first row
                if rows[0]:
                    first_row_cells = rows[0].find_all(['td', 'th'])
                    table_info["column_count"] = len(first_row_cells)
            
            analysis["tables_found"].append(table_info)
            
            # Check if this looks like a results table
            if 'results' in table.get('class', []):
                analysis["table_analysis"][f"table_{i}"] = {
                    "is_results_table": True,
                    "row_count": table_info["row_count"],
                    "has_data_rows": table_info["row_count"] > 1,  # More than just header
                    "has_rider_links": any(row["has_rider_links"] for row in table_info["sample_rows"]),
                    "potential_issues": []
                }
                
                # Identify potential issues
                if table_info["row_count"] == 0:
                    analysis["table_analysis"][f"table_{i}"]["potential_issues"].append("No rows found")
                elif table_info["row_count"] == 1:
                    analysis["table_analysis"][f"table_{i}"]["potential_issues"].append("Only header row found")
                elif not any(row["has_rider_links"] for row in table_info["sample_rows"]):
                    analysis["table_analysis"][f"table_{i}"]["potential_issues"].append("No rider links found")
        
        # Check for JavaScript or dynamic content indicators
        scripts = soup.find_all('script')
        analysis["javascript_indicators"] = {
            "total_scripts": len(scripts),
            "has_ajax": any('ajax' in str(script) for script in scripts),
            "has_fetch": any('fetch' in str(script) for script in scripts),
            "has_xmlhttp": any('xmlhttp' in str(script) for script in scripts)
        }
        
        # Check for CSS that might hide rows
        styles = soup.find_all('style')
        analysis["css_indicators"] = {
            "total_styles": len(styles),
            "has_hide_classes": any('hide' in str(style) for style in styles),
            "has_display_none": any('display: none' in str(style) for style in styles)
        }
        
        return analysis

async def main():
    """Main function to analyze problematic URLs"""
    
    # URLs that failed in the diagnostic files
    test_urls = [
        "https://www.procyclingstats.com/race/vuelta-a-espana/2025/stage-5",
        "https://www.procyclingstats.com/race/gp-de-wallonie/2025/result",
        "https://www.procyclingstats.com/race/tour-of-guangxi/2020/stage-5",
        "https://www.procyclingstats.com/race/tour-de-pologne/2025/stage-1",
        "https://www.procyclingstats.com/race/bretagne-classic/2025/result"
    ]
    
    analyzer = TableStructureAnalyzer()
    
    print("🔍 Analyzing table structure on problematic URLs...")
    print("=" * 80)
    
    all_results = {}
    
    for url in test_urls:
        print(f"\n📄 Analyzing: {url}")
        print("-" * 60)
        
        result = await analyzer.analyze_url(url)
        all_results[url] = result
        
        if "error" in result:
            print(f"❌ Error: {result['error']}")
            continue
        
        print(f"📊 Page Title: {result.get('page_title', 'N/A')}")
        print(f"📋 H1 Content: {result.get('h1_content', 'N/A')}")
        print(f"📊 Total Tables: {result.get('total_tables', 0)}")
        
        # Show results table analysis
        for table_key, table_analysis in result.get("table_analysis", {}).items():
            print(f"\n🔍 {table_key.upper()}:")
            print(f"   Rows: {table_analysis.get('row_count', 0)}")
            print(f"   Has Data: {table_analysis.get('has_data_rows', False)}")
            print(f"   Has Rider Links: {table_analysis.get('has_rider_links', False)}")
            
            issues = table_analysis.get('potential_issues', [])
            if issues:
                print(f"   ⚠️  Issues: {', '.join(issues)}")
        
        # Show JavaScript indicators
        js_indicators = result.get("javascript_indicators", {})
        if js_indicators.get("total_scripts", 0) > 0:
            print(f"\n💻 JavaScript: {js_indicators['total_scripts']} scripts")
            if js_indicators.get("has_ajax") or js_indicators.get("has_fetch"):
                print("   ⚠️  Dynamic content detected!")
        
        # Show CSS indicators
        css_indicators = result.get("css_indicators", {})
        if css_indicators.get("has_hide_classes") or css_indicators.get("has_display_none"):
            print("   ⚠️  Hidden content detected!")
    
    # Save detailed results
    with open('logs/table_structure_analysis.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\n💾 Detailed analysis saved to: logs/table_structure_analysis.json")
    
    # Summary
    print("\n" + "=" * 80)
    print("📋 SUMMARY")
    print("=" * 80)
    
    successful_analyses = [r for r in all_results.values() if "error" not in r]
    if successful_analyses:
        total_tables = sum(r.get("total_tables", 0) for r in successful_analyses)
        results_tables = sum(len(r.get("table_analysis", {})) for r in successful_analyses)
        
        print(f"✅ Successfully analyzed {len(successful_analyses)} URLs")
        print(f"📊 Found {total_tables} total tables")
        print(f"🏁 Found {results_tables} results tables")
        
        # Check for common issues
        issues_found = []
        for result in successful_analyses:
            for table_analysis in result.get("table_analysis", {}).values():
                issues_found.extend(table_analysis.get("potential_issues", []))
        
        if issues_found:
            print(f"⚠️  Common issues: {', '.join(set(issues_found))}")
        
        # Check for dynamic content
        dynamic_content = any(
            r.get("javascript_indicators", {}).get("has_ajax") or 
            r.get("javascript_indicators", {}).get("has_fetch")
            for r in successful_analyses
        )
        
        if dynamic_content:
            print("⚠️  Dynamic content detected - may need JavaScript rendering")

if __name__ == "__main__":
    asyncio.run(main()) 