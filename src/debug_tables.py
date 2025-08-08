#!/usr/bin/env python3

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin

async def analyze_gc_page():
    """Analyze the tables on the 2016 TDF GC page"""
    
    url = "https://www.procyclingstats.com/race/tour-de-france/2016/gc"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            html_content = await response.text()
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all tables
        all_tables = soup.find_all('table')
        print(f"Found {len(all_tables)} total tables")
        
        # Find tables with class='results'
        results_tables = soup.find_all('table', class_='results')
        print(f"Found {len(results_tables)} tables with class='results'")
        
        for i, table in enumerate(results_tables):
            print(f"\nTable {i+1}:")
            
            # Find table headers
            headers = table.find('thead')
            if headers:
                header_cells = headers.find_all(['th', 'td'])
                header_text = [cell.get_text(strip=True) for cell in header_cells]
                print(f"  Headers: {header_text}")
            else:
                # Try finding headers in first row
                first_row = table.find('tr')
                if first_row:
                    header_cells = first_row.find_all(['th', 'td'])
                    header_text = [cell.get_text(strip=True) for cell in header_cells]
                    print(f"  Headers (from first row): {header_text}")
            
            # Get first few data rows
            rows = table.find_all('tr')[1:6]  # Skip header, get first 5 data rows
            print(f"  First 3 data rows:")
            for j, row in enumerate(rows[:3]):
                cells = row.find_all(['td', 'th'])
                cell_text = [cell.get_text(strip=True)[:20] for cell in cells]  # Truncate long text
                print(f"    Row {j+1}: {cell_text}")

if __name__ == "__main__":
    asyncio.run(analyze_gc_page())