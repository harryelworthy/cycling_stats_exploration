#!/usr/bin/env python3
"""
Rider profile scraper for ProCyclingStats
Extracts detailed rider information from individual rider pages
"""

import asyncio
import aiohttp
import aiosqlite
import logging
import re
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, date
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

logger = logging.getLogger(__name__)

class RiderProfileScraper:
    """Scraper for detailed rider profile information"""
    
    def __init__(self, session: aiohttp.ClientSession, database_path: str):
        self.session = session
        self.database_path = database_path
        self.base_url = "https://www.procyclingstats.com"
        
    async def init_rider_tables(self):
        """Initialize rider-related database tables"""
        async with aiosqlite.connect(self.database_path) as db:
            # Create riders table for basic profile info
            await db.execute('''
                CREATE TABLE IF NOT EXISTS riders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rider_name TEXT NOT NULL,
                    rider_url TEXT UNIQUE NOT NULL,
                    date_of_birth TEXT,
                    nationality TEXT,
                    weight_kg INTEGER,
                    height_cm INTEGER,
                    place_of_birth TEXT,
                    uci_ranking INTEGER,
                    pcs_ranking INTEGER,
                    profile_score_climber INTEGER,
                    profile_score_gc INTEGER,
                    profile_score_tt INTEGER,
                    profile_score_sprint INTEGER,
                    profile_score_oneday INTEGER,
                    profile_score_hills INTEGER,
                    total_wins INTEGER,
                    total_grand_tours INTEGER,
                    total_classics INTEGER,
                    active_years_start INTEGER,
                    active_years_end INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create rider teams table for team history
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rider_teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rider_url TEXT NOT NULL,
                    team_name TEXT NOT NULL,
                    year_start INTEGER NOT NULL,
                    year_end INTEGER NOT NULL,
                    team_level TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rider_url) REFERENCES riders (rider_url)
                )
            ''')
            
            # Create rider achievements table for major wins
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rider_achievements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rider_url TEXT NOT NULL,
                    achievement_type TEXT NOT NULL, -- 'gc', 'stage', 'oneday', 'championship'
                    race_name TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    count INTEGER DEFAULT 1,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rider_url) REFERENCES riders (rider_url)
                )
            ''')
            
            await db.commit()
            logger.info("Rider database tables initialized")

    async def get_riders_missing_profiles(self, years: Optional[List[int]] = None) -> List[Dict[str, str]]:
        """Get list of riders from results who don't have profile data yet"""
        async with aiosqlite.connect(self.database_path) as db:
            # Build query to find riders in results but not in riders table
            if years:
                year_placeholders = ','.join('?' for _ in years)
                query = f'''
                    SELECT DISTINCT res.rider_name, res.rider_url
                    FROM results res
                    JOIN stages s ON res.stage_id = s.id
                    JOIN races r ON s.race_id = r.id
                    WHERE res.rider_url IS NOT NULL 
                    AND res.rider_url != ''
                    AND r.year IN ({year_placeholders})
                    AND res.rider_url NOT IN (SELECT rider_url FROM riders)
                    ORDER BY res.rider_name
                '''
                cursor = await db.execute(query, years)
            else:
                query = '''
                    SELECT DISTINCT res.rider_name, res.rider_url
                    FROM results res
                    WHERE res.rider_url IS NOT NULL 
                    AND res.rider_url != ''
                    AND res.rider_url NOT IN (SELECT rider_url FROM riders)
                    ORDER BY res.rider_name
                '''
                cursor = await db.execute(query)
            
            rows = await cursor.fetchall()
            return [{'rider_name': row[0], 'rider_url': row[1]} for row in rows]

    async def scrape_rider_profile(self, rider_url: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed profile information for a single rider"""
        if not rider_url.startswith('http'):
            # Handle relative URLs
            if rider_url.startswith('/'):
                full_url = f"{self.base_url}{rider_url}"
            elif rider_url.startswith('rider/'):
                # URL already has rider/ prefix
                full_url = f"{self.base_url}/{rider_url}"
            else:
                full_url = f"{self.base_url}/rider/{rider_url}"
        else:
            full_url = rider_url
            
        try:
            async with self.session.get(full_url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch rider profile {full_url}: {response.status}")
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                return await self._parse_rider_profile(soup, rider_url)
                
        except Exception as e:
            logger.error(f"Error scraping rider profile {full_url}: {e}")
            return None

    async def _parse_rider_profile(self, soup: BeautifulSoup, rider_url: str) -> Dict[str, Any]:
        """Parse rider profile information from HTML"""
        profile_data = {
            'rider_url': rider_url,
            'rider_name': None,
            'date_of_birth': None,
            'nationality': None,
            'weight_kg': None,
            'height_cm': None,
            'place_of_birth': None,
            'uci_ranking': None,
            'pcs_ranking': None,
            'profile_scores': {},
            'total_wins': None,
            'total_grand_tours': None,
            'total_classics': None,
            'team_history': [],
            'achievements': [],
            'active_years': None
        }
        
        try:
            # Extract rider name from page title or header
            title = soup.find('title')
            if title:
                # Extract name from title like "Tadej Pogačar » Rider profile | ProCyclingStats"
                title_text = title.text.strip()
                if '»' in title_text:
                    profile_data['rider_name'] = title_text.split('»')[0].strip()
            
            # Alternative: get name from h1 header
            if not profile_data['rider_name']:
                h1 = soup.find('h1')
                if h1:
                    profile_data['rider_name'] = h1.text.strip()
            
            # Extract basic info from ul.list elements
            await self._parse_basic_info(soup, profile_data)
            
            # Extract profile scores/specialties from ul.pps.list
            await self._parse_specialties(soup, profile_data)
            
            # Note: Rankings, career stats, team history, and achievements parsing 
            # have been removed as requested - achievements can be derived from race data
            
        except Exception as e:
            logger.error(f"Error parsing rider profile: {e}")
            
        return profile_data

    async def _parse_basic_info(self, soup, profile_data):
        """Parse basic rider information like DOB, nationality, etc."""
        
        # Find all ul.list elements that contain the structured data
        list_elements = soup.find_all('ul', class_='list')
        
        for ul_element in list_elements:
            li_elements = ul_element.find_all('li')
            
            for li in li_elements:
                text = li.get_text(strip=True)
                
                # Parse Date of birth
                if 'Date of birth:' in text:
                    # Extract day, month, year from the div elements
                    divs = li.find_all('div')
                    try:
                        day = None
                        month = None
                        year = None
                        
                        for i, div in enumerate(divs):
                            div_text = div.get_text(strip=True)
                            
                            # Look for day (like "25th")
                            if div_text.endswith(('st', 'nd', 'rd', 'th')) and div_text[:-2].isdigit():
                                day = int(div_text[:-2])
                            
                            # Look for month name
                            month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                                          'July', 'August', 'September', 'October', 'November', 'December']
                            if div_text in month_names:
                                month = month_names.index(div_text) + 1
                            
                            # Look for year (4 digits)
                            if div_text.isdigit() and len(div_text) == 4:
                                year = int(div_text)
                        
                        if day and month and year:
                            profile_data['date_of_birth'] = f"{year}-{month:02d}-{day:02d}"
                    except Exception as e:
                        logger.debug(f"Error parsing date of birth: {e}")
                
                # Parse Nationality
                elif 'Nationality:' in text:
                    # Look for country link
                    country_link = li.find('a')
                    if country_link:
                        profile_data['nationality'] = country_link.get_text(strip=True)
                
                # Parse Weight and Height (they're in the same li)
                elif 'Weight:' in text and 'Height:' in text:
                    divs = li.find_all('div')
                    try:
                        for i, div in enumerate(divs):
                            div_text = div.get_text(strip=True)
                            
                            # Look for weight (number before "kg")
                            if div_text.isdigit() and i + 1 < len(divs):
                                next_div = divs[i + 1].get_text(strip=True)
                                if next_div == 'kg':
                                    profile_data['weight_kg'] = int(div_text)
                            
                            # Look for height (decimal number before "m")
                            if div_text.replace('.', '').isdigit() and '.' in div_text and i + 1 < len(divs):
                                next_div = divs[i + 1].get_text(strip=True)
                                if next_div == 'm':
                                    height_m = float(div_text)
                                    profile_data['height_cm'] = int(height_m * 100)
                    except Exception as e:
                        logger.debug(f"Error parsing weight/height: {e}")

    async def _parse_specialties(self, soup, profile_data):
        """Parse rider specialty scores from ul.pps.list structure"""
        
        # Find the ul element with classes 'pps' and 'list'
        pps_list = soup.find('ul', class_=['pps', 'list'])
        
        if pps_list:
            li_elements = pps_list.find_all('li')
            
            for li in li_elements:
                try:
                    # Find the score value in xvalue div
                    xvalue_div = li.find('div', class_='xvalue')
                    # Find the specialty name in xtitle div
                    xtitle_div = li.find('div', class_='xtitle')
                    
                    if xvalue_div and xtitle_div:
                        score = int(xvalue_div.get_text(strip=True))
                        
                        # Extract specialty name from the link
                        specialty_link = xtitle_div.find('a')
                        if specialty_link:
                            specialty_text = specialty_link.get_text(strip=True).lower()
                            
                            # Map the specialty names to our standard names
                            specialty_mapping = {
                                'onedayraces': 'oneday',
                                'gc': 'gc', 
                                'tt': 'tt',
                                'sprint': 'sprint',
                                'climber': 'climber',
                                'hills': 'hills'
                            }
                            
                            # Find matching specialty
                            for pcs_name, our_name in specialty_mapping.items():
                                if pcs_name in specialty_text:
                                    profile_data['profile_scores'][our_name] = score
                                    break
                            
                except Exception as e:
                    logger.debug(f"Error parsing specialty score: {e}")
        
        # Also try alternative parsing if the structure is different
        if not profile_data['profile_scores']:
            await self._parse_specialties_fallback(soup, profile_data)
    
    async def _parse_specialties_fallback(self, soup, profile_data):
        """Fallback method for parsing specialties using text patterns"""
        page_text = soup.get_text()
        
        patterns = {
            'climber': re.compile(r'(\d+).*?Climber', re.I),
            'gc': re.compile(r'(\d+).*?GC', re.I),
            'tt': re.compile(r'(\d+).*?TT', re.I),
            'sprint': re.compile(r'(\d+).*?Sprint', re.I),
            'oneday': re.compile(r'(\d+).*?Onedayraces', re.I),
            'hills': re.compile(r'(\d+).*?Hills', re.I)
        }
        
        for specialty, pattern in patterns.items():
            match = pattern.search(page_text)
            if match:
                profile_data['profile_scores'][specialty] = int(match.group(1))

    async def _parse_rankings(self, soup, profile_data):
        """Parse current UCI and PCS rankings"""
        # Look for ranking information
        ranking_text = soup.text
        
        # UCI World ranking
        uci_pattern = re.compile(r'UCI World.*?(\d+)', re.I)
        uci_match = uci_pattern.search(ranking_text)
        if uci_match:
            profile_data['uci_ranking'] = int(uci_match.group(1))
        
        # PCS ranking
        pcs_pattern = re.compile(r'PCS Ranking.*?(\d+)', re.I)
        pcs_match = pcs_pattern.search(ranking_text)
        if pcs_match:
            profile_data['pcs_ranking'] = int(pcs_match.group(1))

    async def _parse_career_stats(self, soup, profile_data):
        """Parse career statistics like total wins, grand tours, etc."""
        stats_text = soup.text
        
        # Total wins
        wins_pattern = re.compile(r'(\d+).*?Wins', re.I)
        wins_match = wins_pattern.search(stats_text)
        if wins_match:
            profile_data['total_wins'] = int(wins_match.group(1))
        
        # Grand tours
        gt_pattern = re.compile(r'(\d+).*?Grand tours', re.I)
        gt_match = gt_pattern.search(stats_text)
        if gt_match:
            profile_data['total_grand_tours'] = int(gt_match.group(1))
        
        # Classics
        classics_pattern = re.compile(r'(\d+).*?Classics', re.I)
        classics_match = classics_pattern.search(stats_text)
        if classics_match:
            profile_data['total_classics'] = int(classics_match.group(1))

    async def _parse_team_history(self, soup, profile_data):
        """Parse rider's team history"""
        # Look for team history section
        teams_section = soup.find('div', text=re.compile(r'Teams', re.I))
        if teams_section:
            teams_parent = teams_section.parent
            # Look for years and team names
            team_lines = teams_parent.find_all('div') or teams_parent.find_all('li')
            
            for line in team_lines:
                line_text = line.text.strip()
                # Match patterns like "2026-2030 UAE Team Emirates - XRG (WT)"
                team_pattern = re.compile(r'(\d{4})(?:-(\d{4}))?\s+(.+?)\s*(?:\(.*\))?$')
                match = team_pattern.match(line_text)
                if match:
                    start_year = int(match.group(1))
                    end_year = int(match.group(2)) if match.group(2) else start_year
                    team_name = match.group(3).strip()
                    
                    profile_data['team_history'].append({
                        'team_name': team_name,
                        'year_start': start_year,
                        'year_end': end_year,
                        'team_level': None  # Could be extracted from parentheses
                    })

    async def _parse_achievements(self, soup, profile_data):
        """Parse major achievements/wins"""
        # Look for top results section
        results_section = soup.find('div', text=re.compile(r'Top results', re.I))
        if results_section:
            results_parent = results_section.parent
            # Parse achievement lines
            achievement_lines = results_parent.find_all('li') or results_parent.find_all('div')
            
            for line in achievement_lines:
                line_text = line.text.strip()
                # Match patterns like "3x GC Tour de France ('24, '21, '20)"
                achievement_pattern = re.compile(r'(\d+)x\s+(.+?)\s+(.+?)\s*\((.+?)\)', re.I)
                match = achievement_pattern.match(line_text)
                if match:
                    count = int(match.group(1))
                    achievement_type = match.group(2).strip()
                    race_name = match.group(3).strip()
                    years_str = match.group(4).strip()
                    
                    # Parse years like "'24, '21, '20"
                    years = []
                    for year_str in years_str.split(','):
                        year_str = year_str.strip().replace("'", "")
                        if year_str.isdigit():
                            year = int(year_str)
                            if year < 50:  # Assume 2000s
                                year += 2000
                            elif year < 100:  # Assume 1900s
                                year += 1900
                            years.append(year)
                    
                    profile_data['achievements'].append({
                        'achievement_type': achievement_type.lower(),
                        'race_name': race_name,
                        'count': count,
                        'years': years,
                        'description': line_text
                    })

    async def save_rider_profile(self, profile_data: Dict[str, Any]):
        """Save rider profile data to database"""
        async with aiosqlite.connect(self.database_path) as db:
            try:
                # Insert/update main rider record
                await db.execute('''
                    INSERT OR REPLACE INTO riders (
                        rider_name, rider_url, date_of_birth, nationality,
                        weight_kg, height_cm, place_of_birth, uci_ranking, pcs_ranking,
                        profile_score_climber, profile_score_gc, profile_score_tt,
                        profile_score_sprint, profile_score_oneday, profile_score_hills,
                        total_wins, total_grand_tours, total_classics,
                        active_years_start, active_years_end, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    profile_data['rider_name'],
                    profile_data['rider_url'],
                    profile_data['date_of_birth'],
                    profile_data['nationality'],
                    profile_data['weight_kg'],
                    profile_data['height_cm'],
                    profile_data['place_of_birth'],
                    profile_data['uci_ranking'],
                    profile_data['pcs_ranking'],
                    profile_data['profile_scores'].get('climber'),
                    profile_data['profile_scores'].get('gc'),
                    profile_data['profile_scores'].get('tt'),
                    profile_data['profile_scores'].get('sprint'),
                    profile_data['profile_scores'].get('oneday'),
                    profile_data['profile_scores'].get('hills'),
                    profile_data['total_wins'],
                    profile_data['total_grand_tours'],
                    profile_data['total_classics'],
                    min(team['year_start'] for team in profile_data['team_history']) if profile_data['team_history'] else None,
                    max(team['year_end'] for team in profile_data['team_history']) if profile_data['team_history'] else None,
                    datetime.now().isoformat()
                ))
                
                # Clear existing team history and achievements for this rider
                await db.execute('DELETE FROM rider_teams WHERE rider_url = ?', (profile_data['rider_url'],))
                await db.execute('DELETE FROM rider_achievements WHERE rider_url = ?', (profile_data['rider_url'],))
                
                # Insert team history
                for team in profile_data['team_history']:
                    await db.execute('''
                        INSERT INTO rider_teams (rider_url, team_name, year_start, year_end, team_level)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        profile_data['rider_url'],
                        team['team_name'],
                        team['year_start'],
                        team['year_end'],
                        team['team_level']
                    ))
                
                # Insert achievements
                for achievement in profile_data['achievements']:
                    for year in achievement['years']:
                        await db.execute('''
                            INSERT INTO rider_achievements (
                                rider_url, achievement_type, race_name, year, count, description
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            profile_data['rider_url'],
                            achievement['achievement_type'],
                            achievement['race_name'],
                            year,
                            achievement['count'],
                            achievement['description']
                        ))
                
                await db.commit()
                logger.debug(f"Saved rider profile: {profile_data['rider_name']}")
                
            except Exception as e:
                logger.error(f"Error saving rider profile {profile_data['rider_url']}: {e}")

    async def scrape_riders_batch(self, riders: List[Dict[str, str]], max_concurrent: int = 5) -> Dict[str, int]:
        """Scrape rider profiles in batches with concurrency control"""
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        async def scrape_single_rider(rider_info):
            async with semaphore:
                try:
                    # Add delay between requests
                    await asyncio.sleep(0.2)
                    
                    profile_data = await self.scrape_rider_profile(rider_info['rider_url'])
                    if profile_data and profile_data.get('rider_name'):
                        await self.save_rider_profile(profile_data)
                        results['success'] += 1
                        logger.info(f"✅ Scraped profile: {profile_data['rider_name']}")
                    else:
                        results['failed'] += 1
                        logger.warning(f"❌ Failed to scrape: {rider_info['rider_name']}")
                        
                except Exception as e:
                    results['failed'] += 1
                    logger.error(f"💥 Error scraping {rider_info['rider_name']}: {e}")
        
        # Process riders in batches
        batch_size = 20
        for i in range(0, len(riders), batch_size):
            batch = riders[i:i + batch_size]
            logger.info(f"Processing rider batch {i//batch_size + 1}/{(len(riders) + batch_size - 1)//batch_size} ({len(batch)} riders)")
            
            tasks = [scrape_single_rider(rider) for rider in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Small delay between batches
            await asyncio.sleep(1)
        
        return results

    async def update_rider_data_for_years(self, years: List[int], max_concurrent: int = 5) -> Dict[str, int]:
        """Update rider profile data for all riders who competed in specified years"""
        logger.info(f"🔄 Updating rider data for years: {years}")
        
        # Initialize database tables
        await self.init_rider_tables()
        
        # Get riders missing profiles from specified years
        missing_riders = await self.get_riders_missing_profiles(years)
        
        if not missing_riders:
            logger.info("✅ All riders already have profile data")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"📊 Found {len(missing_riders)} riders missing profile data")
        
        # Scrape missing rider profiles
        results = await self.scrape_riders_batch(missing_riders, max_concurrent)
        
        logger.info(f"🎉 Rider data update completed:")
        logger.info(f"   ✅ Success: {results['success']}")
        logger.info(f"   ❌ Failed: {results['failed']}")
        logger.info(f"   ⏭️  Skipped: {results['skipped']}")
        
        return results

    async def scrape_all_missing_riders(self, max_concurrent: int = 5) -> Dict[str, int]:
        """Scrape profile data for all riders missing from the database"""
        logger.info("🔄 Scraping all missing rider profiles")
        
        # Initialize database tables
        await self.init_rider_tables()
        
        # Get all riders missing profiles
        missing_riders = await self.get_riders_missing_profiles()
        
        if not missing_riders:
            logger.info("✅ All riders already have profile data")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"📊 Found {len(missing_riders)} riders missing profile data")
        
        # Scrape missing rider profiles
        results = await self.scrape_riders_batch(missing_riders, max_concurrent)
        
        logger.info(f"🎉 All missing rider profiles processed:")
        logger.info(f"   ✅ Success: {results['success']}")
        logger.info(f"   ❌ Failed: {results['failed']}")
        logger.info(f"   ⏭️  Skipped: {results['skipped']}")
        
        return results 