#!/usr/bin/env python3
"""
JavaScript-aware scraper for cycling data from procyclingstats.com
This version uses Playwright to handle JavaScript-rendered content
"""

import asyncio
import aiosqlite
import logging
import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import json
import time
from pathlib import Path

# Playwright imports
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available. Install with: pip install playwright && playwright install")

# Import existing modules
from enhanced_error_logger import enhanced_logger
from historical_data_handler import HistoricalDataHandler
from rider_scraper import RiderProfileScraper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class JSScrapingConfig:
    """Configuration for the JavaScript-aware scraper"""
    max_concurrent_requests: int = 10  # Lower for browser instances
    request_delay: float = 1.0  # Longer delay for browser requests
    max_retries: int = 3
    timeout: int = 30
    database_path: str = "data/cycling_data.db"
    headless: bool = True  # Run browser in headless mode
    wait_for_table_timeout: int = 10  # Seconds to wait for table to load
    enable_screenshots: bool = False  # Save screenshots for debugging
    
@dataclass
class JSScrapingStats:
    """Track scraping statistics"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    start_time: float = field(default_factory=time.time)
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests * 100
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time

class JavaScriptAwareScraper:
    """JavaScript-aware scraper for cycling data from procyclingstats.com"""
    
    def __init__(self, config: JSScrapingConfig = None):
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is required for JavaScript-aware scraping. Install with: pip install playwright && playwright install")
        
        self.config = config or JSScrapingConfig()
        self.stats = JSScrapingStats()
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self.rider_scraper: Optional[RiderProfileScraper] = None
        
        # Progress tracking attributes
        self.progress_tracker = None
        self.checkpoint_interval = 300  # 5 minutes
        self.last_checkpoint = time.time()
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.playwright = await async_playwright().start()
        
        # Launch browser
        self.browser = await self.playwright.chromium.launch(
            headless=self.config.headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        await self.init_database()
        
        # Initialize rider scraper (we'll need to pass a session later)
        # self.rider_scraper = RiderProfileScraper(None, self.config.database_path)
        # await self.rider_scraper.init_rider_tables()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def init_database(self):
        """Initialize SQLite database with required tables"""
        async with aiosqlite.connect(self.config.database_path) as db:
            # Create races table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS races (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    race_name TEXT NOT NULL,
                    race_category TEXT,
                    uci_tour TEXT,
                    stage_url TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create stages table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS stages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id INTEGER,
                    stage_url TEXT UNIQUE NOT NULL,
                    is_one_day_race BOOLEAN,
                    distance REAL,
                    stage_type TEXT,
                    winning_attack_length REAL,
                    date TEXT,
                    won_how TEXT,
                    avg_speed_winner REAL,
                    avg_temperature REAL,
                    vertical_meters INTEGER,
                    profile_icon TEXT,
                    profile_score INTEGER,
                    race_startlist_quality_score INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (race_id) REFERENCES races (id)
                )
            ''')
            
            # Create results table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stage_id INTEGER,
                    rider_name TEXT,
                    rider_url TEXT,
                    team_name TEXT,
                    team_url TEXT,
                    rank INTEGER,
                    status TEXT,
                    time TEXT,
                    uci_points INTEGER,
                    pcs_points INTEGER,
                    age INTEGER,
                    gc_rank INTEGER,
                    gc_uci_points INTEGER,
                    points_rank INTEGER,
                    points_uci_points INTEGER,
                    kom_rank INTEGER,
                    kom_uci_points INTEGER,
                    youth_rank INTEGER,
                    youth_uci_points INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (stage_id) REFERENCES stages (id)
                )
            ''')
            
            await db.commit()
            logger.info(f"Database initialized at {self.config.database_path}")
    
    async def create_page(self) -> Page:
        """Create a new browser page with appropriate settings"""
        page = await self.browser.new_page()
        
        # Set user agent
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Set viewport
        await page.set_viewport_size({"width": 1920, "height": 1080})
        
        return page
    
    async def wait_for_table_content(self, page: Page) -> bool:
        """Wait for table content to load dynamically"""
        try:
            # Wait for table to appear
            await page.wait_for_selector('table.results', timeout=self.config.wait_for_table_timeout * 1000)
            
            # Wait for table to have rows (not just header)
            await page.wait_for_function('''
                () => {
                    const table = document.querySelector('table.results');
                    if (!table) return false;
                    const rows = table.querySelectorAll('tr');
                    return rows.length > 1; // More than just header
                }
            ''', timeout=self.config.wait_for_table_timeout * 1000)
            
            return True
            
        except Exception as e:
            logger.warning(f"Timeout waiting for table content: {e}")
            return False
    
    async def make_request(self, url: str, max_retries: int = None) -> Optional[str]:
        """Make a JavaScript-aware HTTP request"""
        max_retries = max_retries or self.config.max_retries
        
        async with self.semaphore:
            for attempt in range(max_retries + 1):
                try:
                    self.stats.total_requests += 1
                    
                    # Add delay for rate limiting
                    if self.config.request_delay > 0:
                        await asyncio.sleep(self.config.request_delay)
                    
                    # Create a new page for this request
                    page = await self.create_page()
                    
                    try:
                        # Navigate to the URL
                        await page.goto(url, wait_until='networkidle', timeout=self.config.timeout * 1000)
                        
                        # Wait for table content to load
                        table_loaded = await self.wait_for_table_content(page)
                        
                        if table_loaded:
                            # Get the HTML content after JavaScript has loaded
                            html_content = await page.content()
                            self.stats.successful_requests += 1
                            
                            # Save screenshot for debugging if enabled
                            if self.config.enable_screenshots:
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                screenshot_path = f"logs/screenshot_{timestamp}.png"
                                await page.screenshot(path=screenshot_path)
                                logger.debug(f"Screenshot saved: {screenshot_path}")
                            
                            return html_content
                        else:
                            logger.warning(f"Table content not loaded for {url}")
                            
                    finally:
                        await page.close()
                        
                except Exception as e:
                    logger.warning(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                    
                    if attempt < max_retries:
                        delay = self.config.retry_delay * (2 ** attempt)  # Exponential backoff
                        await asyncio.sleep(delay)
                    else:
                        self.stats.failed_requests += 1
                        logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts")
                        return None
    
    def clean_race_name(self, race_name: str) -> str:
        """Clean and standardize race names by removing edition numbers and year prefixes"""
        # Remove year prefixes like "2025 " or "2025  »  "
        race_name = re.sub(r'^\d{4}\s*»?\s*', '', race_name)
        
        # Remove edition numbers like "80th " or "65th "
        race_name = re.sub(r'^\d+(?:st|nd|rd|th)\s+', '', race_name)
        
        return race_name.strip()
    
    async def get_races(self, year: int) -> List[str]:
        """Get list of race URLs for a given year"""
        url = f"https://www.procyclingstats.com/races.php?year={year}"
        
        html_content = await self.make_request(url)
        if not html_content:
            return []
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            race_links = []
            
            # Find race links in the main content area
            content_div = soup.find('div', class_='content')
            if content_div:
                links = content_div.find_all('a', href=True)
                
                for link in links:
                    href = link['href']
                    # Look for race URLs (they typically start with 'race/')
                    if href.startswith('race/') and '/result' in href:
                        race_links.append(href)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_races = []
            for race in race_links:
                if race not in seen:
                    seen.add(race)
                    unique_races.append(race)
            
            logger.info(f"Found {len(unique_races)} races for {year}")
            return unique_races
            
        except Exception as e:
            logger.error(f"Error parsing races for {year}: {e}")
            return []
    
    async def get_race_info(self, race_url: str) -> Optional[Dict[str, Any]]:
        """Get race information from race page"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, race_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract race name from h1
            h1 = soup.find('h1')
            race_name = h1.get_text() if h1 else "Unknown Race"
            race_name = self.clean_race_name(race_name)
            
            # Extract race category from h1 (usually in parentheses)
            race_category = None
            if h1:
                category_match = re.search(r'\(([^)]+)\)', h1.get_text())
                if category_match:
                    race_category = category_match.group(1)
            
            # Determine if it's a UCI World Tour race
            uci_tour = None
            if race_category and 'UWT' in race_category:
                uci_tour = 'World Tour'
            elif race_category and 'Pro' in race_category:
                uci_tour = 'Pro Series'
            
            # Get stage URLs
            stage_urls = []
            content_div = soup.find('div', class_='content')
            if content_div:
                links = content_div.find_all('a', href=True)
                
                for link in links:
                    href = link['href']
                    # Look for stage result URLs
                    if href.startswith('race/') and '/result' in href:
                        stage_urls.append(href)
            
            # Remove duplicates
            stage_urls = list(set(stage_urls))
            
            return {
                'race_name': race_name,
                'race_category': race_category,
                'uci_tour': uci_tour,
                'stage_urls': stage_urls
            }
            
        except Exception as e:
            logger.error(f"Error parsing race info for {race_url}: {e}")
            return None
    
    def parse_results_table(self, table, secondary=False) -> List[Dict[str, Any]]:
        """Parse a results table from HTML"""
        results = []
        
        try:
            rows = table.find_all('tr')
            
            for row in rows:
                # Skip header rows
                if row.get('class') and any('header' in cls.lower() for cls in row.get('class', [])):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue
                
                result = {}
                
                # Extract rider name and URL
                rider_link = row.find('a', href=lambda x: x and ('rider/' in x or '/rider/' in x))
                if rider_link:
                    raw_name = rider_link.get_text(strip=True)
                    result['rider_name'] = self.format_rider_name(raw_name)
                    result['rider_url'] = rider_link['href']
                
                # Extract team name and URL
                team_link = row.find('a', href=lambda x: x and ('team/' in x or '/team/' in x))
                if team_link:
                    result['team_name'] = team_link.get_text(strip=True)
                    result['team_url'] = team_link['href']
                
                # Extract rank (usually first column)
                if cells:
                    rank_text = cells[0].get_text(strip=True)
                    try:
                        result['rank'] = int(rank_text) if rank_text.isdigit() else None
                    except:
                        result['rank'] = None
                
                # Extract other data based on column headers
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    cell_classes = cell.get('class', [])
                    
                    # Time column (look for time format with colons)
                    if ':' in text and ('.' in text or text.count(':') >= 2):
                        time_parts = text.split(':')
                        if len(time_parts) >= 2:
                            if len(time_parts[0]) <= 2 and time_parts[0].isdigit():
                                result['time'] = text.split(':')[0] + ':' + text.split(':')[1]
                            else:
                                result['time'] = text
                    
                    # UCI Points column
                    elif 'uci_pnt' in cell_classes and text.isdigit() and int(text) > 0:
                        result['uci_points'] = int(text)
                    
                    # PCS Points column
                    elif 'pnt' in cell_classes and text.isdigit() and int(text) > 0:
                        result['pcs_points'] = int(text)
                    
                    # Points columns - enhanced detection
                    elif text.isdigit() and int(text) > 0:
                        if int(text) != result.get('rank'):
                            if not secondary:
                                if 'pcs_points' not in result and 10 <= int(text) <= 500:
                                    result['pcs_points'] = int(text)
                                elif 'uci_points' not in result and int(text) > 500:
                                    result['uci_points'] = int(text)
                            else:
                                result['uci_points'] = int(text)
                    
                    # Age column
                    elif text.isdigit() and 18 <= int(text) <= 50:
                        result['age'] = int(text)
                    
                    # Status indicators
                    elif text.upper() in ['DNF', 'DNS', 'DSQ', 'OTL']:
                        result['status'] = text.upper()
                
                # Set default values
                result.setdefault('status', 'FINISHED')
                result.setdefault('uci_points', 0)
                result.setdefault('pcs_points', 0)
                
                if result.get('rider_name'):
                    results.append(result)
                    
        except Exception as e:
            logger.error(f"Error parsing results table: {e}")
        
        return results
    
    async def get_stage_info(self, stage_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed stage information and results with JavaScript support"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, stage_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            stage_info = {
                'stage_url': stage_url,
                'is_one_day_race': '/result' in stage_url and '/stage-' not in stage_url,
                'distance': None,
                'stage_type': None,
                'winning_attack_length': None,
                'date': None,
                'won_how': None,
                'avg_speed_winner': None,
                'avg_temperature': None,
                'vertical_meters': None,
                'profile_icon': None,
                'profile_score': None,
                'race_startlist_quality_score': None,
                'results': [],
                'gc': [],
                'points': [],
                'kom': [],
                'youth': []
            }
            
            # Extract stage details from keyvalueList
            keyvalue_list = soup.find('ul', class_='keyvalueList')
            if keyvalue_list:
                for li in keyvalue_list.find_all('li'):
                    title_div = li.find('div', class_='title')
                    value_div = li.find('div', class_='value')
                    
                    if title_div and value_div:
                        title = title_div.get_text(strip=True).lower()
                        value = value_div.get_text(strip=True)
                        
                        # Extract various stage details
                        if 'distance' in title:
                            try:
                                distance_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if distance_str:
                                    stage_info['distance'] = float(distance_str)
                            except:
                                pass
                        elif 'won how' in title:
                            stage_info['won_how'] = value
                        elif 'avg. speed winner' in title:
                            try:
                                speed_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if speed_str:
                                    stage_info['avg_speed_winner'] = float(speed_str)
                            except:
                                pass
                        elif 'vertical meters' in title:
                            try:
                                stage_info['vertical_meters'] = int(value)
                            except:
                                pass
                        elif 'profilescore' in title:
                            try:
                                stage_info['profile_score'] = int(value)
                            except:
                                pass
                        elif 'startlist quality score' in title:
                            try:
                                stage_info['race_startlist_quality_score'] = int(value)
                            except:
                                pass
                        elif 'date' in title:
                            stage_info['date'] = value
                        elif 'avg. temperature' in title:
                            try:
                                temp_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if temp_str:
                                    stage_info['avg_temperature'] = float(temp_str)
                            except:
                                pass
            
            # Enhanced results table extraction
            is_gc_page = stage_url.endswith('/gc')
            
            if is_gc_page:
                # For GC pages, look for the GC classification table first
                gc_tables = soup.find_all('table', class_='results')
                gc_results = []
                
                for table in gc_tables:
                    table_html = str(table)
                    if 'time ar' in table_html and 'time_wonlost' in table_html:
                        gc_results = self.parse_results_table(table)
                        break
                
                if gc_results:
                    stage_info['results'] = gc_results
                    # Extract other classifications
                    for classification in ['points', 'kom', 'youth']:
                        class_table = soup.find('table', {'id': f'{classification}table'})
                        if class_table:
                            stage_info[classification] = self.parse_results_table(class_table, secondary=True)
                else:
                    # Fallback to main results table
                    results_table = soup.find('table', class_='results')
                    if results_table:
                        stage_info['results'] = self.parse_results_table(results_table)
            else:
                # For regular stage pages, use enhanced parsing
                results_table = soup.find('table', class_='results')
                if results_table:
                    stage_info['results'] = self.parse_results_table(results_table)
                else:
                    # Log missing results table
                    enhanced_logger.log_scraping_error(
                        stage="get_stage_info",
                        url=full_url,
                        error=ValueError("No results table found"),
                        html_content=html_content,
                        expected_elements=['table.results'],
                        context={'stage_url': stage_url}
                    )
                
                # Extract secondary classifications
                for classification in ['gc', 'points', 'kom', 'youth']:
                    class_table = soup.find('table', {'id': f'{classification}table'})
                    if class_table:
                        stage_info[classification] = self.parse_results_table(class_table, secondary=True)
            
            # Extract year for historical context
            year = None
            try:
                year = int(stage_url.split('/')[2])
            except:
                pass
            
            # Validate results with more lenient criteria
            if not stage_info['results']:
                # For very early years, missing results might be expected
                if year and year < 1920:
                    logger.warning(f"No results found for {stage_url} - may be expected for {year}")
                else:
                    enhanced_logger.log_scraping_error(
                        stage="get_stage_info",
                        url=full_url,
                        error=ValueError("No results extracted from stage"),
                        html_content=html_content,
                        expected_elements=['table.results', 'table tr', 'rider names'],
                        context={'stage_url': stage_url, 'is_one_day_race': stage_info['is_one_day_race'], 'year': year}
                    )
            
            return stage_info
            
        except Exception as e:
            enhanced_logger.log_scraping_error(
                stage="get_stage_info",
                url=full_url,
                error=e,
                html_content=html_content,
                expected_elements=['table.results', 'ul.infolist', 'table tr'],
                context={'stage_url': stage_url}
            )
            return None
    
    def format_rider_name(self, raw_name: str) -> str:
        """Convert 'LastFirst' style into 'First Last' if needed."""
        if not raw_name or len(raw_name) < 2:
            return raw_name
        if ' ' in raw_name:
            return raw_name
        import re as _re
        match = _re.search(r'([a-z])([A-Z])', raw_name)
        if match:
            split_pos = match.start() + 1
            last = raw_name[:split_pos]
            first = raw_name[split_pos:]
            return f"{first} {last}"
        return raw_name
    
    # Database methods (similar to enhanced scraper)
    async def save_race_data(self, year: int, race_data: Dict[str, Any]) -> Optional[int]:
        """Save race data to SQLite database"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                cursor = await db.execute('''
                    INSERT OR REPLACE INTO races (year, race_name, race_category, uci_tour, stage_url)
                    VALUES (?, ?, ?, ?, ?)
                ''', (year, race_data['race_name'], race_data['race_category'], 
                     race_data['uci_tour'], race_data['stage_urls'][0] if race_data['stage_urls'] else None))
                
                race_id = cursor.lastrowid
                await db.commit()
                return race_id
                
            except Exception as e:
                logger.error(f"Error saving race data: {e}")
                return None
    
    async def save_stage_data(self, race_id: int, stage_data: Dict[str, Any]) -> Optional[int]:
        """Save stage data to SQLite database without ID churn (no REPLACE)."""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Check existing by unique stage_url
                cur = await db.execute('SELECT id FROM stages WHERE stage_url = ?', (stage_data['stage_url'],))
                row = await cur.fetchone()
                if row:
                    stage_id = row[0]
                    # Update fields, keep same ID
                    await db.execute('''
                        UPDATE stages SET
                            race_id = ?, is_one_day_race = ?, distance = ?, stage_type = ?,
                            winning_attack_length = ?, date = ?, won_how = ?, avg_speed_winner = ?,
                            avg_temperature = ?, vertical_meters = ?, profile_icon = ?, profile_score = ?,
                            race_startlist_quality_score = ?
                        WHERE stage_url = ?
                    ''', (
                        race_id,
                        stage_data['is_one_day_race'],
                        stage_data['distance'],
                        stage_data['stage_type'],
                        stage_data['winning_attack_length'],
                        stage_data['date'],
                        stage_data['won_how'],
                        stage_data['avg_speed_winner'],
                        stage_data['avg_temperature'],
                        stage_data['vertical_meters'],
                        stage_data['profile_icon'],
                        stage_data['profile_score'],
                        stage_data['race_startlist_quality_score'],
                        stage_data['stage_url']
                    ))
                    await db.commit()
                    return stage_id
                else:
                    # Insert new
                    cursor = await db.execute('''
                        INSERT INTO stages 
                        (race_id, stage_url, is_one_day_race, distance, stage_type, 
                         winning_attack_length, date, won_how, avg_speed_winner, 
                         avg_temperature, vertical_meters, profile_icon, profile_score, 
                         race_startlist_quality_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (race_id, stage_data['stage_url'], stage_data['is_one_day_race'],
                         stage_data['distance'], stage_data['stage_type'], 
                         stage_data['winning_attack_length'], stage_data['date'],
                         stage_data['won_how'], stage_data['avg_speed_winner'],
                         stage_data['avg_temperature'], stage_data['vertical_meters'],
                         stage_data['profile_icon'], stage_data['profile_score'],
                         stage_data['race_startlist_quality_score']))
                    stage_id = cursor.lastrowid
                    await db.commit()
                    return stage_id
            except Exception as e:
                logger.error(f"Error saving stage data: {e}")
                return None
    
    async def save_results_data(self, stage_id: int, stage_data: Dict[str, Any]):
        """Save results data to SQLite database"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Save main results
                for result in stage_data['results']:
                    await db.execute('''
                        INSERT OR REPLACE INTO results 
                        (stage_id, rider_name, rider_url, team_name, team_url, rank, status, time,
                         uci_points, pcs_points, age, gc_rank, gc_uci_points, points_rank,
                         points_uci_points, kom_rank, kom_uci_points, youth_rank, youth_uci_points)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (stage_id, result.get('rider_name'), result.get('rider_url'),
                         result.get('team_name'), result.get('team_url'), result.get('rank'),
                         result.get('status'), result.get('time'), result.get('uci_points', 0),
                         result.get('pcs_points', 0), result.get('age'),
                         result.get('gc_rank'), result.get('gc_uci_points'),
                         result.get('points_rank'), result.get('points_uci_points'),
                         result.get('kom_rank'), result.get('kom_uci_points'),
                         result.get('youth_rank'), result.get('youth_uci_points')))
                
                await db.commit()
                
            except Exception as e:
                logger.error(f"Error saving results data: {e}")
    
    async def scrape_year(self, year: int):
        """Scrape all races for a given year"""
        logger.info(f"Scraping year {year}")
        
        races = await self.get_races(year)
        if not races:
            logger.warning(f"No races found for {year}")
            return
        
        for race_url in races:
            try:
                race_info = await self.get_race_info(race_url)
                if not race_info:
                    continue
                
                race_id = await self.save_race_data(year, race_info)
                if not race_id:
                    continue
                
                # Process each stage
                for stage_url in race_info['stage_urls']:
                    stage_info = await self.get_stage_info(stage_url)
                    if not stage_info:
                        continue
                    
                    stage_id = await self.save_stage_data(race_id, stage_info)
                    if stage_id:
                        await self.save_results_data(stage_id, stage_info)
                
            except Exception as e:
                logger.error(f"Error processing race {race_url}: {e}")
    
    async def scrape_years(self, years: List[int]):
        """Scrape multiple years"""
        for year in years:
            await self.scrape_year(year)
    
    async def scrape_years_with_progress(self, years: List[int]):
        """Scrape years with progress tracking"""
        for year in years:
            if self.progress_tracker:
                await self.progress_tracker.mark_year_started(year)
            
            try:
                await self.scrape_year(year)
                
                if self.progress_tracker:
                    await self.progress_tracker.mark_year_completed(year)
                    
            except Exception as e:
                logger.error(f"Error scraping year {year}: {e}")
                if self.progress_tracker:
                    await self.progress_tracker.mark_year_failed(year, str(e))

async def main():
    """Test the JavaScript-aware scraper"""
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright not available. Install with: pip install playwright && playwright install")
        return
    
    config = JSScrapingConfig(
        max_concurrent_requests=3,
        request_delay=2.0,
        max_retries=3,
        timeout=30,
        database_path="data/test_js_cycling_data.db",
        headless=True,
        wait_for_table_timeout=15,
        enable_screenshots=True
    )
    
    async with JavaScriptAwareScraper(config) as scraper:
        # Test with a small subset
        test_years = [2025]
        await scraper.scrape_years(test_years)

if __name__ == "__main__":
    asyncio.run(main()) 