import asyncio
import aiohttp
import aiosqlite
import logging
import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import json
import ast
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from contextlib import asynccontextmanager

# Import rider scraper
from rider_scraper import RiderProfileScraper

# Simple error logging (consolidated from enhanced_error_logger.py)
class SimpleErrorLogger:
    def log_scraping_error(self, stage: str, url: str, error: Exception, html_content=None, expected_elements=None, context=None):
        logger.error(f"SCRAPING ERROR - Stage: {stage}, URL: {url}, Error: {type(error).__name__} - {str(error)}")

# Historical data handling (consolidated from historical_data_handler.py)  
class HistoricalDataHandler:
    @staticmethod
    def is_historical_year(year: int) -> bool:
        return year < 1980
    
    @staticmethod
    def adjust_expectations_for_year(year: int) -> Dict[str, Any]:
        if year < 1920:
            return {'min_races_expected': 1, 'expect_team_data': False, 'expect_uci_points': False}
        elif year < 1950:
            return {'min_races_expected': 3, 'expect_team_data': True, 'expect_uci_points': False}
        elif year < 1980:
            return {'min_races_expected': 8, 'expect_team_data': True, 'expect_uci_points': False}
        else:
            return {'min_races_expected': 15, 'expect_team_data': True, 'expect_uci_points': True}

# Global instances
enhanced_logger = SimpleErrorLogger()
historical_handler = HistoricalDataHandler()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScrapingConfig:
    """Configuration for the async scraper"""
    max_concurrent_requests: int = 50
    request_delay: float = 0.1  # Delay between requests in seconds
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: int = 30
    database_path: str = "data/cycling_data.db"
    
@dataclass
class ScrapingStats:
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

class AsyncCyclingDataScraper:
    """Async scraper for cycling data from procyclingstats.com"""
    
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig()
        self.stats = ScrapingStats()
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        self.rider_scraper: Optional[RiderProfileScraper] = None
        
        # Progress tracking attributes
        self.progress_tracker = None
        self.checkpoint_interval = 300  # 5 minutes
        self.last_checkpoint = time.time()
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
        await self.init_database()
        
        # Initialize rider scraper
        self.rider_scraper = RiderProfileScraper(self.session, self.config.database_path)
        await self.rider_scraper.init_rider_tables()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def format_rider_name(self, raw_name: str) -> str:
        """Convert 'LastFirst' concatenated names into 'First Last' when applicable."""
        if not raw_name or len(raw_name) < 2:
            return raw_name
        if ' ' in raw_name:
            return raw_name
        import re as _re
        m = _re.search(r'([a-z])([A-Z])', raw_name)
        if m:
            split_pos = m.start() + 1
            last = raw_name[:split_pos]
            first = raw_name[split_pos:]
            return f"{first} {last}"
        return raw_name
    
    def clean_race_name(self, race_name: str) -> str:
        """Clean and standardize race names by removing edition numbers and year prefixes"""
        if not race_name:
            return race_name
        
        name = race_name.strip()
        
        # Remove year prefix (e.g., "2019   »")
        name = re.sub(r'^\d{4}\s*»\s*', '', name)
        
        # Remove edition numbers (e.g., "102nd", "117th", "1st", "21st")
        name = re.sub(r'(\d+)(st|nd|rd|th)', '', name)
        
        # Remove classification suffixes (e.g., "(1.UWT)", "(WT)", "(SPP)", "(2.UWT)")
        name = re.sub(r'\([^)]*\)$', '', name)
        
        # Standardize race name components
        replacements = {
            # Standardize spacing and dashes
            r'Paris - Roubaix': 'Paris-Roubaix',
            r'Milano-Sanremo': 'Milano-Sanremo', 
            r'Liège - Bastogne - Liège': 'Liège-Bastogne-Liège',
            r'Ronde van Vlaanderen': 'Tour of Flanders',
            
            # Clean up National Championships
            r'National Championships ([^-]+) ME - Road Race': r'National Championships \1 - Road Race',
            r'National Championships ([^-]+) ME - ITT': r'National Championships \1 - Time Trial',
            r'National Championships ([^-]+) - ITT': r'National Championships \1 - Time Trial',
            r'National Championships ([^-]+) ME - Time Trial': r'National Championships \1 - Time Trial',
            r'National Championships ([^-]+)  - Road Race': r'National Championships \1 - Road Race',
            r'National Championships ([^-]+)  - ITT': r'National Championships \1 - Time Trial',
            
            # Remove trailing ME and extra spaces
            r' ME$': '',
            r'  +': ' ',  # Multiple spaces to single space
        }
        
        # Apply replacements
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        
        # Clean up extra whitespace and leading/trailing characters
        name = re.sub(r'\s+', ' ', name).strip()
        name = name.strip('» ')
        
        return name
    
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
            
            # Create classifications table for GC, Points, KOM, Youth standings
            await db.execute('''
                CREATE TABLE IF NOT EXISTS classifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stage_id INTEGER,
                    rider_name TEXT,
                    rider_url TEXT,
                    classification_type TEXT, -- 'gc', 'points', 'kom', 'youth'
                    rank INTEGER,
                    time_gap TEXT,
                    points_total INTEGER,
                    uci_points INTEGER,
                    pcs_points INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (stage_id) REFERENCES stages (id),
                    UNIQUE(stage_id, rider_name, classification_type)
                )
            ''')
            
            await db.commit()
            logger.info(f"Database initialized at {self.config.database_path}")
    
    async def make_request(self, url: str, max_retries: int = None) -> Optional[str]:
        """Make an HTTP request with rate limiting and retry logic"""
        max_retries = max_retries or self.config.max_retries
        
        async with self.semaphore:
            for attempt in range(max_retries + 1):
                try:
                    self.stats.total_requests += 1
                    
                    # Add delay for rate limiting
                    if self.config.request_delay > 0:
                        await asyncio.sleep(self.config.request_delay)
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            self.stats.successful_requests += 1
                            return await response.text()
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            
                except Exception as e:
                    logger.warning(f"Request failed for {url} (attempt {attempt + 1}): {e}")
                    
                    if attempt < max_retries:
                        delay = self.config.retry_delay * (2 ** attempt)  # Exponential backoff
                        await asyncio.sleep(delay)
                    else:
                        self.stats.failed_requests += 1
                        logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts")
                        return None
    
    async def get_races(self, year: int) -> List[str]:
        """Get list of race URLs for a given year"""
        logger.info(f"Fetching races for year {year}")
        
        try:
            # URLs for Pro Tour races and national championships
            base_url = 'https://www.procyclingstats.com/races.php'
            suffixes = [
                f'?season={year}&category=1&racelevel=3&pracelevel=smallerorequal&racenation=&class=NC&filter=Filter&p=uci&s=calendar-plus-filters',
                f'?season={year}&category=1&racelevel=2&pracelevel=smallerorequal&racenation=&class=&filter=Filter&p=uci&s=calendar-plus-filters'
            ]
            
            urls = [base_url + suffix for suffix in suffixes]
            race_urls = set()
            
            # Fetch both URL pages concurrently
            tasks = [self.make_request(url) for url in urls]
            responses = await asyncio.gather(*tasks)
            
            for i, html_content in enumerate(responses):
                if html_content:
                    try:
                        soup = BeautifulSoup(html_content, 'html.parser')
                        race_entries = soup.select('table tr a[href]')
                        
                        for entry in race_entries:
                            race_url = entry['href']
                            if race_url.startswith('race/') or race_url.startswith('/race/'):
                                # Remove the leading slash if present
                                clean_url = race_url[1:] if race_url.startswith('/') else race_url
                                race_urls.add(clean_url)
                    except Exception as e:
                        enhanced_logger.log_scraping_error(
                            stage="get_races",
                            url=urls[i],
                            error=e,
                            html_content=html_content,
                            expected_elements=['table tr a[href]', 'race/ links'],
                            context={'year': year, 'page_index': i}
                        )
                        continue
            
            race_urls = list(race_urls)
            
            # Validate result count (adjust expectations for historical years)
            min_expected_races = 10 if year >= 1980 else (5 if year >= 1950 else 1)  # Lower expectations for very old years
            if len(race_urls) < min_expected_races:
                error_msg = f"Only found {len(race_urls)} races for {year}, expected at least {min_expected_races}"
                enhanced_logger.log_scraping_error(
                    stage="get_races",
                    url=f"races.php?season={year}",
                    error=ValueError(error_msg),
                    html_content=responses[0] if responses else None,
                    expected_elements=['table tr a[href]'],
                    context={'year': year, 'found_count': len(race_urls), 'is_historical': year < 1980}
                )
            
            logger.info(f"Found {len(race_urls)} races for year {year}")
            return race_urls
            
        except Exception as e:
            enhanced_logger.log_scraping_error(
                stage="get_races",
                url=f"races.php?season={year}",
                error=e,
                expected_elements=['table tr a[href]'],
                context={'year': year}
            )
            raise
    
    async def get_race_info(self, race_url: str) -> Optional[Dict[str, Any]]:
        """Get race information and stage URLs"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, race_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
            # Parse race information from HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract race name
            race_name_elem = soup.find('h1')
            raw_race_name = race_name_elem.get_text(strip=True) if race_name_elem else "Unknown"
            race_name = self.clean_race_name(raw_race_name)
            
            # Extract race category and UCI tour info
            race_category = "Unknown"
            uci_tour = "Unknown"
            
            # Look for classification info
            info_elements = soup.find_all('span', class_='classification')
            for elem in info_elements:
                text = elem.get_text(strip=True)
                if 'Cat' in text or 'Class' in text:
                    race_category = text
                elif 'UCI' in text:
                    uci_tour = text
            
            # Find stage URLs - be more specific to this race
            stage_urls = []
            stage_links = soup.find_all('a', href=True)
            
            # Extract the base race path for filtering
            base_race_path = race_url.split('/')[0:2]  # e.g., ['race', 'nc-greece-itt']
            base_race_prefix = '/'.join(base_race_path)
            
            for link in stage_links:
                href = link['href']
                clean_href = href[1:] if href.startswith('/') else href
                
                # Only include URLs that belong to this specific race
                if (clean_href.startswith(base_race_prefix) and 
                    ('/stage-' in href or href.endswith('/result')) and
                    '/route/' not in href and '/startlist' not in href):
                    stage_urls.append(clean_href)
            
            # Remove duplicates and sort
            stage_urls = list(set(stage_urls))
            
            # If no stages found, use the race result page
            if not stage_urls:
                result_url = f"{race_url}/result"
                stage_urls.append(result_url)
            
            # Extract year from race_url for historical context
            year = None
            try:
                year = int(race_url.split('/')[-1])
            except:
                pass
            
            # Validate the extracted data (adjust expectations for historical years)
            if race_name == "Unknown" or not stage_urls:
                # For very early years, this might be expected if the race doesn't exist
                if year and year < 1920:
                    logger.info(f"Race {race_url} not found - may not exist in {year} (very early era)")
                else:
                    enhanced_logger.log_scraping_error(
                        stage="get_race_info",
                        url=full_url,
                        error=ValueError(f"Failed to extract race info - name: {race_name}, stages: {len(stage_urls)}"),
                        html_content=html_content,
                        expected_elements=['h1', 'span.classification', 'stage links'],
                        context={'race_url': race_url, 'year': year}
                    )
            
            race_info = {
                'race_name': race_name,
                'race_category': race_category,
                'uci_tour': uci_tour,
                'stage_urls': stage_urls
            }
            
            # Enhance with historical context if applicable
            if year and HistoricalDataHandler.is_historical_year(year):
                race_info = HistoricalDataHandler.enhance_historical_race_info(year, race_info)
            
            return race_info
            
        except Exception as e:
            enhanced_logger.log_scraping_error(
                stage="get_race_info",
                url=full_url,
                error=e,
                html_content=html_content,
                expected_elements=['h1', 'span.classification', 'a[href*="/stage-"]', 'a[href$="/result"]'],
                context={'race_url': race_url}
            )
            return None
    
    async def get_stage_info(self, stage_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed stage information and results"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, stage_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract race metadata
            race_name_elem = soup.find('h1')
            raw_race_name = race_name_elem.get_text(strip=True) if race_name_elem else None
            race_name = self.clean_race_name(raw_race_name) if raw_race_name else None
            
            # Determine race type based on URL structure
            is_one_day_race = '/result' in stage_url and '/stage-' not in stage_url and '/gc' not in stage_url
            race_type = "one-day" if is_one_day_race else "stage_race"
            
            stage_info = {
                'stage_url': stage_url,
                'race_name': race_name,
                'race_url': stage_url,
                'race_type': race_type,
                'is_one_day_race': is_one_day_race,
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
            
            # Extract stage details from keyvalueList (the actual structure used by the site)
            keyvalue_list = soup.find('ul', class_='keyvalueList')
            if keyvalue_list:
                for li in keyvalue_list.find_all('li'):
                    title_div = li.find('div', class_='title')
                    value_div = li.find('div', class_='value')
                    
                    if title_div and value_div:
                        title = title_div.get_text(strip=True).lower()
                        value = value_div.get_text(strip=True)
                        
                        # Extract distance
                        if 'distance' in title:
                            try:
                                # Extract numeric value from "165.1 km"
                                distance_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if distance_str:
                                    stage_info['distance'] = float(distance_str)
                            except:
                                pass
                        
                        # Extract won how
                        elif 'won how' in title:
                            stage_info['won_how'] = value
                        
                        # Extract average speed
                        elif 'avg. speed winner' in title:
                            try:
                                # Extract numeric value from "33.534 km/h"
                                speed_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if speed_str:
                                    stage_info['avg_speed_winner'] = float(speed_str)
                            except:
                                pass
                        
                        # Extract vertical meters
                        elif 'vertical meters' in title:
                            try:
                                stage_info['vertical_meters'] = int(value)
                            except:
                                pass
                        
                        # Extract profile score
                        elif 'profilescore' in title:
                            try:
                                stage_info['profile_score'] = int(value)
                            except:
                                pass
                        
                        # Extract startlist quality score
                        elif 'startlist quality score' in title:
                            try:
                                stage_info['race_startlist_quality_score'] = int(value)
                            except:
                                pass
                        
                        # Extract date
                        elif 'date' in title:
                            stage_info['date'] = value
                        
                        # Extract average temperature
                        elif 'avg. temperature' in title:
                            try:
                                # Extract numeric value from "26 °C"
                                temp_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if temp_str:
                                    stage_info['avg_temperature'] = float(temp_str)
                            except:
                                pass
            
            # Check if this is a jersey classification page
            is_gc_page = stage_url.endswith('/gc')
            is_points_page = stage_url.endswith('/points')
            is_kom_page = stage_url.endswith('/kom')
            is_youth_page = stage_url.endswith('/youth')
            is_jersey_page = is_gc_page or is_points_page or is_kom_page or is_youth_page
            
            if is_jersey_page:
                # For jersey classification pages, look for the specific classification table
                classification_type = None
                if is_gc_page:
                    classification_type = 'gc'
                elif is_points_page:
                    classification_type = 'points'
                elif is_kom_page:
                    classification_type = 'kom'
                elif is_youth_page:
                    classification_type = 'youth'
                
                # Look for the main classification table
                classification_results = []
                results_tables = soup.find_all('table', class_='results')
                
                if results_tables:
                    # For jersey classification pages, the tables are typically ordered as:
                    # Table 0: Stage result
                    # Table 1: GC classification  
                    # Table 2: Points classification
                    # Table 3: KOM classification
                    # Table 4: Youth classification
                    
                    table_index = 0  # Default to first table
                    
                    if classification_type == 'gc' and len(results_tables) >= 2:
                        table_index = 1  # GC is typically second table
                    elif classification_type == 'points' and len(results_tables) >= 3:
                        table_index = 2  # Points is typically third table
                    elif classification_type == 'kom' and len(results_tables) >= 4:
                        table_index = 3  # KOM is typically fourth table
                    elif classification_type == 'youth' and len(results_tables) >= 5:
                        table_index = 4  # Youth is typically fifth table
                    
                    # Ensure we don't go out of bounds
                    if table_index < len(results_tables):
                        classification_results = self.parse_results_table(results_tables[table_index])
                    else:
                        # Fallback to first table if target table doesn't exist
                        classification_results = self.parse_results_table(results_tables[0])
                
                if classification_results:
                    stage_info['results'] = classification_results
                    # Also extract other classifications if available
                    for other_classification in ['gc', 'points', 'kom', 'youth']:
                        if other_classification != classification_type:
                            class_table = soup.find('table', {'id': f'{other_classification}table'})
                            if class_table:
                                stage_info[other_classification] = self.parse_results_table(class_table, secondary=True)
                else:
                    # Fallback to main results table if classification table not found
                    results_table = soup.find('table', class_='results')
                    if results_table:
                        stage_info['results'] = self.parse_results_table(results_table)
            else:
                # For regular stage pages, use the main results table
                results_table = soup.find('table', class_='results')
                if results_table:
                    stage_info['results'] = self.parse_results_table(results_table)
                else:
                    # Log missing results table as potential issue
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
            
            # Validate results (adjust expectations for historical years)
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
            
            # Merge classification data back into results for fixture testing
            self._merge_classifications_into_results(stage_info)
            
            # Calculate winner (first position rider)
            if stage_info['results']:
                first_result = stage_info['results'][0]
                stage_info['winner'] = first_result.get('rider_name')
            else:
                stage_info['winner'] = None
            
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
    
    async def get_gc_info(self, gc_url: str) -> Optional[Dict[str, Any]]:
        """Get General Classification information and results"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, gc_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            gc_info = {
                'stage_url': gc_url,
                'is_one_day_race': False,  # GC is always multi-stage
                'distance': None,
                'stage_type': 'gc',  # Mark as GC type
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
            
            # Extract GC metadata from keyvalueList
            keyvalue_list = soup.find('ul', class_='keyvalueList')
            if keyvalue_list:
                for li in keyvalue_list.find_all('li'):
                    title_div = li.find('div', class_='title')
                    value_div = li.find('div', class_='value')
                    
                    if title_div and value_div:
                        title = title_div.get_text(strip=True).lower()
                        value = value_div.get_text(strip=True)
                        
                        if 'average speed' in title and 'winner' in title:
                            try:
                                gc_info['avg_speed_winner'] = float(value.split()[0])
                            except:
                                pass
                        elif 'won how' in title:
                            gc_info['won_how'] = value
                        elif 'startlist quality score' in title:
                            try:
                                gc_info['race_startlist_quality_score'] = int(value)
                            except:
                                pass
            
            # For GC pages, look for the main classification table
            main_table = soup.find('table', class_='results')
            if not main_table:
                # Try alternative table selectors for GC pages
                main_table = soup.find('table')
            
            if main_table:
                gc_info['results'] = self.parse_results_table(main_table)
                # Also populate gc field for consistency
                gc_info['gc'] = self.parse_results_table(main_table, secondary=True)
            
            # Extract secondary classifications if they exist
            for classification in ['points', 'kom', 'youth']:
                class_table = soup.find('table', {'id': f'{classification}table'})
                if class_table:
                    gc_info[classification] = self.parse_results_table(class_table, secondary=True)
            
            # Extract race metadata from the page
            race_name_elem = soup.find('h1')
            raw_race_name = race_name_elem.get_text(strip=True) if race_name_elem else None
            race_name = self.clean_race_name(raw_race_name) if raw_race_name else None
            
            gc_info['race_name'] = race_name
            gc_info['race_url'] = gc_url
            gc_info['race_type'] = "stage_race"  # GC pages are always for stage races
            
            # Merge classification data back into results for fixture testing
            self._merge_classifications_into_results(gc_info)
            
            # Calculate winner (first position rider in GC)
            if gc_info['results']:
                first_result = gc_info['results'][0]
                gc_info['winner'] = first_result.get('rider_name')
            else:
                gc_info['winner'] = None
            
            return gc_info
            
        except Exception as e:
            logger.error(f"Error parsing GC info for {gc_url}: {e}")
            return None
    
    def _merge_classifications_into_results(self, stage_info: Dict[str, Any]) -> None:
        """Merge classification data back into results for fixture testing compatibility"""
        results = stage_info.get('results', [])
        if not results:
            return
            
        # Create rider URL to result mapping for fast lookup
        results_by_rider = {r.get('rider_url'): r for r in results}
        
        # Merge GC data
        for gc_result in stage_info.get('gc', []):
            rider_url = gc_result.get('rider_url')
            if rider_url in results_by_rider:
                result = results_by_rider[rider_url]
                result['gc_rank'] = gc_result.get('rank') or gc_result.get('position')
                result['gc_time'] = gc_result.get('time')
                
        # Merge Points data  
        for points_result in stage_info.get('points', []):
            rider_url = points_result.get('rider_url')
            if rider_url in results_by_rider:
                result = results_by_rider[rider_url]
                result['points_rank'] = points_result.get('rank') or points_result.get('position')
                result['points_total'] = points_result.get('pcs_points')
                
        # Merge KOM data
        for kom_result in stage_info.get('kom', []):
            rider_url = kom_result.get('rider_url')
            if rider_url in results_by_rider:
                result = results_by_rider[rider_url]
                result['kom_rank'] = kom_result.get('rank') or kom_result.get('position')
                result['kom_points'] = kom_result.get('pcs_points')
                
        # Merge Youth data
        for youth_result in stage_info.get('youth', []):
            rider_url = youth_result.get('rider_url')
            if rider_url in results_by_rider:
                result = results_by_rider[rider_url]
                result['youth_rank'] = youth_result.get('rank') or youth_result.get('position')
                result['youth_time'] = youth_result.get('time')
    
    def parse_results_table(self, table, secondary=False) -> List[Dict[str, Any]]:
        """Parse a results table from HTML"""
        results = []
        
        try:
            rows = table.find_all('tr')  # Don't skip first row - it might be data
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue
                
                result = {}
                
                # Extract rider name and URL (handle URLs with or without leading slash)
                rider_link = row.find('a', href=lambda x: x and ('rider/' in x or '/rider/' in x))
                if rider_link:
                    # Check for structured name format: <span class="uppercase">LASTNAME</span> Firstname
                    uppercase_span = rider_link.find('span', class_='uppercase')
                    if uppercase_span:
                        lastname = uppercase_span.get_text(strip=True)
                        # Get full text and remove the uppercase part to get firstname
                        full_text = rider_link.get_text(strip=True)
                        firstname = full_text.replace(lastname, '').strip()
                        if firstname and lastname:
                            result['rider_name'] = f"{firstname} {lastname}"
                        else:
                            result['rider_name'] = full_text
                    else:
                        # Fallback to existing logic for other formats
                        raw_name = rider_link.get_text(strip=True)
                        result['rider_name'] = self.format_rider_name(raw_name)
                    
                    result['rider_url'] = rider_link['href']
                
                # Extract team name and URL (handle URLs with or without leading slash)
                team_link = row.find('a', href=lambda x: x and ('team/' in x or '/team/' in x))
                if team_link:
                    team_name = team_link.get_text(strip=True)
                    result['team_name'] = team_name
                    result['team'] = team_name  # Also add 'team' field for compatibility
                    result['team_url'] = team_link['href']
                
                # Extract rank (usually first column)
                if cells:
                    rank_text = cells[0].get_text(strip=True)
                    try:
                        rank_value = int(rank_text) if rank_text.isdigit() else None
                        result['rank'] = rank_value
                        result['position'] = rank_value  # Also add 'position' field for compatibility
                    except:
                        result['rank'] = None
                        result['position'] = None
                
                # Extract specialty from specialty column  
                specialty_cell = row.find('td', class_='specialty')
                if specialty_cell:
                    specialty_span = specialty_cell.find('span', class_='fs10')
                    if specialty_span:
                        specialty_text = specialty_span.get_text(strip=True)
                        if specialty_text:
                            result['specialty'] = specialty_text

                # Extract age from age column
                age_cell = row.find('td', class_='age')
                if age_cell:
                    age_text = age_cell.get_text(strip=True)
                    if age_text.isdigit() and 15 <= int(age_text) <= 60:
                        result['age'] = int(age_text)

                # Extract bib from bib column
                bib_cell = row.find('td', class_='bibs')
                if bib_cell:
                    bib_text = bib_cell.get_text(strip=True)
                    if bib_text.isdigit():
                        result['bib'] = int(bib_text)
                
                # Extract other data based on column headers
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    cell_classes = cell.get('class', [])
                    
                    # Time column - check for time cell class or time format 
                    if 'time' in cell_classes or (':' in text and any(c.isdigit() for c in text)):
                        # Look for hidden span with full time first
                        hidden_time_span = cell.find('span', class_='hide')
                        if hidden_time_span:
                            time_text = hidden_time_span.get_text(strip=True)
                            if ':' in time_text:
                                result['time'] = time_text
                        elif ':' in text:
                            # Fallback to visible text, clean up if needed
                            time_parts = text.split(':')
                            if len(time_parts) >= 2:
                                # For times like "5:25" try to add seconds if missing
                                if len(time_parts) == 2 and time_parts[0].isdigit() and time_parts[1].isdigit():
                                    # Check if this looks like minutes:seconds (likely missing hour)
                                    if int(time_parts[0]) < 10 and int(time_parts[1]) < 60:
                                        result['time'] = f"{time_parts[0]}:{time_parts[1]}:00"
                                    else:
                                        result['time'] = text
                                else:
                                    result['time'] = text
                    
                    # UCI Points column - specifically look for cells with 'uci_pnt' class
                    elif 'uci_pnt' in cell_classes and text.isdigit() and int(text) > 0:
                        result['uci_points'] = int(text)
                    
                    # PCS Points column - specifically look for cells with 'pnt' class
                    elif 'pnt' in cell_classes and text.isdigit() and int(text) > 0:
                        result['pcs_points'] = int(text)
                    
                    # Points columns - for other tables or fallback
                    elif text.isdigit() and int(text) > 0:
                        # Don't assign rank numbers as points
                        if int(text) != result.get('rank'):
                            if not secondary:
                                # For GC tables, look for the points column specifically
                                # The points column is typically the one with moderate numbers (not too high, not too low)
                                if 'pcs_points' not in result:
                                    # Only assign as PCS points if it's a reasonable value (not age, not bib number, etc.)
                                    if 10 <= int(text) <= 500:  # PCS points are typically in this range
                                        result['pcs_points'] = int(text)
                                # UCI points are typically not shown in older GC tables
                                # Only assign if we're confident it's UCI points (very high values)
                                elif 'uci_points' not in result and int(text) > 500:  # UCI points are typically much higher
                                    result['uci_points'] = int(text)
                            else:
                                # For secondary classifications, this is UCI points
                                result['uci_points'] = int(text)
                    
                    
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
    
    async def save_race_data(self, year: int, race_data: Dict[str, Any]) -> Optional[int]:
        """Save race data to SQLite database"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Insert race record
                race_cursor = await db.execute('''
                    INSERT OR IGNORE INTO races (year, race_name, race_category, uci_tour, stage_url)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    year,
                    race_data['race_name'],
                    race_data['race_category'],
                    race_data['uci_tour'],
                    race_data['stage_urls'][0] if race_data['stage_urls'] else ''
                ))
                
                race_id = race_cursor.lastrowid
                
                # If race already exists, get its ID
                if race_id == 0:
                    cursor = await db.execute(
                        'SELECT id FROM races WHERE stage_url = ?',
                        (race_data['stage_urls'][0] if race_data['stage_urls'] else '',)
                    )
                    row = await cursor.fetchone()
                    race_id = row[0] if row else None
                
                await db.commit()
                return race_id
                
            except Exception as e:
                logger.error(f"Error saving race data: {e}")
                return None
    
    async def save_stage_data(self, race_id: int, stage_data: Dict[str, Any]) -> Optional[int]:
        """Save stage data to SQLite database"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Insert stage record
                stage_cursor = await db.execute('''
                    INSERT OR IGNORE INTO stages (
                        race_id, stage_url, is_one_day_race, distance, stage_type,
                        winning_attack_length, date, won_how, avg_speed_winner,
                        avg_temperature, vertical_meters, profile_icon, profile_score,
                        race_startlist_quality_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    race_id,
                    stage_data['stage_url'],
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
                    stage_data['race_startlist_quality_score']
                ))
                
                stage_id = stage_cursor.lastrowid
                
                # If stage already exists, get its ID
                if stage_id == 0:
                    cursor = await db.execute(
                        'SELECT id FROM stages WHERE stage_url = ?',
                        (stage_data['stage_url'],)
                    )
                    row = await cursor.fetchone()
                    stage_id = row[0] if row else None
                
                await db.commit()
                return stage_id
                
            except Exception as e:
                logger.error(f"Error saving stage data: {e}")
                return None
    
    async def save_results_data(self, stage_id: int, stage_data: Dict[str, Any]):
        """Save results data to SQLite database"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Prepare results with secondary classifications
                results = stage_data.get('results', [])
                gc_results = {r.get('rider_url'): r for r in stage_data.get('gc', [])}
                points_results = {r.get('rider_url'): r for r in stage_data.get('points', [])}
                kom_results = {r.get('rider_url'): r for r in stage_data.get('kom', [])}
                youth_results = {r.get('rider_url'): r for r in stage_data.get('youth', [])}
                
                for result in results:
                    rider_url = result.get('rider_url')
                    
                    # Merge secondary classification data
                    gc_data = gc_results.get(rider_url, {})
                    points_data = points_results.get(rider_url, {})
                    kom_data = kom_results.get(rider_url, {})
                    youth_data = youth_results.get(rider_url, {})
                    
                    await db.execute('''
                        INSERT OR IGNORE INTO results (
                            stage_id, rider_name, rider_url, team_name, team_url,
                            rank, status, time, uci_points, pcs_points, age,
                            gc_rank, gc_uci_points, points_rank, points_uci_points,
                            kom_rank, kom_uci_points, youth_rank, youth_uci_points
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stage_id,
                        result.get('rider_name'),
                        result.get('rider_url'),
                        result.get('team_name'),
                        result.get('team_url'),
                        result.get('rank'),
                        result.get('status'),
                        result.get('time'),
                        result.get('uci_points'),
                        result.get('pcs_points'),
                        result.get('age'),
                        gc_data.get('rank'),
                        gc_data.get('uci_points'),
                        points_data.get('rank'),
                        points_data.get('uci_points'),
                        kom_data.get('rank'),
                        kom_data.get('uci_points'),
                        youth_data.get('rank'),
                        youth_data.get('uci_points')
                    ))
                
                # Save classifications data to separate table
                classifications = [
                    ('gc', stage_data.get('gc', [])),
                    ('points', stage_data.get('points', [])), 
                    ('kom', stage_data.get('kom', [])),
                    ('youth', stage_data.get('youth', []))
                ]
                
                for classification_type, classification_results in classifications:
                    for result in classification_results:
                        if result.get('rider_name'):  # Only save if we have rider data
                            await db.execute('''
                                INSERT OR REPLACE INTO classifications (
                                    stage_id, rider_name, rider_url, classification_type,
                                    rank, time_gap, points_total, uci_points, pcs_points
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                stage_id,
                                result.get('rider_name'),
                                result.get('rider_url'),
                                classification_type,
                                result.get('rank'),
                                result.get('time'),  # For GC this is time gap, for points it's None
                                result.get('pcs_points') if classification_type == 'points' else None,  # Total points
                                result.get('uci_points'),
                                result.get('pcs_points')
                            ))
                
                await db.commit()
                logger.debug(f"Saved {len(results)} results and classifications for stage {stage_id}")
                
            except Exception as e:
                logger.error(f"Error saving results data: {e}")
    
    async def scrape_year(self, year: int):
        """Scrape all data for a given year"""
        logger.info(f"Starting scrape for year {year}")
        start_time = time.time()
        
        # Get all race URLs for the year
        race_urls = await self.get_races(year)
        logger.info(f"Found {len(race_urls)} races for {year}")
        
        # Process races in batches to avoid overwhelming the server
        batch_size = 10
        total_stages = 0
        
        for i in range(0, len(race_urls), batch_size):
            batch = race_urls[i:i + batch_size]
            logger.info(f"Processing race batch {i//batch_size + 1}/{(len(race_urls) + batch_size - 1)//batch_size}")
            
            # Get race info for batch
            race_info_tasks = [self.get_race_info(race_url) for race_url in batch]
            race_infos = await asyncio.gather(*race_info_tasks)
            
            # Process each race
            for race_url, race_info in zip(batch, race_infos):
                if not race_info:
                    continue
                
                # Save race data
                race_id = await self.save_race_data(year, race_info)
                if not race_id:
                    continue
                
                logger.info(f"Processing race: {race_info['race_name']} ({len(race_info['stage_urls'])} stages)")
                
                # Process stages for this race
                stage_tasks = [self.get_stage_info(stage_url) for stage_url in race_info['stage_urls']]
                stage_infos = await asyncio.gather(*stage_tasks)
                
                for stage_info in stage_infos:
                    if stage_info:
                        stage_id = await self.save_stage_data(race_id, stage_info)
                        if stage_id:
                            await self.save_results_data(stage_id, stage_info)
                            total_stages += 1
                
                # Add small delay between races
                await asyncio.sleep(0.5)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed scraping {year}: {total_stages} stages in {elapsed_time:.2f}s")
        logger.info(f"Stats: {self.stats.successful_requests}/{self.stats.total_requests} requests successful ({self.stats.success_rate:.1f}%)")
    
    async def scrape_years(self, years: List[int]):
        """Scrape data for multiple years (legacy method without progress tracking)"""
        logger.info(f"Starting scrape for years: {years}")
        
        for year in years:
            try:
                await self.scrape_year(year)
            except Exception as e:
                logger.error(f"Error scraping year {year}: {e}")
        
        logger.info(f"Scraping completed. Total stats: {self.stats.successful_requests}/{self.stats.total_requests} requests successful")
    
    async def scrape_years_with_progress(self, years: List[int]):
        """Scrape data for multiple years with comprehensive progress tracking"""
        logger.info(f"Starting scrape with progress tracking for years: {years}")
        
        for i, year in enumerate(years):
            try:
                # Check if year should be skipped
                if self.progress_tracker and await self.progress_tracker.should_skip_year(year):
                    logger.info(f"⏭️  Skipping year {year} - already completed")
                    continue
                
                logger.info(f"🚀 Processing year {year} ({i+1}/{len(years)})")
                
                # Show progress report periodically
                if self.progress_tracker and i > 0:
                    report = await self.progress_tracker.get_status_report(years)
                    logger.info(f"📊 Progress Update:\n{report}")
                
                await self.scrape_year_with_progress(year)
                
                # Mark year as completed
                if self.progress_tracker:
                    await self.progress_tracker.mark_year_completed(year)
                
                logger.info(f"✅ Year {year} completed successfully")
                
            except Exception as e:
                logger.error(f"💥 Error scraping year {year}: {e}")
                
                # Mark year as failed but continue with next year
                if self.progress_tracker:
                    await self.progress_tracker.mark_year_failed(year, str(e))
                
                # Continue with next year rather than stopping
                continue
        
        logger.info(f"🏁 Scraping completed. Total stats: {self.stats.successful_requests}/{self.stats.total_requests} requests successful")
        
        # Final checkpoint
        if self.progress_tracker:
            await self.progress_tracker.create_checkpoint("Final completion checkpoint")
    
    async def scrape_year_with_progress(self, year: int):
        """Scrape all data for a given year with progress tracking"""
        logger.info(f"Starting scrape for year {year}")
        start_time = time.time()
        
        # Get all race URLs for the year
        race_urls = await self.get_races(year)
        logger.info(f"Found {len(race_urls)} races for {year}")
        
        # Process races in batches to avoid overwhelming the server
        batch_size = 10
        total_stages = 0
        
        for i in range(0, len(race_urls), batch_size):
            batch = race_urls[i:i + batch_size]
            logger.info(f"Processing race batch {i//batch_size + 1}/{(len(race_urls) + batch_size - 1)//batch_size}")
            
            # Get race info for batch
            race_info_tasks = [self.get_race_info(race_url) for race_url in batch]
            race_infos = await asyncio.gather(*race_info_tasks)
            
            # Process each race
            for race_url, race_info in zip(batch, race_infos):
                try:
                    # Check if race should be skipped
                    if self.progress_tracker and await self.progress_tracker.should_skip_race(race_url):
                        logger.debug(f"⏭️  Skipping race {race_url} - already completed")
                        continue
                    
                    if not race_info:
                        if self.progress_tracker:
                            await self.progress_tracker.mark_race_failed(race_url, "Failed to get race info")
                        continue
                    
                    # Save race data
                    race_id = await self.save_race_data(year, race_info)
                    if not race_id:
                        if self.progress_tracker:
                            await self.progress_tracker.mark_race_failed(race_url, "Failed to save race data")
                        continue
                    
                    logger.info(f"Processing race: {race_info['race_name']} ({len(race_info['stage_urls'])} stages)")
                    
                    # Process stages for this race
                    stage_tasks = [self.get_stage_info(stage_url) for stage_url in race_info['stage_urls']]
                    stage_infos = await asyncio.gather(*stage_tasks)
                    
                    race_stages = 0
                    race_results = 0
                    
                    for stage_info in stage_infos:
                        if stage_info:
                            stage_id = await self.save_stage_data(race_id, stage_info)
                            if stage_id:
                                await self.save_results_data(stage_id, stage_info)
                                race_stages += 1
                                race_results += len(stage_info.get('results', []))
                                total_stages += 1
                    
                    # Mark race as completed
                    if self.progress_tracker:
                        await self.progress_tracker.mark_race_completed(race_url, race_stages, race_results)
                    
                    # Periodic checkpoint
                    if self.progress_tracker and time.time() - self.last_checkpoint > self.checkpoint_interval:
                        await self.progress_tracker.create_checkpoint(f"Processing year {year}")
                        self.last_checkpoint = time.time()
                    
                except Exception as e:
                    logger.error(f"Error processing race {race_url}: {e}")
                    if self.progress_tracker:
                        await self.progress_tracker.mark_race_failed(race_url, str(e))
                    # Continue with next race
                    continue
                
                # Add small delay between races
                await asyncio.sleep(0.5)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed scraping {year}: {total_stages} stages in {elapsed_time:.2f}s")
        logger.info(f"Stats: {self.stats.successful_requests}/{self.stats.total_requests} requests successful ({self.stats.success_rate:.1f}%)")

    async def scrape_riders_for_years(self, years: List[int], enable_rider_scraping: bool = True) -> Dict[str, int]:
        """Scrape rider profile data for all riders who competed in the specified years"""
        if not enable_rider_scraping or not self.rider_scraper:
            logger.info("🚫 Rider scraping disabled or not initialized")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"🏃 Starting rider profile scraping for years: {years}")
        
        # Use conservative concurrency for rider scraping to be polite
        max_concurrent = min(5, self.config.max_concurrent_requests // 10)
        
        # Scrape rider profiles for the specified years
        results = await self.rider_scraper.update_rider_data_for_years(years, max_concurrent)
        
        logger.info(f"🎉 Rider scraping completed for years {years}")
        return results

    async def scrape_all_missing_riders(self) -> Dict[str, int]:
        """Scrape profile data for all riders missing from the database"""
        if not self.rider_scraper:
            logger.error("🚫 Rider scraper not initialized")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info("🏃 Starting rider profile scraping for all missing riders")
        
        # Use conservative concurrency for rider scraping to be polite
        max_concurrent = min(5, self.config.max_concurrent_requests // 10)
        
        # Scrape all missing rider profiles
        results = await self.rider_scraper.scrape_all_missing_riders(max_concurrent)
        
        logger.info("🎉 All missing rider profile scraping completed")
        return results

    async def scrape_years_with_riders(self, years: List[int], enable_rider_scraping: bool = True):
        """Scrape race data and then rider profiles for specified years"""
        logger.info(f"🚀 Starting comprehensive scraping (races + riders) for years: {years}")
        
        # First scrape race data
        await self.scrape_years_with_progress(years)
        
        # Then scrape rider profiles if enabled
        if enable_rider_scraping:
            logger.info("🔄 Starting rider profile collection phase...")
            rider_results = await self.scrape_riders_for_years(years, enable_rider_scraping)
            
            logger.info(f"📊 Final rider scraping results:")
            logger.info(f"   ✅ Riders processed: {rider_results['success']}")
            logger.info(f"   ❌ Failed: {rider_results['failed']}")
            logger.info(f"   ⏭️  Skipped: {rider_results['skipped']}")
        else:
            logger.info("⏭️  Skipping rider profile collection")
        
        logger.info(f"🏁 Comprehensive scraping completed for years: {years}")

    async def update_rider_data_for_years(self, years: List[int]) -> Dict[str, int]:
        """Standalone function to update rider data for specific years"""
        if not self.rider_scraper:
            logger.error("🚫 Rider scraper not initialized")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        logger.info(f"🔄 Updating rider data for years: {years}")
        
        # Use conservative concurrency
        max_concurrent = min(5, self.config.max_concurrent_requests // 10)
        
        results = await self.rider_scraper.update_rider_data_for_years(years, max_concurrent)
        
        logger.info(f"✅ Rider data update completed for years {years}")
        return results


# Example usage
async def main():
    """Example usage of the async scraper"""
    config = ScrapingConfig(
        max_concurrent_requests=30,  # Conservative for testing
        request_delay=0.1,
        database_path="data/cycling_data.db"
    )
    
    async with AsyncCyclingDataScraper(config) as scraper:
        # Scrape data for recent years
        await scraper.scrape_years([2023, 2024])

if __name__ == "__main__":
    asyncio.run(main())
