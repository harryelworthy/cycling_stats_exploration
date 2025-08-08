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

# Import enhanced error logging and historical data handler
from enhanced_error_logger import enhanced_logger
from historical_data_handler import HistoricalDataHandler
from rider_scraper import RiderProfileScraper

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
    overwrite_existing_data: bool = False  # Default to no overwriting
    overwrite_stages: bool = False         # Control stage overwrites specifically
    overwrite_results: bool = False        # Control result overwrites specifically
    
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

class ImprovedAsyncCyclingDataScraper:
    """Improved async scraper for cycling data from procyclingstats.com"""
    
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
    
    def clean_race_name(self, race_name: str) -> str:
        """Clean and standardize race names by removing edition numbers and year prefixes"""
        if not race_name:
            return "Unknown"
        
        # Remove PCS prefix formatting like "» " at the start
        race_name = re.sub(r'^»\s*', '', race_name)
        
        # Handle edition numbers at the start like "103rdTour de France" -> "Tour de France"
        race_name = re.sub(r'^\d+\w*(?=[A-Z])', '', race_name)
        
        # Remove year prefixes like "2024 " or "2024 - "
        race_name = re.sub(r'^\d{4}\s*[-–—]?\s*', '', race_name)
        
        # Remove edition numbers like " (2024)" or " - 2024"
        race_name = re.sub(r'\s*[-–—]?\s*\d{4}\s*$', '', race_name)
        
        # Remove edition numbers like " (1)" or " - 1"
        race_name = re.sub(r'\s*[-–—]?\s*\(\d+\)\s*$', '', race_name)
        race_name = re.sub(r'\s*[-–—]?\s*\d+\s*$', '', race_name)
        
        # Remove extra whitespace and clean up
        race_name = race_name.strip()
        
        return race_name if race_name else "Unknown"
    
    def format_rider_name(self, raw_name: str) -> str:
        """Convert 'LastNameFirstName' format to 'FirstName LastName' format"""
        if not raw_name or len(raw_name) < 2:
            return raw_name
        
        # Handle names that are already properly formatted (contain spaces)
        if ' ' in raw_name:
            return raw_name
        
        # Look for pattern where lowercase letter is followed by uppercase letter
        # This indicates the boundary between last name and first name
        match = re.search(r'([a-z])([A-Z])', raw_name)
        if match:
            split_pos = match.start() + 1
            last_name = raw_name[:split_pos]
            first_name = raw_name[split_pos:]
            return f"{first_name} {last_name}"
        
        # If no clear boundary found, return as-is
        return raw_name
    
    def detect_race_category(self, race_name: str, uci_tour: str, stage_urls: List[str]) -> str:
        """Improved race category detection"""
        race_name_lower = race_name.lower()
        
        # Grand Tours
        if any(tour in race_name_lower for tour in ['tour de france', 'giro d\'italia', 'vuelta a españa', 'vuelta a espana']):
            return "Grand Tour"
        
        # Monuments
        if any(monument in race_name_lower for monument in [
            'milano-sanremo', 'tour of flanders', 'paris-roubaix', 
            'liege-bastogne-liege', 'il lombardia', 'giro di lombardia'
        ]):
            return "Monument"
        
        # World Championships
        if 'world championship' in race_name_lower:
            return "World Championship"
        
        # Olympic Games
        if 'olympic' in race_name_lower:
            return "Olympic Games"
        
        # Continental Championships
        if any(cont in race_name_lower for cont in [
            'african championship', 'asian championship', 'european championship',
            'panamerican championship', 'oceania championship'
        ]):
            return "Continental Championship"
        
        # National Championships
        if 'national championship' in race_name_lower or 'nc ' in race_name_lower:
            return "National Championship"
        
        # UCI World Tour races
        if uci_tour and 'world tour' in uci_tour.lower():
            return "UCI World Tour"
        
        # UCI ProSeries races
        if uci_tour and 'proseries' in uci_tour.lower():
            return "UCI ProSeries"
        
        # Stage races (more than 1 stage)
        if len(stage_urls) > 1:
            return "Stage Race"
        
        # One-day races
        if len(stage_urls) == 1 and '/result' in stage_urls[0]:
            return "One-Day Race"
        
        return "Unknown"
    
    def generate_race_key(self, year: int, race_name: str) -> str:
        """Generate a unique key for a race to prevent duplicates"""
        # Clean the race name and create a consistent key
        clean_name = self.clean_race_name(race_name)
        return f"{year}_{clean_name}"
    
    def generate_classification_urls(self, race_url: str, stage_urls: List[str]) -> List[str]:
        """Generate classification URLs for multi-stage races"""
        classification_urls = []
        
        # Only generate classification URLs for multi-stage races
        if len(stage_urls) <= 1:
            return classification_urls
        
        # Extract stage numbers from existing stage URLs
        stage_numbers = []
        for url in stage_urls:
            if '/stage-' in url:
                try:
                    # Extract stage number from URLs like "race/tour-de-france/2024/stage-15"
                    stage_part = url.split('/stage-')[-1]
                    stage_num = int(stage_part.split('/')[0].split('-')[0])  # Handle URLs with additional parts
                    stage_numbers.append(stage_num)
                except (ValueError, IndexError):
                    continue
        
        if not stage_numbers:
            return classification_urls
        
        # Get the race base URL (e.g., "race/tour-de-france/2024")
        base_race_url = race_url.rstrip('/')
        
        # Sort stage numbers to find the final stage
        stage_numbers.sort()
        final_stage_num = max(stage_numbers)
        
        # Classification types to generate
        classification_types = ['gc', 'points', 'kom', 'youth']
        
        # Generate final classification URLs (e.g., "race/tour-de-france/2024/gc")
        for classification in classification_types:
            final_classification_url = f"{base_race_url}/{classification}"
            classification_urls.append(final_classification_url)
        
        # Generate mid-race classification URLs for a few key stages
        # We'll generate them for stages at 25%, 50%, 75% and final-1 of the race
        if len(stage_numbers) >= 4:  # Only for races with 4+ stages
            key_stages = [
                stage_numbers[len(stage_numbers) // 4],      # 25%
                stage_numbers[len(stage_numbers) // 2],      # 50% 
                stage_numbers[3 * len(stage_numbers) // 4],  # 75%
                final_stage_num - 1 if final_stage_num > 1 else final_stage_num  # Second to last
            ]
            
            # Remove duplicates and sort
            key_stages = sorted(list(set(key_stages)))
            
            for stage_num in key_stages:
                if stage_num != final_stage_num:  # Don't duplicate final stage
                    for classification in classification_types:
                        mid_race_url = f"{base_race_url}/stage-{stage_num}-{classification}"
                        classification_urls.append(mid_race_url)
        
        logger.debug(f"Generated {len(classification_urls)} classification URLs for {race_url}")
        return classification_urls
    
    def _should_fetch_classifications(self, stage_url: str) -> bool:
        """Determine if we should fetch classification data for this stage"""
        # Fetch classification data for all stages of multi-stage races
        # This ensures we have complete GC progression throughout the race
        
        # Fetch for all stages (not just final stages)
        if '/stage-' in stage_url:
            return True
            
        # Don't fetch for one-day races (results pages)
        if '/result' in stage_url and '/stage-' not in stage_url:
            return False
            
        return False
    
    async def _fetch_and_merge_classifications(self, stage_info: Dict[str, Any]):
        """Fetch classification data from separate URLs and merge with stage results"""
        stage_url = stage_info['stage_url']
        
        # Extract race URL and stage number from stage_url
        # e.g., "race/tour-de-france/2024/stage-21" -> "race/tour-de-france/2024" and "21"
        url_parts = stage_url.split('/')
        if len(url_parts) >= 4 and '/stage-' in stage_url:
            race_base = '/'.join(url_parts[:-1])  # "race/tour-de-france/2024"
            stage_part = url_parts[-1]  # "stage-21"
            
            try:
                stage_num = int(stage_part.split('-')[-1])  # 21
            except (ValueError, IndexError):
                logger.warning(f"Could not extract stage number from {stage_url}")
                return
            
            # Determine if this is the final stage by checking available classification URLs
            classification_types = ['gc', 'points', 'kom', 'youth']
            
            for classification in classification_types:
                # Try final classification first (e.g., "race/tour-de-france/2024/gc")
                final_classification_url = f"{race_base}/{classification}"
                classification_data = await self._fetch_classification_data(final_classification_url)
                
                if classification_data:
                    # Use final classification data
                    stage_info[classification] = classification_data
                    logger.debug(f"Fetched final {classification} data from {final_classification_url}")
                else:
                    # Try stage-specific classification (e.g., "race/tour-de-france/2024/stage-21-gc")
                    stage_classification_url = f"{race_base}/stage-{stage_num}-{classification}"
                    classification_data = await self._fetch_classification_data(stage_classification_url)
                    
                    if classification_data:
                        stage_info[classification] = classification_data
                        logger.debug(f"Fetched stage {classification} data from {stage_classification_url}")
                    else:
                        logger.debug(f"No {classification} data found for {stage_url}")
    
    def _merge_classifications_into_results(self, stage_info: Dict[str, Any]):
        """Merge classification data into stage results for test compatibility"""
        results = stage_info.get('results', [])
        gc_results = {r.get('rider_url'): r for r in stage_info.get('gc', [])}
        points_results = {r.get('rider_url'): r for r in stage_info.get('points', [])}
        kom_results = {r.get('rider_url'): r for r in stage_info.get('kom', [])}
        youth_results = {r.get('rider_url'): r for r in stage_info.get('youth', [])}
        
        # Debug logging for classification data
        if gc_results:
            logger.debug(f"Found {len(gc_results)} GC results for merging")
        else:
            logger.debug("No GC results found for merging")
        
        # Merge classification data into each result
        for result in results:
            rider_url = result.get('rider_url')
            
            # Add GC data
            gc_data = gc_results.get(rider_url, {})
            result['gc_rank'] = gc_data.get('rank')
            result['gc_uci_points'] = gc_data.get('uci_points')
            
            # Add points data
            points_data = points_results.get(rider_url, {})
            result['points_rank'] = points_data.get('rank')
            result['points_uci_points'] = points_data.get('uci_points')
            
            # Add KOM data
            kom_data = kom_results.get(rider_url, {})
            result['kom_rank'] = kom_data.get('rank')
            result['kom_uci_points'] = kom_data.get('uci_points')
            
            # Add youth data
            youth_data = youth_results.get(rider_url, {})
            result['youth_rank'] = youth_data.get('rank')
            result['youth_uci_points'] = youth_data.get('uci_points')
    
    async def _fetch_classification_data(self, classification_url: str) -> List[Dict[str, Any]]:
        """Fetch and parse classification data from a classification URL"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, classification_url)
        
        logger.debug(f"Fetching classification data from: {full_url}")
        
        html_content = await self.make_request(full_url)
        if not html_content:
            logger.debug(f"No HTML content received for classification URL: {full_url}")
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the correct classification table
            # For classification pages, we need to find the table with classification data,
            # not the stage results table
            results_table = self._find_classification_table(soup, classification_url)
            if results_table:
                results = self.parse_results_table(results_table, secondary=True)
                logger.debug(f"Parsed {len(results)} classification results from {classification_url}")
                return results
            else:
                logger.debug(f"No classification results table found in {classification_url}")
                return []
                
        except Exception as e:
            logger.error(f"Error parsing classification data from {classification_url}: {e}")
            return []
    
    def _find_classification_table(self, soup, classification_url: str):
        """Find the correct classification table on a classification page"""
        # Get all tables with results class
        all_tables = soup.find_all('table', class_='results')
        
        # Determine what type of classification this is
        classification_type = None
        if '/gc' in classification_url:
            classification_type = 'gc'
        elif '/points' in classification_url:
            classification_type = 'points'
        elif '/kom' in classification_url:
            classification_type = 'kom'
        elif '/youth' in classification_url:
            classification_type = 'youth'
        
        logger.debug(f"Looking for {classification_type} table in {len(all_tables)} tables")
        
        # For each table, analyze its headers to determine if it's the right classification table
        for i, table in enumerate(all_tables):
            table_score = self._score_classification_table(table, classification_type)
            logger.debug(f"Table {i+1} score: {table_score}")
            
            # If this table has a good score for the classification type, use it
            if table_score > 0.7:  # Threshold for confidence
                logger.debug(f"Selected table {i+1} for {classification_type} (score: {table_score})")
                return table
        
        # If no table meets the threshold, fall back to the second table if available
        # (first table is often stage results, second is often classification)
        if len(all_tables) >= 2:
            logger.debug(f"Using fallback table 2 for {classification_type}")
            return all_tables[1]
        elif len(all_tables) >= 1:
            logger.debug(f"Using fallback table 1 for {classification_type}")
            return all_tables[0]
        
        return None
    
    def _score_classification_table(self, table, classification_type: str) -> float:
        """Score how likely a table is to be the correct classification table"""
        score = 0.0
        
        # Find headers
        headers = []
        header_row = table.find('thead')
        if not header_row:
            header_row = table.find('tr')
        
        if header_row:
            header_cells = header_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True).lower() for cell in header_cells]
        
        # Check for classification-specific indicators
        header_text = ' '.join(headers)
        
        # Classification tables should have:
        # - Rank column
        if any('rnk' in h or 'rank' in h for h in headers):
            score += 0.3
        
        # - Time-related columns for GC
        if classification_type == 'gc':
            if any('time' in h for h in headers):
                score += 0.2
            # Previous rank column is a strong indicator
            if any('prev' in h for h in headers):  
                score += 0.4
            # GC tables often have arrows for rank changes (very strong indicator)
            if '▼▲' in header_text or '▲' in header_text or '▼' in header_text:
                score += 0.4
            # Time won/lost is specific to GC tables
            if any('time won' in h or 'time lost' in h for h in headers):
                score += 0.3
            # If this looks like a stage result table (has GC column), penalize it
            if any('gc' == h.lower() for h in headers):  # Exact match to 'gc' column
                score -= 0.5
        
        # - Points for points/kom classifications
        elif classification_type in ['points', 'kom']:
            if any('pnt' in h or 'point' in h for h in headers):
                score += 0.4
        
        # Check first few data rows for classification indicators
        data_rows = table.find_all('tr')[1:4]  # Skip header, check first 3 rows
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) > 0:
                # Check if first cell looks like a rank (1, 2, 3, etc.)
                first_cell = cells[0].get_text(strip=True)
                if first_cell.isdigit() and int(first_cell) <= 10:
                    score += 0.1
                    
                # For GC, check for time gaps
                if classification_type == 'gc':
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        if ':' in cell_text and ('+' in cell_text or 'h' in cell_text):
                            score += 0.1
                            break
        
        return min(score, 1.0)  # Cap at 1.0
    
    async def init_database(self):
        """Initialize SQLite database with improved schema"""
        async with aiosqlite.connect(self.config.database_path) as db:
            # Create races table with improved schema
            await db.execute('''
                CREATE TABLE IF NOT EXISTS races (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    race_name TEXT NOT NULL,
                    race_category TEXT,
                    uci_tour TEXT,
                    race_key TEXT UNIQUE NOT NULL,
                    stage_url TEXT,
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
            
            # Create indexes for better performance
            await db.execute('CREATE INDEX IF NOT EXISTS idx_races_year ON races(year)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_races_key ON races(race_key)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_stages_race_id ON stages(race_id)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_results_stage_id ON results(stage_id)')
            
            # Add unique constraint for results to prevent duplicates
            # Note: This will be applied after cleaning existing duplicates
            try:
                await db.execute('''
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_results_unique 
                    ON results(stage_id, rider_name, rank, time, status)
                ''')
                logger.debug("Unique constraint on results created successfully")
            except Exception as e:
                logger.warning(f"Could not create unique constraint on results (duplicates may exist): {e}")
            
            # Performance index for duplicate checking
            await db.execute('CREATE INDEX IF NOT EXISTS idx_results_lookup ON results(stage_id, rider_name)')
            
            await db.commit()
            logger.info(f"Database initialized at {self.config.database_path}")
    
    async def make_request(self, url: str, max_retries: int = None) -> Optional[str]:
        """Make HTTP request with retry logic and improved error handling"""
        max_retries = max_retries or self.config.max_retries
        
        async with self.semaphore:
            for attempt in range(max_retries + 1):
                try:
                    self.stats.total_requests += 1
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            self.stats.successful_requests += 1
                            return await response.text()
                        elif response.status == 404:
                            logger.warning(f"Page not found: {url}")
                            return None
                        elif response.status == 429:
                            wait_time = (attempt + 1) * self.config.retry_delay
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            if attempt < max_retries:
                                await asyncio.sleep(self.config.retry_delay)
                                continue
                            return None
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {url}, attempt {attempt + 1}")
                    if attempt < max_retries:
                        await asyncio.sleep(self.config.retry_delay)
                        continue
                    return None
                except Exception as e:
                    logger.error(f"Request error for {url}: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(self.config.retry_delay)
                        continue
                    return None
            
            self.stats.failed_requests += 1
            return None
    
    async def get_races(self, year: int) -> List[str]:
        """Get list of race URLs for a given year with improved deduplication"""
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
            min_expected_races = 10 if year >= 1980 else (5 if year >= 1950 else 1)
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
        """Get race information and stage URLs with improved category detection"""
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
            
            # Store classification URLs for later use (don't add to stage_urls)
            # We'll use these to enhance final stage data with GC information
            self._current_race_classification_urls = self.generate_classification_urls(race_url, stage_urls) if len(stage_urls) > 1 else []
            
            # Extract year from race_url for historical context
            year = None
            try:
                year = int(race_url.split('/')[-1])
            except:
                pass
            
            # Improved category detection
            detected_category = self.detect_race_category(race_name, uci_tour, stage_urls)
            if detected_category != "Unknown":
                race_category = detected_category
            
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
    
    async def save_race_data(self, year: int, race_data: Dict[str, Any]) -> Optional[int]:
        """Save race data to SQLite database with improved duplicate prevention"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Generate unique race key
                race_key = self.generate_race_key(year, race_data['race_name'])
                
                # Check if race already exists - try both new and old race key formats
                cursor = await db.execute(
                    'SELECT id FROM races WHERE race_key = ?',
                    (race_key,)
                )
                existing_race = await cursor.fetchone()
                
                # Also check for old race key format (with » prefix) in case of legacy data
                if not existing_race:
                    old_race_key = f"{year}_»{race_data['race_name']}"
                    cursor = await db.execute(
                        'SELECT id FROM races WHERE race_key = ?',
                        (old_race_key,)
                    )
                    existing_race = await cursor.fetchone()
                
                # As a final fallback, check by stage_url (since that's what's causing the constraint failure)
                if not existing_race and race_data['stage_urls']:
                    cursor = await db.execute(
                        'SELECT id FROM races WHERE stage_url = ? AND year = ?',
                        (race_data['stage_urls'][0], year)
                    )
                    existing_race = await cursor.fetchone()
                
                if existing_race:
                    # Race already exists
                    if self.config.overwrite_existing_data:
                        # Delete existing race and all associated data
                        logger.debug(f"Race exists, overwriting: {race_data['race_name']} ({year})")
                        race_id = existing_race[0]
                        
                        # Delete in correct order due to foreign keys
                        await db.execute('DELETE FROM results WHERE stage_id IN (SELECT id FROM stages WHERE race_id = ?)', (race_id,))
                        await db.execute('DELETE FROM stages WHERE race_id = ?', (race_id,))
                        await db.execute('DELETE FROM races WHERE id = ?', (race_id,))
                        await db.commit()
                        
                        # Continue to insert new race data below
                    else:
                        # Skip overwrite, return existing ID
                        logger.debug(f"Race already exists: {race_data['race_name']} ({year})")
                        return existing_race[0]
                
                # Insert new race record
                race_cursor = await db.execute('''
                    INSERT INTO races (year, race_name, race_category, uci_tour, race_key, stage_url)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    year,
                    race_data['race_name'],
                    race_data['race_category'],
                    race_data['uci_tour'],
                    race_key,
                    race_data['stage_urls'][0] if race_data['stage_urls'] else ''
                ))
                
                race_id = race_cursor.lastrowid
                await db.commit()
                
                logger.debug(f"Saved new race: {race_data['race_name']} ({year}) with ID {race_id}")
                return race_id
                
            except Exception as e:
                logger.error(f"Error saving race data: {e}")
                return None
    
    async def save_stage_data(self, race_id: int, stage_data: Dict[str, Any]) -> Optional[int]:
        """Save stage data to SQLite database with overwrite control"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Check if stage already exists
                cursor = await db.execute(
                    'SELECT id FROM stages WHERE stage_url = ?',
                    (stage_data['stage_url'],)
                )
                existing_stage = await cursor.fetchone()
                
                if existing_stage:
                    # Stage already exists
                    should_overwrite = (self.config.overwrite_existing_data or 
                                      self.config.overwrite_stages)
                    
                    if not should_overwrite:
                        # Skip overwrite, return existing ID
                        logger.debug(f"Stage exists, skipping: {stage_data['stage_url']}")
                        return existing_stage[0]
                    else:
                        # Update existing stage
                        logger.debug(f"Stage exists, overwriting: {stage_data['stage_url']}")
                        await db.execute('''
                            UPDATE stages SET
                                race_id = ?, is_one_day_race = ?, distance = ?, stage_type = ?,
                                winning_attack_length = ?, date = ?, won_how = ?, avg_speed_winner = ?,
                                avg_temperature = ?, vertical_meters = ?, profile_icon = ?, 
                                profile_score = ?, race_startlist_quality_score = ?
                            WHERE stage_url = ?
                        ''', (
                            race_id, stage_data['is_one_day_race'], stage_data['distance'],
                            stage_data['stage_type'], stage_data['winning_attack_length'],
                            stage_data['date'], stage_data['won_how'], stage_data['avg_speed_winner'],
                            stage_data['avg_temperature'], stage_data['vertical_meters'],
                            stage_data['profile_icon'], stage_data['profile_score'],
                            stage_data['race_startlist_quality_score'], stage_data['stage_url']
                        ))
                        await db.commit()
                        return existing_stage[0]
                
                # Insert stage record
                stage_cursor = await db.execute('''
                    INSERT INTO stages (
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
                await db.commit()
                return stage_id
                
            except Exception as e:
                logger.error(f"Error saving stage data: {e}")
                return None
    
    async def save_results_data(self, stage_id: int, stage_data: Dict[str, Any]):
        """Save results data to SQLite database with duplicate prevention and overwrite control"""
        async with aiosqlite.connect(self.config.database_path) as db:
            try:
                # Check if results already exist for this stage
                cursor = await db.execute(
                    'SELECT COUNT(*) FROM results WHERE stage_id = ?',
                    (stage_id,)
                )
                existing_count = await cursor.fetchone()
                
                if existing_count[0] > 0:
                    should_overwrite = (self.config.overwrite_existing_data or 
                                      self.config.overwrite_results)
                    
                    if not should_overwrite:
                        logger.debug(f"Results already exist for stage {stage_id}, skipping (overwrite=False)")
                        return
                    else:
                        # Delete existing results for this stage before inserting new ones
                        logger.debug(f"Results exist for stage {stage_id}, overwriting (overwrite=True)")
                        await db.execute('DELETE FROM results WHERE stage_id = ?', (stage_id,))
                
                # Prepare results with secondary classifications
                results = stage_data.get('results', [])
                gc_results = {r.get('rider_url'): r for r in stage_data.get('gc', [])}
                points_results = {r.get('rider_url'): r for r in stage_data.get('points', [])}
                kom_results = {r.get('rider_url'): r for r in stage_data.get('kom', [])}
                youth_results = {r.get('rider_url'): r for r in stage_data.get('youth', [])}
                
                if not results:
                    logger.warning(f"No results found for stage {stage_id}")
                    return
                
                for result in results:
                    rider_url = result.get('rider_url')
                    
                    # Merge secondary classification data
                    gc_data = gc_results.get(rider_url, {})
                    points_data = points_results.get(rider_url, {})
                    kom_data = kom_results.get(rider_url, {})
                    youth_data = youth_results.get(rider_url, {})
                    
                    # Insert result record (with OR IGNORE as safety against duplicates)
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
                        rider_url,
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
                
                await db.commit()
                logger.debug(f"Saved {len(results)} results for stage {stage_id}")
                
            except Exception as e:
                logger.error(f"Error saving results data: {e}")
    
    async def get_stage_info(self, stage_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed stage information and results"""
        base_url = 'https://www.procyclingstats.com/'
        full_url = urljoin(base_url, stage_url)
        
        html_content = await self.make_request(full_url)
        if not html_content:
            return None
        
        try:
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
                        
                        # Extract distance
                        if 'distance' in title:
                            try:
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
                                temp_str = ''.join(c for c in value if c.isdigit() or c == '.')
                                if temp_str:
                                    stage_info['avg_temperature'] = float(temp_str)
                            except:
                                pass
            
            # Extract results from tables
            results_table = soup.find('table', class_='results')
            if results_table:
                stage_info['results'] = self.parse_results_table(results_table)
            
            # Extract secondary classifications from separate URLs
            # Only fetch for final stages to get complete GC standings
            if self._should_fetch_classifications(stage_url):
                await self._fetch_and_merge_classifications(stage_info)
                
                # Merge classification data into stage results for test compatibility
                self._merge_classifications_into_results(stage_info)
            
            return stage_info
            
        except Exception as e:
            enhanced_logger.log_scraping_error(
                stage="get_stage_info",
                url=full_url,
                error=e,
                html_content=html_content,
                expected_elements=['table.results', 'ul.keyvalueList'],
                context={'stage_url': stage_url}
            )
            return None
    
    def parse_results_table(self, table, secondary=False) -> List[Dict[str, Any]]:
        """Parse results table from HTML"""
        results = []
        
        try:
            rows = table.find_all('tr')
            
            # Check if first row is actually data (has rider links) or header
            start_row = 0
            if rows:
                first_row = rows[0]
                first_row_cells = first_row.find_all(['td', 'th'])
                has_rider_links = False
                for cell in first_row_cells:
                    if cell.find('a', href=lambda x: x and 'rider/' in x):
                        has_rider_links = True
                        break
                
                # If first row has no rider links, it's likely a header
                if not has_rider_links:
                    start_row = 1
            
            rows = rows[start_row:]  # Skip header if detected
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue
                
                result = {}
                
                if secondary:
                    # Classification pages have different structure
                    # Rank is in cell 0, rider info typically in cell with links
                    rider_info_found = False
                    
                    # Extract rank from cell 0
                    try:
                        result['rank'] = int(cells[0].get_text(strip=True))
                    except:
                        result['rank'] = None
                    
                    # Search for rider and team links in all cells
                    rider_links = []
                    team_links = []
                    
                    for cell in cells:
                        links = cell.find_all('a')
                        for link in links:
                            href = link.get('href', '')
                            if 'rider/' in href:
                                rider_links.append(link)
                            elif 'team/' in href:
                                team_links.append(link)
                    
                    # Extract rider info
                    if rider_links:
                        rider_link = rider_links[0]  # Take first rider link
                        raw_rider_name = rider_link.get_text(strip=True)
                        result['rider_name'] = self.format_rider_name(raw_rider_name)
                        result['rider_url'] = rider_link.get('href', '')
                        rider_info_found = True
                    
                    # Extract team info
                    if team_links:
                        team_link = team_links[0]  # Take first team link
                        result['team_name'] = team_link.get_text(strip=True)
                        result['team_url'] = team_link.get('href', '')
                    else:
                        result['team_name'] = ''
                        result['team_url'] = ''
                    
                    # If no rider info found, skip this row
                    if not rider_info_found:
                        continue
                        
                else:
                    # Regular stage results - use same structure as secondary (modern PCS format)
                    # Extract rank from cell 0 
                    try:
                        result['rank'] = int(cells[0].get_text(strip=True))
                    except:
                        result['rank'] = None
                    
                    # Search for rider and team links in all cells (same as secondary logic)
                    rider_links = []
                    team_links = []
                    
                    for cell in cells:
                        links = cell.find_all('a')
                        for link in links:
                            href = link.get('href', '')
                            if 'rider/' in href:
                                rider_links.append(link)
                            elif 'team/' in href:
                                team_links.append(link)
                    
                    # Extract rider info
                    if rider_links:
                        rider_link = rider_links[0]  # Take first rider link
                        raw_rider_name = rider_link.get_text(strip=True)
                        result['rider_name'] = self.format_rider_name(raw_rider_name)
                        result['rider_url'] = rider_link.get('href', '')
                    else:
                        # Skip rows without rider info
                        continue
                    
                    # Extract team info
                    if team_links:
                        team_link = team_links[0]  # Take first team link
                        result['team_name'] = team_link.get_text(strip=True)
                        result['team_url'] = team_link.get('href', '')
                    else:
                        result['team_name'] = ''
                        result['team_url'] = ''
                
                # Extract time gap
                if len(cells) > 3:
                    time_cell = cells[3] if not secondary else cells[2]
                    result['time'] = time_cell.get_text(strip=True)
                
                # Extract UCI and PCS points with header-based column identification
                result['uci_points'] = None
                result['pcs_points'] = None
                
                # Get table header to identify column positions
                table = row.find_parent('table')
                if table:
                    header_row = table.find('thead')
                    if not header_row:
                        header_row = table.find('tr')  # First row might be header
                    
                    if header_row:
                        header_cells = header_row.find_all(['th', 'td'])
                        uci_col_idx = None
                        pcs_col_idx = None
                        
                        # Look for UCI and PCS column headers
                        for idx, header_cell in enumerate(header_cells):
                            header_text = header_cell.get_text(strip=True).lower()
                            if 'uci' in header_text:
                                uci_col_idx = idx
                            elif 'pcs' in header_text:
                                pcs_col_idx = idx
                        
                        # Extract points using identified column positions
                        if uci_col_idx is not None and len(cells) > uci_col_idx:
                            try:
                                uci_text = cells[uci_col_idx].get_text(strip=True)
                                if uci_text and uci_text.isdigit():
                                    result['uci_points'] = int(uci_text)
                            except (ValueError, IndexError):
                                pass
                        
                        if pcs_col_idx is not None and len(cells) > pcs_col_idx:
                            try:
                                pcs_text = cells[pcs_col_idx].get_text(strip=True)
                                if pcs_text and pcs_text.isdigit():
                                    result['pcs_points'] = int(pcs_text)
                            except (ValueError, IndexError):
                                pass
                
                # Fallback: try the old fixed position approach if header-based didn't work
                if result['uci_points'] is None and len(cells) > 4:
                    try:
                        uci_cell = cells[4] if not secondary else cells[3]
                        uci_text = uci_cell.get_text(strip=True)
                        if uci_text and uci_text.isdigit():
                            result['uci_points'] = int(uci_text)
                    except (ValueError, IndexError):
                        pass
                
                if result['pcs_points'] is None and len(cells) > 5:
                    try:
                        pcs_cell = cells[5] if not secondary else cells[4]
                        pcs_text = pcs_cell.get_text(strip=True)
                        if pcs_text and pcs_text.isdigit():
                            result['pcs_points'] = int(pcs_text)
                    except (ValueError, IndexError):
                        pass
                
                results.append(result)
                
        except Exception as e:
            logger.error(f"Error parsing results table: {e}")
        
        return results
    
    async def scrape_year_with_progress(self, year: int):
        """Scrape a single year with progress tracking and improved error handling"""
        logger.info(f"Starting to scrape year {year}")
        
        try:
            # Get all races for the year
            race_urls = await self.get_races(year)
            if not race_urls:
                logger.warning(f"No races found for year {year}")
                return
            
            # Process races in batches
            batch_size = 10
            total_races = len(race_urls)
            total_stages = 0
            total_results = 0
            
            for i in range(0, total_races, batch_size):
                batch = race_urls[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (total_races + batch_size - 1) // batch_size
                
                logger.info(f"Processing race batch {batch_num}/{total_batches}")
                
                # Get race info for all races in batch
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
                        
                        total_results += race_results
                        
                        # Add delay between races
                        await asyncio.sleep(self.config.request_delay)
                        
                    except Exception as e:
                        logger.error(f"Error processing race {race_url}: {e}")
                        if self.progress_tracker:
                            await self.progress_tracker.mark_race_failed(race_url, str(e))
                        continue
                
                # Checkpoint and backup
                current_time = time.time()
                if current_time - self.last_checkpoint >= self.checkpoint_interval:
                    if self.progress_tracker:
                        await self.progress_tracker.create_checkpoint(f"Processing year {year}")
                    self.last_checkpoint = current_time
            
            logger.info(f"Completed scraping {year}: {total_stages} stages in {self.stats.elapsed_time:.2f}s")
            logger.info(f"Stats: {self.stats.successful_requests}/{self.stats.total_requests} requests successful ({self.stats.success_rate:.1f}%)")
            
            # Final checkpoint
            if self.progress_tracker:
                await self.progress_tracker.create_checkpoint(f"Completed year {year}")
            
            logger.info(f"✅ Year {year} completed successfully")
            
        except Exception as e:
            logger.error(f"Error scraping year {year}: {e}")
            if self.progress_tracker:
                await self.progress_tracker.mark_year_failed(year, str(e))
            raise
    
    async def scrape_years_with_progress(self, years: List[int]):
        """Scrape multiple years with progress tracking"""
        for year in years:
            await self.scrape_year_with_progress(year)
        
        logger.info("🏁 Scraping completed. Total stats: {}/{} requests successful".format(
            self.stats.successful_requests, self.stats.total_requests
        ))
        
        # Final checkpoint
        if self.progress_tracker:
            await self.progress_tracker.create_checkpoint("Final completion checkpoint") 