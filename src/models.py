"""
Data models for cycling scraper
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

@dataclass
class RiderResult:
    """Individual rider result for a stage"""
    rider_name: str
    rider_url: str
    team_name: Optional[str] = None
    team_url: Optional[str] = None
    rank: Optional[int] = None
    status: str = "FINISHED"
    time: Optional[str] = None
    uci_points: int = 0
    pcs_points: int = 0
    age: Optional[int] = None

@dataclass
class SecondaryClassification:
    """Secondary classification result (GC, Points, KOM, Youth)"""
    rider_url: str
    rank: Optional[int] = None
    uci_points: int = 0

@dataclass
class StageInfo:
    """Stage information and results"""
    stage_url: str
    race_id: Optional[int] = None
    is_one_day_race: bool = False
    distance: Optional[float] = None
    stage_type: Optional[str] = None
    winning_attack_length: Optional[float] = None
    date: Optional[str] = None
    won_how: Optional[str] = None
    avg_speed_winner: Optional[float] = None
    avg_temperature: Optional[float] = None
    vertical_meters: Optional[int] = None
    profile_icon: Optional[str] = None
    profile_score: Optional[int] = None
    race_startlist_quality_score: Optional[int] = None
    results: List[RiderResult] = field(default_factory=list)
    gc: List[SecondaryClassification] = field(default_factory=list)
    points: List[SecondaryClassification] = field(default_factory=list)
    kom: List[SecondaryClassification] = field(default_factory=list)
    youth: List[SecondaryClassification] = field(default_factory=list)

@dataclass
class RaceInfo:
    """Race information"""
    race_name: str
    year: int
    race_category: str = "Unknown"
    uci_tour: str = "Unknown"
    stage_urls: List[str] = field(default_factory=list)
    stages: List[StageInfo] = field(default_factory=list)

@dataclass
class ScrapingSession:
    """Information about a scraping session"""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    years_scraped: List[int] = field(default_factory=list)
    total_races: int = 0
    total_stages: int = 0
    total_results: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    
    @property
    def duration(self) -> Optional[float]:
        """Duration of scraping session in seconds"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Success rate of HTTP requests"""
        total = self.successful_requests + self.failed_requests
        if total == 0:
            return 0.0
        return self.successful_requests / total * 100
