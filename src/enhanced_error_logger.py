#!/usr/bin/env python3
"""
Enhanced error logging and diagnostic tools for the cycling data scraper
Provides detailed error information to help diagnose and fix scraping issues
"""

import logging
import json
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

@dataclass
class ScrapingDiagnostic:
    """Detailed diagnostic information for scraping failures"""
    timestamp: datetime = field(default_factory=datetime.now)
    stage: str = ""  # get_races, get_race_info, get_stage_info, parse_results
    url: str = ""
    error_type: str = ""
    error_message: str = ""
    stack_trace: str = ""
    html_preview: Optional[str] = None
    expected_elements: List[str] = field(default_factory=list)
    found_elements: List[str] = field(default_factory=list)
    css_selectors_tested: List[Dict[str, Any]] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)
    
class EnhancedErrorLogger:
    """Enhanced error logging with diagnostic capabilities"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.diagnostics: List[ScrapingDiagnostic] = []
    
    def log_scraping_error(self, 
                          stage: str,
                          url: str, 
                          error: Exception, 
                          html_content: Optional[str] = None,
                          expected_elements: List[str] = None,
                          context: Dict[str, Any] = None) -> ScrapingDiagnostic:
        """Log a detailed scraping error with diagnostic information"""
        
        diagnostic = ScrapingDiagnostic(
            stage=stage,
            url=url,
            error_type=type(error).__name__,
            error_message=str(error),
            stack_trace=traceback.format_exc(),
            expected_elements=expected_elements or []
        )
        
        # Analyze HTML content if available
        if html_content:
            diagnostic.html_preview = self._create_html_preview(html_content)
            diagnostic.found_elements = self._find_available_elements(html_content)
            diagnostic.css_selectors_tested = self._test_css_selectors(html_content, stage)
        
        # Generate suggestions based on the error
        diagnostic.suggestions = self._generate_suggestions(diagnostic, context)
        
        # Log the error
        logger.error(f"SCRAPING ERROR - Stage: {stage}")
        logger.error(f"URL: {url}")
        logger.error(f"Error: {diagnostic.error_type} - {diagnostic.error_message}")
        
        if diagnostic.suggestions:
            logger.error("Suggestions for fixing:")
            for suggestion in diagnostic.suggestions:
                logger.error(f"  • {suggestion}")
        
        # Store diagnostic
        self.diagnostics.append(diagnostic)
        
        # Save to file
        self._save_diagnostic_to_file(diagnostic)
        
        return diagnostic
    
    def _create_html_preview(self, html_content: str, max_length: int = 1000) -> str:
        """Create a preview of HTML content around key elements"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for key elements that might indicate the page structure
            key_selectors = [
                'table.results',
                'h1',
                '.infolist',
                'table tr',
                '.classification',
                '.timeline2'
            ]
            
            preview_parts = []
            
            for selector in key_selectors:
                elements = soup.select(selector)
                if elements:
                    element = elements[0]
                    preview_parts.append(f"\n--- {selector} ---")
                    preview_parts.append(str(element)[:200] + "..." if len(str(element)) > 200 else str(element))
            
            if not preview_parts:
                # Fallback: show beginning of body or html
                body = soup.find('body') or soup
                preview_parts.append(str(body)[:max_length])
            
            return '\n'.join(preview_parts)
            
        except Exception as e:
            return f"HTML preview failed: {e}"
    
    def _find_available_elements(self, html_content: str) -> List[str]:
        """Find available elements in the HTML that might be useful"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            found_elements = []
            
            # Check for tables
            tables = soup.find_all('table')
            for i, table in enumerate(tables):
                classes = ' '.join(table.get('class', []))
                id_attr = table.get('id', '')
                found_elements.append(f"table[{i}] - class: '{classes}' id: '{id_attr}'")
            
            # Check for links with specific patterns
            links = soup.find_all('a', href=True)
            race_links = [link for link in links if 'race/' in link['href']]
            rider_links = [link for link in links if 'rider/' in link['href']]
            team_links = [link for link in links if 'team/' in link['href']]
            
            found_elements.append(f"race links: {len(race_links)}")
            found_elements.append(f"rider links: {len(rider_links)}")
            found_elements.append(f"team links: {len(team_links)}")
            
            # Check for classification elements
            classifications = soup.find_all(class_='classification')
            found_elements.append(f"classification elements: {len(classifications)}")
            
            # Check for info lists
            infolists = soup.find_all(class_='infolist')
            found_elements.append(f"infolist elements: {len(infolists)}")
            
            return found_elements
            
        except Exception as e:
            return [f"Element analysis failed: {e}"]
    
    def _test_css_selectors(self, html_content: str, stage: str) -> List[Dict[str, Any]]:
        """Test various CSS selectors to see what's available"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Define selectors to test based on the stage
            selector_tests = []
            
            if stage == "get_races":
                test_selectors = [
                    'table tr a[href]',
                    'table a[href*="race/"]',
                    '.results a',
                    'tbody tr a'
                ]
            elif stage == "get_race_info":
                test_selectors = [
                    'h1',
                    '.classification',
                    'a[href*="/stage-"]',
                    'a[href$="/result"]',
                    '.infolist'
                ]
            elif stage == "get_stage_info":
                test_selectors = [
                    'table.results',
                    'table tr',
                    'a[href*="rider/"]',
                    'a[href*="team/"]',
                    '.infolist li'
                ]
            else:
                test_selectors = [
                    'table',
                    'tr',
                    'a[href]',
                    '.results',
                    '.classification'
                ]
            
            for selector in test_selectors:
                elements = soup.select(selector)
                selector_tests.append({
                    'selector': selector,
                    'count': len(elements),
                    'sample': str(elements[0])[:100] + "..." if elements else None
                })
            
            return selector_tests
            
        except Exception as e:
            return [{'error': f"Selector testing failed: {e}"}]
    
    def _generate_suggestions(self, diagnostic: ScrapingDiagnostic, context: Dict[str, Any] = None) -> List[str]:
        """Generate actionable suggestions for fixing the error"""
        suggestions = []
        
        # URL-based suggestions
        if 'qinghai' in diagnostic.url.lower():
            suggestions.append("This appears to be a Qinghai race - these are known to have formatting issues. Consider adding to exclusion list.")
        
        if diagnostic.stage == "get_races":
            if "insufficient_results" in diagnostic.error_type.lower():
                suggestions.append("Website may have changed race listing format. Check the CSS selectors for race links.")
                suggestions.append("Verify the URL parameters for the race listing pages are still correct.")
            elif "connection" in diagnostic.error_type.lower():
                suggestions.append("Network connectivity issue. Check internet connection and procyclingstats.com availability.")
                
        elif diagnostic.stage == "get_race_info":
            if not diagnostic.found_elements or all('0' in elem for elem in diagnostic.found_elements if 'links:' in elem):
                suggestions.append("No race/stage links found. The website may have changed its link structure.")
                suggestions.append("Check if the race URL format has changed (e.g., race/name/year vs race/name-year).")
            suggestions.append("Verify that h1 element still contains race name.")
            suggestions.append("Check if classification elements have changed class names.")
                
        elif diagnostic.stage == "get_stage_info":
            if "table" not in ' '.join(diagnostic.found_elements).lower():
                suggestions.append("No results table found. Website may have changed from table-based to different layout.")
                suggestions.append("Check if results are now in div/list format instead of table.")
            suggestions.append("Verify CSS selector 'table.results' is still correct.")
            suggestions.append("Check if rider/team link formats have changed.")
                
        elif diagnostic.stage == "parse_results":
            suggestions.append("Results table structure may have changed. Check column order and content.")
            suggestions.append("Verify rider name and team name extraction logic.")
            suggestions.append("Check if rank column format has changed.")
        
        # Historical year suggestions
        if "year" in diagnostic.url:
            year_in_url = None
            for potential_year in range(1903, 2030):
                if str(potential_year) in diagnostic.url:
                    year_in_url = potential_year
                    break
            
            if year_in_url and year_in_url < 1950:
                suggestions.append(f"This is a very historical year ({year_in_url}) - data may be sparse or in different format.")
                suggestions.append("Very early cycling data may not exist on procyclingstats.com.")
                suggestions.append("Consider checking if this race/year actually exists in the database.")
            elif year_in_url and year_in_url < 1980:
                suggestions.append(f"This is an older year ({year_in_url}) - data format may be simplified.")
                suggestions.append("Historical data may have fewer fields (teams, times, etc.).")
            elif year_in_url and year_in_url < 2000:
                suggestions.append(f"This is pre-2000 data ({year_in_url}) - format may differ from modern races.")
        
        # General suggestions
        if diagnostic.error_type == "TimeoutError":
            suggestions.append("Request timed out. Consider increasing timeout or reducing concurrent requests.")
        
        # HTML-based suggestions
        if diagnostic.html_preview and "404" in diagnostic.html_preview:
            suggestions.append("Page not found (404). Race URL may be incorrect or race may not exist for this year.")
            if "year" in diagnostic.url and any(str(y) in diagnostic.url for y in range(1903, 1950)):
                suggestions.append("This very early year may not have data available - many races didn't exist yet.")
        
        if diagnostic.html_preview and "access denied" in diagnostic.html_preview.lower():
            suggestions.append("Access denied. IP may be rate-limited. Consider reducing request frequency.")
        
        return suggestions
    
    def _save_diagnostic_to_file(self, diagnostic: ScrapingDiagnostic):
        """Save diagnostic information to a file"""
        timestamp_str = diagnostic.timestamp.strftime('%Y%m%d_%H%M%S_%f')[:-3]
        filename = f"diagnostic_{diagnostic.stage}_{timestamp_str}.json"
        filepath = self.log_dir / filename
        
        # Convert to serializable format
        diagnostic_dict = {
            'timestamp': diagnostic.timestamp.isoformat(),
            'stage': diagnostic.stage,
            'url': diagnostic.url,
            'error_type': diagnostic.error_type,
            'error_message': diagnostic.error_message,
            'stack_trace': diagnostic.stack_trace,
            'html_preview': diagnostic.html_preview,
            'expected_elements': diagnostic.expected_elements,
            'found_elements': diagnostic.found_elements,
            'css_selectors_tested': diagnostic.css_selectors_tested,
            'suggestions': diagnostic.suggestions
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(diagnostic_dict, f, indent=2, default=str)
            logger.info(f"📄 Diagnostic saved to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save diagnostic: {e}")
    
    def generate_summary_report(self) -> str:
        """Generate a summary report of all diagnostics"""
        if not self.diagnostics:
            return "No diagnostics to report."
        
        report_lines = [
            "SCRAPING DIAGNOSTICS SUMMARY",
            "=" * 40,
            f"Total Errors: {len(self.diagnostics)}",
            f"Time Range: {self.diagnostics[0].timestamp} to {self.diagnostics[-1].timestamp}",
            ""
        ]
        
        # Group by stage
        stage_counts = {}
        for diag in self.diagnostics:
            stage_counts[diag.stage] = stage_counts.get(diag.stage, 0) + 1
        
        report_lines.append("Errors by Stage:")
        for stage, count in stage_counts.items():
            report_lines.append(f"  {stage}: {count}")
        
        report_lines.append("")
        
        # Group by error type
        error_counts = {}
        for diag in self.diagnostics:
            error_counts[diag.error_type] = error_counts.get(diag.error_type, 0) + 1
        
        report_lines.append("Errors by Type:")
        for error_type, count in error_counts.items():
            report_lines.append(f"  {error_type}: {count}")
        
        report_lines.append("")
        report_lines.append("Most Common Suggestions:")
        
        # Collect all suggestions
        all_suggestions = []
        for diag in self.diagnostics:
            all_suggestions.extend(diag.suggestions)
        
        suggestion_counts = {}
        for suggestion in all_suggestions:
            suggestion_counts[suggestion] = suggestion_counts.get(suggestion, 0) + 1
        
        # Show top 5 suggestions
        top_suggestions = sorted(suggestion_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for suggestion, count in top_suggestions:
            report_lines.append(f"  ({count}x) {suggestion}")
        
        return '\n'.join(report_lines)

# Global error logger instance
enhanced_logger = EnhancedErrorLogger() 