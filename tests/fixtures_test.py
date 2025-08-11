#!/usr/bin/env python3
import asyncio
import sys
from typing import Dict, Any

# Ensure src is importable when running from project root
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from async_scraper import AsyncCyclingDataScraper, ScrapingConfig
from tests.urls import TARGET_URLS, BASE_URL
from tests.fixture_utils import read_fixture, expected_path
import json


def compare_expected(slug: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
  """Compare expected vs actual data and return detailed statistics"""
  p = expected_path(slug)
  if not p.exists():
    return {"error": "No expected file found"}
  
  expected = json.loads(p.read_text(encoding="utf-8"))
  
  stats = {
    "total_fields": 0,
    "correct": 0,
    "incorrect": 0, 
    "missing": 0,
    "errors": [],
    "correct_fields": [],
    "incorrect_fields": [],
    "missing_fields": []
  }
  
  def check_field(field_path: str, expected_val: Any, actual_val: Any):
    stats["total_fields"] += 1
    if actual_val is None:
      stats["missing"] += 1
      stats["missing_fields"].append(field_path)
      stats["errors"].append(f"{field_path} missing: None != {expected_val}")
    elif actual_val == expected_val:
      stats["correct"] += 1
      stats["correct_fields"].append(field_path)
    else:
      stats["incorrect"] += 1
      stats["incorrect_fields"].append(field_path)
      stats["errors"].append(f"{field_path} incorrect: {actual_val} != {expected_val}")
  
  # Check all top-level fields
  for field, expected_val in expected.items():
    if field == "results":
      continue  # Handle results separately
    
    actual_val = parsed.get(field)
    check_field(field, expected_val, actual_val)
  
  # Check results structure in detail
  if "results" in expected and expected["results"]:
    expected_results = expected["results"]
    actual_results = parsed.get("results", [])
    
    # Check if results exist at all
    if not actual_results:
      stats["total_fields"] += 1
      stats["missing"] += 1
      stats["missing_fields"].append("results")
      stats["errors"].append("results missing: [] != expected results")
    else:
      # Check first result fields in detail
      if expected_results:
        expected_first = expected_results[0]
        actual_first = actual_results[0] if actual_results else {}
        
        for field, expected_val in expected_first.items():
          actual_val = actual_first.get(field)
          check_field(f"results[0].{field}", expected_val, actual_val)
        
        # Check second result if it exists in expected
        if len(expected_results) > 1 and len(actual_results) > 1:
          expected_second = expected_results[1]
          actual_second = actual_results[1]
          
          for field, expected_val in expected_second.items():
            actual_val = actual_second.get(field)
            check_field(f"results[1].{field}", expected_val, actual_val)
  
  return stats


async def run() -> int:
  print("[fixtures-only] Parsing using existing fixtures (no network)...\n")

  config = ScrapingConfig(
    max_concurrent_requests=1,
    request_delay=0.0,
    max_retries=0,
    timeout=10,
    database_path="test_cycling_data.db",
  )

  async with AsyncCyclingDataScraper(config) as scraper:
    original_make_request = scraper.make_request

    async def make_request_override(url: str, max_retries: int | None = None):
      if url.startswith(BASE_URL):
        slug = url[len(BASE_URL):]
        html = read_fixture(slug)
        if html is None:
          raise AssertionError(f"missing fixture for {slug}")
        return html
      return await original_make_request(url, max_retries)

    scraper.make_request = make_request_override  # type: ignore

    all_stats = []
    overall_stats = {"total_fields": 0, "correct": 0, "incorrect": 0, "missing": 0}
    
    for slug in TARGET_URLS:
      try:
        # Use appropriate function based on URL type
        if '/gc' in slug:
          parsed = await scraper.get_gc_info(slug)
        else:
          parsed = await scraper.get_stage_info(slug)
        
        if parsed is None:
          print(f"  - FAIL {slug}: no data parsed")
          continue
        
        if not isinstance(parsed.get("results"), list) or len(parsed["results"]) == 0:
          print(f"  - FAIL {slug}: no results parsed")
          continue
        
        first = parsed["results"][0]
        if "rider_name" not in first or not first["rider_name"]:
          print(f"  - FAIL {slug}: missing rider_name in first result")
          continue
        
        # Get detailed statistics
        stats = compare_expected(slug, parsed)
        if "error" in stats:
          print(f"  - SKIP {slug}: {stats['error']}")
          continue
        
        all_stats.append((slug, stats))
        
        # Add to overall stats
        overall_stats["total_fields"] += stats["total_fields"]
        overall_stats["correct"] += stats["correct"]
        overall_stats["incorrect"] += stats["incorrect"]
        overall_stats["missing"] += stats["missing"]
        
        # Calculate percentages for this fixture
        total = stats["total_fields"]
        if total > 0:
          correct_pct = (stats["correct"] / total) * 100
          incorrect_pct = (stats["incorrect"] / total) * 100
          missing_pct = (stats["missing"] / total) * 100
          
          print(f"  - {slug}:")
          print(f"    Fields: {stats['correct']}/{total} correct ({correct_pct:.1f}%), {stats['incorrect']} incorrect ({incorrect_pct:.1f}%), {stats['missing']} missing ({missing_pct:.1f}%)")
          
          # Show worst problems (top 5)
          if stats["errors"][:5]:
            print(f"    Top issues: {'; '.join(stats['errors'][:5])}")
        
      except Exception as e:
        print(f"  - ERROR {slug}: {e}")

    # Print overall statistics
    print(f"\n=== OVERALL SCRAPER ACCURACY ===")
    total = overall_stats["total_fields"]
    if total > 0:
      correct_pct = (overall_stats["correct"] / total) * 100
      incorrect_pct = (overall_stats["incorrect"] / total) * 100
      missing_pct = (overall_stats["missing"] / total) * 100
      
      print(f"Total Fields Tested: {total}")
      print(f"✅ Correct: {overall_stats['correct']} ({correct_pct:.1f}%)")
      print(f"❌ Incorrect: {overall_stats['incorrect']} ({incorrect_pct:.1f}%)")
      print(f"⚠️  Missing: {overall_stats['missing']} ({missing_pct:.1f}%)")
      print(f"\nAccuracy Score: {correct_pct:.1f}%")
    
    # Show most common problems across all fixtures
    all_errors = []
    all_missing = []
    for slug, stats in all_stats:
      all_errors.extend(stats["incorrect_fields"])
      all_missing.extend(stats["missing_fields"])
    
    from collections import Counter
    if all_errors:
      common_errors = Counter(all_errors).most_common(5)
      print(f"\nMost Common Incorrect Fields:")
      for field, count in common_errors:
        print(f"  - {field}: {count} fixtures affected")
    
    if all_missing:
      common_missing = Counter(all_missing).most_common(5)
      print(f"\nMost Common Missing Fields:")
      for field, count in common_missing:
        print(f"  - {field}: {count} fixtures affected")

    print(f"\n[fixtures-only] Done. {len(all_stats)} fixtures analyzed.\n")
    return 0 if correct_pct > 50 else 1  # Pass if >50% accuracy


if __name__ == "__main__":
  sys.exit(asyncio.run(run()))