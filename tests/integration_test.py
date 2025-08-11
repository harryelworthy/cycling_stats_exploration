#!/usr/bin/env python3
import asyncio
import sys
from typing import Dict, Any

import aiohttp

# Ensure src is importable when running from project root
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from async_scraper import AsyncCyclingDataScraper, ScrapingConfig
from tests.urls import TARGET_URLS, BASE_URL
from tests.fixture_utils import (
  read_fixture,
  write_fixture,
  page_path,
  expected_path,
  normalize_html,
)
import json


HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
}


async def fetch_live_html(full_url: str, timeout_seconds: int = 45) -> str:
  timeout = aiohttp.ClientTimeout(total=timeout_seconds)
  async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS) as session:
    async with session.get(full_url) as resp:
      resp.raise_for_status()
      return await resp.text()


async def refresh_fixtures() -> None:
  print("[fixtures] Refreshing...\n")
  updated = 0
  for slug in TARGET_URLS:
    full_url = f"{BASE_URL}{slug}"
    try:
      live_html = await fetch_live_html(full_url)
    except Exception as e:
      print(f"  - SKIP {slug} (fetch failed): {e}")
      continue

    existing = read_fixture(slug)
    if existing is None or normalize_html(existing) != normalize_html(live_html):
      write_fixture(slug, live_html)
      updated += 1
      action = "created" if existing is None else "updated"
      print(f"  - {action.upper()} {page_path(slug)}")
    else:
      print(f"  - OK {page_path(slug)} (no change)")
  print(f"\n[fixtures] Done. {updated} file(s) changed.\n")


def compare_expected(slug: str, parsed: Dict[str, Any]) -> None:
  p = expected_path(slug)
  if not p.exists():
    return
  expected = json.loads(p.read_text(encoding="utf-8"))

  # Minimal, flexible checks; add more fields in JSON as desired
  if "winner" in expected:
    winner = parsed.get("results", [{}])[0].get("rider_name") if parsed.get("results") else None
    assert winner == expected["winner"], f"winner mismatch: {winner} != {expected['winner']}"

  if "result_count" in expected:
    assert len(parsed.get("results", [])) == int(expected["result_count"]), (
      f"result_count mismatch: {len(parsed.get('results', []))} != {expected['result_count']}"
    )

  # Optional numeric comparisons with small tolerance
  def approx_equal(a: float, b: float, tol: float = 1e-2) -> bool:
    try:
      return abs(float(a) - float(b)) <= tol
    except Exception:
      return False

  if "avg_speed_winner" in expected and parsed.get("avg_speed_winner") is not None:
    assert approx_equal(parsed["avg_speed_winner"], expected["avg_speed_winner"], tol=0.2), (
      f"avg_speed_winner mismatch: {parsed['avg_speed_winner']} != {expected['avg_speed_winner']}"
    )

  if "distance" in expected and parsed.get("distance") is not None:
    assert approx_equal(parsed["distance"], expected["distance"], tol=0.5), (
      f"distance mismatch: {parsed['distance']} != {expected['distance']}"
    )


async def parse_with_fixtures() -> int:
  print("[parse] Using fixtures to parse stage pages...\n")

  config = ScrapingConfig(
    max_concurrent_requests=2,
    request_delay=0.0,
    max_retries=1,
    timeout=30,
    database_path="test_cycling_data.db",
  )

  async with AsyncCyclingDataScraper(config) as scraper:
    original_make_request = scraper.make_request

    async def make_request_override(url: str, max_retries: int | None = None):
      if url.startswith(BASE_URL):
        slug = url[len(BASE_URL):]
        html = read_fixture(slug)
        if html is not None:
          return html
      return await original_make_request(url, max_retries)

    scraper.make_request = make_request_override  # type: ignore

    failures = 0
    for slug in TARGET_URLS:
      try:
        parsed = await scraper.get_stage_info(slug)
        assert parsed is not None, f"no data parsed for {slug}"
        assert isinstance(parsed.get("results"), list) and len(parsed["results"]) > 0, (
          f"no results parsed for {slug}"
        )
        first = parsed["results"][0]
        assert "rider_name" in first and first["rider_name"], f"missing rider_name in first result for {slug}"

        compare_expected(slug, parsed)
        print(f"  - OK {slug}: {len(parsed['results'])} results")
      except AssertionError as e:
        failures += 1
        print(f"  - FAIL {slug}: {e}")
      except Exception as e:
        failures += 1
        print(f"  - ERROR {slug}: {e}")

    print(f"\n[parse] Done. {len(TARGET_URLS) - failures} passed, {failures} failed.\n")
    return 1 if failures else 0


async def main() -> int:
  await refresh_fixtures()
  return await parse_with_fixtures()


if __name__ == "__main__":
  sys.exit(asyncio.run(main()))