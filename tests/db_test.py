#!/usr/bin/env python3
import sqlite3
from pathlib import Path

DB_PATH = Path("test_cycling_data.db")


def run() -> int:
  print("[db] Checking test database integrity...\n")
  if not DB_PATH.exists():
    print(f"  - SKIP: {DB_PATH} does not exist. Run integration or scraper first.")
    return 0

  conn = sqlite3.connect(str(DB_PATH))
  cur = conn.cursor()

  failures = 0

  # Basic existence checks
  try:
    cur.execute("SELECT COUNT(*) FROM races")
    races = cur.fetchone()[0]
    print(f"  - races: {races}")
  except Exception as e:
    failures += 1
    print(f"  - FAIL races table: {e}")

  try:
    cur.execute("SELECT COUNT(*) FROM stages")
    stages = cur.fetchone()[0]
    print(f"  - stages: {stages}")
  except Exception as e:
    failures += 1
    print(f"  - FAIL stages table: {e}")

  try:
    cur.execute("SELECT COUNT(*) FROM results")
    results = cur.fetchone()[0]
    print(f"  - results: {results}")
  except Exception as e:
    failures += 1
    print(f"  - FAIL results table: {e}")

  # Uniqueness check for stage_url
  try:
    cur.execute("SELECT stage_url, COUNT(*) FROM stages GROUP BY stage_url HAVING COUNT(*) > 1")
    dups = cur.fetchall()
    assert len(dups) == 0, f"duplicate stage_url rows: {dups[:3]}"
    print("  - OK unique stage_url in stages")
  except Exception as e:
    failures += 1
    print(f"  - FAIL unique stage_url: {e}")

  conn.close()

  print(f"\n[db] Done. {'OK' if failures == 0 else f'{failures} failure(s)'}\n")
  return 1 if failures else 0


if __name__ == "__main__":
  raise SystemExit(run())