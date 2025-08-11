from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).parent
FIXTURES_PAGES_DIR = BASE_DIR / "fixtures" / "pages"
FIXTURES_EXPECTED_DIR = BASE_DIR / "fixtures" / "expected"


def ensure_dirs() -> None:
  FIXTURES_PAGES_DIR.mkdir(parents=True, exist_ok=True)
  FIXTURES_EXPECTED_DIR.mkdir(parents=True, exist_ok=True)


def slug_to_stem(slug: str) -> str:
  # Replace path separators and keep it filesystem-safe
  return slug.strip("/ ").replace("/", "__")


def page_path(slug: str) -> Path:
  return FIXTURES_PAGES_DIR / f"{slug_to_stem(slug)}.html"


def expected_path(slug: str) -> Path:
  return FIXTURES_EXPECTED_DIR / f"{slug_to_stem(slug)}.json"


def read_fixture(slug: str) -> Optional[str]:
  p = page_path(slug)
  if not p.exists():
    return None
  return p.read_text(encoding="utf-8")


def write_fixture(slug: str, content: str) -> None:
  ensure_dirs()
  page_path(slug).write_text(content, encoding="utf-8")


def normalize_html(html: str) -> str:
  # Keep normalization light to avoid false positives/negatives
  return html.strip()


def content_hash(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()