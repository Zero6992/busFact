#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Section 1A extraction and keyword counting utilities.
"""

from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

try:
    from sec_api import ExtractorApi
except Exception:
    ExtractorApi = None  # type: ignore[assignment]

from busfactor.config import SLEEP, UA_DEFAULT
from busfactor.data import canon_url, detect_url_column
from busfactor.sec.client import fetch_text
from busfactor.utils.progress import tqdm
from busfactor.utils.text import replace_nbsp, strip_page_tokens

LOGGER = logging.getLogger(__name__)

# Keyword patterns grouped by strategy dimension.
PATTERN_GROUPS: Dict[str, Iterable[str]] = {
    "Differentiation strategy": [
        r"differenti\w*",
        r"unique\w*",
        r"superior\w*",
        r"premium\w*",
        r"high\w*\s+pric\w*",
        r"high\w*\s+margin\w*",
        r"high\w*\s+end\w*",
        r"inelasticity",
        r"excellen\w*",
        r"leading\s+edge",
        r"upscale",
    ],
    "Product": [
        r"innovate\w*",
        r"creativ\w*",
        r"research and development",
        r"\bR&D\b",
        r"techni\w*",
        r"technology\w*",
        r"patent\w*",
        r"proprietar\w*",
        r"new\w*\s+product\w*",
    ],
    "Market": [
        r"marketing\w*",
        r"advertis\w*",
        r"brand\w*",
        r"reputation\w*",
        r"trademark\w*",
    ],
    "Operational efficiency": [
        r"efficien\w*",
        r"high\w*\s+yield\w*",
        r"process\w*\s+improvement\w*",
        r"asset\w*\s+utilization\w*",
        r"capacity\w*\s+utilization\w*",
    ],
    "Human resource": [
        r"talent\w*",
        r"train\w*",
        r"skill\w*",
        r"intellectual\w*\s+propert\w*",
        r"human\s+capital\w*",
    ],
    "Cost strategy": [
        r"cost\s+leader\w*",
        r"low\w*\s+pric\w*",
        r"low\w*\s+cost\w*",
        r"cost\s+advantage\w*",
        r"competitive\s+pric\w*",
        r"aggressive\s+pric\w*",
    ],
    "Cost control": [
        r"control\w*\s+(?:cost|expense|overhead)\w*",
        r"minimiz\w*\s+(?:cost|expense|overhead)\w*",
        r"reduce\w*\s+(?:cost|expens|overhead)\w*",
        r"cut\w*\s+(?:cost|expens|overhead)\w*",
        r"decreas\w*\s+(?:cost|expens|overhead)\w*",
        r"monitor\w*\s+(?:cost|expens|overhead)\w*",
        r"sav\w*\s+(?:cost|expens|overhead)\w*",
        r"improve\w*\s+cost\w*",
        r"cost\w*\s+(?:control|improvement|minimization|reduction|saving)\w*",
        r"expense\w*\s+(?:control|improvement|minimization|reduction|saving)\w*",
        r"overhead\w*\s+(?:control|improvement|minimization|reduction|saving)\w*",
    ],
    "Customer": [
        r"customer\w*\s+service\w*",
        r"consumer\w*\s+service\w*",
        r"customer\w*\s+need\w*",
        r"sales\s+support\w*",
        r"post[-\s]*purchase\s+service\w*",
        r"customer\w*\s+preference\w*",
        r"consumer\w*\s+preference\w*",
        r"consumer\w*\s+(?:relation\w*|experience\w*|support\w*)",
        r"loyalty\w*",
        r"customiz\w*",
        r"tailor\w*",
        r"personaliz\w*",
    ],
}

# Regular expressions for locating Item 1A boundaries.
ITEM_HEADER_RE = re.compile(r"\bitem\s+1a\b", re.I)
ITEM_SECTION_RE = re.compile(
    r"""
    (?P<header>\bitem\s+1a\b[^A-Za-z0-9]{0,40})         # Item 1A heading
    (?P<body>.*?)                                      # Captured section text
    (?=
        \bitem\s+1b\b|
        \bitem\s+2\b|
        \bitem\s+2a\b|
        \bitem\s+3\b|
        \bitem\s+4\b|
        \bitem\s+5\b|
        \bitem\s+6\b|
        \bitem\s+7\b|
        \bitem\s+7a\b|
        \bitem\s+8\b|
        \bitem\s+9\b|
        \Z
    )
""",
    re.I | re.S | re.X,
)

TRAILING_NUMBER_RE = re.compile(r"\s\d+$")
RISK_PHRASE_RE = re.compile(r"risk\s+factor", re.I)
TRAILING_SECTION_PATTERNS = [
    re.compile(r"\bPART\s+I\.\s+FINANCIAL\s+INFORMATION\b", re.I),
    re.compile(r"\bPART\s+II\.\s+OTHER\s+INFORMATION\b", re.I),
]
TABLE_OF_CONTENTS_RE = re.compile(r"\b\d*\s*Table of Contents\b", re.I)
ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
SEC_API_KEY_ENV = "SEC_API_KEY"

_ENV_LOADED = False
_SEC_API_KEY_WARNED = False
_EXTRACTOR: Optional[ExtractorApi] = None
_EXTRACTOR_INIT_FAILED = False
_ENV_LOCK = Lock()


def _ensure_env_loaded(path: Path = ENV_PATH) -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    with _ENV_LOCK:
        if _ENV_LOADED:
            return
        if not path.exists():
            _ENV_LOADED = True
            return
        try:
            with path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if (
                        len(value) >= 2
                        and value[0] == value[-1]
                        and value[0] in {"'", '"'}
                    ):
                        value = value[1:-1]
                    existing = os.environ.get(key, "")
                    if key and not existing.strip():
                        os.environ[key] = value
        except OSError:
            LOGGER.warning("Unable to read environment file at %s", path)
        finally:
            _ENV_LOADED = True


def _get_sec_api_key() -> Optional[str]:
    global _SEC_API_KEY_WARNED
    _ensure_env_loaded()
    key = os.environ.get(SEC_API_KEY_ENV, "").strip()
    if key:
        return key
    if not _SEC_API_KEY_WARNED:
        LOGGER.warning(
            "Missing SEC_API_KEY; SEC Extractor API word counts are disabled."
        )
        _SEC_API_KEY_WARNED = True
    return None


def _get_extractor() -> Optional[ExtractorApi]:
    global _EXTRACTOR, _EXTRACTOR_INIT_FAILED
    if _EXTRACTOR is not None:
        return _EXTRACTOR
    if _EXTRACTOR_INIT_FAILED:
        return None
    if ExtractorApi is None:
        LOGGER.warning(
            "Package 'sec-api' is not installed; SEC Extractor API cannot be used."
        )
        _EXTRACTOR_INIT_FAILED = True
        return None
    api_key = _get_sec_api_key()
    if not api_key:
        _EXTRACTOR_INIT_FAILED = True
        return None
    try:
        _EXTRACTOR = ExtractorApi(api_key)
    except Exception:
        LOGGER.exception("Failed to initialize SEC Extractor API.")
        _EXTRACTOR_INIT_FAILED = True
        return None
    return _EXTRACTOR


def _fetch_section_1a_via_sec_api(url: str, lowered: Optional[str] = None) -> Optional[str]:
    extractor = _get_extractor()
    if extractor is None:
        return None

    lowered = (lowered or url).lower().strip()
    if not lowered.endswith((".htm", ".html", ".txt")):
        return None

    fmt = "html" if lowered.endswith((".htm", ".html")) else "text"
    try:
        section = extractor.get_section(url, "part2item1a", fmt)
    except Exception as exc:
        LOGGER.exception("SEC Extractor API failed for %s", url)
        if "429" in str(exc):
            time.sleep(2.0)
        return None

    if not section:
        return None

    if fmt == "html":
        section = _soup_to_text(section)
    return section


def _soup_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for element in soup(["script", "style"]):
        element.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    text = soup.get_text(separator=" ", strip=True)
    return text


def _normalize_spaces(text: str) -> str:
    text = replace_nbsp(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_item_1a(text: str) -> Optional[str]:
    if not text:
        return None
    norm = _normalize_spaces(text)
    matches = list(ITEM_SECTION_RE.finditer(norm))
    if not matches:
        return None

    bad_header_chars = {",", ")", "("}

    def _score(match: re.Match) -> Optional[tuple[int, int, int]]:
        body = match.group("body") or ""
        body = body.strip()
        if not body:
            return None
        header = match.group("header") or ""
        header = header.strip()
        has_risk = bool(RISK_PHRASE_RE.search(header)) or bool(RISK_PHRASE_RE.search(body[:512]))
        clean_header = 0 if any(ch in header for ch in bad_header_chars) else 1
        return (1 if has_risk else 0, clean_header, len(body))

    scored: list[tuple[tuple[int, int, int], re.Match]] = []
    for match in matches:
        score = _score(match)
        if score is not None:
            scored.append((score, match))
    if not scored:
        return None
    best = max(scored, key=lambda item: item[0])[1]
    section = best.group("body").strip()
    section = _trim_trailing_sections(section)
    return section or None


def _trim_trailing_sections(section: str) -> str:
    if not section:
        return section
    cutoff = len(section)
    for pattern in TRAILING_SECTION_PATTERNS:
        match = pattern.search(section)
        if match and match.start() > 100:
            cutoff = min(cutoff, match.start())
    if cutoff != len(section):
        section = section[:cutoff].rstrip()
    return section


def _clean_section_text(text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = replace_nbsp(text)
    cleaned = strip_page_tokens(cleaned)
    cleaned = TABLE_OF_CONTENTS_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = TRAILING_NUMBER_RE.sub("", cleaned).strip()
    return cleaned or None


def get_clean_1a_text(filing_url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Fetch Item 1A text via the SEC Extractor API (preferred) with a legacy fallback.
    """
    if not isinstance(filing_url, str):
        return None
    url = canon_url(filing_url)
    lowered = url.lower().strip()
    if not lowered.endswith((".htm", ".html", ".txt")):
        return None

    section_text = _fetch_section_1a_via_sec_api(url, lowered)
    if section_text:
        return _clean_section_text(section_text)

    headers = headers or {
        "User-Agent": UA_DEFAULT,
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    html, status = fetch_text(url, headers)
    if not html:
        LOGGER.warning("Failed to fetch content for %s (status %s)", url, status)
        return None

    try:
        if lowered.endswith((".htm", ".html")):
            raw_text = _soup_to_text(html)
        else:
            raw_text = html
    except Exception:
        LOGGER.exception("Failed to parse HTML for %s", url)
        return None

    section_text = _extract_item_1a(raw_text)
    return _clean_section_text(section_text)


def count_keywords(text: Optional[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {key: 0 for key in PATTERN_GROUPS}
    if not text:
        return counts
    for key, patterns in PATTERN_GROUPS.items():
        total = 0
        for patt in patterns:
            total += len(re.findall(patt, text, flags=re.IGNORECASE))
        counts[key] = total
    return counts


def count_words(text: Optional[str]) -> int:
    if not text:
        return 0
    return len(text.split())


def _auto_worker_count(max_workers: Optional[int] = None) -> int:
    if isinstance(max_workers, int) and max_workers > 0:
        return max(1, max_workers)
    cpu_count = os.cpu_count() or 4
    return max(2, min(8, cpu_count))


def enrich_with_section_1a(
    df: pd.DataFrame,
    *,
    user_agent: str = UA_DEFAULT,
    rate: float = SLEEP,
    keep_text: bool = False,
    no_progress: bool = False,
    max_workers: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch filing text, count keywords, and compute total words for each filing row.
    """
    data = df.copy()
    url_col = detect_url_column(data)
    if not url_col:
        raise ValueError("Could not detect filing URL column.")

    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    result_cols = list(PATTERN_GROUPS) + ["total_words"]
    for col in result_cols:
        if col not in data.columns:
            data[col] = 0
    if keep_text and "section_1a_text" not in data.columns:
        data["section_1a_text"] = pd.NA

    indices = data.index.tolist()

    def _process_index(idx: Any) -> tuple[Any, Dict[str, int], int, Optional[str]]:
        url = data.at[idx, url_col]
        text = None
        if isinstance(url, str) and url.strip():
            try:
                text = get_clean_1a_text(url, headers=headers)
            except Exception:
                LOGGER.exception("Error while processing %s", url)
                text = None
        counts = count_keywords(text)
        words = count_words(text)
        text_value = text if keep_text else None
        return idx, counts, words, text_value

    def _apply_result(idx: Any, counts: Dict[str, int], words: int, text_value: Optional[str]) -> None:
        for col, value in counts.items():
            data.at[idx, col] = int(value)
        data.at[idx, "total_words"] = int(words)
        if keep_text:
            data.at[idx, "section_1a_text"] = text_value

    total = len(indices)
    if rate and rate > 0:
        iterator = tqdm(indices, desc="Item 1A", unit="row") if (tqdm and not no_progress) else indices
        for pos, idx in enumerate(iterator):
            idx, counts, words, text_value = _process_index(idx)
            _apply_result(idx, counts, words, text_value)
            if rate and pos < total - 1:
                time.sleep(rate)
    else:
        workers = _auto_worker_count(max_workers)
        progress = tqdm(total=total, desc="Item 1A", unit="row") if (tqdm and not no_progress) else None
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for idx, counts, words, text_value in executor.map(_process_index, indices):
                _apply_result(idx, counts, words, text_value)
                if progress:
                    progress.update(1)
        if progress:
            progress.close()

    return data


def deduplicate_quarters(
    df: pd.DataFrame,
    *,
    group_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Deduplicate filings by year/quarter using the provided grouping columns.
    Preference order for duplicates:
        1. Keep rows where total_words > 0 (drop all-zero rows).
        2. Among remaining rows, keep the latest filedAt.
        3. If all rows have total_words == 0, keep the latest filedAt.
    """
    if group_cols is None:
        group_cols = ("cik", "fyear", "quarter")

    data = df.copy()
    group_cols = list(group_cols)
    missing = [col for col in group_cols if col not in data.columns]
    if missing:
        raise ValueError(f"Missing required grouping columns: {', '.join(missing)}")

    if "filedAt" not in data.columns:
        raise ValueError("Column 'filedAt' is required for deduplication.")

    data["_filedAt_dt"] = pd.to_datetime(data["filedAt"], utc=True, errors="coerce")
    data["_filedAt_dt"] = data["_filedAt_dt"].fillna(pd.Timestamp(0, tz="UTC"))

    def _select_index(group: pd.DataFrame) -> int:
        non_zero = group[group["total_words"].fillna(0) > 0]
        if not non_zero.empty:
            return int(non_zero["_filedAt_dt"].idxmax())
        return int(group["_filedAt_dt"].idxmax())

    grouped = data.groupby(group_cols, dropna=False, group_keys=False)
    keep_indices = grouped.apply(_select_index, include_groups=False)  # type: ignore[arg-type]
    keep_indices = [int(x) for x in keep_indices.tolist()]
    keep_indices = sorted(set(keep_indices))
    result = data.loc[keep_indices].drop(columns=["_filedAt_dt"])
    result = result.sort_values(group_cols + ["filedAt"], ascending=True).reset_index(drop=True)
    return result


__all__ = [
    "PATTERN_GROUPS",
    "get_clean_1a_text",
    "count_keywords",
    "count_words",
    "enrich_with_section_1a",
    "deduplicate_quarters",
]
