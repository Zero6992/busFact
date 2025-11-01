#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Section 1A extraction and keyword counting utilities.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Dict, Iterable, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

from business_factor.config import SLEEP, UA_DEFAULT
from business_factor.data import canon_url, detect_url_column
from business_factor.sec.client import fetch_text
from business_factor.utils.progress import tqdm

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

PAGE_TOKEN_RE = re.compile(
    r"(?:<PAGE>|\#\#PAGE|Page\s*\d+(?:\s*of\s*\d+)?|\d+\s*PAGE)", re.IGNORECASE
)
TRAILING_NUMBER_RE = re.compile(r"\s\d+$")


def _soup_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for element in soup(["script", "style"]):
        element.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    text = soup.get_text(separator=" ", strip=True)
    return text


def _normalize_spaces(text: str) -> str:
    text = text.replace("\xa0", " ")
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
    # Prefer the longest match to avoid selecting table-of-contents snippets.
    longest = max(matches, key=lambda m: len(m.group("body")))
    section = longest.group("body").strip()
    if not section:
        return None
    return section


def _clean_section_text(text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = PAGE_TOKEN_RE.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = TRAILING_NUMBER_RE.sub("", cleaned).strip()
    return cleaned or None


def get_clean_1a_text(filing_url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Fetch filing content and return cleaned textual content.

    Historically this function extracted only the Item 1A section.
    It now returns the entire filing text so keyword counts cover the
    full document.
    """
    if not isinstance(filing_url, str):
        return None
    headers = headers or {
        "User-Agent": UA_DEFAULT,
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    url = canon_url(filing_url)
    lowered = url.lower().strip()
    if not lowered.endswith((".htm", ".html", ".txt")):
        return None

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

    return _clean_section_text(raw_text)


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


def enrich_with_section_1a(
    df: pd.DataFrame,
    *,
    user_agent: str = UA_DEFAULT,
    rate: float = SLEEP,
    keep_text: bool = False,
    no_progress: bool = False,
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
    iterator = tqdm(indices, desc="Item 1A", unit="row") if (tqdm and not no_progress) else indices

    for pos, idx in enumerate(iterator):
        url = data.at[idx, url_col]
        text = None
        if isinstance(url, str) and url.strip():
            try:
                text = get_clean_1a_text(url, headers=headers)
            except Exception:
                LOGGER.exception("Error while processing %s", url)
                text = None
            if rate and pos < len(indices) - 1:
                time.sleep(rate)

        counts = count_keywords(text)
        for col, value in counts.items():
            data.at[idx, col] = int(value)

        words = count_words(text)
        data.at[idx, "total_words"] = int(words)

        if keep_text:
            data.at[idx, "section_1a_text"] = text

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
