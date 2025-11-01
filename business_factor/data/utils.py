#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helpers for working with inbound and outbound CSV data.
"""

import os
import re
from typing import Optional

import pandas as pd

from business_factor.config import MONTH_MAP
from business_factor.parsing.patterns import ACCESSION_FOLDER_RE


def ensure_dir(path: str):
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def detect_url_column(df: pd.DataFrame) -> Optional[str]:
    prefer = [
        "filingUrl",
        "url",
        "link",
        "documentUrl",
        "docUrl",
        "primary_document",
        "primary_doc",
    ]
    for column in prefer:
        if column in df.columns:
            series = df[column].astype(str)
            if series.str.contains(r"https?://|/Archives/edgar/", case=False, regex=True).any():
                return column
    best_col, best_hits = None, -1
    sample = df.head(1000) if len(df) > 1000 else df
    for column in df.columns:
        hits = (
            sample[column]
            .astype(str)
            .str.contains(r"https?://|/Archives/edgar/", case=False, regex=True)
            .sum()
        )
        if hits > best_hits:
            best_col, best_hits = column, hits
    return best_col if best_hits > 0 else None


def accession_from_url(url: str) -> Optional[str]:
    if not isinstance(url, str):
        return None
    match = ACCESSION_FOLDER_RE.search(url)
    if not match:
        return None
    accession = match.group(2)
    return accession.replace("-", "") if "-" in accession else accession


def canon_url(url: str) -> str:
    if url is None or (isinstance(url, float) and pd.isna(url)):
        return ""
    value = str(url).strip() if not isinstance(url, str) else url.strip()
    if not value or value.lower() == "nan":
        return ""
    value = value.replace("\xa0", " ").replace("&nbsp;", " ")
    value = re.sub(r"^https?://www\.sec\.gov/ix\?doc=", "https://www.sec.gov", value, flags=re.I)
    return value


def month_word_to_int(word: str) -> Optional[int]:
    if not word:
        return None
    return MONTH_MAP.get(word.lower().strip().rstrip("."))


__all__ = [
    "ensure_dir",
    "detect_url_column",
    "accession_from_url",
    "canon_url",
    "month_word_to_int",
]
