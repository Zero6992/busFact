#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regular expressions and helpers for parsing SEC filings.
"""

import re
from typing import List, Tuple, Optional

import pandas as pd

from src.config import DATE_ANY

COVER_RE = re.compile(
    rf"""
    \bfor\s+the\s+
    (?:
        (?:fiscal\s+)?quarter(?:ly)?(?:\s+(?:report\s+for\s+the\s+)?period)?
        |
        (?:(?:three|3|thirteen|13)\s+(?:months?|weeks?))
    )
    \s+ended
    \s*[:\-–—]?\s*
    (?P<date>{DATE_ANY})
    """,
    re.I | re.X,
)

# Reusable optional parenthetical fragments (e.g., "(unaudited)", "(in thousands)")
PAREN_OPT = r"(?:\s*\([^)]{0,80}\))?"

# ===== "as of A and B" pattern =====
BAL_ASOF_RE = re.compile(
    rf"""
    \b(?:condensed\s+)?(?:consolidated\s+)?balance\s+sheets?
    {PAREN_OPT}\s*                               # optional "(unaudited)" immediately after the heading
    (?:as\s+(?:of|at)|as\s+of|as\s+at)\s+
    (?P<d1>{DATE_ANY})
    {PAREN_OPT}\s*                               # allow parentheses after the first date
    (?:,)?\s*(?:and|,?\s*and)\s*
    (?P<d2>{DATE_ANY})
    {PAREN_OPT}?                                 # sometimes present after the second date
    """,
    re.I | re.X,
)

SOFP_ASOF_RE = re.compile(
    rf"""
    \b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+financial\s+position
    {PAREN_OPT}\s*
    (?:as\s+(?:of|at)|as\s+of|as\s+at)\s+
    (?P<d1>{DATE_ANY})
    {PAREN_OPT}\s*
    (?:,)?\s*(?:and|,?\s*and)\s*
    (?P<d2>{DATE_ANY})
    {PAREN_OPT}?
    """,
    re.I | re.X,
)

COND_ASOF_RE = re.compile(
    rf"""
    \b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+(?:financial\s+)?condition
    {PAREN_OPT}\s*
    (?:as\s+(?:of|at)|as\s+of|as\s+at)\s+
    (?P<d1>{DATE_ANY})
    {PAREN_OPT}\s*
    (?:,)?\s*(?:and|,?\s*and)\s*
    (?P<d2>{DATE_ANY})
    {PAREN_OPT}?
    """,
    re.I | re.X,
)

SAL_ASOF_RE =  re.compile(
    rf"""
    \b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+assets?\s+(?:and|&)\s+liabilities
    {PAREN_OPT}\s*
    (?:as\s+(?:of|at)|as\s+of|as\s+at)\s+
    (?P<d1>{DATE_ANY})
    {PAREN_OPT}\s*
    (?:,)?\s*(?:and|,?\s*and)\s*
    (?P<d2>{DATE_ANY})
    {PAREN_OPT}?
    """,
    re.I | re.X,
)

ASOF_PATTERNS = (BAL_ASOF_RE, SOFP_ASOF_RE, COND_ASOF_RE, SAL_ASOF_RE)


TOKENS = ["CONSOLIDATED","CONDENSED","BALANCE","SHEETS",
          "STATEMENTS","FINANCIAL","POSITION","CONDITION",
          "ASSETS","LIABILITIES"]

# ===== Heading patterns (uppercase first) =====
UPPER_HEAD_PATTS = [
    # Balance Sheets
    re.compile(r"\bCONDENSED\s+CONSOLIDATED\s+BALANCE\s+SHEETS?\b"),
    re.compile(r"\bCONSOLIDATED\s+CONDENSED\s+BALANCE\s+SHEETS?\b"),
    re.compile(r"\bCONSOLIDATED\s+BALANCE\s+SHEETS?\b"),
    re.compile(r"\bCONDENSED\s+BALANCE\s+SHEETS?\b"),
    re.compile(r"\bBALANCE\s+SHEETS?\b"),
    # Statements of Financial Position
    re.compile(r"\bCONDENSED\s+CONSOLIDATED\s+STATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b"),
    re.compile(r"\bCONSOLIDATED\s+CONDENSED\s+STATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b"),
    re.compile(r"\bCONSOLIDATED\s+STATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b"),
    re.compile(r"\bCONDENSED\s+STATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b"),
    re.compile(r"\bSTATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b"),
    # NEW: Statements of (Financial) Condition
    re.compile(r"\bCONDENSED\s+CONSOLIDATED\s+STATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b"),
    re.compile(r"\bCONSOLIDATED\s+CONDENSED\s+STATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b"),
    re.compile(r"\bCONSOLIDATED\s+STATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b"),
    re.compile(r"\bCONDENSED\s+STATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b"),
    re.compile(r"\bSTATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b"),
    # Statements of Assets and Liabilities
    re.compile(r"\bCONDENSED\s+CONSOLIDATED\s+STATEMENTS?\s+OF\s+ASSETS?\s+(?:AND|&)\s+LIABILITIES\b"),
    re.compile(r"\bCONSOLIDATED\s+CONDENSED\s+STATEMENTS?\s+OF\s+ASSETS?\s+(?:AND|&)\s+LIABILITIES\b"),
    re.compile(r"\bCONSOLIDATED\s+STATEMENTS?\s+OF\s+ASSETS?\s+(?:AND|&)\s+LIABILITIES\b"),
    re.compile(r"\bCONDENSED\s+STATEMENTS?\s+OF\s+ASSETS?\s+(?:AND|&)\s+LIABILITIES\b"),
    re.compile(r"\bSTATEMENTS?\s+OF\s+ASSETS?\s+(?:AND|&)\s+LIABILITIES\b"),
]

# Title Case / lowercase
MIX_HEAD_PATTS = [
    # Balance Sheets
    re.compile(r"\bCondensed\s+Consolidated\s+Balance\s+Sheets?\b"),
    re.compile(r"\bConsolidated\s+Condensed\s+Balance\s+Sheets?\b"),
    re.compile(r"\bConsolidated\s+Balance\s+Sheets?\b"),
    re.compile(r"\bCondensed\s+Balance\s+Sheets?\b"),
    re.compile(r"\bBalance\s+Sheets?\b"),
    # Statements of Financial Position
    re.compile(r"\bCondensed\s+Consolidated\s+Statements?\s+of\s+Financial\s+Position\b"),
    re.compile(r"\bConsolidated\s+Condensed\s+Statements?\s+of\s+Financial\s+Position\b"),
    re.compile(r"\bConsolidated\s+Statements?\s+of\s+Financial\s+Position\b"),
    re.compile(r"\bCondensed\s+Statements?\s+of\s+Financial\s+Position\b"),
    re.compile(r"\bStatements?\s+of\s+Financial\s+Position\b"),
    # NEW: Statements of (Financial) Condition
    re.compile(r"\bCondensed\s+Consolidated\s+Statements?\s+of\s+(?:Financial\s+)?Condition\b"),
    re.compile(r"\bConsolidated\s+Condensed\s+Statements?\s+of\s+(?:Financial\s+)?Condition\b"),
    re.compile(r"\bConsolidated\s+Statements?\s+of\s+(?:Financial\s+)?Condition\b"),
    re.compile(r"\bCondensed\s+Statements?\s+of\s+(?:Financial\s+)?Condition\b"),
    re.compile(r"\bStatements?\s+of\s+(?:Financial\s+)?Condition\b"),
    # Statements of Assets and Liabilities
    re.compile(r"\bCondensed\s+Consolidated\s+Statements?\s+of\s+Assets?\s+(?:and|&)\s+Liabilities\b"),
    re.compile(r"\bConsolidated\s+Condensed\s+Statements?\s+of\s+Assets?\s+(?:and|&)\s+Liabilities\b"),
    re.compile(r"\bConsolidated\s+Statements?\s+of\s+Assets?\s+(?:and|&)\s+Liabilities\b"),
    re.compile(r"\bCondensed\s+Statements?\s+of\s+Assets?\s+(?:and|&)\s+Liabilities\b"),
    re.compile(r"\bStatements?\s+of\s+Assets?\s+(?:and|&)\s+Liabilities\b"),
]

ANCHOR_UPPER_PATTS = [
    re.compile(r"\b(CONSOLIDATED|CONDENSED)\b.{0,100}\bBALANCE\s+SHEETS?\b", re.S),
    re.compile(r"\b(CONSOLIDATED|CONDENSED)\b.{0,100}\bSTATEMENTS?\s+OF\s+FINANCIAL\s+POSITION\b", re.S),
    re.compile(r"\b(CONSOLIDATED|CONDENSED)\b.{0,100}\bSTATEMENTS?\s+OF\s+(?:FINANCIAL\s+)?CONDITION\b", re.S),
]

# Generic fallback (case-insensitive)
GENERIC_HEAD_PATTS = [
    re.compile(r"\b(?:condensed\s+)?(?:consolidated\s+)?balance\s+sheets?\b", re.I),
    re.compile(r"\b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+financial\s+position\b", re.I),
    re.compile(r"\b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+(?:financial\s+)?condition\b", re.I),
    re.compile(r"\b(?:condensed\s+)?(?:consolidated\s+)?statements?\s+of\s+assets?\s+(?:and|&)\s+liabilities\b", re.I),
]

def _heading_score(text_slice: str, is_upper: bool, is_anchor: bool) -> float:
    hits = 0
    for token in TOKENS:
        if re.search(rf"\b{token}\b", text_slice, flags=re.I):
            hits += 1
    base = 30.0 if is_upper else (25.0 if is_anchor else 20.0)
    return base + hits + 0.001 * len(text_slice)

def iter_balance_sheet_headings(text: str) -> List[re.Match]:
    candidates: List[Tuple[float, re.Match]] = []

    for patt in UPPER_HEAD_PATTS:
        for match in patt.finditer(text):
            candidates.append((_heading_score(match.group(0), True, False), match))

    for patt in ANCHOR_UPPER_PATTS:
        for match in patt.finditer(text):
            candidates.append((_heading_score(match.group(0), False, True), match))

    for patt in MIX_HEAD_PATTS:
        for match in patt.finditer(text):
            s = match.group(0)
            has_bs = re.search(r"\bBalance\b", s) and re.search(r"\bSheets?\b", s)
            has_fp = (re.search(r"\bStatements?\b", s) and
                      re.search(r"\bFinancial\b", s) and
                      re.search(r"\bPosition\b", s))
            has_fc = (re.search(r"\bStatements?\b", s) and
                      re.search(r"\bCondition\b", s))
            if not (has_bs or has_fp or has_fc):
                continue
            candidates.append((_heading_score(s, False, False), match))

    # Fallback: use generic patterns if nothing matched above
    if not candidates:
        for patt in GENERIC_HEAD_PATTS:
            for match in patt.finditer(text):
                candidates.append((15.0 + 0.001 * len(match.group(0)), match))

    candidates.sort(key=lambda item: (-item[0], item[1].start()))
    return [match for _, match in candidates]

MONTH_WORD = (
    r"(Jan(?:\.|uary)?|Feb(?:\.|ruary)?|Mar(?:\.|ch)?|Apr(?:\.|il)?|May|Jun(?:\.|e)?|"
    r"Jul(?:\.|y)?|Aug(?:\.|ust)?|Sep(?:\.|t\.|tember)?|Oct(?:\.|ober)?|Nov(?:\.|ember)?|"
    r"Dec(?:\.|ember)?)"
)

FYE_PATTS = [
    re.compile(
        rf"\bfiscal\s+year[-\s]*end(?:ed)?\s*(?:is|:|on|to|at)?\s*{MONTH_WORD}(?:\s+\d{{1,2}})?(?:,\s*\d{{4}})?",
        re.I,
    ),
    re.compile(rf"\bfor\s+the\s+fiscal\s+year\s+ended\s+{MONTH_WORD}\s+\d{{1,2}}(?:,\s*\d{{4}})?", re.I),
    re.compile(rf"\byear\s+ended\s+{MONTH_WORD}\s+\d{{1,2}}(?:,\s*\d{{4}})?", re.I),
    re.compile(rf"{MONTH_WORD}\s+\d{{1,2}}\s+fiscal\s+year\s+end", re.I),
]

PERIOD_HDR_RE = re.compile(r"CONFORMED PERIOD OF REPORT:\s*(\d{8})", re.I)
ACCESSION_FOLDER_RE = re.compile(r"/Archives/edgar/data/(\d+)/(\d{18}|\d{10}-\d{2}-\d{6})/", re.I)

__all__ = [
    "COVER_RE",
    "BAL_ASOF_RE",
    "SOFP_ASOF_RE",
    "COND_ASOF_RE",
    "SAL_ASOF_RE",
    "ASOF_PATTERNS",
    "UPPER_HEAD_PATTS",
    "MIX_HEAD_PATTS",
    "ANCHOR_UPPER_PATTS",
    "GENERIC_HEAD_PATTS",
    "iter_balance_sheet_headings",
    "MONTH_WORD",
    "FYE_PATTS",
    "PERIOD_HDR_RE",
    "ACCESSION_FOLDER_RE",
]

# ===== Inline XBRL DEI (P0) =====
IX_TAG = r"ix:(?:nonNumeric|nonnumeric)"  # tolerate case variations
DEI_PF_RE = re.compile(
    rf"<{IX_TAG}[^>]*\bname\s*=\s*['\"]dei:DocumentFiscalPeriodFocus['\"][^>]*>\s*(Q[1-4]|FY)\s*</{IX_TAG}>",
    re.I | re.S,
)
DEI_YF_RE = re.compile(
    rf"<{IX_TAG}[^>]*\bname\s*=\s*['\"]dei:DocumentFiscalYearFocus['\"][^>]*>\s*(\d{{4}})\s*</{IX_TAG}>",
    re.I | re.S,
)
# Allow 12/31 or --12-31 / -12-31
DEI_FYE_RE = re.compile(
    rf"<{IX_TAG}[^>]*\bname\s*=\s*['\"]dei:CurrentFiscalYearEndDate['\"][^>]*>\s*(\d{{1,2}}/\d{{1,2}}|--?\d{{2}}-\d{{2}})\s*</{IX_TAG}>",
    re.I | re.S,
)
# Optional: grab the period end date directly
DEI_DPE_RE = re.compile(
    rf"<{IX_TAG}[^>]*\bname\s*=\s*['\"]dei:DocumentPeriodEndDate['\"][^>]*>\s*(\d{{4}}-\d{{2}}-\d{{2}}|\d{{1,2}}/\d{{1,2}}/\d{{2,4}})\s*</{IX_TAG}>",
    re.I | re.S,
)

def _parse_mm_from_fye_text(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    # 12/31
    m = re.match(r"^\s*(\d{1,2})/\d{1,2}\s*$", s)
    if m:
        mm = int(m.group(1))
        return mm if 1 <= mm <= 12 else None
    # --12-31 or -12-31
    m = re.match(r"^\s*-{1,2}(\d{2})-\d{2}\s*$", s)
    if m:
        mm = int(m.group(1))
        return mm if 1 <= mm <= 12 else None
    return None

def extract_dei_from_html(html: str) -> dict:
    """Return {'pf': 'Q1|Q2|Q3|Q4|FY' or None, 'yf': int|None, 'fye_month': int|None, 'period_end': Timestamp|None}."""
    out = {"pf": None, "yf": None, "fye_month": None, "period_end": None}
    if not html:
        return out
    # Scan the raw HTML for <ix:nonNumeric ...>
    m = DEI_PF_RE.search(html)
    if m:
        out["pf"] = m.group(1).upper()
    m = DEI_YF_RE.search(html)
    if m:
        try:
            out["yf"] = int(m.group(1))
        except Exception:
            pass
    m = DEI_FYE_RE.search(html)
    if m:
        mm = _parse_mm_from_fye_text(m.group(1))
        if mm:
            out["fye_month"] = mm
    m = DEI_DPE_RE.search(html)
    if m:
        try:
            out["period_end"] = pd.to_datetime(m.group(1), errors="coerce")
        except Exception:
            pass
    return out


__all__ += ["extract_dei_from_html"]
