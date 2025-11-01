#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core configuration constants shared across the Business Factor pipeline.
"""

UA_DEFAULT = "MegaTsai (jordan890522@gmail.com)"
TIMEOUT = 30
SLEEP = 0.20

FP_TO_Q = {"Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "H1": "Q2", "M9": "Q3"}

DATE_WORD = (
    r"(?:Jan(?:\.|uary)?|Feb(?:\.|ruary)?|Mar(?:\.|ch)?|Apr(?:\.|il)?|May|Jun(?:\.|e)?|"
    r"Jul(?:\.|y)?|Aug(?:\.|ust)?|Sep(?:\.|t\.|tember)?|Oct(?:\.|ober)?|Nov(?:\.|ember)?|"
    r"Dec(?:\.|ember)?)\s+\d{1,2},\s*\d{4}"
)
DATE_NUM = r"\d{1,2}/\d{1,2}/\d{2,4}"
DATE_ISO = r"\d{4}-\d{2}-\d{2}"
DATE_ANY = rf"(?:{DATE_WORD}|{DATE_NUM}|{DATE_ISO})"

MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

ANNUAL_FORMS_RAW = {
    "10-K",
    "10-KT",
    "10-K/A",
    "10-KT/A",
    "10K",
    "10KT",
    "10K/A",
    "10KT/A",
    "10-KSB",
    "10-KSB40",
    "10-K405",
    "10-K405/A",
    "10-KSB/A",
    "10-KSB40/A",
    "10KSB",
    "10KSB40",
    "10K405",
    "10K405/A",
    "10KSB/A",
    "10KSB40/A",
    "20-F",
    "20-F/A",
    "40-F",
    "40-F/A",
}

__all__ = [
    "UA_DEFAULT",
    "TIMEOUT",
    "SLEEP",
    "FP_TO_Q",
    "DATE_WORD",
    "DATE_NUM",
    "DATE_ISO",
    "DATE_ANY",
    "MONTH_MAP",
    "ANNUAL_FORMS_RAW",
]
