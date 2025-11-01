#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from pathlib import Path
from typing import List
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from business_factor.config import SLEEP, UA_DEFAULT
from business_factor.data import ensure_dir
from business_factor.pipeline import (
    PATTERN_GROUPS,
    enrich_with_section_1a,
    deduplicate_quarters,
)

DEFAULT_INPUT = ROOT_DIR / "data" / "outputs" / "bsq_quarter.final.csv"
DEFAULT_OUTPUT = ROOT_DIR / "data" / "outputs" / "bsq_quarter.item1a.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch filing text, count strategy keywords, and deduplicate filings."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input CSV path.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    parser.add_argument("--rate", type=float, default=SLEEP, help="Sleep seconds between HTTP requests.")
    parser.add_argument(
        "--keep-text",
        action="store_true",
        help="Keep the raw filing text (column named section_1a_text for backwards compatibility).",
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar.")
    parser.add_argument(
        "--group-cols",
        type=str,
        default="cik,fyear,quarter",
        help="Comma-separated columns used for deduplication (default: cik,fyear,quarter).",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Skip deduplication step (keep all filings).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Process only the first N rows (for debugging).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def _parse_group_cols(value: str) -> List[str]:
    cols = [col.strip() for col in value.split(",") if col.strip()]
    return cols or ["cik", "fyear", "quarter"]


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    if not args.input.exists():
        raise FileNotFoundError(f"Input CSV not found: {args.input}")

    df = pd.read_csv(args.input)
    if args.max_rows is not None:
        df = df.head(args.max_rows)

    enriched = enrich_with_section_1a(
        df,
        user_agent=UA_DEFAULT,
        rate=args.rate,
        keep_text=args.keep_text,
        no_progress=args.no_progress,
    )

    if not args.no_dedupe:
        group_cols = _parse_group_cols(args.group_cols)
        enriched = deduplicate_quarters(enriched, group_cols=group_cols)

    # Ensure integer columns.
    for col in list(PATTERN_GROUPS) + ["total_words"]:
        enriched[col] = enriched[col].fillna(0).astype(int)

    if not args.keep_text and "section_1a_text" in enriched.columns:
        enriched = enriched.drop(columns=["section_1a_text"])

    ensure_dir(str(args.output))
    enriched.to_csv(args.output, index=False)
    print(f"Saved output to {args.output}")


if __name__ == "__main__":
    main()
