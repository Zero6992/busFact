#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path

import pandas as pd

from business_factor.config import SLEEP, UA_DEFAULT
from business_factor.data import ensure_dir
from business_factor.pipeline import (
    finalize,
    stats_and_save,
    step1_sub,
    step2_fye_api,
    step3_html_parse,
    step4_compute_quarter,
)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "outputs"
OUTPUT_PREFIX = OUTPUT_DIR / "bsq_quarter"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Add 'quarter' via sub_map + SEC APIs (FYE) + HTML (period & FYE with exclusion rules)."
    )
    parser.add_argument("--bsq", required=True, help="BS_Q.csv")
    parser.add_argument("--submap", required=True, help="sub_map.csv (adsh, fp, period)")
    parser.add_argument("--user-agent", type=str, default=UA_DEFAULT, help="SEC-compliant User-Agent")
    parser.add_argument("--rate", type=float, default=SLEEP, help="Seconds between HTTP/API calls")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars")
    return parser.parse_args()


def run():
    args = parse_args()
    ensure_dir(str(OUTPUT_PREFIX))

    original_df = pd.read_csv(args.bsq, dtype=str, engine="python", on_bad_lines="skip")

    step1 = step1_sub(original_df, args.submap)
    stats_and_save(step1, f"{OUTPUT_PREFIX}.step1_sub.csv", filled=int(step1["quarter"].notna().sum()))

    step2, fye_tbl = step2_fye_api(step1, args.user_agent, args.rate)
    stats_and_save(step2, f"{OUTPUT_PREFIX}.step2_fye_api.csv", filled=int(step2["quarter"].notna().sum()))
    fye_tbl.to_csv(f"{OUTPUT_PREFIX}.FYE_by_company_year.csv", index=False)

    step3 = step3_html_parse(step2, args.user_agent, args.rate, no_progress=args.no_progress)
    stats_and_save(step3, f"{OUTPUT_PREFIX}.step3_html.csv", filled=int(step3["quarter"].notna().sum()))

    step4 = step4_compute_quarter(step3)
    filled4 = int(step4["_filled_this_step"].iloc[-1]) if "_filled_this_step" in step4.columns else 0
    step4_no_flag = step4.drop(columns=["_filled_this_step"], errors="ignore")
    stats_and_save(step4_no_flag, f"{OUTPUT_PREFIX}.step4_compute.csv", filled=filled4)

    finalize(original_df, step4_no_flag, f"{OUTPUT_PREFIX}.final.csv")

    print("DONE.")
    print("Outputs:")
    for path in [
        f"{OUTPUT_PREFIX}.step1_sub.csv",
        f"{OUTPUT_PREFIX}.step2_fye_api.csv",
        f"{OUTPUT_PREFIX}.step3_html.csv",
        f"{OUTPUT_PREFIX}.step4_compute.csv",
        f"{OUTPUT_PREFIX}.final.csv",
        f"{OUTPUT_PREFIX}.FYE_by_company_year.csv",
    ]:
        print(" -", Path(path).relative_to(BASE_DIR))


if __name__ == "__main__":
    run()
