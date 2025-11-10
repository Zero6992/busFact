#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import sys

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from busfactor.config import SLEEP, UA_DEFAULT
from busfactor.data import ensure_dir
from busfactor.pipeline import (
    finalize,
    stats_and_save,
    step1_sub,
    step2_fye_api,
    step3_html_parse,
    step4_compute_quarter,
)

OUTPUT_DIR = BASE_DIR / "data" / "outputs"
OUTPUT_PREFIX = OUTPUT_DIR / "bsq_quarter"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Add 'quarter' via sub_map + SEC APIs (FYE) + HTML (period & FYE with exclusion rules)."
    )
    parser.add_argument("--bsq", required=True, help="BS_Q.csv")
    parser.add_argument("--submap", required=True, help="sub_map.csv (adsh, fp, period)")
    parser.add_argument("--rate", type=float, default=SLEEP, help="Seconds between HTTP/API calls")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars")
    parser.add_argument(
        "--start-step",
        choices=("step1_sub", "step2_fye_api", "step3_html_parse", "step4_compute"),
        default="step1_sub",
        help=(
            "Resume pipeline from the given step (requires the corresponding "
            "bsq_quarter.<step>.csv to exist)."
        ),
    )
    return parser.parse_args()


def run():
    args = parse_args()
    ensure_dir(str(OUTPUT_PREFIX))

    def _output_path(suffix: str) -> Path:
        return Path(f"{OUTPUT_PREFIX}.{suffix}")

    def _load_step(step_name: str) -> pd.DataFrame:
        step_paths = {
            "step1_sub": _output_path("step1_sub.csv"),
            "step2_fye_api": _output_path("step2_fye_api.csv"),
            "step3_html_parse": _output_path("step3_html.csv"),
            "step4_compute": _output_path("step4_compute.csv"),
        }
        path = step_paths[step_name]
        if not path.exists():
            raise FileNotFoundError(f"Cannot resume at {step_name}: {path} not found.")
        df_loaded = pd.read_csv(path, engine="python", on_bad_lines="skip")
        first_col = df_loaded.columns[0]
        if df_loaded[first_col].eq("__STATS__").any():
            df_loaded = df_loaded[df_loaded[first_col] != "__STATS__"].reset_index(drop=True)
        print(f"[resume] Loaded {step_name} from {path}")
        return df_loaded

    original_df = pd.read_csv(args.bsq, dtype=str, engine="python", on_bad_lines="skip")
    start_step = args.start_step
    order = ["step1_sub", "step2_fye_api", "step3_html_parse", "step4_compute"]
    start_index = order.index(start_step)

    if start_index <= order.index("step1_sub"):
        step1 = step1_sub(original_df, args.submap)
        stats_and_save(step1, f"{OUTPUT_PREFIX}.step1_sub.csv", filled=int(step1["quarter"].notna().sum()))
    else:
        step1 = _load_step("step1_sub")

    if start_index <= order.index("step2_fye_api"):
        step2, fye_tbl = step2_fye_api(step1, UA_DEFAULT, args.rate)
        stats_and_save(step2, f"{OUTPUT_PREFIX}.step2_fye_api.csv", filled=int(step2["quarter"].notna().sum()))
        fye_tbl.to_csv(f"{OUTPUT_PREFIX}.FYE_by_company_year.csv", index=False)
    else:
        step2 = _load_step("step2_fye_api")

    if start_index <= order.index("step3_html_parse"):
        step3 = step3_html_parse(step2, UA_DEFAULT, args.rate, no_progress=args.no_progress)
        stats_and_save(step3, f"{OUTPUT_PREFIX}.step3_html.csv", filled=int(step3["quarter"].notna().sum()))
    else:
        step3 = _load_step("step3_html_parse")

    if start_index <= order.index("step4_compute"):
        step4 = step4_compute_quarter(step3)
        filled4 = int(step4["_filled_this_step"].iloc[-1]) if "_filled_this_step" in step4.columns else 0
        step4_no_flag = step4.drop(columns=["_filled_this_step"], errors="ignore")
        stats_and_save(step4_no_flag, f"{OUTPUT_PREFIX}.step4_compute.csv", filled=filled4)
    else:
        step4_no_flag = _load_step("step4_compute")

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
