#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEC API helpers for gathering fiscal year-end information.
"""

import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from business_factor.config import ANNUAL_FORMS_RAW
from business_factor.utils.progress import tqdm
from .client import get_json, status_counts


def pad_cik(cik: str) -> str:
    digits = "".join(ch for ch in str(cik) if ch.isdigit())
    return digits.zfill(10)


def norm_form(form: str) -> str:
    return (form or "").replace("-", "").upper()


ANNUAL_FORMS_NORM = {norm_form(form) for form in ANNUAL_FORMS_RAW}


def fetch_companyfacts(cik10: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    return get_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json", headers)


def fetch_submissions_all(
    cik10: str, headers: Dict[str, str], sleep: float
) -> List[Dict[str, Any]]:
    base = get_json(f"https://data.sec.gov/submissions/CIK{cik10}.json", headers) or {}
    rows: List[Dict[str, Any]] = []

    def rows_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        recent = (payload.get("filings") or {}).get("recent") or {}
        size = len(recent.get("accessionNumber", []))
        keys = list(recent.keys())
        extracted = []
        for idx in range(size):
            entry = {key: recent.get(key, [None] * size)[idx] for key in keys}
            extracted.append(entry)
        return extracted

    rows.extend(rows_from_payload(base))
    files = (base.get("filings") or {}).get("files") or []
    for item in files:
        name = item.get("name")
        if not name:
            continue
        older = get_json(f"https://data.sec.gov/submissions/{name}", headers)
        if older:
            rows.extend(rows_from_payload(older))
            time.sleep(sleep)
    return rows


def extract_fye_companyfacts(cik10: str, facts_json: Dict[str, Any]) -> pd.DataFrame:
    facts = (facts_json or {}).get("facts") or {}
    dei = facts.get("dei") or {}
    dpe = dei.get("DocumentPeriodEndDate") or {}
    rows: List[Dict[str, Any]] = []
    units = (dpe.get("units") or {})
    for arr in units.values():
        if not isinstance(arr, list):
            continue
        for item in arr:
            try:
                fp = item.get("fp")
                form = item.get("form") or ""
                fy = item.get("fy")
                end = item.get("end")
                filed = item.get("filed")
                accn = item.get("accn")
                if fp == "FY" and norm_form(form) in ANNUAL_FORMS_NORM and fy is not None and end:
                    rows.append(
                        {
                            "cik10": cik10,
                            "fyear": int(fy),
                            "fye_date": end,
                            "form": form,
                            "filed": filed,
                            "accn": accn,
                            "source": "companyfacts",
                        }
                    )
            except Exception:
                pass
    if not rows:
        return pd.DataFrame(
            columns=["cik10", "fyear", "fye_date", "form", "filed", "accn", "source"]
        )
    df = pd.DataFrame(rows)
    df = df.sort_values(["cik10", "fyear", "filed"]).groupby(["cik10", "fyear"], as_index=False).tail(1)
    return df


def extract_fye_submissions(cik10: str, subs_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for row in subs_rows:
        form = row.get("form") or ""
        if norm_form(form) not in ANNUAL_FORMS_NORM:
            continue
        report_date = row.get("reportDate") or row.get("periodOfReport")
        filed = row.get("filingDate") or row.get("filedAsOfDate")
        accn = row.get("accessionNumber")
        if not report_date:
            continue
        try:
            fyear = int(str(report_date)[:4])
        except Exception:
            continue
        rows.append(
            {
                "cik10": cik10,
                "fyear": fyear,
                "fye_date": report_date,
                "form": form,
                "filed": filed,
                "accn": accn,
                "source": "submissions",
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=["cik10", "fyear", "fye_date", "form", "filed", "accn", "source"]
        )
    df = pd.DataFrame(rows)
    df = df.sort_values(["cik10", "fyear", "filed"]).groupby(["cik10", "fyear"], as_index=False).tail(1)
    return df


def build_fye_map(dfq: pd.DataFrame, headers: Dict[str, str], sleep: float) -> pd.DataFrame:
    base = dfq[["cik", "fyear"]].dropna().drop_duplicates().copy()
    base["cik10"] = base["cik"].astype(str).apply(pad_cik)
    cik_list = sorted(base["cik10"].unique().tolist())
    rows: List[pd.DataFrame] = []
    iterable: Any = tqdm(cik_list, desc="Fetch FYE via SEC APIs") if tqdm else cik_list

    for cik10 in iterable:
        companyfacts = fetch_companyfacts(cik10, headers) or {}
        d1 = extract_fye_companyfacts(cik10, companyfacts)
        if d1.empty:
            subs_rows = fetch_submissions_all(cik10, headers, sleep=sleep)
            d2 = extract_fye_submissions(cik10, subs_rows)
            rows.append(d2)
        else:
            rows.append(d1)
        time.sleep(sleep)

    if rows:
        out = pd.concat(rows, ignore_index=True)
    else:
        out = pd.DataFrame(columns=["cik10", "fyear", "fye_date", "form", "filed", "accn", "source"])

    out["fye_month_api"] = pd.to_datetime(out["fye_date"], errors="coerce").dt.month

    # Fetch the HTTP status summary for this phase and write it to the output
    stats = status_counts(reset=True)  # grab counts and reset for the next stage
    # Print to stdout
    print("[FYE API] HTTP status summary:", stats)

    # Append a marker row whose last column stores the status summary
    if not out.empty:
        last_col = out.columns[-1]
        row = {c: "" for c in out.columns}
        row[out.columns[0]] = "__HTTP_STATUS__"
        row[last_col] = "; ".join([f"{k}:{v}" for k, v in sorted(stats.items())]) if stats else "N/A"
        out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
    else:
        # Even with no data, return a single row carrying the status summary
        cols = ["cik10", "fyear", "fye_date", "form", "filed", "accn", "source", "fye_month_api"]
        out = pd.DataFrame([{c: "" for c in cols}])
        out.loc[0, cols[0]] = "__HTTP_STATUS__"
        out.loc[0, cols[-1]] = "; ".join([f"{k}:{v}" for k, v in sorted(stats.items())]) if stats else "N/A"

    return out


__all__ = [
    "pad_cik",
    "norm_form",
    "fetch_companyfacts",
    "fetch_submissions_all",
    "extract_fye_companyfacts",
    "extract_fye_submissions",
    "build_fye_map",
]
