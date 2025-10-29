#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
End-to-end pipeline steps for enriching BS_Q data with quarter information.
"""

import time
from typing import Optional, Tuple

import pandas as pd

from business_factor.config import FP_TO_Q
from business_factor.data import accession_from_url, canon_url, detect_url_column
from business_factor.parsing import (
    extract_dei_from_html,
    fetch_html_then_txt_period,
    html_to_text,
    probe_fye_from_balance_asof,
    probe_fye_from_balance_window,
    probe_fye_from_text,
    probe_period,
)
from business_factor.sec.api import build_fye_map, pad_cik
from business_factor.sec.client import fetch_text
from business_factor.utils.progress import tqdm


def effective_period_month(period_dt: pd.Timestamp, cutoff: int = 10) -> Optional[int]:
    """
    對於 13 週季末落在月初幾天（例：7/3、7/5）時，把有效月份往前挪一個月。
    cutoff=10 表示 1~10 日都視為上個月。
    """
    if period_dt is None or pd.isna(period_dt):
        return None
    period_dt = pd.to_datetime(period_dt)
    if period_dt.day <= cutoff:
        # 減去 cutoff 天再取月份，避免 1~10 號都落在同月
        return int((period_dt - pd.Timedelta(days=cutoff)).month)
    return int(period_dt.month)

def stats_row(df: pd.DataFrame, filled: int) -> pd.DataFrame:
    remain = int((df["quarter"].isna() | df["quarter"].astype(str).str.strip().eq("")).sum())
    row = {col: "" for col in df.columns}
    row[df.columns[0]] = "__STATS__"
    row["quarter"] = f"FILLED={filled}; REMAIN={remain}"
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True)


def choose_sort_col(df: pd.DataFrame) -> Optional[str]:
    for column in ["company", "companyName", "conm", "name", "CompanyName", "issuer", "ticker"]:
        if column in df.columns:
            return column
    return "ticker" if "ticker" in df.columns else None


def step1_sub(original_df: pd.DataFrame, sub_path: str) -> pd.DataFrame:
    df = original_df.copy()
    url_col = detect_url_column(df)
    if not url_col:
        raise SystemExit("Could not detect URL column in BS_Q.")
    df["_adsh_nodash"] = df[url_col].astype(str).apply(accession_from_url)
    sub = pd.read_csv(sub_path, dtype=str, engine="python", on_bad_lines="skip")
    if "adsh" not in sub.columns or "fp" not in sub.columns:
        raise SystemExit("sub_map.csv must include 'adsh' and 'fp'.")
    sub = sub.drop_duplicates("adsh")
    merged = df.merge(sub[["adsh", "fp", "period"]], left_on="_adsh_nodash", right_on="adsh", how="left")
    df["quarter"] = merged["fp"].map(FP_TO_Q)
    df["_period_end_date_sub"] = pd.to_datetime(merged["period"], format="%Y%m%d", errors="coerce")
    return df


def step2_fye_api(df1: pd.DataFrame, ua: str, rate: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df1.copy()
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate", "Connection": "keep-alive"}
    fye_tbl = build_fye_map(df, headers, sleep=rate)
    df["cik10"] = df["cik"].astype(str).apply(pad_cik) if "cik" in df.columns else ""
    df["fyear"] = pd.to_numeric(df["fyear"], errors="coerce").astype("Int64")
    fye_tbl["fyear"] = pd.to_numeric(fye_tbl["fyear"], errors="coerce").astype("Int64")
    fye_tbl = fye_tbl.rename(columns={"fye_month_api": "_fye_month_api"})
    fye_tbl["_fye_date_api"] = pd.to_datetime(fye_tbl["fye_date"], errors="coerce").dt.normalize()
    df = df.merge(
        fye_tbl[["cik10", "fyear", "_fye_month_api", "_fye_date_api"]],
        on=["cik10", "fyear"],
        how="left",
    )
    df.drop(columns=["cik10"], inplace=True, errors="ignore")
    return df, fye_tbl


def step3_html_parse(df2: pd.DataFrame, ua: str, rate: float, no_progress: bool=False) -> pd.DataFrame:
    df = df2.copy()
    url_col = detect_url_column(df)
    headers = {"User-Agent": ua, "Accept-Encoding":"gzip, deflate", "Connection":"keep-alive"}
    # 欄位初始化
    if "_period_end_date_html" not in df.columns:
        df["_period_end_date_html"] = pd.NaT
    if "_fye_month_html" not in df.columns:
        df["_fye_month_html"] = pd.NA

    quarter_missing = df["quarter"].isna() if "quarter" in df.columns else pd.Series(True, index=df.index, dtype=bool)
    if "_period_end_date_sub" in df.columns:
        period_missing = df["_period_end_date_sub"].isna() & df["_period_end_date_html"].isna()
    else:
        period_missing = df["_period_end_date_html"].isna()

    # 需要補抓 HTML 的情境：缺季、缺期末日、或 API 也沒給 FYE 月
    html_fye_missing = df["_fye_month_html"].isna()
    if "_fye_month_api" in df.columns:
        api_fye_missing = df["_fye_month_api"].isna()
    else:
        api_fye_missing = pd.Series(True, index=df.index, dtype=bool)
    fye_missing = html_fye_missing & api_fye_missing

    idxs = df.index[(quarter_missing | period_missing | fye_missing)].tolist()
    use_bar = (not no_progress) and (tqdm is not None)
    it = tqdm(idxs, desc="HTML probe (DEI → period & FYE)", unit="row") if use_bar else idxs

    for i in it:
        url = canon_url(df.at[i, url_col]) if url_col in df.columns else None
        if not url:
            continue

        # 取 HTML 原文一次，避免重覆抓
        pm: Optional[int] = None
        text_cache: Optional[str] = None
        html, _ = fetch_text(url, headers)

        # ---------- P0: Inline XBRL DEI（最高優先） ----------
        if html:
            dei = extract_dei_from_html(html)
            # period_end（若有）
            if isinstance(dei.get("period_end"), pd.Timestamp) and pd.notna(dei["period_end"]):
                normalized = pd.to_datetime(dei["period_end"]).normalize()
                df.at[i, "_period_end_date_html"] = normalized
                pm = int(normalized.month)
            # FYE 月（若有）
            if dei.get("fye_month"):
                df.at[i, "_fye_month_html"] = int(dei["fye_month"])
            # 直接可用的 Quarter（若 pf ∈ Q1~Q3）
            pf = (dei.get("pf") or "").upper()
            if pf in {"Q1","Q2","Q3"} and pd.isna(df.at[i, "quarter"]):
                df.at[i, "quarter"] = pf
                # 後續仍可利用文本補 period/FYE

        # ---------- P1: 封面句抓 period_end ----------
        if pd.isna(df.at[i, "_period_end_date_html"]):
            dt = None
            if html:
                text_cache = html_to_text(html) if text_cache is None else text_cache
                dt = probe_period(text_cache)
            if dt is None:
                # fallback .txt header
                dt = fetch_html_then_txt_period(url, headers)
            if isinstance(dt, pd.Timestamp) and pd.notna(dt):
                normalized = pd.to_datetime(dt).normalize()
                df.at[i, "_period_end_date_html"] = normalized
                pm = int(normalized.month)
        elif pm is None and pd.notna(df.at[i, "_period_end_date_html"]):
            pm = int(pd.to_datetime(df.at[i, "_period_end_date_html"]).month)

        # ---------- P2/P3: FYE（as-of 句 / 標題視窗 / 文字句型） ----------
        if html and pd.isna(df.at[i, "_fye_month_html"]):
            text_cache = html_to_text(html) if text_cache is None else text_cache
            # P1(as-of second date) → P2(標題視窗) → P3(文字句)
            mm = probe_fye_from_balance_asof(text_cache, pm)
            if not mm:
                mm = probe_fye_from_balance_window(text_cache, pm, window_lo=500, window_hi=1500)
            if not mm:
                mm = probe_fye_from_text(text_cache, pm)
            if mm:
                df.at[i, "_fye_month_html"] = int(mm)

        time.sleep(max(rate, 0.0))

    return df

def quarter_from(period_month: int, fye_month: int) -> Optional[str]:
    if not period_month or not fye_month:
        return None
    quarter = ((int(period_month) - int(fye_month) - 1) % 12) // 3 + 1
    return f"Q{quarter}" if quarter in (1, 2, 3) else None


def step4_compute_quarter(df3: pd.DataFrame) -> pd.DataFrame:
    df = df3.copy()
    df["_period_end_date"] = df["_period_end_date_sub"].fillna(df["_period_end_date_html"])

    def choose_fye_month(row: pd.Series):
        pm = None
        if pd.notna(row["_period_end_date"]):
            pm = int(pd.to_datetime(row["_period_end_date"]).month)
        html_month = row.get("_fye_month_html", pd.NA)
        api_month = row.get("_fye_month_api", pd.NA)
        if pd.notna(html_month):
            if pm is not None and int(html_month) == pm:
                return int(api_month) if pd.notna(api_month) else pd.NA
            return int(html_month)
        return int(api_month) if pd.notna(api_month) else pd.NA

    df["_fye_month"] = df.apply(choose_fye_month, axis=1).astype("Int64")
    before = int((df["quarter"].isna() | df["quarter"].astype(str).str.strip().eq("")).sum())
    mask = df["quarter"].isna() & df["_period_end_date"].notna() & df["_fye_month"].notna()
    if mask.any():
        iterator = tqdm(df.index[mask], desc="Compute quarter (FYE + period)") if tqdm else df.index[mask]
        for idx in iterator:
            pdt = pd.to_datetime(df.at[idx, "_period_end_date"])
            fm  = int(df.at[idx, "_fye_month"])
            pm  = effective_period_month(pdt, cutoff=10)  # ← 這行是關鍵
            quarter = quarter_from(pm, fm)
            if quarter:
                df.at[idx, "quarter"] = quarter
    filled = before - int((df["quarter"].isna() | df["quarter"].astype(str).str.strip().eq("")).sum())
    df["_filled_this_step"] = filled
    return df


def finalize(original_df: pd.DataFrame, df_all: pd.DataFrame, out_path: str):
    final = original_df.copy().reset_index(drop=True)
    df_all = df_all.reset_index(drop=True)

    final["quarter"] = df_all["quarter"].values

    period_series = (
        pd.to_datetime(df_all["_period_end_date"], errors="coerce").dt.normalize()
        if "_period_end_date" in df_all.columns
        else pd.Series(pd.NaT, index=df_all.index, dtype="datetime64[ns]")
    )
    formatted_period = period_series.dt.strftime("%Y-%m-%d")
    final["periodOfReport"] = formatted_period.fillna("")

    if "_fye_month" in df_all.columns:
        fye_month_series = pd.to_numeric(df_all["_fye_month"], errors="coerce").astype("Int64")
    else:
        fye_month_series = pd.Series(pd.NA, index=df_all.index, dtype="Int64")

    if "_fye_date_api" in df_all.columns:
        fye_date_series = pd.to_datetime(df_all["_fye_date_api"], errors="coerce").dt.normalize()
    else:
        fye_date_series = pd.Series(pd.NaT, index=df_all.index, dtype="datetime64[ns]")

    def _format_fye_month(val) -> str:
        if pd.isna(val):
            return ""
        try:
            month = int(val)
        except (TypeError, ValueError):
            return ""
        return f"{month:02d}" if 1 <= month <= 12 else ""

    fye_mm = fye_month_series.apply(_format_fye_month)
    fye_mmdd = fye_date_series.dt.strftime("%m/%d")
    fye_out = fye_mmdd.where(fye_date_series.notna(), fye_mm)
    final["fye"] = fye_out.fillna("")

    sort_col = None
    for column in ["company", "companyName", "conm", "name", "CompanyName", "issuer", "ticker"]:
        if column in final.columns:
            sort_col = column
            break
    if sort_col:
        if "fyear" in final.columns:
            final = final.sort_values([sort_col, "fyear"], kind="mergesort")
        else:
            final = final.sort_values([sort_col], kind="mergesort")
    final.to_csv(out_path, index=False)


def stats_and_save(df: pd.DataFrame, path: str, filled: int):
    out = df.copy()
    remain = int((out["quarter"].isna() | out["quarter"].astype(str).str.strip().eq("")).sum())
    row = {col: "" for col in out.columns}
    row[list(out.columns)[0]] = "__STATS__"
    row["quarter"] = f"FILLED={filled}; REMAIN={remain}"
    out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
    out.to_csv(path, index=False)


__all__ = [
    "stats_row",
    "choose_sort_col",
    "step1_sub",
    "step2_fye_api",
    "step3_html_parse",
    "quarter_from",
    "step4_compute_quarter",
    "finalize",
    "stats_and_save",
]
