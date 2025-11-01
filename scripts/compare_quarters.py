#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

DEFAULT_CSV1 = Path("../data/outputs/test_ans.csv")
DEFAULT_CSV2 = Path("../data/outputs/bsq_quarter.final.csv")

def print_section(title: str):
    bar = "=" * len(title)
    print(f"\n{bar}\n{title}\n{bar}")

def print_subsection(title: str):
    print(f"\n{title}")
    print("-" * len(title))

def load_and_normalize(path: str):
    df = pd.read_csv(path)
    required = {"filedAt", "quarter"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} 缺少欄位: {', '.join(sorted(missing))}")

    df = df.copy()
    df["filedAt_norm"] = pd.to_datetime(df["filedAt"], utc=True, errors="coerce")
    df["quarter_norm"] = df["quarter"].astype("string").str.strip()

    has_ticker = "ticker" in df.columns
    if has_ticker:
        df["ticker_norm"] = df["ticker"].astype("string").str.strip()
    else:
        df["ticker_norm"] = pd.NA
        print(f"[警告] {path} 無 'ticker' 欄位，將無法列出此檔的 ticker。")

    bad = df["filedAt_norm"].isna().sum()
    if bad:
        print(f"[警告] {path} 有 {bad} 列 filedAt 無法解析，將略過。")

    keep_cols = ["filedAt_norm", "quarter_norm", "ticker_norm"]
    return df.loc[df["filedAt_norm"].notna(), keep_cols], has_ticker

def format_for_display(df: pd.DataFrame, datetime_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in datetime_cols:
        if col in out.columns and is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%SZ")
    return out

def find_internal_inconsistency(df: pd.DataFrame) -> pd.DataFrame:
    nunique = df.groupby("filedAt_norm")["quarter_norm"].nunique(dropna=False)
    bad_keys = nunique[nunique > 1].index
    inconsistent = (
        df[df["filedAt_norm"].isin(bad_keys)]
        .drop_duplicates()
        .sort_values(["filedAt_norm", "quarter_norm"])
    )
    return inconsistent

def agg_by_filedAt(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    out = (
        df.groupby("filedAt_norm", dropna=False)
          .agg(
              **{
                  f"quarters_{prefix}": ("quarter_norm", lambda s: tuple(sorted({x for x in s if pd.notna(x)}))),
                  f"tickers_{prefix}":  ("ticker_norm",  lambda s: tuple(sorted({x for x in s if pd.notna(x)}))),
              }
          )
          .reset_index()
    )
    return out

def collect_unique_tickers(df: pd.DataFrame, cols: list[str]) -> list[str]:
    uniq = set()
    for c in cols:
        if c in df.columns:
            for v in df[c].dropna():
                if isinstance(v, tuple):
                    uniq.update([x for x in v if x])
                elif v:
                    uniq.add(v)
    return sorted(uniq)

def main(csv1: Path, csv2: Path, max_show: int):
    csv1 = Path(csv1)
    csv2 = Path(csv2)
    df1, has_ticker1 = load_and_normalize(csv1)
    df2, has_ticker2 = load_and_normalize(csv2)

    # Step 1: internal consistency check
    inc1 = find_internal_inconsistency(df1)
    inc2 = find_internal_inconsistency(df2)

    print_section("1) 內部檢查：同一 filedAt 是否對到多個 quarter")
    if inc1.empty:
        print(f"- {csv1.name}: 無矛盾")
    else:
        print(f"- {csv1.name}: 發現 {inc1['filedAt_norm'].nunique()} 個 filedAt 有多個 quarter")
        display = format_for_display(inc1.head(max_show), ["filedAt_norm"])
        print(display.to_string(index=False))
        if len(inc1) > max_show:
            print(f"...（僅顯示前 {max_show} 列）")

    if inc2.empty:
        print(f"- {csv2.name}: 無矛盾")
    else:
        print(f"- {csv2.name}: 發現 {inc2['filedAt_norm'].nunique()} 個 filedAt 有多個 quarter")
        display = format_for_display(inc2.head(max_show), ["filedAt_norm"])
        print(display.to_string(index=False))
        if len(inc2) > max_show:
            print(f"...（僅顯示前 {max_show} 列）")

    # Step 2: cross-file comparison
    a1 = agg_by_filedAt(df1, "1")
    a2 = agg_by_filedAt(df2, "2")
    merged = a1.merge(a2, on="filedAt_norm", how="outer", indicator=True)

    both = merged[merged["_merge"] == "both"].copy()
    both["is_equal"] = (both["quarters_1"] == both["quarters_2"])
    equal_rows = both[both["is_equal"]]
    diff_rows = both[~both["is_equal"]]
    only1 = merged[merged["_merge"] == "left_only"][["filedAt_norm", "quarters_1", "tickers_1"]]
    only2 = merged[merged["_merge"] == "right_only"][["filedAt_norm", "quarters_2", "tickers_2"]]

    # Step 3: summary
    print_section("2) 兩檔比對摘要（依 filedAt）")
    summary_rows = [
        ("兩檔皆有的 filedAt", len(both)),
        ("┣ Quarter 完全相同", len(equal_rows)),
        ("┗ Quarter 不同", len(diff_rows)),
        (f"僅在 {csv1.name} 出現", len(only1)),
        (f"僅在 {csv2.name} 出現", len(only2)),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["項目", "筆數"])
    print(summary_df.to_string(index=False))

    # Step 4: detailed differences (including ticker)
    if not diff_rows.empty:
        print_section(f"3) quarter 不同的 filedAt（最多顯示 {max_show} 列）")
        to_show = diff_rows.sort_values("filedAt_norm").head(max_show)
        cols = ["filedAt_norm", "quarters_1", "quarters_2", "tickers_1", "tickers_2"]
        display = format_for_display(to_show.loc[:, cols], ["filedAt_norm"])
        print(display.to_string(index=False))
        if len(diff_rows) > max_show:
            print(f"...（僅顯示前 {max_show} 列）")

        mismatch_tickers = collect_unique_tickers(diff_rows, ["tickers_1", "tickers_2"])
        if mismatch_tickers:
            print_subsection("有差異的 ticker 清單（去重後）")
            print(", ".join(mismatch_tickers))
        elif not (has_ticker1 or has_ticker2):
            print("\n[提示] 兩個檔案都沒有 'ticker' 欄位，無法列出 ticker。")

    if not only1.empty:
        print_section(f"4) 僅在 {csv1.name} 出現的 filedAt（最多顯示 {max_show} 列）")
        display = format_for_display(only1.sort_values("filedAt_norm").head(max_show), ["filedAt_norm"])
        print(display.to_string(index=False))
        if len(only1) > max_show:
            print(f"...（僅顯示前 {max_show} 列）")

    if not only2.empty:
        print_section(f"5) 僅在 {csv2.name} 出現的 filedAt（最多顯示 {max_show} 列）")
        display = format_for_display(only2.sort_values("filedAt_norm").head(max_show), ["filedAt_norm"])
        print(display.to_string(index=False))
        if len(only2) > max_show:
            print(f"...（僅顯示前 {max_show} 列）")

    # Step 5: conclusion
    print_section("6) 結論")
    ok_internal = inc1.empty and inc2.empty
    ok_interfile = len(diff_rows) == 0
    if ok_internal and ok_interfile:
        print("[結果] ✅ PASS：同 filedAt 的 quarter 在兩檔皆一致，且各檔內部無矛盾。")
    else:
        print("[結果] ❌ FAIL：存在差異或內部矛盾，請參考上方明細。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="比較兩個 CSV 中同 filedAt 的 quarter 是否相同（顯示差異時一併輸出 ticker）")
    parser.add_argument("--max-show", type=int, default=20, help="各區段最多顯示的列數（預設 20）")
    args = parser.parse_args()

    for path in (DEFAULT_CSV1, DEFAULT_CSV2):
        if not path.exists():
            raise FileNotFoundError(f"找不到預設檔案：{path}")

    main(DEFAULT_CSV1, DEFAULT_CSV2, args.max_show)
