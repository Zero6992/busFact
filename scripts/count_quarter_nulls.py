#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


def count_quarter_values(csv_path: Path) -> None:
    df = pd.read_csv(csv_path, low_memory=False)

    if "quarter" not in df.columns:
        raise SystemExit(f"找不到欄位 'quarter'。可用欄位：{list(df.columns)}")

    col = df["quarter"]
    empty_mask = col.isna() | col.astype(str).str.strip().eq("")

    empty_count = int(empty_mask.sum())
    filled_count = int((~empty_mask).sum())

    print(f"quarter 空值筆數：{empty_count}")
    print(f"quarter 有值筆數：{filled_count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="計算 CSV 中 quarter 欄位的空值/有值筆數。"
    )
    parser.add_argument("csv", help="輸入 CSV 檔案路徑")
    args = parser.parse_args()

    count_quarter_values(Path(args.csv))


if __name__ == "__main__":
    main()
