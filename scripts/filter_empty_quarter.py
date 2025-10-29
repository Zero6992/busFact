# filter_empty_quarter.py
import argparse
from pathlib import Path
import pandas as pd

def main(in_path, out_path=None, inplace=False):
    in_path = Path(in_path)

    if inplace and out_path:
        raise SystemExit("請擇一：--inplace 或 -o/--output。")

    if not inplace:
        out_path = Path(out_path) if out_path else in_path.with_name(
            f"{in_path.stem}_quarter_empty_only{in_path.suffix}"
        )
    else:
        out_path = in_path

    # 讀檔
    df = pd.read_csv(in_path, low_memory=False)

    if "quarter" not in df.columns:
        raise SystemExit(f"找不到欄位 'quarter'。可用欄位：{list(df.columns)}")

    q = df["quarter"]
    # quarter 為 NaN 或去除前後空白後為空字串 -> 視為空值
    mask_empty = q.isna() | q.astype(str).str.strip().eq("")

    kept = df[mask_empty]
    kept.to_csv(out_path, index=False)

    print(f"保留下來（quarter 為空）的列數：{int(mask_empty.sum())}")
    print(f"被移除（quarter 有值）的列數：{int((~mask_empty).sum())}")
    print(f"已輸出到：{out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="只保留 quarter 欄位為空的列。")
    parser.add_argument("csv", help="輸入 CSV 檔案路徑")
    parser.add_argument("-o", "--output", help="輸出 CSV 檔案路徑（未指定則自動命名）")
    parser.add_argument("--inplace", action="store_true", help="直接覆寫原檔")
    args = parser.parse_args()

    main(args.csv, out_path=args.output, inplace=args.inplace)
