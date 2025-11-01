Business Factor Toolkit
=======================

Utilities for inferring SEC filing quarters and enriching Item 1A risk-factor disclosures with keyword and word-count metrics.

## English – How to Use

### Prerequisites
- Python 3.12 or newer.
- An internet connection (SEC filings are downloaded on demand).
- CSV inputs: `BS_Q.csv` (filings) and `sub_map.csv` (CIK-to-submission mapping). Sample files live under `data/samples/`.

### Setup
```bash
python -m venv .venv
source .venv/bin/activate        # On Windows use: .venv\Scripts\activate
pip install pandas requests beautifulsoup4 tqdm
```

### Step 1 – Determine quarters
Run the pipeline entry point with your filings CSV plus the submission map. The command below produces `data/outputs/bsq_quarter.final.csv` together with intermediate checkpoints.

```bash
python3 main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
```

Key flags:
- `--bsq` – input filings CSV (requires columns such as `cik`, `filingUrl`, `filedAt`).
- `--submap` – lookup table used to decide the fiscal quarter.
- `--max-rows` – optional cap for smoke tests.

### Step 2 – Enrich Item 1A
Feed the quarter CSV into the Item 1A enricher. It fetches the risk section, counts strategy keywords, computes total words, and deduplicates repeated filings.

```bash
python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
```

Useful options:
- `--output` – override the destination CSV (default `data/outputs/bsq_quarter.item1a.csv`).
- `--max-rows` – run a quick check against the first N filings.
- `--keep-text` – retain the raw Item 1A text in the output.
- `--no-dedupe` – skip `(cik, fyear, quarter)` deduplication if you want every row.

### Quick smoke test
```bash
python3 scripts/enrich_item1a.py --input data/samples/BS_Q_test.csv --max-rows 5 --no-progress
```

### Run the regression test
```bash
pytest tests/test_section1a.py
```

## 繁體中文使用說明

### 執行前準備
- 需安裝 Python 3.12 以上版本。
- 需可連線至網際網路（程式會即時下載 SEC 申報文件）。
- 準備兩個 CSV：`BS_Q.csv`（申報清單）與 `sub_map.csv`（CIK 對應表）。範例檔位於 `data/samples/`。

### 環境設定
```bash
python -m venv .venv
source .venv/bin/activate        # Windows 請改用 .venv\Scripts\activate
pip install pandas requests beautifulsoup4 tqdm
```

### 第一步：判定季度
以 `main.py` 為入口，輸入申報清單及對應表，程式會在 `data/outputs/` 產生 `bsq_quarter.final.csv` 及中間產物。

```bash
python3 main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
```

常用參數：
- `--bsq`：欲處理的申報 CSV，需包含 `cik`、`filingUrl`、`filedAt` 等欄位。
- `--submap`：CIK 與申報檔案的對照表。
- `--max-rows`：可選，用於少量筆數測試。

### 第二步：擴充 Item 1A
將第一步產出的季度 CSV 交給 `scripts/enrich_item1a.py`。此腳本會擷取風險因素章節、計算關鍵字以及字數，並針對同季度重複申報做去重。

```bash
python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
```

常用選項：
- `--output`：指定輸出檔名（預設為 `data/outputs/bsq_quarter.item1a.csv`）。
- `--max-rows`：只處理前 N 筆，方便快速檢查。
- `--keep-text`：保留原始 Item 1A 文字內容。
- `--no-dedupe`：略過 `(cik, fyear, quarter)` 去重邏輯，保留全部紀錄。

### 快速測試
```bash
python3 scripts/enrich_item1a.py --input data/samples/BS_Q_test.csv --max-rows 5 --no-progress
```

### 執行回歸測試
```bash
pytest tests/test_section1a.py
```
