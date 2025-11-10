
### 需求
- Python 3.12 以上
- 可上網（需線上下載 SEC 申報文件）
- 準備兩個 CSV：`BS_Q.csv`（申報清單）與 `sub_map.csv`（CIK 對應表）

### 環境設定
```bash
python -m venv .venv
source .venv/bin/activate        # Windows 請改用 .venv\Scripts\activate
pip install pandas requests beautifulsoup4 tqdm sec-api
```

在專案根目錄建立 `.env` 檔並寫入你的 SEC Extractor API 金鑰：

```bash
echo "SEC_API_KEY=你的SEC_API金鑰" >> .env
```

`SEC_API_KEY` 會用來透過 SEC API 擷取 Item 1A 文字以計算字數。

### 專案結構
```
src/busfactor/   核心流程與共用模組。
scripts/         指令列工具與資料檢查腳本。
data/            輸入與輸出工作區（不納入 git）。
```

### 第一步：判斷季度和FYE
以 `main.py` 為入口，輸入申報清單及對應表，程式會在 `data/outputs/` 產生 `bsq_quarter.final.csv` 附上`fye`和`quarters`欄位


```bash
python3 main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
```

常用參數：
- `--bsq`：欲處理的申報 CSV，需包含 `cik`、`filingUrl`、`filedAt` 等欄位。
- `--submap`：CIK 與申報檔案的對照表。
- `--max-rows`：可選，用於少量筆數測試。

### 第二步：計算 Item 1A 字數
將第一步產出的季度 CSV 交給 `scripts/enrich_item1a.py`。此腳本會先刪除沒有 `ticker` 的列，透過 SEC Extractor API 擷取風險因素章節（關鍵字與字數使用同一份來源），計算關鍵字與字數，針對同季度重複申報做去重，輸出時依 `ticker` 英文字母排序（若有該欄位），且在 `--rate 0`（預設值）時會並行向 SEC 發出請求以縮短時間。

```bash
python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
```

常用選項：
- `--output`：指定輸出檔名（預設為 `data/outputs/bsq_quarter.item1a.csv`）。
- `--rate`：每次請求間的停頓秒數；設定為大於 0 可強制改成逐筆串行模式。
- `--max-workers`：`--rate 0`（預設）時最多同時對 SEC 發出幾個請求（預設自動，最多 8）。
- `--max-rows`：只處理前 N 筆，方便快速檢查。
- `--keep-text`：保留原始 Item 1A 文字內容。
- `--no-dedupe`：略過 `(cik, fyear, quarter)` 去重邏輯，保留全部紀錄。

### 快速測試
```bash
python3 scripts/enrich_item1a.py --input data/samples/BS_Q_test.csv --max-rows 5
```
