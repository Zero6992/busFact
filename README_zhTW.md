
### 需求
- Python 3.12 以上
- 可上網（需線上下載 SEC 申報文件）
- 準備兩個 CSV：`BS_Q.csv`（申報清單）與 `sub_map.csv`（CIK 對應表）
- `data/samples/BS_Q.sample.csv` 與 `data/samples/sub_map.sample.csv` 可作為欄位模板，確認檔案放置位置與欄位名稱後再換成正式資料

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

或是複製 `.env.sample` 為 `.env`，再把範例值換成自己的金鑰；請把 `.env` 保留在專案根目錄方便程式載入環境變數。

`SEC_API_KEY` 會用來透過 SEC API 擷取 Item 1A 文字以計算字數。

### 專案結構
```
src/      核心流程與共用模組。
scripts/  指令列工具與資料檢查腳本。
data/     輸入與輸出工作區（不納入 git）。
```

### 第一步：季度與 FYE 判斷邏輯

#### 輸入資料
- `BS_Q.csv` 提供每筆申報的 `cik`、`filingUrl`、`filedAt` 及可用來推測期間的欄位（如 `periodOfReport`）；
- `sub_map.csv` 出自 SEC Financial Statement Data Sets 的 `sub.txt`，篩選出需要的 `adsh` 列，再抽出 `fp`（Q1/Q2/Q3/FY）與 `period`（YYYYMMDD）。

#### 流程
- 先以 filing URL 解析 accession（無 dash `adsh`），再與 `sub_map.csv` 連結取得 `fp` 與 `period`；`FP_TO_Q` 只把 `Q1/Q2/Q3/H1/M9` 對應到季，`FY` 或缺值會留給後續推算。
- 若原始資料沒有 quarter，會依序套用多層偵測器：
  1. Inline XBRL 的 DEI 區段：直接讀取 `pf`（僅接受 `Q1-Q3`）以及 `period_end`、`fye_month`。
  2. Cover page 正則 `COVER_RE`：於純文字化的 HTML 中搜尋「for the quarterly period ended <日期>」等語句，`DATE_ANY` 可一次捕捉書寫式、`MM/DD/YY` 與 ISO 日期；若 HTML 失敗再退到 `.txt` 檔。
  3. Balance Sheet 區段：利用 `BAL_ASOF_RE` / `SOFP_ASOF_RE` / `COND_ASOF_RE` 等 regex，先鎖定「Balance Sheets / Statements of Financial Position」標題，再在限定範圍內找 `as of <date> and <date>` 的兩個日期，排除與期末月份相差不到一個月的值，剩餘者視為 FYE 月份。
  4. 如果 balance sheet 沒有年份，會用 `_fallback_month_only_from_balance_block` 在同一視窗尋找月份字詞並加權計分；最後還有 `FYE_PATTS` 針對「fiscal year ending <month>」文字描述。
- 上述偵測只要補齊末期日期 (`_period_end_date`) 與 FYE 月份，就會套用 `effective_period_month`（把當月 1-10 日的季度結束視為前一個月，以符合 13 週財報）再餵入 `quarter_from((pm - fm - 1) % 12 // 3 + 1)`，僅輸出 `Q1-Q3`，`FY` 仍保持空白讓使用者自行判讀。
- 每個步驟都會把補齊的筆數寫入 `__STATS__` 行，方便確認還有多少紀錄尚未判定季度。

### 第二步：Item 1A 字數與關鍵字邏輯
- 先移除 `ticker` 為空的列，確保 `(cik, fyear, quarter)` 可順利分組去重。
- `get_clean_1a_text` 取得文字時，優先呼叫 SEC Extractor API 的 `part2item1a`，若 API 無法用，就下載 HTML/TXT：以 BeautifulSoup 拿掉 script/style，再用 `ITEM_SECTION_RE` 從 `item 1a` 標題切到下一個 `item 1b~7a` 或新的 PART 標題；`TABLE_OF_CONTENTS_RE`、`strip_page_tokens` 會把頁碼、目錄與換頁符號清掉，確保只留下章節本體。
- 關鍵字統計採用 `PATTERN_GROUPS` 內的 regex，大小寫與複數變化不會影響計數。
- 字數統計直接對清理後的同一份文字做 `len(text.split())`，保持與關鍵字來源一致，避免標題或頁尾重複計入。
- `deduplicate_quarters` 會依 `(cik, fyear, quarter)` 進行偏好排序：優先保留 `total_words > 0` 的列，其次依 `filedAt` 最新者，以確保相同年季度只留下最完整的一筆資料。
- 最終輸出按照`ticker`字母標準化成大寫排序。

### 執行指令與常用選項

#### 第一步
```bash
python3 main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
```

#### 第二步
```bash
python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
```

常用選項：
- `--bsq`：欲處理的申報 CSV，需包含 `cik`、`filingUrl`、`filedAt` 等欄位。
- `--submap`：CIK 與申報檔案的對照表。
- `--output`：指定輸出檔名（預設為 `data/outputs/bsq_quarter.item1a.csv`）。
- `--rate`：每次請求間的停頓秒數；設定為大於 0 可強制改成逐筆串行模式。
- `--max-workers`：`--rate 0`（預設）時最多同時對 SEC 發出幾個請求（預設自動，最多 8）。
- `--max-rows`：只處理前 N 筆。
- `--keep-text`：保留原始 Item 1A 文字內容。
- `--no-dedupe`：略過 `(cik, fyear, quarter)` 去重邏輯，保留全部紀錄。
