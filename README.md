## How to Use

### Prerequisites
- Python 3.12
- Internet connection (SEC filings are downloaded on demand).
- CSV inputs: `BS_Q.csv` (filings) and `sub_map.csv` (CIK-to-submission mapping). Sample files live under `data/samples/`.

### Setup
```bash
python -m venv .venv
source .venv/bin/activate        # On Windows use: .venv\Scripts\activate
pip install pandas requests beautifulsoup4 tqdm sec-api
```

Add your SEC Extractor API key to a `.env` file in the project root:

```bash
echo "SEC_API_KEY=your_sec_api_key_here" >> .env
```

The `SEC_API_KEY` is required to retrieve Item 1A word counts via the SEC API.

### Project layout
```
src/busfactor/   Core pipeline modules and shared helpers.
scripts/         CLI utilities (enrichment, data quality tools, etc.).
data/            Workspace for inputs/outputs (ignored by git).
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
Feed the quarter CSV into the Item 1A enricher. It drops rows that lack a `ticker`, fetches the risk section via the SEC Extractor API (same source for keywords and word counts), computes totals, deduplicates repeated filings, sorts the final CSV alphabetically by `ticker` (when present), and issues SEC requests concurrently whenever `--rate 0` (the default) to minimize wall-clock time.

```bash
python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
```

Useful options:
- `--output` – override the destination CSV (default `data/outputs/bsq_quarter.item1a.csv`).
- `--rate` – seconds to sleep between requests; set `> 0` to force sequential throttling.
- `--max-workers` – cap the number of parallel SEC requests when `--rate 0` (default auto, max 8).
- `--max-rows` – run a quick check against the first N filings.
- `--keep-text` – retain the raw Item 1A text in the output.
- `--no-dedupe` – skip `(cik, fyear, quarter)` deduplication if you want every row.

### Quick smoke test
```bash
python3 scripts/enrich_item1a.py --input data/samples/BS_Q_test.csv --max-rows 5
```
