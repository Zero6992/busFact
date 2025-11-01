## How to Use

### Prerequisites
- Python 3.12
- Internet connection (SEC filings are downloaded on demand).
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
python3 scripts/enrich_item1a.py --input data/samples/BS_Q_test.csv --max-rows 5
```

### Run the regression test
```bash
pytest tests/test_section1a.py
```
