Business Factor Toolkit
=======================

This repository now follows a package-first layout that keeps reusable logic under
`business_factor/`, sample data under `data/`, and ad-hoc helpers inside `scripts/`.

Key pieces
----------
- `main.py` – CLI entry point; keeps the original interface and imports the packaged pipeline.
- `business_factor/` – installable-style package grouped by responsibility:
  - `config.py` exposes shared constants.
  - `data/` contains IO helpers for CSV files.
  - `parsing/` holds HTML parsers and regex patterns.
  - `pipeline/` provides the four enrichment stages and post-processing helpers.
  - `sec/` wraps API calls and HTTP client utilities.
  - `utils/` currently exposes the optional tqdm dependency.
- `data/`
  - `samples/` demo inputs such as `BS_Q.csv` and `sub_map.csv`.
  - `outputs/` previously generated artifacts (safe to delete/regenerate).
  - `archive/` historical files retained for reference.
- `scripts/`
  - `compare_quarters.py` diff utility for quarter outputs.
  - `filter_empty_quarter.py` helper to filter rows with empty quarter values.

Usage
-----
Run the pipeline exactly as before:

```bash
python main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
```

Generated CSVs now land in `data/outputs/` with the shared prefix `bsq_quarter`.

Development tips
----------------
- Import helpers via the package, e.g. `from business_factor.pipeline import step1_sub`.
- Reuse the sample datasets from `data/samples/` for quick smoke tests.
- Additional utilities belong in `scripts/` to keep the package focused.
