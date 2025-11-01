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
  - `enrich_item1a.py` pulls Item 1A text, counts strategy keywords, and deduplicates filings.

Usage
-----
The full enrichment pass is split into two stages.

- **Quarter inference (`main.py`)**
  - Determines the filing quarter using `sub_map.csv`, SEC APIs, and HTML probing.
  - Uses the SEC-compliant UA `MegaTsai (jordan890522@gmail.com)` for every outbound request.
  - Example (writes intermediate CSVs plus `bsq_quarter.final.csv`):

    ```bash
    python3 main.py --bsq data/samples/BS_Q.csv --submap data/samples/sub_map.csv
    ```

- **Item 1A keyword & word-count enrichment (`scripts/enrich_item1a.py`)**
  - Fetches each filing’s Item 1A section, strips HTML, counts the configured strategy keywords, and measures total words.
  - Automatically deduplicates filings per `(cik, fyear, quarter)` by keeping the latest non-empty `total_words` entry (or latest overall if all are zero).
  - Respects the same fixed UA and exposes rate/diagnostic options for throttling or debugging.
  - Example (updates/creates `data/outputs/bsq_quarter.item1a.csv`):

    ```bash
    python3 scripts/enrich_item1a.py --input data/outputs/bsq_quarter.final.csv
    ```

Generated artifacts share the `bsq_quarter` prefix under `data/outputs/`. You can re-run either stage at any time; the second stage accepts the latest quarter CSV (including partially completed data).

Development tips
----------------
- Import helpers via the package, e.g. `from business_factor.pipeline import step1_sub`.
- Reuse the sample datasets from `data/samples/` for quick smoke tests.
- Additional utilities belong in `scripts/` to keep the package focused.
- Future work: integrate the Item 1A enrichment directly into the main pipeline once remaining downstream consumers are ready, and expand testing around SEC request failures/timeouts.
