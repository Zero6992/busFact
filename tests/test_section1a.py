#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from business_factor.pipeline.section1a import _extract_item_1a


def test_extract_item_1a_skips_cross_reference_in_mda():
    doc = (
        "Table of Contents Item 1A. Risk Factors 35 Item 1B. Other Matters "
        "Item 2. Management's Discussion and Analysis of Financial Condition and Results "
        "of Operations includes various forward-looking statements. "
        "Within this discussion the company may reference Item 1A). We undertake no "
        "obligation to update forward-looking statements made in Item 1A). "
        "Additional narrative continues for a number of paragraphs before transitioning "
        "to the next section. Item 3. Quantitative and Qualitative Disclosures About "
        "Market Risk follows in the quarterly report. Part II. Other Information begins "
        "with Item 1. Legal Proceedings before the actual risk factors section appears. "
        "Item 1A. Risk Factors Our operations face numerous risks and uncertainties "
        "that could adversely affect our business and financial results. Additional "
        "context about risks follows here to ensure the section is long enough to stand "
        "out from the brief cross reference. Item 2. Unregistered Sales of Equity "
        "Securities and Use of Proceeds concludes the excerpt."
    )

    section = _extract_item_1a(doc)

    assert section is not None
    assert section.startswith(
        "Risk Factors Our operations face numerous risks and uncertainties"
    )
    assert "We undertake no obligation to update forward-looking statements" not in section
