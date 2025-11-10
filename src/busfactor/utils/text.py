#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helpers for normalizing filing text.
"""

from __future__ import annotations

import re
from typing import Final

PAGE_TOKEN_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:<PAGE>|##PAGE|Page\s*\d+(?:\s*of\s*\d+)?|\d+\s*PAGE)",
    re.IGNORECASE,
)


def replace_nbsp(text: str) -> str:
    if not text:
        return text
    return text.replace("\xa0", " ")


def strip_page_tokens(text: str) -> str:
    if not text:
        return text
    return PAGE_TOKEN_RE.sub(" ", text)


__all__ = ["PAGE_TOKEN_RE", "replace_nbsp", "strip_page_tokens"]
