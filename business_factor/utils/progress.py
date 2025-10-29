#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Optional tqdm progress bar wrapper.
"""

try:
    from tqdm.auto import tqdm
except Exception:
    tqdm = None

__all__ = ["tqdm"]
