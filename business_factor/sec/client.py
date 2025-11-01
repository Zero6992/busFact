#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Thin wrapper around requests with retry and status aggregation for SEC endpoints.
"""

import random
import time
from collections import Counter
from typing import Any, Dict, Optional, Tuple

from business_factor.config import TIMEOUT

try:
    import requests
except Exception:
    requests = None  # type: ignore[assignment]

# HTTP status codes that trigger a retry
RETRYABLE_STATUS = {429, 403, 503, 500, 502, 504}

# Aggregate outcomes for this phase
_STATUS_COUNTS: Counter = Counter()

def status_counts(reset: bool = False) -> Dict[str, int]:
    """Return the accumulated status counts; reset when requested."""
    out = dict(_STATUS_COUNTS)
    if reset:
        _STATUS_COUNTS.clear()
    # Convert keys to strings to keep downstream serialization uniform
    return {str(k): int(v) for k, v in out.items()}

def _bump(code: Optional[int]) -> None:
    if code is None:
        _STATUS_COUNTS["EXC"] += 1
    else:
        _STATUS_COUNTS[str(code)] += 1

def _sleep_with_jitter(base_wait: float) -> None:
    # Sleep for base_wait plus up to 5 seconds of jitter
    time.sleep(base_wait + random.uniform(0, 5))

def _request_with_retry(url: str,
                        headers: Dict[str, str],
                        timeout: int = TIMEOUT,
                        max_retries: int = 5,
                        base_wait: float = 30.0):
    """Issue a GET with retries and record the final outcome."""
    if requests is None:
        _bump(None)
        return None, None

    last_resp = None
    last_code: Optional[int] = None

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            code = resp.status_code
            last_resp, last_code = resp, code

            # Success
            if code == 200 and resp.text:
                _bump(200)
                return resp, 200

            # Non-retryable -> record and return
            if code not in RETRYABLE_STATUS:
                _bump(code)
                return resp, code

            # Retryable -> wait and try again
            if attempt < max_retries - 1:
                _sleep_with_jitter(base_wait)

        except Exception:
            last_resp, last_code = None, None
            if attempt < max_retries - 1:
                _sleep_with_jitter(base_wait)

    # Exhausted retries (or never reached 200)
    _bump(last_code)
    return last_resp, last_code


def fetch_text(url: str, headers: Dict[str, str]) -> Tuple[Optional[str], Optional[int]]:
    resp, code = _request_with_retry(url, headers, timeout=TIMEOUT, max_retries=5, base_wait=30.0)
    if resp is not None and code == 200 and resp.text:
        return resp.text, code
    return None, code


def get_json(url: str, headers: Dict[str, str], timeout: int = TIMEOUT) -> Optional[Dict[str, Any]]:
    resp, code = _request_with_retry(url, headers, timeout=timeout, max_retries=5, base_wait=30.0)
    if resp is not None and code == 200:
        try:
            return resp.json()
        except Exception:
            return None
    return None


__all__ = ["fetch_text", "get_json", "requests", "status_counts"]
