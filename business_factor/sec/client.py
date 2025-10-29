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

# 會重試的 HTTP 狀態碼
RETRYABLE_STATUS = {429, 403, 503, 500, 502, 504}

# 全域統計（本階段所有請求的最終結果）
_STATUS_COUNTS: Counter = Counter()

def status_counts(reset: bool = False) -> Dict[str, int]:
    """
    回傳目前累計的狀態碼統計（字典），如果 reset=True 則在回傳後清空。
    Key 可能是整數字串（'200','429'...）或 'EXC'（發生例外）。
    """
    out = dict(_STATUS_COUNTS)
    if reset:
        _STATUS_COUNTS.clear()
    # 轉成字串鍵，避免下游序列化/顯示時混型別
    return {str(k): int(v) for k, v in out.items()}

def _bump(code: Optional[int]) -> None:
    if code is None:
        _STATUS_COUNTS["EXC"] += 1
    else:
        _STATUS_COUNTS[str(code)] += 1

def _sleep_with_jitter(base_wait: float) -> None:
    # 固定 30s + 0~5s 抖動
    time.sleep(base_wait + random.uniform(0, 5))

def _request_with_retry(url: str,
                        headers: Dict[str, str],
                        timeout: int = TIMEOUT,
                        max_retries: int = 5,
                        base_wait: float = 30.0):
    """
    發出 GET，針對 429/403/503/5xx 做最多 max_retries 次重試，每次等 base_wait 秒。
    回傳 (response 或 None, status_code 或 None)；把「最終結果」記入全域統計。
    """
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

            # 成功
            if code == 200 and resp.text:
                _bump(200)
                return resp, 200

            # 不可重試 → 紀錄後返回
            if code not in RETRYABLE_STATUS:
                _bump(code)
                return resp, code

            # 可重試 → 等待再試
            if attempt < max_retries - 1:
                _sleep_with_jitter(base_wait)

        except Exception:
            last_resp, last_code = None, None
            if attempt < max_retries - 1:
                _sleep_with_jitter(base_wait)

    # 全數失敗（或最後仍非 200）
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
