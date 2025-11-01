#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities for probing HTML filings to derive fiscal information.
"""

import html as pyhtml
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

from business_factor.config import DATE_ANY
from business_factor.parsing import patterns as pat
from business_factor.sec.client import fetch_text
from business_factor.utils.text import replace_nbsp, strip_page_tokens

# 放在檔頭或常數區（不動你的 DATE_ANY；這是補充用）
DATE_MD_DASH = r"--?\d{2}-\d{2}"   # --12-31 或 -12-31
DATE_ANY_OR_MD = rf"(?:{DATE_ANY}|{DATE_MD_DASH})"

DATE_ANY_OR_MD_RE = re.compile(DATE_ANY_OR_MD)

# ---- 新增：Month Day（無年份）與 Month-only 的候補正則 ----
MONTH_ONLY_WORD = (
    r"(Jan(?:\.|uary)?|Feb(?:\.|ruary)?|Mar(?:\.|ch)?|Apr(?:\.|il)?|May|Jun(?:\.|e)?|"
    r"Jul(?:\.|y)?|Aug(?:\.|ust)?|Sep(?:\.|t\.|tember)?|Oct(?:\.|ober)?|Nov(?:\.|ember)?|"
    r"Dec(?:\.|ember)?)"
)
MONTH_DAY_NOYEAR_RE = re.compile(rf"\b{MONTH_ONLY_WORD}\s+\d{{1,2}}(?:,)?\b", re.I)
MONTH_ONLY_RE       = re.compile(rf"\b{MONTH_ONLY_WORD}\b", re.I)

# e.g. "December 31"  ... (Unaudited) ...  "2010"
SPLIT_DATE_RE = re.compile(
    rf"\b(?P<mon>{MONTH_ONLY_WORD})\s+(?P<day>\d{{1,2}})\s*,?\s*"
    rf"(?:\([^)]{{0,60}}\)\s*){{0,2}}(?:,?\s*)?"
    rf"(?P<year>\d{{4}})\b",
    re.I | re.S
)

def _extract_split_dates_from_block(block: str, limit: int = 12):
    """把被斷行/插入括號的 'Month DD ... YYYY' 重新合併為完整日期"""
    out = []
    seen = set()
    for m in SPLIT_DATE_RE.finditer(block):
        ds = f"{m.group('mon')} {m.group('day')}, {m.group('year')}"
        if ds in seen:
            continue
        seen.add(ds)
        dt = pd.to_datetime(ds, errors="coerce")
        if pd.notna(dt):
            out.append((ds, pd.to_datetime(dt.date()), int(dt.month), m.span()))
        if len(out) >= limit:
            break
    return out

# ---- 已有：Assets 錨點（確保不是目錄）；記得不分大小寫 ----
ASSETS_NEAR_RE = re.compile(r"\b(?:total\s+)?assets\b[:\s]?", re.I)

STOP_HEAD_PATTS = [
    re.compile(r"\b(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+OPERATIONS\b", re.I),
    re.compile(r"\b(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+INCOME\b", re.I),
    re.compile(r"\b(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+CASH\s+FLOWS\b", re.I),
    re.compile(r"\b(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+(?:SHAREHOLDERS'|STOCKHOLDERS')?\s*EQUITY\b", re.I),
    re.compile(r"\b(CONSOLIDATED\s+)?STATEMENTS?\s+OF\s+CHANGES\s+IN\s+(?:EQUITY|SHAREHOLDERS'|STOCKHOLDERS')\b", re.I),
    re.compile(r"\bNOTES\s+TO\s+(?:CONDENSED\s+)?(?:CONSOLIDATED\s+)?FINANCIAL\s+STATEMENTS\b", re.I),
]

def _truncate_at_next_section(block: str) -> str:
    cut = len(block)
    for patt in STOP_HEAD_PATTS:
        m = patt.search(block)
        if m and m.start() < cut:
            cut = m.start()
    return block[:cut]

# ---- 月名 → 月號 ----
_MONTH_MAP = {
    "jan":1,"jan.":1,"january":1,
    "feb":2,"feb.":2,"february":2,
    "mar":3,"mar.":3,"march":3,
    "apr":4,"apr.":4,"april":4,
    "may":5,
    "jun":6,"jun.":6,"june":6,
    "jul":7,"jul.":7,"july":7,
    "aug":8,"aug.":8,"august":8,
    "sep":9,"sept":9,"sep.":9,"sept.":9,"september":9,
    "oct":10,"oct.":10,"october":10,
    "nov":11,"nov.":11,"november":11,
    "dec":12,"dec.":12,"december":12,
}
def _mon_word_to_num(s: str) -> Optional[int]:
    if not s: return None
    key = s.strip().lower().rstrip(".")
    return _MONTH_MAP.get(key)

def _wrap_month(m: int) -> int:
    return ((int(m) - 1) % 12) + 1

def _near_months_set(pm: Optional[int]) -> set:
    if not pm or pd.isna(pm):
        return set()
    p = int(pm)
    return {_wrap_month(p - 1), _wrap_month(p), _wrap_month(p + 1)}


def _month_distance(pm: int, m: int) -> int:
    return (m - pm) % 12

def _score_candidate_month_only(mm: int, period_month: Optional[int], ctx: str) -> float:
    score = 0.0
    if period_month:
        dist = _month_distance(period_month, mm)
        # 偏好 3/6/9 月距（常見 FYE 與季末關係）
        if dist in {3,6,9}: score += 6.0
        elif dist >= 2:     score += 3.0
        # 遠離 ±1 再加分
        if dist not in {0,1,11}: score += 2.0
    # 若上下文有 fiscal year / year end 字樣，加分
    if re.search(r"\b(fiscal\s+year|year[-\s]*end(?:ed)?)\b", ctx, flags=re.I):
        score += 4.0
    return score


FY_PHRASE_RE   = re.compile(r"\b(fiscal\s+year|year[-\s]*end(?:ed)?)\b", re.I)

def _mm_from_dash_md(s: str) -> Optional[int]:
    m = re.fullmatch(r"-{1,2}(\d{2})-\d{2}", s.strip())
    return int(m.group(1)) if m else None

def _month_distance(pm: int, m: int) -> int:
    # 0..11 的圓環距離
    return (m - pm) % 12

def _score_candidate(ds: str, dt: Optional[pd.Timestamp], m: int, pm: Optional[int], ctx: str) -> float:
    score = 0.0
    # 1) 距離分數：偏好 3/6/9 月距（Q1/Q2/Q3 常見）
    if pm:
        dist = _month_distance(pm, m)
        if dist in {3,6,9}:
            score += 6.0
        elif dist >= 2:
            score += 3.0
        # 遠離 ±1 的再加分
        if dist not in {0,1,11}:
            score += 2.0
    # 2) 月底日加分
    if isinstance(dt, pd.Timestamp) and dt.day in {28,29,30,31}:
        score += 1.0
    # 3) 上下文關鍵詞
    if FY_PHRASE_RE.search(ctx):
        score += 4.0
    return score

def _extract_dates_from_block(block: str, limit: int = 16) -> List[Tuple[str, Optional[pd.Timestamp], Optional[int], Tuple[int,int]]]:
    """
    回傳 [(raw, dt|None, month|None, (s,e))]
    先合併斷行日期，再跑一般 DATE_ANY 掃描與 --MM-DD。
    """
    out: List[Tuple[str, Optional[pd.Timestamp], Optional[int], Tuple[int, int]]] = []
    seen_spans: List[Tuple[int, int]] = []

    merged = _extract_split_dates_from_block(block, limit=limit)
    out.extend(merged)
    seen_spans.extend([sp for _, _, _, sp in merged])

    for m in DATE_ANY_OR_MD_RE.finditer(block):
        s, e = m.span()
        if any(not (e <= ss or s >= ee) for (ss, ee) in seen_spans):
            continue

        ds = m.group(0).strip()
        if re.fullmatch(DATE_MD_DASH, ds):
            mm = _mm_from_dash_md(ds)
            out.append((ds, None, mm, (s, e)))
        else:
            dt = pd.to_datetime(ds, errors="coerce")
            if pd.notna(dt):
                out.append((ds, pd.to_datetime(dt.date()), dt.month, (s, e)))

        if len(out) >= limit:
            break
    return out


def html_to_text(html: str) -> str:
    text = replace_nbsp(pyhtml.unescape(html))
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = strip_page_tokens(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def probe_period(text: str) -> Optional[pd.Timestamp]:
    match = pat.COVER_RE.search(text)
    if match:
        raw_date = match.group("date")
        dt = pd.to_datetime(raw_date, errors="coerce")
        if pd.notna(dt):
            return pd.to_datetime(dt.date())
    return None


def month_in_near_set(candidate_m: int, period_m: Optional[int]) -> bool:
    if not period_m or pd.isna(period_m):
        return False
    return int(candidate_m) in _near_months_set(period_m)


def probe_fye_from_balance_asof(text: str, period_month: Optional[int]) -> Optional[int]:
    bad_near = _near_months_set(period_month)
    for patt in pat.ASOF_PATTERNS:
        for m in patt.finditer(text):
            for key in ("d2", "d1"):
                ds = m.group(key)
                dt = pd.to_datetime(ds, errors="coerce")
                if pd.notna(dt):
                    mm = int(dt.month)
                    if mm not in bad_near:
                        return mm
    return None

def probe_fye_from_balance_window(
    text: str,
    period_month: Optional[int],
    window_lo: int = 500,
    window_hi: int = 2500
) -> Optional[int]:
    """
    改良版：
      1) 先找最像標題的候選，但必須通過「表格就緒」門檻（附近要看到 Assets）
      2) 在標題後視窗抓多種日期（含 --MM-DD）
      3) 先試 as-of 句型（Balance Sheets / SOFP / Condition）→ 第二個日期若非 period ±1 月 → 採用
      4) 否則對候選日期打分，挑最高分（同分以出現位置較前者勝）
      5) 仍無解 → 最後候補用「只靠月份」從標題區域抓月
    """
    bad_near = _near_months_set(period_month)

    for head in pat.iter_balance_sheet_headings(text):
        start = head.end()

        # 表格就緒門檻：避免抓到目錄
        gate_block = text[start:start + max(window_lo, 800)]
        if not ASSETS_NEAR_RE.search(gate_block):
            continue

        block_raw = text[start:start + window_hi]
        block = _truncate_at_next_section(block_raw)

        # 先試 as-of 句型（Balance Sheets / SOFP / Condition）
        asof = None
        for _re in (pat.BAL_ASOF_RE, pat.SOFP_ASOF_RE, pat.COND_ASOF_RE, pat.SAL_ASOF_RE):
            m = _re.search(block)
            if m:
                asof = m
                break

        if asof:
            d1 = pd.to_datetime(asof.group("d1"), errors="coerce")
            d2 = pd.to_datetime(asof.group("d2"), errors="coerce")
            cand = []
            for dt in (d1, d2):
                if pd.notna(dt):
                    mm = int(dt.month)
                    if mm not in bad_near:
                        cand.append((mm, dt))
            if len(cand) == 1:
                return cand[0][0]
            if len(cand) == 2:
                # 兩個都合格 → 用打分挑一個
                scored = []
                for mm, dt in cand:
                    scored.append((
                        _score_candidate(str(dt.date()), dt, mm, period_month, block),
                        mm
                    ))
                scored.sort(reverse=True)
                return scored[0][1]

        # 泛化掃描（含 --MM-DD）
        items = _extract_dates_from_block(block, limit=16)
        cands = []
        for raw, dt, mm, span in items:
            if not mm:
                continue
            if period_month and mm in bad_near:
                continue
            s, e = span
            ctx = block[max(0, s-60): min(len(block), e+60)]
            score = _score_candidate(raw, dt, mm, period_month, ctx)
            # 帶上出現位置，供同分排序
            cands.append((score, s, mm))

        if cands:
            # 先比分數，再比出現位置（越前越好）
            cands.sort(key=lambda x: (-x[0], x[1]))
            return cands[0][2]

    # --- 最後候補：只有月份（月份/年份分行或沒有年份時） ---
    mm_last = _fallback_month_only_from_balance_block(
        text, period_month, window_lo=500, window_hi=2500
    )
    if mm_last:
        return mm_last

    return None


def probe_fye_from_text(text: str, period_month: Optional[int]) -> Optional[int]:
    for pattern in pat.FYE_PATTS:
        for match in pattern.finditer(text):
            month = _mon_word_to_num(match.group(1))
            if not month:
                continue
            if period_month is None or not month_in_near_set(month, period_month):
                return month
    return None


def fetch_html_then_txt_period(url: str, headers: Dict[str, str]) -> Optional[pd.Timestamp]:
    text, _ = fetch_text(url, headers)
    if text:
        dt = probe_period(html_to_text(text))
        if isinstance(dt, pd.Timestamp) and pd.notna(dt):
            return dt
    match = pat.ACCESSION_FOLDER_RE.search(url or "")
    if not match:
        return None
    cik = match.group(1)
    acc = match.group(2)
    if "-" in acc:
        acc_dash = acc
        acc_nodash = acc.replace("-", "")
    else:
        acc_nodash = acc
        acc_dash = f"{acc[:10]}-{acc[10:12]}-{acc[12:]}"
    candidates = [
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{acc_dash}.txt",
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{acc_nodash}.txt",
    ]
    for txt_url in candidates:
        txt, _ = fetch_text(txt_url, headers)
        if txt:
            match_txt = re.search(r"CONFORMED PERIOD OF REPORT:\s*(\d{8})", txt, flags=re.I)
            if match_txt:
                ymd = match_txt.group(1)
                try:
                    return pd.to_datetime(ymd, format="%Y%m%d")
                except Exception:
                    pass
    return None


def fetch_html_fye_month(
    url: str, headers: Dict[str, str], period_month: Optional[int]
) -> Optional[int]:
    text, _ = fetch_text(url, headers)
    if not text:
        return None
    content = html_to_text(text)
    month = probe_fye_from_balance_asof(content, period_month)
    if month:
        return month
    month = probe_fye_from_balance_window(content, period_month, window_lo=500, window_hi=1500)
    if month:
        return month
    month = probe_fye_from_text(content, period_month)
    if month:
        return month
    return None

def _fallback_month_only_from_balance_block(text: str, period_month: Optional[int],
                                            window_lo: int = 500, window_hi: int = 2500) -> Optional[int]:
    """
    最後候補：
      - 有偵測到 Balance Sheets / SOFP / Condition 標題
      - 標題後視窗近處含 'Assets'（避免抓到目錄）
      - 視窗內擷取 'Month Day'（無年份）與單獨 'Month' 作為候選
      - 排除 period_end 月份的 ±1 月
      - 用打分（距離 / fiscal year 關鍵詞）選一個月作為 FYE 月
    """
    bad_near = _near_months_set(period_month)

    for head in pat.iter_balance_sheet_headings(text):
        start = head.end()

        # 先做「表格就緒」門檻（避免目錄）
        gate_block = text[start:start + max(window_lo, 800)]
        if not ASSETS_NEAR_RE.search(gate_block):
            continue

        block = text[start:start + window_hi]

        candidates: List[Tuple[float, int, int]] = []  # (score, month, pos)

        # 1) Month Day（無年份），e.g., "March 31"、"December 31"
        for m in MONTH_DAY_NOYEAR_RE.finditer(block):
            mon_name = m.group(1)
            mm = _mon_word_to_num(mon_name)
            if not mm: 
                continue
            if period_month and mm in bad_near:
                continue
            s, e = m.span()
            ctx = block[max(0, s-60): min(len(block), e+60)]
            score = _score_candidate_month_only(mm, period_month, ctx)
            candidates.append((score, mm, s))

        # 2) 若 1) 沒抓到任何東西，再退到 Month-only（更弱）
        if not candidates:
            for m in MONTH_ONLY_RE.finditer(block):
                mon_name = m.group(1)
                mm = _mon_word_to_num(mon_name)
                if not mm:
                    continue
                if period_month and mm in bad_near:
                    continue
                s, e = m.span()
                ctx = block[max(0, s-60): min(len(block), e+60)]
                score = _score_candidate_month_only(mm, period_month, ctx) - 1.0  # 更弱，稍微扣分
                candidates.append((score, mm, s))

        if not candidates:
            continue

        # 去重（同一月份重複出現，只取分數最高、最靠前）
        best_by_month: Dict[int, Tuple[float, int]] = {}
        for sc, mm, pos in candidates:
            if (mm not in best_by_month) or (sc > best_by_month[mm][0]) or (sc == best_by_month[mm][0] and pos < best_by_month[mm][1]):
                best_by_month[mm] = (sc, pos)

        # 依分數排序；同分取位置較前
        final = sorted([(sc, mm, pos) for mm, (sc, pos) in best_by_month.items()],
                       key=lambda x: (-x[0], x[2]))
        return final[0][1]

    return None


__all__ = [
    "html_to_text",
    "probe_period",
    "month_in_near_set",
    "probe_fye_from_balance_asof",
    "probe_fye_from_balance_window",
    "probe_fye_from_text",
    "fetch_html_then_txt_period",
    "fetch_html_fye_month",
]
