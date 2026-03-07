from __future__ import annotations

import hashlib
import html
import json
import re
from datetime import datetime, timezone
from typing import Any


_SPACE_RE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]+")
_LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:INCORPORATED|INC|CORPORATION|CORP|COMPANY|CO|LLC|L\.L\.C|LTD|LIMITED|LP|L\.P|PLC|HOLDINGS|GROUP)\b\.?$",
    re.IGNORECASE,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_id(*parts: str) -> str:
    payload = "|".join(part.strip() for part in parts if part is not None)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def normalize_space(value: str) -> str:
    return _SPACE_RE.sub(" ", value or "").strip()


def strip_html(value: str) -> str:
    if not value:
        return ""
    without_tags = _HTML_TAG_RE.sub(" ", value)
    return normalize_space(html.unescape(without_tags))


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def json_loads(value: str, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def normalize_company_name(company_name: str) -> str:
    cleaned = normalize_space(company_name)
    if not cleaned:
        return ""

    while True:
        updated = _LEGAL_SUFFIX_RE.sub("", cleaned).strip(" ,.-")
        if updated == cleaned:
            break
        cleaned = updated
    return normalize_space(cleaned)


def company_key(company_name: str) -> str:
    normalized = normalize_company_name(company_name).upper()
    return _NON_ALNUM_RE.sub("", normalized)


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = normalize_space(value)
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def first_number(value: str) -> float | None:
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", value or "")
    if not match:
        return None
    return float(match.group(0).replace(",", ""))
