from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def read_last_load_dt(checkpoint_path: Path) -> str | None:
    if not checkpoint_path.exists():
        return None
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    last = str(payload.get("last_load_dt", "")).strip()
    return last or None


def write_checkpoint(
    checkpoint_path: Path,
    *,
    last_load_dt: str,
    close_case_since: str,
    limit: int,
    sort_by: str,
) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "last_success_utc": (
            datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        ),
        "last_load_dt": last_load_dt,
        "close_case_date_gt": close_case_since,
        "limit": limit,
        "sort": "asc",
        "sort_by": sort_by,
    }
    checkpoint_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8"
    )
