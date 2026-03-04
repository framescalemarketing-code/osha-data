from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable


def write_rows(
    csv_path: Path,
    *,
    rows: Iterable[dict[str, object]],
    columns: list[str],
    append: bool = False,
) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append and csv_path.exists() else "w"
    wrote_header = mode == "w"
    row_count = 0
    with csv_path.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        if wrote_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in columns})
            row_count += 1
    return row_count


def ensure_header_only(csv_path: Path, columns: list[str]) -> None:
    if csv_path.exists():
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()


def csv_data_row_count(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)

