from __future__ import annotations

import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, TextIO

if os.name == "nt":
    import msvcrt
else:
    import fcntl


@contextmanager
def _exclusive_file_lock(lock_path: Path) -> Iterator[TextIO]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        if os.name == "nt":
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write("0")
                handle.flush()
            handle.seek(0)
            while True:
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield handle
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class GlobalRateLimiter:
    def __init__(
        self,
        *,
        min_interval_seconds: float,
        state_file: Path,
        lock_file: Path,
    ) -> None:
        self.min_interval_seconds = min_interval_seconds
        self.state_file = state_file
        self.lock_file = lock_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def wait_for_slot(self, label: str = "api") -> None:
        with _exclusive_file_lock(self.lock_file):
            last_call_ts = self._read_last_call_timestamp()
            now = time.time()
            if last_call_ts is not None:
                elapsed = now - last_call_ts
                if elapsed < self.min_interval_seconds:
                    wait_seconds = self.min_interval_seconds - elapsed
                    time.sleep(wait_seconds)
                    now = time.time()
            self._write_last_call_timestamp(now)

    def _read_last_call_timestamp(self) -> float | None:
        if not self.state_file.exists():
            return None
        raw = self.state_file.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.timestamp()
            except ValueError:
                return None

    def _write_last_call_timestamp(self, ts: float) -> None:
        # Store both unix timestamp and UTC ISO for easier support/debug.
        iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        self.state_file.write_text(f"{ts}\n{iso}\n", encoding="utf-8")
