from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from pipeline.extract import query_endpoint_to_csv


class _FailingClient:
    def iter_pages(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("upstream failure")


class _StaticClient:
    def __init__(self, pages):
        self._pages = pages

    def iter_pages(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return iter(self._pages)


class ExtractTests(unittest.TestCase):
    def _make_temp_dir(self) -> Path:
        base_dir = Path.cwd() / ".tmp-tests"
        base_dir.mkdir(exist_ok=True)
        temp_dir = base_dir / f"case-{uuid.uuid4().hex}"
        temp_dir.mkdir()
        return temp_dir

    def test_query_endpoint_to_csv_preserves_existing_file_on_failure(self) -> None:
        tmp_dir = self._make_temp_dir()
        try:
            csv_path = tmp_dir / "violation_recent.csv"
            csv_path.write_text("id,name\n1,existing\n", encoding="utf-8")

            with self.assertRaises(RuntimeError):
                query_endpoint_to_csv(
                    client=_FailingClient(),
                    endpoint="violation",
                    out_csv=csv_path,
                    limit=100,
                    max_pages=1,
                    columns_override=["id", "name"],
                )

            self.assertEqual(csv_path.read_text(encoding="utf-8"), "id,name\n1,existing\n")
            self.assertFalse((tmp_dir / "violation_recent.csv.tmp").exists())
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_query_endpoint_to_csv_replaces_existing_file_on_success(self) -> None:
        tmp_dir = self._make_temp_dir()
        try:
            csv_path = tmp_dir / "violation_recent.csv"
            csv_path.write_text("id,name\n1,existing\n", encoding="utf-8")
            client = _StaticClient([[{"id": "2", "name": "fresh"}]])

            row_count = query_endpoint_to_csv(
                client=client,
                endpoint="violation",
                out_csv=csv_path,
                limit=100,
                max_pages=1,
                columns_override=["id", "name"],
            )

            self.assertEqual(row_count, 1)
            self.assertEqual(csv_path.read_text(encoding="utf-8"), "id,name\n2,fresh\n")
            self.assertFalse((tmp_dir / "violation_recent.csv.tmp").exists())
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
