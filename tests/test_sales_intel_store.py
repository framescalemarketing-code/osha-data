from __future__ import annotations

import json
import shutil
import unittest
import uuid
from pathlib import Path

from pipeline.sales_intel_store import SalesIntelStore


class SalesIntelStoreTests(unittest.TestCase):
    def _temp_root(self) -> Path:
        root = Path(".tmp-tests") / f"sales-intel-{uuid.uuid4().hex}"
        root.mkdir(parents=True)
        return root

    def test_rebuilds_intersections_and_exports_snapshots(self) -> None:
        temp_root = self._temp_root()
        try:
            schema_path = Path("sql") / "sales_intel_schema.sql"
            snapshot_dir = temp_root / "snapshots"
            store = SalesIntelStore(
                db_path=temp_root / "sales_intel.sqlite",
                schema_path=schema_path,
                snapshot_dir=snapshot_dir,
            )
            store.initialize()
            store.replace_current_signals(
                [
                    {
                        "id": "current-1",
                        "company_name": "Acme Manufacturing",
                        "company_name_normalized": "ACMEMANUFACTURING",
                        "region": "Bay Area",
                        "overall_sales_score": 72,
                        "matched_sources": "FDA | EPA",
                        "raw_payload": "{}",
                    }
                ]
            )
            store.upsert_event(
                {
                    "id": "event-1",
                    "raw_item_id": "raw-1",
                    "company_name": "Acme Manufacturing",
                    "company_name_normalized": "ACMEMANUFACTURING",
                    "industry": "Manufacturing",
                    "event_type": "new facility opening",
                    "headline": "Acme Manufacturing opens new plant",
                    "summary": "New plant expansion",
                    "source_name": "IndustryWeek",
                    "source_url": "https://example.com/acme",
                    "published_at": "2026-03-06T10:00:00Z",
                    "location": "Phoenix, AZ",
                    "event_score": 84,
                    "signal_strength": "high",
                    "review_status": "accepted",
                    "raw_keywords": json.dumps(["plant"]),
                    "scale_clues": json.dumps(["300 jobs"]),
                    "classification_reason": "test",
                    "raw_payload": "{}",
                }
            )
            store.rebuild_intersections()
            store.export_snapshots()

            summary = json.loads((snapshot_dir / "summary.json").read_text(encoding="utf-8"))
            intersections = json.loads(
                (snapshot_dir / "intersections.json").read_text(encoding="utf-8")
            )

            self.assertEqual(summary["intersectionCount"], 1)
            self.assertEqual(len(intersections), 1)
            self.assertEqual(intersections[0]["companyName"], "Acme Manufacturing")
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
