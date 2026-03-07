from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from pipeline.config import PipelineConfig, env_value, load_dotenv
from pipeline.sales_intel_common import company_key, json_dumps, json_loads, stable_id, utc_now_iso


def _store_path(config: PipelineConfig) -> Path:
    dotenv_values = load_dotenv(config.paths.dotenv_path)
    configured = env_value("SALES_INTEL_DB_PATH", dotenv_values, "")
    if configured.strip():
        return Path(configured)
    return config.paths.data_dir / "sales_intel" / "sales_intel.sqlite"


def _snapshot_dir(config: PipelineConfig) -> Path:
    dotenv_values = load_dotenv(config.paths.dotenv_path)
    configured = env_value("SALES_INTEL_SNAPSHOT_DIR", dotenv_values, "")
    if configured.strip():
        return Path(configured)
    return config.paths.data_dir / "dashboard"


class SalesIntelStore:
    def __init__(self, *, db_path: Path, schema_path: Path, snapshot_dir: Path) -> None:
        self.db_path = db_path
        self.schema_path = schema_path
        self.snapshot_dir = snapshot_dir

    @classmethod
    def from_config(cls, config: PipelineConfig) -> "SalesIntelStore":
        return cls(
            db_path=_store_path(config),
            schema_path=config.paths.sql_dir / "sales_intel_schema.sql",
            snapshot_dir=_snapshot_dir(config),
        )

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        sql_text = self.schema_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(sql_text)

    def replace_current_signals(self, signals: list[dict[str, Any]]) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute("DELETE FROM current_pipeline_signals")
            for row in signals:
                conn.execute(
                    """
                    INSERT INTO current_pipeline_signals (
                      id, company_name, company_name_normalized, region, site_address, site_city,
                      site_state, site_zip, industry_segment, current_priority, current_action,
                      overall_sales_score, matched_sources, reason_to_contact, reason_to_call_now,
                      why_fit, why_now, raw_payload, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["id"],
                        row["company_name"],
                        row["company_name_normalized"],
                        row.get("region", ""),
                        row.get("site_address", ""),
                        row.get("site_city", ""),
                        row.get("site_state", ""),
                        row.get("site_zip", ""),
                        row.get("industry_segment", ""),
                        row.get("current_priority", ""),
                        row.get("current_action", ""),
                        float(row.get("overall_sales_score", 0.0)),
                        row.get("matched_sources", ""),
                        row.get("reason_to_contact", ""),
                        row.get("reason_to_call_now", ""),
                        row.get("why_fit", ""),
                        row.get("why_now", ""),
                        row.get("raw_payload", "{}"),
                        now,
                        now,
                    ),
                )

    def upsert_raw_feed_item(self, row: dict[str, Any]) -> str:
        now = utc_now_iso()
        item_id = row["id"]
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO raw_feed_items (
                  id, feed_name, feed_url, item_title, item_summary, item_url, published_at,
                  raw_payload, processed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  feed_name = excluded.feed_name,
                  feed_url = excluded.feed_url,
                  item_title = excluded.item_title,
                  item_summary = excluded.item_summary,
                  item_url = excluded.item_url,
                  published_at = excluded.published_at,
                  raw_payload = excluded.raw_payload,
                  processed_at = excluded.processed_at
                """,
                (
                    item_id,
                    row["feed_name"],
                    row["feed_url"],
                    row["item_title"],
                    row["item_summary"],
                    row["item_url"],
                    row.get("published_at", ""),
                    row["raw_payload"],
                    now,
                    now,
                ),
            )
        return item_id

    def upsert_event(self, row: dict[str, Any]) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events (
                  id, raw_item_id, company_name, company_name_normalized, industry, event_type,
                  headline, summary, source_name, source_url, published_at, location, event_score,
                  signal_strength, review_status, raw_keywords, scale_clues,
                  classification_reason, raw_payload, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  raw_item_id = excluded.raw_item_id,
                  company_name = excluded.company_name,
                  company_name_normalized = excluded.company_name_normalized,
                  industry = excluded.industry,
                  event_type = excluded.event_type,
                  headline = excluded.headline,
                  summary = excluded.summary,
                  source_name = excluded.source_name,
                  source_url = excluded.source_url,
                  published_at = excluded.published_at,
                  location = excluded.location,
                  event_score = excluded.event_score,
                  signal_strength = excluded.signal_strength,
                  review_status = excluded.review_status,
                  raw_keywords = excluded.raw_keywords,
                  scale_clues = excluded.scale_clues,
                  classification_reason = excluded.classification_reason,
                  raw_payload = excluded.raw_payload,
                  updated_at = excluded.updated_at
                """,
                (
                    row["id"],
                    row.get("raw_item_id", ""),
                    row["company_name"],
                    row["company_name_normalized"],
                    row["industry"],
                    row["event_type"],
                    row["headline"],
                    row["summary"],
                    row["source_name"],
                    row["source_url"],
                    row.get("published_at", ""),
                    row.get("location", ""),
                    float(row["event_score"]),
                    row["signal_strength"],
                    row["review_status"],
                    row["raw_keywords"],
                    row["scale_clues"],
                    row["classification_reason"],
                    row["raw_payload"],
                    now,
                    now,
                ),
            )

    def replace_event_contacts(self, *, event_id: str, contacts: list[dict[str, Any]]) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute("DELETE FROM event_contacts WHERE event_id = ?", (event_id,))
            for contact in contacts:
                contact_id = contact.get("id") or stable_id(
                    company_key(contact["company_name"]),
                    contact["contact_type"],
                    contact["contact_value"],
                )
                conn.execute(
                    """
                    INSERT INTO contacts (
                      id, company_name, company_name_normalized, contact_type, contact_value,
                      name, title, source_name, source_url, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                      company_name = excluded.company_name,
                      company_name_normalized = excluded.company_name_normalized,
                      contact_type = excluded.contact_type,
                      contact_value = excluded.contact_value,
                      name = excluded.name,
                      title = excluded.title,
                      source_name = excluded.source_name,
                      source_url = excluded.source_url,
                      updated_at = excluded.updated_at
                    """,
                    (
                        contact_id,
                        contact["company_name"],
                        contact["company_name_normalized"],
                        contact["contact_type"],
                        contact["contact_value"],
                        contact.get("name", ""),
                        contact.get("title", ""),
                        contact["source_name"],
                        contact["source_url"],
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO event_contacts (event_id, contact_id) VALUES (?, ?)",
                    (event_id, contact_id),
                )

    def rebuild_intersections(self) -> None:
        now = utc_now_iso()
        with self._connect() as conn:
            conn.execute("DELETE FROM intersection_alerts")
            rows = conn.execute(
                """
                SELECT
                  c.id AS current_signal_id,
                  c.company_name AS current_company_name,
                  c.company_name_normalized AS company_name_normalized,
                  c.overall_sales_score AS current_score,
                  e.id AS event_id,
                  e.company_name AS event_company_name,
                  e.event_score AS event_score,
                  e.event_type AS event_type
                FROM current_pipeline_signals c
                JOIN events e
                  ON c.company_name_normalized = e.company_name_normalized
                WHERE e.review_status = 'accepted'
                """
            ).fetchall()

            for row in rows:
                company_name = row["current_company_name"] or row["event_company_name"]
                alert_score = min(100.0, round((float(row["current_score"]) + float(row["event_score"])) / 2.0 + 10.0, 1))
                alert_reason = (
                    f"Existing current-pipeline signal intersects with RSS opportunity event "
                    f"({row['event_type']})."
                )
                conn.execute(
                    """
                    INSERT INTO intersection_alerts (
                      id, company_name, company_name_normalized, current_signal_id, event_id,
                      alert_score, alert_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        stable_id(row["current_signal_id"], row["event_id"]),
                        company_name,
                        row["company_name_normalized"],
                        row["current_signal_id"],
                        row["event_id"],
                        alert_score,
                        alert_reason,
                        now,
                        now,
                    ),
                )

    def export_snapshots(self) -> None:
        current_signals = self._query_current_signals()
        opportunity_events = self._query_events(review_status="accepted")
        review_queue = self._query_events(review_status="review")
        intersections = self._query_intersections()
        summary = {
            "generatedAt": utc_now_iso(),
            "currentSignalCount": len(current_signals),
            "opportunityEventCount": len(opportunity_events),
            "reviewQueueCount": len(review_queue),
            "intersectionCount": len(intersections),
            "highPriorityCurrentCount": len(
                [row for row in current_signals if row.get("overallSalesScore", 0) >= 58]
            ),
            "highOpportunityCount": len(
                [row for row in opportunity_events if row.get("eventScore", 0) >= 75]
            ),
        }

        self._write_snapshot("summary.json", summary)
        self._write_snapshot("current-signals.json", current_signals)
        self._write_snapshot("opportunity-events.json", opportunity_events)
        self._write_snapshot("review-queue.json", review_queue)
        self._write_snapshot("intersections.json", intersections)

    def _query_current_signals(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM current_pipeline_signals
                ORDER BY overall_sales_score DESC, company_name
                """
            ).fetchall()

        return [
            {
                "id": row["id"],
                "companyName": row["company_name"],
                "companyNameNormalized": row["company_name_normalized"],
                "region": row["region"],
                "siteAddress": row["site_address"],
                "siteCity": row["site_city"],
                "siteState": row["site_state"],
                "siteZip": row["site_zip"],
                "industrySegment": row["industry_segment"],
                "currentPriority": row["current_priority"],
                "currentAction": row["current_action"],
                "overallSalesScore": row["overall_sales_score"],
                "matchedSources": row["matched_sources"],
                "reasonToContact": row["reason_to_contact"],
                "reasonToCallNow": row["reason_to_call_now"],
                "whyFit": row["why_fit"],
                "whyNow": row["why_now"],
                "rawPayload": json_loads(row["raw_payload"], {}),
            }
            for row in rows
        ]

    def _query_events(self, *, review_status: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM events
                WHERE review_status = ?
                ORDER BY event_score DESC, published_at DESC, headline
                """,
                (review_status,),
            ).fetchall()

            event_ids = [row["id"] for row in rows]
            contacts_by_event: dict[str, list[dict[str, Any]]] = {event_id: [] for event_id in event_ids}
            if event_ids:
                placeholders = ", ".join("?" for _ in event_ids)
                contact_rows = conn.execute(
                    f"""
                    SELECT ec.event_id, c.*
                    FROM event_contacts ec
                    JOIN contacts c ON c.id = ec.contact_id
                    WHERE ec.event_id IN ({placeholders})
                    ORDER BY c.contact_type, c.contact_value
                    """,
                    event_ids,
                ).fetchall()
                for contact in contact_rows:
                    contacts_by_event[contact["event_id"]].append(
                        {
                            "id": contact["id"],
                            "contactType": contact["contact_type"],
                            "contactValue": contact["contact_value"],
                            "name": contact["name"],
                            "title": contact["title"],
                            "sourceName": contact["source_name"],
                            "sourceUrl": contact["source_url"],
                        }
                    )

        return [
            {
                "id": row["id"],
                "companyName": row["company_name"],
                "companyNameNormalized": row["company_name_normalized"],
                "industry": row["industry"],
                "eventType": row["event_type"],
                "headline": row["headline"],
                "summary": row["summary"],
                "sourceName": row["source_name"],
                "sourceUrl": row["source_url"],
                "publishedAt": row["published_at"],
                "location": row["location"],
                "eventScore": row["event_score"],
                "signalStrength": row["signal_strength"],
                "reviewStatus": row["review_status"],
                "rawKeywords": json_loads(row["raw_keywords"], []),
                "scaleClues": json_loads(row["scale_clues"], []),
                "classificationReason": row["classification_reason"],
                "contacts": contacts_by_event.get(row["id"], []),
                "rawPayload": json_loads(row["raw_payload"], {}),
            }
            for row in rows
        ]

    def _query_intersections(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                  a.id,
                  a.company_name,
                  a.company_name_normalized,
                  a.alert_score,
                  a.alert_reason,
                  a.updated_at,
                  c.id AS current_signal_id,
                  c.region,
                  c.current_priority,
                  c.current_action,
                  c.overall_sales_score,
                  c.matched_sources,
                  e.id AS event_id,
                  e.headline,
                  e.event_type,
                  e.industry,
                  e.location,
                  e.event_score,
                  e.source_name
                FROM intersection_alerts a
                JOIN current_pipeline_signals c ON c.id = a.current_signal_id
                JOIN events e ON e.id = a.event_id
                ORDER BY a.alert_score DESC, a.updated_at DESC
                """
            ).fetchall()

        return [
            {
                "id": row["id"],
                "companyName": row["company_name"],
                "companyNameNormalized": row["company_name_normalized"],
                "alertScore": row["alert_score"],
                "alertReason": row["alert_reason"],
                "updatedAt": row["updated_at"],
                "currentSignal": {
                    "id": row["current_signal_id"],
                    "region": row["region"],
                    "currentPriority": row["current_priority"],
                    "currentAction": row["current_action"],
                    "overallSalesScore": row["overall_sales_score"],
                    "matchedSources": row["matched_sources"],
                },
                "event": {
                    "id": row["event_id"],
                    "headline": row["headline"],
                    "eventType": row["event_type"],
                    "industry": row["industry"],
                    "location": row["location"],
                    "eventScore": row["event_score"],
                    "sourceName": row["source_name"],
                },
            }
            for row in rows
        ]

    def _write_snapshot(self, filename: str, payload: Any) -> None:
        path = self.snapshot_dir / filename
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = OFF")
        return conn
