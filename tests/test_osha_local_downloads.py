from __future__ import annotations

import unittest
from pathlib import Path
import shutil
from unittest.mock import patch

from pipeline.config import ApiSafetyConfig, ComplianceConfig, PipelineConfig, RuntimePaths
from pipeline.osha_local_downloads import (
    LocalDownloadSpec,
    _sanitize_digits_value,
    run_osha_local_downloads_ingest,
)


class OshaLocalDownloadsTests(unittest.TestCase):
    def test_sanitize_digits_value_removes_non_digits(self) -> None:
        self.assertEqual(_sanitize_digits_value("36-283962"), "36283962")

    def test_sanitize_digits_value_blanks_placeholder(self) -> None:
        self.assertEqual(_sanitize_digits_value("Enter EIN"), "")

    def test_run_local_ingest_refreshes_helper_and_sales_outputs(self) -> None:
        temp_root = Path(".tmp-tests") / "osha_local_downloads"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        temp_root.mkdir(parents=True)

        try:
            repo_root = temp_root
            data_dir = repo_root / "data"
            sql_dir = repo_root / "sql"
            downloads_dir = data_dir / "downloads"
            downloads_dir.mkdir(parents=True)
            sql_dir.mkdir(parents=True)

            source_path = downloads_dir / "demo.csv"
            source_path.write_text("id,name\n1,Demo\n", encoding="utf-8")
            (sql_dir / "refresh_osha_local_downloads.sql").write_text("SELECT 1", encoding="utf-8")
            (sql_dir / "refresh_sales_priority_outputs.sql").write_text("SELECT 2", encoding="utf-8")

            config = PipelineConfig(
                project_id="demo-project",
                dataset="osha_raw",
                fda_project_id="demo-project",
                fda_dataset="fda_raw",
                epa_project_id="demo-project",
                epa_dataset="epa_raw",
                nih_project_id="demo-project",
                nih_dataset="nih_raw",
                ca_sos_project_id="demo-project",
                ca_sos_dataset="ca_sos_raw",
                api_key="",
                openfda_api_key="",
                ca_sos_subscription_key="",
                census_api_key="",
                bls_api_key="",
                census_cbp_year=2022,
                fda_lookback_years=10,
                nih_lookback_years=5,
                since_date="2026-01-01",
                api_limit=100,
                api_max_pages=1,
                accident_api_limit=100,
                accident_api_max_pages=1,
                paths=RuntimePaths(
                    repo_root=repo_root,
                    data_dir=data_dir,
                    sql_dir=sql_dir,
                    sql_refresh_file=sql_dir / "refresh_sales_followup_v2.sql",
                    dotenv_path=repo_root / ".env",
                ),
                api_safety=ApiSafetyConfig(
                    min_interval_seconds=1.0,
                    timeout_seconds=60,
                    max_retries=3,
                    base_backoff_seconds=1.0,
                    max_requests_per_run=100,
                    user_agent="test-agent",
                    state_file=data_dir / "state.txt",
                    lock_file=data_dir / "lock.txt",
                ),
                compliance=ComplianceConfig(
                    strict_mode=False,
                    terms_accepted=True,
                    policy_url="",
                    policy_reviewed_on="",
                    contact_email="",
                    intended_use="",
                    max_retention_days=365,
                ),
            )

            spec = LocalDownloadSpec(
                label="Demo",
                table="demo_raw",
                relative_path="data/downloads/demo.csv",
            )

            with patch("pipeline.osha_local_downloads._source_specs", return_value=[spec]):
                with patch("pipeline.osha_local_downloads.bq_load_csv") as load_mock:
                    with patch("pipeline.osha_local_downloads.run_sql_refresh") as refresh_mock:
                        run_osha_local_downloads_ingest(config)

            self.assertEqual(load_mock.call_count, 1)
            self.assertEqual(refresh_mock.call_count, 2)
        finally:
            if temp_root.exists():
                shutil.rmtree(temp_root)


if __name__ == "__main__":
    unittest.main()
