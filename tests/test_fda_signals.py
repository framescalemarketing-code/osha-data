from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from pipeline.config import ApiSafetyConfig, ComplianceConfig, PipelineConfig, RuntimePaths
from pipeline.fda_signals import _extract_join_keys
from pipeline.sql_refresh import run_sql_refresh


class FdaSignalsTests(unittest.TestCase):
    def _make_temp_dir(self) -> Path:
        base_dir = Path.cwd() / ".tmp-tests"
        base_dir.mkdir(exist_ok=True)
        temp_dir = base_dir / f"case-{uuid.uuid4().hex}"
        temp_dir.mkdir()
        return temp_dir

    def _make_config(self, repo_root: Path) -> PipelineConfig:
        paths = RuntimePaths(
            repo_root=repo_root,
            data_dir=repo_root / "data",
            sql_dir=repo_root / "sql",
            sql_refresh_file=repo_root / "sql" / "refresh_sales_followup_v2.sql",
            dotenv_path=repo_root / ".env",
        )
        api_safety = ApiSafetyConfig(
            min_interval_seconds=1.0,
            timeout_seconds=60,
            max_retries=3,
            base_backoff_seconds=1.0,
            max_requests_per_run=100,
            user_agent="test-agent",
            state_file=repo_root / "rate_state.txt",
            lock_file=repo_root / "rate_lock.txt",
        )
        compliance = ComplianceConfig(
            strict_mode=False,
            terms_accepted=True,
            policy_url="https://example.com",
            policy_reviewed_on="2026-03-06",
            contact_email="ops@example.com",
            intended_use="test",
            max_retention_days=365,
        )
        return PipelineConfig(
            project_id="osha-project",
            dataset="osha_raw",
            fda_project_id="fda-project",
            fda_dataset="fda_raw",
            epa_project_id="epa-project",
            epa_dataset="epa_raw",
            nih_project_id="nih-project",
            nih_dataset="nih_raw",
            ca_sos_project_id="ca-sos-project",
            ca_sos_dataset="ca_sos_raw",
            api_key="",
            openfda_api_key="",
            ca_sos_subscription_key="",
            census_api_key="",
            bls_api_key="",
            census_cbp_year=2022,
            fda_lookback_years=10,
            nih_lookback_years=5,
            since_date="2025-01-01",
            api_limit=5000,
            api_max_pages=2,
            accident_api_limit=1000,
            accident_api_max_pages=1,
            paths=paths,
            api_safety=api_safety,
            compliance=compliance,
        )

    def test_extract_join_keys_prefers_registration_and_fei(self) -> None:
        keys = _extract_join_keys(
            {
                "registration_number": ["1001", "1001"],
                "fei_number": ["2002"],
            }
        )

        self.assertEqual(keys, [("registration", "1001"), ("fei", "2002")])

    def test_run_sql_refresh_replaces_placeholders_and_strips_bom(self) -> None:
        temp_dir = self._make_temp_dir()
        try:
            sql_dir = temp_dir / "sql"
            sql_dir.mkdir()
            (sql_dir / "refresh_sales_priority_outputs.sql").write_text(
                "\ufeffSELECT '{{FDA_PROJECT_ID}}' AS fda_project, '{{EPA_PROJECT_ID}}' AS epa_project, '{{OSHA_PROJECT_ID}}' AS osha_project",
                encoding="utf-8",
            )
            config = self._make_config(temp_dir)

            with patch("pipeline.sql_refresh.bq_query_sql") as query_mock:
                run_sql_refresh(
                    config=config,
                    sql_filename="refresh_sales_priority_outputs.sql",
                    project_id=config.project_id,
                )

            self.assertEqual(query_mock.call_count, 1)
            _, kwargs = query_mock.call_args
            self.assertEqual(kwargs["project_id"], "osha-project")
            self.assertNotIn("\ufeff", kwargs["sql_text"])
            self.assertIn("fda-project", kwargs["sql_text"])
            self.assertIn("epa-project", kwargs["sql_text"])
            self.assertIn("osha-project", kwargs["sql_text"])
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
