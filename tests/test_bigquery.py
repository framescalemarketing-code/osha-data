from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.bigquery import _resolve_command, bq_query_sql


class BigQueryTests(unittest.TestCase):
    def test_bq_query_sql_strips_leading_bom(self) -> None:
        with patch("pipeline.bigquery._run") as run_mock:
            bq_query_sql(
                repo_root=Path.cwd(),
                project_id="demo-project",
                sql_text="\ufeffSELECT 1",
            )

        self.assertEqual(run_mock.call_count, 1)
        _, kwargs = run_mock.call_args
        self.assertEqual(kwargs["input_text"], "SELECT 1")

    def test_bq_load_csv_passes_field_delimiter(self) -> None:
        with patch("pipeline.bigquery._run") as run_mock:
            from pipeline.bigquery import bq_load_csv

            bq_load_csv(
                repo_root=Path.cwd(),
                project_id="demo-project",
                dataset="demo_dataset",
                table="demo_table",
                csv_path=Path("demo.txt"),
                autodetect=True,
                field_delimiter="|",
            )

        args, _ = run_mock.call_args
        command = args[0]
        self.assertIn("--field_delimiter=|", command)

    def test_resolve_command_prefers_windows_bq_cmd(self) -> None:
        with patch("pipeline.bigquery.os.name", "nt"):
            with patch("pipeline.bigquery.shutil.which") as which_mock:
                which_mock.side_effect = [None, r"C:\tooling\bq.cmd", None, None]
                resolved = _resolve_command(["bq", "query"])

        self.assertEqual(resolved[0], r"C:\tooling\bq.cmd")
        self.assertEqual(resolved[1:], ["query"])


if __name__ == "__main__":
    unittest.main()
