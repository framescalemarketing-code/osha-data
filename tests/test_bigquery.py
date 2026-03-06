from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.bigquery import bq_query_sql


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


if __name__ == "__main__":
    unittest.main()
