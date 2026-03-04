from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create/update OSHA daily task")
    parser.add_argument("--task-name", default="OSHA Full Pipeline Daily")
    parser.add_argument("--start-time", default="09:10")
    parser.add_argument("--project-id", default="osha-data-live-20260303")
    parser.add_argument("--dataset", default="osha_raw")
    parser.add_argument("--since-date", default="")
    parser.add_argument("--api-limit", type=int, default=5000)
    parser.add_argument("--api-max-pages", type=int, default=2)
    parser.add_argument(
        "--disable-legacy-tasks",
        dest="disable_legacy_tasks",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--keep-legacy-tasks",
        dest="disable_legacy_tasks",
        action="store_false",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    python_exe = Path(sys.executable).resolve()
    runner_script = (repo_root / "scripts" / "run_daily_full_pipeline.py").resolve()
    runner = [
        str(python_exe),
        str(runner_script),
        "--project-id",
        args.project_id,
        "--dataset",
        args.dataset,
        "--api-limit",
        str(args.api_limit),
        "--api-max-pages",
        str(args.api_max_pages),
    ]
    if args.since_date.strip():
        runner.extend(["--since-date", args.since_date])

    task_command = " ".join(f'"{part}"' if " " in part else part for part in runner)
    schtasks_cmd = [
        "schtasks",
        "/Create",
        "/SC",
        "DAILY",
        "/TN",
        args.task_name,
        "/TR",
        task_command,
        "/ST",
        args.start_time,
        "/F",
    ]
    _run(schtasks_cmd)

    if args.disable_legacy_tasks:
        for legacy in (
            "OSHA Ingest to BigQuery",
            "OSHA Ingest BayArea to BigQuery",
            "OSHA Enrichment Daily",
        ):
            try:
                _run(["schtasks", "/Change", "/TN", legacy, "/DISABLE"])
            except subprocess.CalledProcessError:
                pass

    print(f"Scheduled task ready: {args.task_name} @ {args.start_time}")
    print(f"Repo root: {repo_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
