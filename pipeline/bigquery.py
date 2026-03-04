from __future__ import annotations

import subprocess
from pathlib import Path


class BigQueryCommandError(RuntimeError):
    pass


def _run(command: list[str], cwd: Path) -> None:
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise BigQueryCommandError(
            f"Command failed ({proc.returncode}): {' '.join(command)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )


def bq_load_csv(
    *,
    repo_root: Path,
    project_id: str,
    dataset: str,
    table: str,
    csv_path: Path,
    autodetect: bool = False,
    schema: str | None = None,
    allow_quoted_newlines: bool = False,
) -> None:
    target = f"{project_id}:{dataset}.{table}"
    cmd = [
        "bq",
        "load",
        "--replace",
        "--source_format=CSV",
        "--skip_leading_rows=1",
    ]
    if autodetect:
        cmd.append("--autodetect")
        cmd.append("--column_name_character_map=V2")
    if allow_quoted_newlines:
        cmd.append("--allow_quoted_newlines")
    if schema:
        cmd.append(f"--schema={schema}")

    cmd.extend([target, str(csv_path)])
    _run(cmd, cwd=repo_root)


def bq_query_sql(*, repo_root: Path, project_id: str, sql_text: str) -> None:
    proc = subprocess.run(
        ["bq", "query", f"--project_id={project_id}", "--use_legacy_sql=false"],
        cwd=str(repo_root),
        check=False,
        capture_output=True,
        text=True,
        input=sql_text,
    )
    if proc.returncode != 0:
        raise BigQueryCommandError(
            f"bq query failed ({proc.returncode}).\nstdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

