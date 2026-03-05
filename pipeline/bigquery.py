from __future__ import annotations

import logging
import os
import subprocess
import time
from pathlib import Path


class BigQueryCommandError(RuntimeError):
    pass


_RETRYABLE_ERROR_SNIPPETS = (
    "error retrieving auth credentials from gcloud",
    "unable to retrieve identity pool subject token",
    "upstream connect error or disconnect/reset before headers",
    "reset reason: overflow",
    "service unavailable",
    "temporarily unavailable",
)


def _is_retryable_failure(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    return any(snippet in combined for snippet in _RETRYABLE_ERROR_SNIPPETS)


def _format_command_failure(command: list[str], returncode: int, stdout: str, stderr: str) -> str:
    return (
        f"Command failed ({returncode}): {' '.join(command)}\n"
        f"stdout:\n{stdout}\n"
        f"stderr:\n{stderr}"
    )


def _run(
    command: list[str],
    *,
    cwd: Path,
    input_text: str | None = None,
    max_attempts: int = 3,
    initial_delay_seconds: float = 5.0,
) -> None:
    exec_command = command
    if os.name == "nt":
        exec_command = ["cmd", "/c", *command]

    for attempt in range(1, max_attempts + 1):
        proc = subprocess.run(
            exec_command,
            cwd=str(cwd),
            check=False,
            capture_output=True,
            text=True,
            input=input_text,
        )
        if proc.returncode == 0:
            return

        if attempt >= max_attempts or not _is_retryable_failure(proc.stdout, proc.stderr):
            raise BigQueryCommandError(
                _format_command_failure(command, proc.returncode, proc.stdout, proc.stderr)
            )

        delay = initial_delay_seconds * attempt
        logging.warning(
            "Retrying BigQuery command after transient auth/service failure "
            "(attempt %s/%s, sleep %.1fs): %s",
            attempt,
            max_attempts,
            delay,
            " ".join(command),
        )
        time.sleep(delay)


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
    normalized_sql = sql_text.lstrip("\ufeff")
    command = ["bq", "query", f"--project_id={project_id}", "--use_legacy_sql=false"]
    _run(command, cwd=repo_root, input_text=normalized_sql)

