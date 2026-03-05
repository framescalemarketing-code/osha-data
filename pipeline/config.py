from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(value: str | None, default: int) -> int:
    if value is None or value.strip() == "":
        return default
    return int(value)


def _parse_float(value: str | None, default: float) -> float:
    if value is None or value.strip() == "":
        return default
    return float(value)


def load_dotenv(dotenv_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not dotenv_path.exists():
        return values

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def env_value(name: str, dotenv_values: dict[str, str], default: str = "") -> str:
    if name in os.environ and os.environ[name].strip() != "":
        return os.environ[name]
    return dotenv_values.get(name, default)


def default_since_date() -> str:
    return (date.today() - timedelta(days=365)).isoformat()


@dataclass(frozen=True)
class RuntimePaths:
    repo_root: Path
    data_dir: Path
    sql_dir: Path
    sql_refresh_file: Path
    dotenv_path: Path


@dataclass(frozen=True)
class ApiSafetyConfig:
    min_interval_seconds: float
    timeout_seconds: int
    max_retries: int
    base_backoff_seconds: float
    max_requests_per_run: int
    user_agent: str
    state_file: Path
    lock_file: Path


@dataclass(frozen=True)
class ComplianceConfig:
    strict_mode: bool
    terms_accepted: bool
    policy_url: str
    policy_reviewed_on: str
    contact_email: str
    intended_use: str
    max_retention_days: int


@dataclass(frozen=True)
class PipelineConfig:
    project_id: str
    dataset: str
    api_key: str
    census_api_key: str
    bls_api_key: str
    census_cbp_year: int
    since_date: str
    api_limit: int
    api_max_pages: int
    accident_api_limit: int
    accident_api_max_pages: int
    paths: RuntimePaths
    api_safety: ApiSafetyConfig
    compliance: ComplianceConfig


def load_pipeline_config(repo_root: Path) -> PipelineConfig:
    dotenv_path = repo_root / ".env"
    dotenv_values = load_dotenv(dotenv_path)

    project_id = env_value("PROJECT_ID", dotenv_values, "osha-data-live-20260303")
    dataset = env_value("BQ_DATASET", dotenv_values, "osha_raw")
    api_key = env_value("DOL_API_KEY", dotenv_values, "")
    census_api_key = env_value("CENSUS_API_KEY", dotenv_values, "")
    bls_api_key = env_value("BLS_API_KEY", dotenv_values, "")
    census_cbp_year = _parse_int(env_value("CENSUS_CBP_YEAR", dotenv_values, "2022"), 2022)
    since_date = env_value("SINCE_DATE", dotenv_values, default_since_date())
    api_limit = _parse_int(env_value("API_LIMIT", dotenv_values, "5000"), 5000)
    api_max_pages = _parse_int(env_value("API_MAX_PAGES", dotenv_values, "2"), 2)
    accident_api_limit = _parse_int(
        env_value("ACCIDENT_API_LIMIT", dotenv_values, str(min(api_limit, 1000))),
        min(api_limit, 1000),
    )
    accident_api_max_pages = _parse_int(
        env_value("ACCIDENT_API_MAX_PAGES", dotenv_values, "1"), 1
    )

    data_dir = repo_root / "data"
    sql_dir = repo_root / "sql"
    sql_refresh_file = sql_dir / "refresh_sales_followup_v2.sql"

    min_interval_seconds = _parse_float(
        env_value("API_MIN_INTERVAL_SECONDS", dotenv_values, "1.0"), 1.0
    )
    timeout_seconds = _parse_int(
        env_value("API_TIMEOUT_SECONDS", dotenv_values, "120"), 120
    )
    max_retries = _parse_int(env_value("API_MAX_RETRIES", dotenv_values, "5"), 5)
    base_backoff_seconds = _parse_float(
        env_value("API_BASE_BACKOFF_SECONDS", dotenv_values, "1.0"), 1.0
    )
    max_requests_per_run = _parse_int(
        env_value("API_MAX_REQUESTS_PER_RUN", dotenv_values, "600"), 600
    )
    user_agent = env_value(
        "API_USER_AGENT",
        dotenv_values,
        "osha-sales-pipeline/1.0 (+compliance-contact-required)",
    )

    state_file = Path(
        env_value(
            "API_RATE_LIMIT_STATE_FILE",
            dotenv_values,
            str(data_dir / "api_rate_limit_last_call_utc.txt"),
        )
    )
    lock_file = Path(
        env_value(
            "API_RATE_LIMIT_LOCK_FILE",
            dotenv_values,
            str(data_dir / "api_rate_limit.lock"),
        )
    )

    compliance = ComplianceConfig(
        strict_mode=_parse_bool(
            env_value("COMPLIANCE_STRICT_MODE", dotenv_values, "true"), True
        ),
        terms_accepted=_parse_bool(
            env_value("DOL_API_TERMS_ACCEPTED", dotenv_values, "false"), False
        ),
        policy_url=env_value(
            "DOL_API_POLICY_URL", dotenv_values, "https://data.dol.gov/"
        ),
        policy_reviewed_on=env_value("DOL_API_POLICY_REVIEWED_ON", dotenv_values, ""),
        contact_email=env_value("DOL_API_CONTACT_EMAIL", dotenv_values, ""),
        intended_use=env_value("DOL_API_INTENDED_USE", dotenv_values, ""),
        max_retention_days=_parse_int(
            env_value("DATA_RETENTION_DAYS", dotenv_values, "365"), 365
        ),
    )

    paths = RuntimePaths(
        repo_root=repo_root,
        data_dir=data_dir,
        sql_dir=sql_dir,
        sql_refresh_file=sql_refresh_file,
        dotenv_path=dotenv_path,
    )

    api_safety = ApiSafetyConfig(
        min_interval_seconds=min_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        base_backoff_seconds=base_backoff_seconds,
        max_requests_per_run=max_requests_per_run,
        user_agent=user_agent,
        state_file=state_file,
        lock_file=lock_file,
    )

    return PipelineConfig(
        project_id=project_id,
        dataset=dataset,
        api_key=api_key,
        census_api_key=census_api_key,
        bls_api_key=bls_api_key,
        census_cbp_year=census_cbp_year,
        since_date=since_date,
        api_limit=api_limit,
        api_max_pages=api_max_pages,
        accident_api_limit=accident_api_limit,
        accident_api_max_pages=accident_api_max_pages,
        paths=paths,
        api_safety=api_safety,
        compliance=compliance,
    )
