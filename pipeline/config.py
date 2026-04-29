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
    public_project_id: str
    public_dataset: str
    rss_project_id: str
    rss_dataset: str
    rss_feed_urls: str
    rss_lookback_days: int
    rss_max_items_per_feed: int
    rss_company_search_limit: int
    fda_project_id: str
    fda_dataset: str
    epa_project_id: str
    epa_dataset: str
    nih_project_id: str
    nih_dataset: str
    ca_sos_project_id: str
    ca_sos_dataset: str
    api_key: str
    openfda_api_key: str
    ca_sos_subscription_key: str
    census_api_key: str
    bls_api_key: str
    census_cbp_year: int
    fda_lookback_years: int
    nih_lookback_years: int
    since_date: str
    api_limit: int
    api_max_pages: int
    accident_api_limit: int
    accident_api_max_pages: int
    paths: RuntimePaths
    api_safety: ApiSafetyConfig
    compliance: ComplianceConfig


def load_pipeline_config(repo_root: Path) -> PipelineConfig:
    dotenv_path_local = repo_root / ".env.local"
    dotenv_path_fallback = repo_root / ".env"
    dotenv_path = dotenv_path_local if dotenv_path_local.exists() else dotenv_path_fallback
    dotenv_values = load_dotenv(dotenv_path)

    project_id = env_value("PROJECT_ID", dotenv_values, "cold-lead-pipeline")
    dataset = env_value("BQ_DATASET", dotenv_values, "osha_raw")
    public_project_id = env_value("PUBLIC_PROJECT_ID", dotenv_values, project_id)
    public_dataset = env_value("PUBLIC_BQ_DATASET", dotenv_values, "public_signals")
    rss_project_id = env_value("RSS_PROJECT_ID", dotenv_values, project_id)
    rss_dataset = env_value("RSS_BQ_DATASET", dotenv_values, "rss_feed")
    rss_feed_urls = env_value(
        "RSS_FEED_URLS",
        dotenv_values,
        (
            "fierce_biotech|https://www.fiercebiotech.com/rss/xml;"
            "fierce_pharma|https://www.fiercepharma.com/rss/xml;"
            "biospace|https://www.biospace.com/rss-feeds;"
            "biopharma_dive|https://www.biopharmadive.com/feeds/news/;"
            "industry_week|https://www.industryweek.com/rss.xml;"
            "manufacturing_net|https://www.manufacturing.net/rss;"
            "manufacturing_dive|https://www.manufacturingdive.com/feeds/news/;"
            "assembly_magazine|https://www.assemblymag.com/rss;"
            "construction_dive|https://www.constructiondive.com/feeds/news/;"
            "engineering_news_record|https://www.enr.com/rss;"
            "food_processing|https://www.foodprocessing.com/rss/;"
            "food_dive|https://www.fooddive.com/feeds/news/;"
            "chemical_processing|https://www.chemicalprocessing.com/rss;"
            "chemical_engineering_news|https://cen.acs.org/rss;"
            "energy_news_network|https://energynews.us/feed/;"
            "oil_and_gas_journal|https://www.ogj.com/rss;"
            "supply_chain_dive|https://www.supplychaindive.com/feeds/news/;"
            "dc_velocity|https://www.dcvelocity.com/rss;"
            "automotive_news|https://www.autonews.com/rss;"
            "ee_times|https://www.eetimes.com/feed/"
        ),
    )
    rss_lookback_days = _parse_int(
        env_value("RSS_LOOKBACK_DAYS", dotenv_values, "30"),
        30,
    )
    rss_max_items_per_feed = _parse_int(
        env_value("RSS_MAX_ITEMS_PER_FEED", dotenv_values, "40"),
        40,
    )
    rss_company_search_limit = _parse_int(
        env_value("RSS_COMPANY_SEARCH_LIMIT", dotenv_values, "0"),
        0,
    )
    fda_project_id = env_value("FDA_PROJECT_ID", dotenv_values, project_id)
    fda_dataset = env_value("FDA_BQ_DATASET", dotenv_values, "fda_raw")
    epa_project_id = env_value("EPA_PROJECT_ID", dotenv_values, project_id)
    epa_dataset = env_value("EPA_BQ_DATASET", dotenv_values, "epa_raw")
    nih_project_id = env_value("NIH_PROJECT_ID", dotenv_values, project_id)
    nih_dataset = env_value("NIH_BQ_DATASET", dotenv_values, "nih_raw")
    ca_sos_project_id = env_value("CA_SOS_PROJECT_ID", dotenv_values, project_id)
    ca_sos_dataset = env_value("CA_SOS_BQ_DATASET", dotenv_values, "ca_sos_raw")
    api_key = env_value("DOL_API_KEY", dotenv_values, "")
    openfda_api_key = env_value("OPENFDA_API_KEY", dotenv_values, "")
    ca_sos_subscription_key = env_value("CA_SOS_SUBSCRIPTION_KEY", dotenv_values, "")
    census_api_key = env_value("CENSUS_API_KEY", dotenv_values, "")
    bls_api_key = env_value("BLS_API_KEY", dotenv_values, "")
    census_cbp_year = _parse_int(env_value("CENSUS_CBP_YEAR", dotenv_values, "2022"), 2022)
    fda_lookback_years = _parse_int(
        env_value("FDA_LOOKBACK_YEARS", dotenv_values, "10"),
        10,
    )
    nih_lookback_years = _parse_int(
        env_value("NIH_LOOKBACK_YEARS", dotenv_values, "5"),
        5,
    )
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
        public_project_id=public_project_id,
        public_dataset=public_dataset,
        rss_project_id=rss_project_id,
        rss_dataset=rss_dataset,
        rss_feed_urls=rss_feed_urls,
        rss_lookback_days=rss_lookback_days,
        rss_max_items_per_feed=rss_max_items_per_feed,
        rss_company_search_limit=rss_company_search_limit,
        fda_project_id=fda_project_id,
        fda_dataset=fda_dataset,
        epa_project_id=epa_project_id,
        epa_dataset=epa_dataset,
        nih_project_id=nih_project_id,
        nih_dataset=nih_dataset,
        ca_sos_project_id=ca_sos_project_id,
        ca_sos_dataset=ca_sos_dataset,
        api_key=api_key,
        openfda_api_key=openfda_api_key,
        ca_sos_subscription_key=ca_sos_subscription_key,
        census_api_key=census_api_key,
        bls_api_key=bls_api_key,
        census_cbp_year=census_cbp_year,
        fda_lookback_years=fda_lookback_years,
        nih_lookback_years=nih_lookback_years,
        since_date=since_date,
        api_limit=api_limit,
        api_max_pages=api_max_pages,
        accident_api_limit=accident_api_limit,
        accident_api_max_pages=accident_api_max_pages,
        paths=paths,
        api_safety=api_safety,
        compliance=compliance,
    )
