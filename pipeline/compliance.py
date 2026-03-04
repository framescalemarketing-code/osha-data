from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from pipeline.config import ComplianceConfig


class ComplianceError(RuntimeError):
    pass


@dataclass(frozen=True)
class ComplianceReport:
    checks_run: int
    checks_failed: int


def _is_valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value.strip()))


def validate_compliance(config: ComplianceConfig) -> ComplianceReport:
    failures: list[str] = []

    if not config.terms_accepted:
        failures.append(
            "DOL_API_TERMS_ACCEPTED must be true to confirm policy/terms review."
        )
    if not config.policy_reviewed_on:
        failures.append("DOL_API_POLICY_REVIEWED_ON must be set (YYYY-MM-DD).")
    else:
        try:
            datetime.strptime(config.policy_reviewed_on, "%Y-%m-%d")
        except ValueError:
            failures.append("DOL_API_POLICY_REVIEWED_ON must use YYYY-MM-DD format.")
    if not config.contact_email or not _is_valid_email(config.contact_email):
        failures.append("DOL_API_CONTACT_EMAIL must be a valid monitored email.")
    if not config.intended_use.strip():
        failures.append("DOL_API_INTENDED_USE must describe your allowed business use.")
    if not config.policy_url.strip().startswith("http"):
        failures.append("DOL_API_POLICY_URL must be an http/https URL.")
    if config.max_retention_days <= 0:
        failures.append("DATA_RETENTION_DAYS must be greater than 0.")

    if failures and config.strict_mode:
        joined = " | ".join(failures)
        raise ComplianceError(f"Compliance preflight failed: {joined}")

    return ComplianceReport(checks_run=6, checks_failed=len(failures))
