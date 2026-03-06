from __future__ import annotations

import unittest

from pipeline.compliance import ComplianceError, validate_compliance
from pipeline.config import ComplianceConfig


def make_config(*, strict_mode: bool) -> ComplianceConfig:
    return ComplianceConfig(
        strict_mode=strict_mode,
        terms_accepted=True,
        policy_url="https://www.osha.gov",
        policy_reviewed_on="2026-03-05",
        contact_email="ops@example.com",
        intended_use="Daily operational safety sales outreach.",
        max_retention_days=365,
    )


class ComplianceTests(unittest.TestCase):
    def test_validate_compliance_raises_in_strict_mode(self) -> None:
        config = make_config(strict_mode=True)
        config = ComplianceConfig(
            strict_mode=config.strict_mode,
            terms_accepted=config.terms_accepted,
            policy_url=config.policy_url,
            policy_reviewed_on=config.policy_reviewed_on,
            contact_email="not-an-email",
            intended_use=config.intended_use,
            max_retention_days=config.max_retention_days,
        )

        with self.assertRaises(ComplianceError):
            validate_compliance(config)

    def test_validate_compliance_reports_failures_when_not_strict(self) -> None:
        config = make_config(strict_mode=False)
        config = ComplianceConfig(
            strict_mode=config.strict_mode,
            terms_accepted=False,
            policy_url="bad-url",
            policy_reviewed_on="20260305",
            contact_email="",
            intended_use="",
            max_retention_days=0,
        )

        report = validate_compliance(config)

        self.assertEqual(report.checks_run, 6)
        self.assertGreater(report.checks_failed, 0)


if __name__ == "__main__":
    unittest.main()
