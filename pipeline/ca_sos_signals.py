from __future__ import annotations

import logging

from pipeline.config import PipelineConfig


def run_ca_sos_signals_ingest(config: PipelineConfig) -> None:
    if not config.ca_sos_subscription_key.strip():
        logging.info(
            "Skipping California SOS signals stage because CA_SOS_SUBSCRIPTION_KEY is not set."
        )
        return

    logging.warning(
        "California SOS signals stage is scaffolded but not enabled in this repo yet."
    )
