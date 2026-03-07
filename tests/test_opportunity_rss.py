from __future__ import annotations

import unittest

from pipeline.opportunity_rss import FeedItem, OpportunitySettings, classify_feed_item


class OpportunityRssTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = OpportunitySettings(
            max_items_per_feed=10,
            accepted_score_threshold=65,
            review_score_threshold=50,
            timeout_seconds=30,
            max_retries=1,
            article_fetch_limit=5,
            user_agent="test-agent",
        )

    def test_accepts_operational_expansion_signal(self) -> None:
        item = FeedItem(
            feed_name="IndustryWeek",
            feed_url="https://example.com/feed.xml",
            industry="Manufacturing",
            title="Acme Manufacturing opens new plant in Phoenix, AZ and hires 300 workers",
            summary="The company will add a new production facility and manufacturing line buildout.",
            url="https://example.com/acme-plant",
            published_at="Fri, 06 Mar 2026 10:00:00 GMT",
            raw_payload={},
        )

        result = classify_feed_item(item, settings=self.settings)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["review_status"], "accepted")
        self.assertEqual(result["event_type"], "new facility opening")
        self.assertGreaterEqual(result["event_score"], 70)

    def test_rejects_non_operational_news(self) -> None:
        item = FeedItem(
            feed_name="Fierce Pharma",
            feed_url="https://example.com/feed.xml",
            industry="Biopharma and life sciences",
            title="Acme Pharma reports positive phase 2 clinical trial data",
            summary="The drug candidate met the primary endpoint in a phase 2 trial.",
            url="https://example.com/acme-trial",
            published_at="Fri, 06 Mar 2026 11:00:00 GMT",
            raw_payload={},
        )

        result = classify_feed_item(item, settings=self.settings)

        self.assertIsNone(result)

    def test_routes_borderline_signal_to_review(self) -> None:
        item = FeedItem(
            feed_name="BioSpace",
            feed_url="https://example.com/feed.xml",
            industry="Biopharma and life sciences",
            title="Acme Biotech modernizes facility in Boston, MA",
            summary="The project upgrades an existing facility and operational footprint.",
            url="https://example.com/acme-modernization",
            published_at="Fri, 06 Mar 2026 12:00:00 GMT",
            raw_payload={},
        )

        result = classify_feed_item(item, settings=self.settings)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["review_status"], "review")
        self.assertEqual(result["event_type"], "factory modernization")


if __name__ == "__main__":
    unittest.main()
