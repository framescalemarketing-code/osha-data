from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from pipeline.rss_signals import RssFeedSpec, parse_feed_document, parse_feed_specs


class RssSignalsTests(unittest.TestCase):
    def test_parse_feed_specs_supports_name_url_pairs(self) -> None:
        specs = parse_feed_specs(
            "osha_news|https://example.com/osha.xml;fda_medwatch|https://example.com/fda.xml"
        )

        self.assertEqual(len(specs), 2)
        self.assertEqual(specs[0].key, "osha_news")
        self.assertEqual(specs[1].url, "https://example.com/fda.xml")

    def test_parse_feed_document_reads_rss_items_and_scores_keywords(self) -> None:
        pub_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        xml_text = f"""
        <rss version="2.0">
          <channel>
            <title>Industry News</title>
            <item>
              <title>Biotech manufacturer raises funding to expand cleanroom production</title>
              <link>https://example.com/articles/1</link>
              <guid>article-1</guid>
              <pubDate>{pub_date}</pubDate>
              <description>The company is hiring technicians as it scales the facility.</description>
            </item>
          </channel>
        </rss>
        """

        items = parse_feed_document(
            xml_text=xml_text,
            spec=RssFeedSpec(key="safety_news", url="https://example.com/rss.xml"),
            lookback_days=30,
            max_items=10,
        )

        self.assertEqual(len(items), 1)
        self.assertGreaterEqual(items[0].eyewear_relevance_score, 20)
        self.assertIn("Funding Capital", items[0].signal_summary)

    def test_parse_feed_document_reads_atom_entries(self) -> None:
        updated = (datetime.now(timezone.utc) - timedelta(days=1)).replace(microsecond=0).isoformat()
        xml_text = f"""
        <feed xmlns="http://www.w3.org/2005/Atom">
          <title>Life Sciences Deals</title>
          <entry>
            <title>Medtech company acquires plant and expands manufacturing footprint</title>
            <id>tag:example.com,2026:entry-1</id>
            <updated>{updated}</updated>
            <link href="https://example.com/alerts/1" />
            <summary>The deal adds production capacity and new hiring plans.</summary>
          </entry>
        </feed>
        """

        items = parse_feed_document(
            xml_text=xml_text,
            spec=RssFeedSpec(key="device_alerts", url="https://example.com/atom.xml"),
            lookback_days=30,
            max_items=10,
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].article_link, "https://example.com/alerts/1")
        self.assertEqual(items[0].article_priority, "High")

    def test_parse_feed_document_filters_sports_eye_injury_noise(self) -> None:
        pub_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        xml_text = f"""
        <rss version="2.0">
          <channel>
            <title>General News</title>
            <item>
              <title>Star player returns after eye injury in playoff game</title>
              <link>https://example.com/articles/sports-1</link>
              <guid>sports-1</guid>
              <pubDate>{pub_date}</pubDate>
              <description>The athlete wore protective goggles after the game injury.</description>
            </item>
          </channel>
        </rss>
        """

        items = parse_feed_document(
            xml_text=xml_text,
            spec=RssFeedSpec(key="general_news", url="https://example.com/general.xml"),
            lookback_days=30,
            max_items=10,
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].eyewear_relevance_score, 0)
        self.assertEqual(items[0].article_priority, "Low")

    def test_parse_feed_document_filters_generic_workplace_safety_noise(self) -> None:
        pub_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        xml_text = f"""
        <rss version="2.0">
          <channel>
            <title>Operations News</title>
            <item>
              <title>Warehouse leaders expand automation and worker safety training</title>
              <link>https://example.com/articles/ops-1</link>
              <guid>ops-1</guid>
              <pubDate>{pub_date}</pubDate>
              <description>The distribution network is investing in compliance programs and PPE training.</description>
            </item>
          </channel>
        </rss>
        """

        items = parse_feed_document(
            xml_text=xml_text,
            spec=RssFeedSpec(key="operations_news", url="https://example.com/ops.xml"),
            lookback_days=30,
            max_items=10,
        )

        self.assertEqual(len(items), 1)
        self.assertGreaterEqual(items[0].eyewear_relevance_score, 16)
        self.assertIn(items[0].article_priority, {"High", "Medium"})


if __name__ == "__main__":
    unittest.main()
