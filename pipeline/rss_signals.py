from __future__ import annotations

import csv
import hashlib
import html
import io
import logging
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from pipeline.bigquery import _resolve_command, bq_load_csv
from pipeline.config import PipelineConfig
from pipeline.sql_refresh import run_sql_refresh


RSS_ITEM_SCHEMA = (
    "feed_key:STRING,feed_url:STRING,feed_title:STRING,article_guid:STRING,article_link:STRING,"
    "article_title:STRING,article_summary:STRING,article_text:STRING,article_author:STRING,"
    "article_published_at:TIMESTAMP,eyewear_relevance_score:INTEGER,urgency_score:INTEGER,"
    "article_priority:STRING,signal_summary:STRING,load_dt:TIMESTAMP"
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_SUFFIX_RE = re.compile(r"[^a-z0-9]+")

_KEYWORD_GROUPS: tuple[tuple[str, int, tuple[str, ...]], ...] = (
    (
        "Prescription Safety",
        18,
        (
            "prescription safety",
            "prescription eyewear",
            "prescription safety glasses",
            "rx safety",
            "rx eyewear",
        ),
    ),
    (
        "Eye Face PPE",
        14,
        (
            "eye protection",
            "face protection",
            "protective eyewear",
            "safety glasses",
            "safety goggles",
            "face shield",
            "eye and face",
        ),
    ),
    (
        "Splash Chemical",
        10,
        (
            "chemical splash",
            "caustic",
            "corrosive",
            "solvent",
            "acid",
            "hazmat",
        ),
    ),
    (
        "Dust Debris",
        10,
        (
            "dust",
            "debris",
            "grinding",
            "cutting",
            "machining",
            "abrasive",
        ),
    ),
    (
        "Impact",
        10,
        (
            "impact",
            "flying particles",
            "construction",
            "fabrication",
            "welding",
            "shipyard",
        ),
    ),
    (
        "UV Bright Light",
        8,
        (
            "uv",
            "ultraviolet",
            "laser",
            "bright light",
            "radiation",
        ),
    ),
    (
        "Lab Medical",
        8,
        (
            "laboratory",
            "cleanroom",
            "medical device",
            "biotech",
            "pharma",
            "research",
        ),
    ),
    (
        "Safety Compliance",
        6,
        (
            "osha",
            "citation",
            "violation",
            "inspection",
            "ppe",
            "occupational safety",
        ),
    ),
)

_URGENCY_TERMS: tuple[tuple[str, int], ...] = (
    ("recall", 12),
    ("warning letter", 10),
    ("outbreak", 10),
    ("injury", 8),
    ("fatal", 10),
    ("death", 10),
    ("hospitalized", 8),
    ("loss of eye", 14),
    ("severe", 6),
    ("citation", 5),
)


@dataclass(frozen=True)
class RssFeedSpec:
    key: str
    url: str


@dataclass(frozen=True)
class RssFeedItem:
    feed_key: str
    feed_url: str
    feed_title: str
    article_guid: str
    article_link: str
    article_title: str
    article_summary: str
    article_text: str
    article_author: str
    article_published_at: str
    eyewear_relevance_score: int
    urgency_score: int
    article_priority: str
    signal_summary: str
    load_dt: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def parse_feed_specs(value: str) -> list[RssFeedSpec]:
    specs: list[RssFeedSpec] = []
    for raw_part in value.split(";"):
        part = raw_part.strip()
        if not part:
            continue
        if "|" in part:
            raw_key, raw_url = part.split("|", 1)
        else:
            raw_key, raw_url = part, part
        key = _slugify(raw_key)
        url = raw_url.strip()
        if not key or not url:
            continue
        specs.append(RssFeedSpec(key=key, url=url))
    if not specs:
        raise RuntimeError("RSS_FEED_URLS is empty after parsing.")
    return specs


def _slugify(value: str) -> str:
    normalized = _SUFFIX_RE.sub("_", value.strip().lower()).strip("_")
    return normalized or "rss_feed"


def _http_text(
    *,
    url: str,
    timeout_seconds: int = 45,
    max_retries: int = 3,
    base_backoff_seconds: float = 1.0,
) -> str:
    headers = {
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.1",
        "User-Agent": "cold-lead-rss/1.0",
    }
    for attempt in range(1, max_retries + 2):
        req = Request(url=url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                return response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"HTTP {exc.code} calling {url}") from exc
        except URLError as exc:
            if attempt > max_retries:
                raise RuntimeError(f"Network error calling {url}: {exc}") from exc
        sleep_seconds = base_backoff_seconds * (2 ** (attempt - 1))
        logging.warning(
            "RSS fetch failed for %s (attempt %s/%s); retrying in %.1fs",
            url,
            attempt,
            max_retries + 1,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)
    raise RuntimeError(f"Failed to fetch RSS feed: {url}")


def _tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _direct_child_text(element: ET.Element, name: str) -> str:
    for child in list(element):
        if _tag_name(child.tag) == name:
            return "".join(child.itertext()).strip()
    return ""


def _direct_child_attr(element: ET.Element, name: str, attr: str) -> str:
    for child in list(element):
        if _tag_name(child.tag) == name:
            return str(child.attrib.get(attr, "")).strip()
    return ""


def _clean_text(value: str) -> str:
    stripped = _HTML_TAG_RE.sub(" ", value or "")
    return _SPACE_RE.sub(" ", html.unescape(stripped)).strip()


def _parse_timestamp(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _score_article(title: str, summary: str, text: str) -> tuple[int, int, str, str]:
    combined = " ".join(part for part in [title, summary, text] if part).lower()
    matched_signals: list[str] = []
    relevance_score = 0
    for label, points, keywords in _KEYWORD_GROUPS:
        if any(keyword in combined for keyword in keywords):
            relevance_score += points
            matched_signals.append(label)

    urgency_score = 0
    for keyword, points in _URGENCY_TERMS:
        if keyword in combined:
            urgency_score += points

    relevance_score = min(relevance_score, 100)
    urgency_score = min(urgency_score, 100)

    priority = "Low"
    if relevance_score >= 32 or (relevance_score >= 24 and urgency_score >= 10):
        priority = "High"
    elif relevance_score >= 16 or urgency_score >= 10:
        priority = "Medium"

    signal_summary = " | ".join(matched_signals[:5])
    return relevance_score, urgency_score, priority, signal_summary


def parse_feed_document(
    *,
    xml_text: str,
    spec: RssFeedSpec,
    lookback_days: int,
    max_items: int,
) -> list[RssFeedItem]:
    root = ET.fromstring(xml_text)
    feed_title = _direct_child_text(root, "title")
    items: list[RssFeedItem] = []
    load_dt = _utc_now_iso()
    cutoff = _utc_now() - timedelta(days=max(1, lookback_days))

    if _tag_name(root.tag) == "feed":
        entry_nodes = [child for child in list(root) if _tag_name(child.tag) == "entry"]
        if not feed_title:
            feed_title = _direct_child_text(root, "title") or spec.key
        for entry in entry_nodes[:max_items]:
            title = _clean_text(_direct_child_text(entry, "title"))
            summary = _clean_text(_direct_child_text(entry, "summary"))
            content = _clean_text(_direct_child_text(entry, "content"))
            link = _direct_child_attr(entry, "link", "href")
            guid = _direct_child_text(entry, "id") or link or title
            published = (
                _parse_timestamp(_direct_child_text(entry, "updated"))
                or _parse_timestamp(_direct_child_text(entry, "published"))
            )
            if published:
                published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if published_dt < cutoff:
                    continue
            author = _clean_text(_direct_child_text(entry, "name") or _direct_child_text(entry, "author"))
            article_text = _clean_text(" ".join(part for part in [title, summary, content] if part))
            items.append(
                _build_item(
                    spec=spec,
                    feed_title=feed_title or spec.key,
                    guid=guid,
                    link=link,
                    title=title,
                    summary=summary,
                    text=article_text,
                    author=author,
                    published=published,
                    load_dt=load_dt,
                )
            )
        return items

    channel = next((child for child in list(root) if _tag_name(child.tag) == "channel"), root)
    if not feed_title:
        feed_title = _direct_child_text(channel, "title") or spec.key
    item_nodes = [child for child in list(channel) if _tag_name(child.tag) == "item"]
    for node in item_nodes[:max_items]:
        title = _clean_text(_direct_child_text(node, "title"))
        summary = _clean_text(
            _direct_child_text(node, "description") or _direct_child_text(node, "encoded")
        )
        link = _clean_text(_direct_child_text(node, "link"))
        guid = _clean_text(_direct_child_text(node, "guid")) or link or title
        published = _parse_timestamp(
            _direct_child_text(node, "pubDate") or _direct_child_text(node, "date")
        )
        if published:
            published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            if published_dt < cutoff:
                continue
        author = _clean_text(_direct_child_text(node, "creator") or _direct_child_text(node, "author"))
        article_text = _clean_text(" ".join(part for part in [title, summary] if part))
        items.append(
            _build_item(
                spec=spec,
                feed_title=feed_title or spec.key,
                guid=guid,
                link=link,
                title=title,
                summary=summary,
                text=article_text,
                author=author,
                published=published,
                load_dt=load_dt,
            )
        )
    return items


def _build_item(
    *,
    spec: RssFeedSpec,
    feed_title: str,
    guid: str,
    link: str,
    title: str,
    summary: str,
    text: str,
    author: str,
    published: str,
    load_dt: str,
) -> RssFeedItem:
    stable_guid = guid.strip() or hashlib.sha256(
        f"{spec.key}|{link}|{title}|{published}".encode("utf-8")
    ).hexdigest()
    relevance_score, urgency_score, priority, signal_summary = _score_article(title, summary, text)
    return RssFeedItem(
        feed_key=spec.key,
        feed_url=spec.url,
        feed_title=feed_title.strip() or spec.key,
        article_guid=stable_guid,
        article_link=link.strip(),
        article_title=title.strip(),
        article_summary=summary.strip(),
        article_text=text.strip(),
        article_author=author.strip(),
        article_published_at=published,
        eyewear_relevance_score=relevance_score,
        urgency_score=urgency_score,
        article_priority=priority,
        signal_summary=signal_summary,
        load_dt=load_dt,
    )


def _write_items_csv(path: Path, items: list[RssFeedItem]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "feed_key",
        "feed_url",
        "feed_title",
        "article_guid",
        "article_link",
        "article_title",
        "article_summary",
        "article_text",
        "article_author",
        "article_published_at",
        "eyewear_relevance_score",
        "urgency_score",
        "article_priority",
        "signal_summary",
        "load_dt",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for item in items:
            writer.writerow(
                [
                    item.feed_key,
                    item.feed_url,
                    item.feed_title,
                    item.article_guid,
                    item.article_link,
                    item.article_title,
                    item.article_summary,
                    item.article_text,
                    item.article_author,
                    item.article_published_at,
                    item.eyewear_relevance_score,
                    item.urgency_score,
                    item.article_priority,
                    item.signal_summary,
                    item.load_dt,
                ]
            )
    return len(items)


def _load_company_names(config: PipelineConfig) -> list[str]:
    if config.rss_company_search_limit <= 0:
        return []

    sql = f"""
    SELECT DISTINCT `Account Name`
    FROM `{config.project_id}.{config.dataset}.eyewear_opportunity_actionable_current`
    WHERE `Account Name` IS NOT NULL
      AND `Account Name` != ''
    ORDER BY `Account Name`
    LIMIT {config.rss_company_search_limit}
    """
    command = _resolve_command(
        [
            "bq",
            "query",
            f"--project_id={config.project_id}",
            "--use_legacy_sql=false",
            "--format=csv",
        ]
    )
    proc = subprocess.run(
        command,
        cwd=str(config.paths.repo_root),
        check=False,
        capture_output=True,
        text=True,
        input=sql,
    )
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        logging.warning("RSS company search seed query failed: %s", detail)
        return []

    rows = list(csv.DictReader(io.StringIO(proc.stdout)))
    companies: list[str] = []
    for row in rows:
        name = str(row.get("Account Name", "")).strip()
        if name:
            companies.append(name)
    return companies


def _build_company_news_feeds(config: PipelineConfig) -> list[RssFeedSpec]:
    specs: list[RssFeedSpec] = []
    for company in _load_company_names(config):
        query = quote(f'"{company}" OSHA OR safety OR injury OR PPE OR eyewear')
        specs.append(
            RssFeedSpec(
                key=f"company_news_{_slugify(company)}",
                url=(
                    "https://news.google.com/rss/search?q="
                    f"{query}&hl=en-US&gl=US&ceid=US:en"
                ),
            )
        )
    return specs


def run_rss_signals_ingest(config: PipelineConfig) -> None:
    base_specs = parse_feed_specs(config.rss_feed_urls)
    company_specs = _build_company_news_feeds(config)
    specs_by_key = {spec.key: spec for spec in [*base_specs, *company_specs]}
    specs = list(specs_by_key.values())
    items: list[RssFeedItem] = []
    for spec in specs:
        try:
            xml_text = _http_text(url=spec.url)
            feed_items = parse_feed_document(
                xml_text=xml_text,
                spec=spec,
                lookback_days=config.rss_lookback_days,
                max_items=config.rss_max_items_per_feed,
            )
            items.extend(feed_items)
            logging.info("RSS feed %s produced %s items.", spec.key, len(feed_items))
        except Exception as exc:
            logging.warning("RSS feed fetch failed for %s: %s", spec.url, exc)

    csv_path = config.paths.data_dir / "rss" / "feed_items_raw.csv"
    row_count = _write_items_csv(csv_path, items)
    logging.info("RSS ingest prepared %s feed items.", row_count)
    bq_load_csv(
        repo_root=config.paths.repo_root,
        project_id=config.rss_project_id,
        dataset=config.rss_dataset,
        table="feed_items_raw",
        csv_path=csv_path,
        autodetect=False,
        schema=RSS_ITEM_SCHEMA,
        allow_quoted_newlines=True,
    )
    run_sql_refresh(
        config=config,
        sql_filename="refresh_rss_signals.sql",
        project_id=config.rss_project_id,
    )
