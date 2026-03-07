from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse, urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from pipeline.config import PipelineConfig, env_value, load_dotenv
from pipeline.sales_intel_common import (
    company_key,
    json_dumps,
    normalize_company_name,
    normalize_space,
    stable_id,
    strip_html,
    unique_preserving_order,
)
from pipeline.sales_intel_store import SalesIntelStore


@dataclass(frozen=True)
class FeedSpec:
    name: str
    url: str
    industry: str


@dataclass(frozen=True)
class FeedItem:
    feed_name: str
    feed_url: str
    industry: str
    title: str
    summary: str
    url: str
    published_at: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class OpportunitySettings:
    max_items_per_feed: int
    accepted_score_threshold: int
    review_score_threshold: int
    timeout_seconds: int
    max_retries: int
    article_fetch_limit: int
    user_agent: str


FEEDS: tuple[FeedSpec, ...] = (
    FeedSpec("Fierce Biotech", "https://www.fiercebiotech.com/rss/xml", "Biopharma and life sciences"),
    FeedSpec("Fierce Pharma", "https://www.fiercepharma.com/rss/xml", "Biopharma and life sciences"),
    FeedSpec("BioSpace", "https://www.biospace.com/rss-feeds", "Biopharma and life sciences"),
    FeedSpec("BioPharma Dive", "https://www.biopharmadive.com/feeds/news/", "Biopharma and life sciences"),
    FeedSpec("IndustryWeek", "https://www.industryweek.com/rss.xml", "Manufacturing"),
    FeedSpec("Manufacturing.net", "https://www.manufacturing.net/rss", "Manufacturing"),
    FeedSpec("Manufacturing Dive", "https://www.manufacturingdive.com/feeds/news/", "Manufacturing"),
    FeedSpec("Assembly Magazine", "https://www.assemblymag.com/rss", "Manufacturing"),
    FeedSpec("Construction Dive", "https://www.constructiondive.com/feeds/news/", "Construction and infrastructure"),
    FeedSpec("Engineering News Record", "https://www.enr.com/rss", "Construction and infrastructure"),
    FeedSpec("Food Processing", "https://www.foodprocessing.com/rss/", "Food production and food manufacturing"),
    FeedSpec("Food Dive", "https://www.fooddive.com/feeds/news/", "Food production and food manufacturing"),
    FeedSpec("Chemical Processing", "https://www.chemicalprocessing.com/rss", "Chemicals and materials"),
    FeedSpec("Chemical and Engineering News", "https://cen.acs.org/rss", "Chemicals and materials"),
    FeedSpec("Energy News Network", "https://energynews.us/feed/", "Energy and utilities"),
    FeedSpec("Oil and Gas Journal", "https://www.ogj.com/rss", "Energy and utilities"),
    FeedSpec("Supply Chain Dive", "https://www.supplychaindive.com/feeds/news/", "Logistics and warehousing"),
    FeedSpec("DC Velocity", "https://www.dcvelocity.com/rss", "Logistics and warehousing"),
    FeedSpec("Automotive News", "https://www.autonews.com/rss", "Automotive manufacturing"),
    FeedSpec("EE Times", "https://www.eetimes.com/feed/", "Electronics and semiconductor manufacturing"),
)

EVENT_PATTERNS: dict[str, tuple[str, ...]] = {
    "new facility opening": (
        "new facility",
        "opens new plant",
        "open new plant",
        "opening new facility",
        "site opening",
        "new campus",
        "new location",
    ),
    "plant construction": (
        "plant construction",
        "build new plant",
        "construct new plant",
        "groundbreaking",
        "breaks ground",
        "new manufacturing plant",
        "assembly plant",
    ),
    "factory expansion": (
        "factory expansion",
        "plant expansion",
        "site expansion",
        "expands facility",
        "expansion project",
        "expand operations",
        "operational expansion",
    ),
    "manufacturing line expansion": (
        "production line",
        "new line",
        "line expansion",
        "adds line",
        "adds production line",
        "line buildout",
    ),
    "warehouse construction": (
        "distribution center",
        "warehouse construction",
        "new warehouse",
        "new distribution center",
        "fulfillment center",
    ),
    "laboratory expansion": (
        "lab expansion",
        "laboratory expansion",
        "new lab",
        "research campus",
        "biologics plant",
    ),
    "production capacity expansion": (
        "capacity expansion",
        "expand capacity",
        "production capacity",
        "boost capacity",
        "scale production",
    ),
    "industrial construction project": (
        "construction project",
        "industrial park",
        "fabrication facility",
        "processing facility",
        "refinery",
        "buildout",
    ),
    "major capital investment": (
        "capital investment",
        "invest",
        "investment in facility",
        "operational investment",
        "manufacturing investment",
    ),
    "acquisition expanding operations": (
        "acquisition",
        "acquire",
        "acquires facility",
        "expands footprint",
        "expands manufacturing network",
    ),
    "large workforce hiring": (
        "hiring",
        "jobs",
        "new hires",
        "add employees",
        "workforce expansion",
        "staffing up",
    ),
    "factory modernization": (
        "modernization",
        "modernize plant",
        "modernizes facility",
        "upgrade facility",
        "retrofit",
        "equipment upgrade",
    ),
}

NEGATIVE_PATTERNS: tuple[str, ...] = (
    "clinical trial",
    "phase 1",
    "phase 2",
    "phase 3",
    "fda approval",
    "drug approval",
    "product launch",
    "launches product",
    "market commentary",
    "earnings call",
    "policy update",
    "regulatory update",
    "guidance",
)

PPE_KEYWORDS: tuple[str, ...] = (
    "plant",
    "factory",
    "facility",
    "manufacturing plant",
    "production facility",
    "processing facility",
    "distribution center",
    "warehouse",
    "refinery",
    "biologics plant",
    "manufacturing expansion",
    "capacity expansion",
    "construction project",
    "industrial park",
    "production line",
    "fabrication facility",
    "assembly plant",
    "site opening",
    "site expansion",
    "new location",
    "new campus",
    "new lab",
    "buildout",
    "operational expansion",
)

HIRING_PATTERNS: tuple[str, ...] = (
    "jobs",
    "employees",
    "workers",
    "hiring",
    "workforce",
    "staff",
    "headcount",
)

TITLE_VERBS: tuple[str, ...] = (
    "opens",
    "open",
    "opening",
    "expands",
    "expand",
    "plans",
    "will",
    "builds",
    "build",
    "invests",
    "acquires",
    "launches",
    "modernizes",
)

EVENT_TYPE_SCORES = {
    "new facility opening": 40,
    "plant construction": 38,
    "factory expansion": 36,
    "manufacturing line expansion": 34,
    "warehouse construction": 36,
    "laboratory expansion": 30,
    "production capacity expansion": 34,
    "industrial construction project": 30,
    "major capital investment": 28,
    "acquisition expanding operations": 26,
    "large workforce hiring": 28,
    "factory modernization": 24,
}

INDUSTRY_SCORES = {
    "Manufacturing": 18,
    "Construction and infrastructure": 18,
    "Biopharma and life sciences": 16,
    "Food production and food manufacturing": 16,
    "Chemicals and materials": 18,
    "Energy and utilities": 17,
    "Logistics and warehousing": 16,
    "Automotive manufacturing": 18,
    "Electronics and semiconductor manufacturing": 18,
    "Aerospace and defense": 18,
}


class FeedDiscoveryParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.discovered_urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        href = attr_map.get("href", "")
        rel = attr_map.get("rel", "").lower()
        type_attr = attr_map.get("type", "").lower()
        if tag.lower() == "link" and "alternate" in rel and "rss" in type_attr and href:
            self.discovered_urls.append(urljoin(self.base_url, href))
        if tag.lower() == "a" and href:
            href_lower = href.lower()
            if "rss" in href_lower or "feed" in href_lower:
                self.discovered_urls.append(urljoin(self.base_url, href))


class LinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[tuple[str, str]] = []
        self._current_href = ""
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        href = attr_map.get("href", "")
        if not href:
            return
        self._current_href = urljoin(self.base_url, href)
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._current_href:
            return
        self.links.append((self._current_href, normalize_space("".join(self._text_parts))))
        self._current_href = ""
        self._text_parts = []


def _load_settings(config: PipelineConfig) -> OpportunitySettings:
    dotenv_values = load_dotenv(config.paths.dotenv_path)
    return OpportunitySettings(
        max_items_per_feed=int(env_value("OPPORTUNITY_RSS_MAX_ITEMS_PER_FEED", dotenv_values, "20")),
        accepted_score_threshold=int(env_value("OPPORTUNITY_RSS_ACCEPT_SCORE", dotenv_values, "65")),
        review_score_threshold=int(env_value("OPPORTUNITY_RSS_REVIEW_SCORE", dotenv_values, "50")),
        timeout_seconds=int(env_value("OPPORTUNITY_RSS_TIMEOUT_SECONDS", dotenv_values, "45")),
        max_retries=int(env_value("OPPORTUNITY_RSS_MAX_RETRIES", dotenv_values, "3")),
        article_fetch_limit=int(env_value("OPPORTUNITY_RSS_ARTICLE_FETCH_LIMIT", dotenv_values, "20")),
        user_agent=env_value(
            "OPPORTUNITY_RSS_USER_AGENT",
            dotenv_values,
            "osha-sales-pipeline-opportunity-rss/1.0",
        ),
    )


def _fetch_text(url: str, *, settings: OpportunitySettings, accept: str = "*/*") -> str:
    headers = {
        "Accept": accept,
        "User-Agent": settings.user_agent,
    }
    for attempt in range(1, settings.max_retries + 2):
        request = Request(url=url, method="GET", headers=headers)
        try:
            with urlopen(request, timeout=settings.timeout_seconds) as response:
                return response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            if attempt > settings.max_retries:
                raise RuntimeError(f"HTTP {exc.code} fetching {url}") from exc
        except URLError as exc:
            if attempt > settings.max_retries:
                raise RuntimeError(f"Network error fetching {url}: {exc}") from exc
        time.sleep(float(attempt))
    raise RuntimeError(f"Could not fetch {url}")


def _resolved_feed_candidates(feed: FeedSpec, settings: OpportunitySettings) -> list[tuple[str, str]]:
    payload = _fetch_text(feed.url, settings=settings, accept="application/rss+xml, application/atom+xml, text/xml, application/xml, text/html")
    text = payload.lstrip()
    if text.startswith("<?xml") or text.startswith("<rss") or text.startswith("<feed"):
        return [(feed.url, payload)]

    parser = FeedDiscoveryParser(feed.url)
    parser.feed(payload)
    urls = unique_preserving_order(parser.discovered_urls)
    if not urls:
        return []

    candidates: list[tuple[str, str]] = []
    for url in urls[:3]:
        try:
            candidates.append((url, _fetch_text(url, settings=settings, accept="application/rss+xml, application/atom+xml, text/xml, application/xml")))
        except Exception as exc:
            logging.warning("Skipping discovered feed %s for %s: %s", url, feed.name, exc)
    return candidates


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _first_child_text(element: ElementTree.Element, names: tuple[str, ...]) -> str:
    for child in element.iter():
        if _local_name(child.tag) in names:
            return normalize_space("".join(child.itertext()))
    return ""


def _parse_items(feed: FeedSpec, feed_url: str, xml_text: str, *, settings: OpportunitySettings) -> list[FeedItem]:
    root = ElementTree.fromstring(xml_text)
    items: list[FeedItem] = []
    for element in root.iter():
        local_name = _local_name(element.tag)
        if local_name not in {"item", "entry"}:
            continue
        title = _first_child_text(element, ("title",))
        summary = _first_child_text(element, ("description", "summary", "content", "encoded"))
        item_url = ""
        for child in element.iter():
            if _local_name(child.tag) == "link":
                href = child.attrib.get("href", "")
                if href:
                    item_url = href
                    break
                if child.text and child.text.strip():
                    item_url = child.text.strip()
                    break
        published_at = _first_child_text(element, ("pubDate", "published", "updated"))
        if not title or not item_url:
            continue
        items.append(
            FeedItem(
                feed_name=feed.name,
                feed_url=feed_url,
                industry=feed.industry,
                title=strip_html(title),
                summary=strip_html(summary),
                url=item_url,
                published_at=published_at,
                raw_payload={
                    "title": title,
                    "summary": summary,
                    "url": item_url,
                    "published_at": published_at,
                },
            )
        )
        if len(items) >= settings.max_items_per_feed:
            break
    return items


def _detect_event_type(text: str) -> str | None:
    lowered = text.lower()
    matches: list[tuple[int, str]] = []
    for event_type, patterns in EVENT_PATTERNS.items():
        count = sum(1 for pattern in patterns if pattern in lowered)
        if count:
            matches.append((count, event_type))
    if not matches:
        return None
    matches.sort(reverse=True)
    return matches[0][1]


def _extract_keywords(text: str) -> list[str]:
    lowered = text.lower()
    matches = [keyword for keyword in PPE_KEYWORDS if keyword in lowered]
    return unique_preserving_order(matches)


def _extract_scale_clues(text: str) -> list[str]:
    patterns = (
        r"\$\s?\d[\d,.]*(?:\s?(?:million|billion|m|bn))?",
        r"\b\d[\d,.-]*\s+(?:jobs|employees|workers|hires)\b",
        r"\b\d[\d,.-]*\s*(?:square feet|square-foot|sq\.?\s*ft\.?|sqft)\b",
        r"\b\d[\d,.-]*\s+(?:facilities|plants|sites|lines)\b",
    )
    matches: list[str] = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return unique_preserving_order(matches)


def _extract_location(text: str) -> str:
    patterns = (
        r"\b(?:in|at|near)\s+([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b",
        r"\b([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b",
        r"\b(?:in|at|near)\s+([A-Z][A-Za-z .'-]+)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return normalize_space(match.group(1))
    return ""


def _extract_company_name(title: str) -> str:
    title_clean = normalize_space(title)
    title_patterns = (
        r"^(?P<company>[A-Z][A-Za-z0-9&.,'()/-]+(?:\s+[A-Z][A-Za-z0-9&.,'()/-]+){0,5})\s+(?:plans|will|to|opens|open|opening|expands|expand|builds|build|invests|acquires|modernizes)\b",
        r"^(?P<company>[A-Z][A-Za-z0-9&.,'()/-]+(?:\s+[A-Z][A-Za-z0-9&.,'()/-]+){0,5})\s*:\s*",
        r"^(?P<company>[A-Z][A-Za-z0-9&.,'()/-]+(?:\s+[A-Z][A-Za-z0-9&.,'()/-]+){0,5})\s+-\s+",
    )
    for pattern in title_patterns:
        match = re.search(pattern, title_clean)
        if match:
            return normalize_company_name(match.group("company"))

    words = title_clean.split()
    candidate: list[str] = []
    for word in words:
        if word.lower() in TITLE_VERBS:
            break
        if not re.match(r"^[A-Z0-9&.,'()/-]+$", word):
            break
        candidate.append(word)
        if len(candidate) >= 6:
            break
    return normalize_company_name(" ".join(candidate))


def _score_event(
    *,
    industry: str,
    event_type: str,
    keywords: list[str],
    scale_clues: list[str],
    text: str,
    location: str,
) -> float:
    score = EVENT_TYPE_SCORES.get(event_type, 0)
    score += INDUSTRY_SCORES.get(industry, 14)
    score += min(12, len(keywords) * 2)
    if any(pattern in text.lower() for pattern in HIRING_PATTERNS):
        score += 10
    if any(term in text.lower() for term in ("manufacturing", "production", "warehouse", "facility", "plant", "refinery", "fab")):
        score += 8
    if location:
        score += 4
    if scale_clues:
        score += min(12, 4 + len(scale_clues) * 2)
    if any(keyword in text.lower() for keyword in ("construction", "capacity", "expansion", "new site", "new location")):
        score += 6
    return min(100.0, float(score))


def classify_feed_item(
    item: FeedItem,
    *,
    settings: OpportunitySettings,
) -> dict[str, Any] | None:
    text = normalize_space(f"{item.title} {item.summary}")
    lowered = text.lower()
    if any(pattern in lowered for pattern in NEGATIVE_PATTERNS) and not any(
        keyword in lowered for keyword in PPE_KEYWORDS
    ):
        return None

    event_type = _detect_event_type(text)
    if not event_type:
        return None

    keywords = _extract_keywords(text)
    if not keywords:
        return None

    company_name = _extract_company_name(item.title)
    if not company_name:
        return None

    scale_clues = _extract_scale_clues(text)
    location = _extract_location(text)
    event_score = _score_event(
        industry=item.industry,
        event_type=event_type,
        keywords=keywords,
        scale_clues=scale_clues,
        text=text,
        location=location,
    )
    review_status = "accepted"
    if event_score < settings.accepted_score_threshold:
        if event_score < settings.review_score_threshold:
            return None
        review_status = "review"

    signal_strength = "high" if event_score >= 75 else "medium" if event_score >= 60 else "low"
    reason_parts = [
        f"{event_type} signal detected",
        f"{len(keywords)} PPE-relevant keyword matches",
    ]
    if scale_clues:
        reason_parts.append(f"scale clues: {', '.join(scale_clues[:2])}")
    if location:
        reason_parts.append(f"location found: {location}")

    return {
        "id": stable_id(item.url),
        "company_name": company_name,
        "company_name_normalized": company_key(company_name),
        "industry": item.industry,
        "event_type": event_type,
        "headline": item.title,
        "summary": item.summary,
        "source_name": item.feed_name,
        "source_url": item.url,
        "published_at": item.published_at,
        "location": location,
        "event_score": event_score,
        "signal_strength": signal_strength,
        "review_status": review_status,
        "raw_keywords": json_dumps(keywords),
        "scale_clues": json_dumps(scale_clues),
        "classification_reason": "; ".join(reason_parts),
        "raw_payload": json_dumps(item.raw_payload),
    }


def _emails(text: str) -> list[str]:
    return unique_preserving_order(
        re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.IGNORECASE)
    )


def _phones(text: str) -> list[str]:
    return unique_preserving_order(
        re.findall(r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}", text)
    )


def _contact_people(text: str) -> list[str]:
    matches = re.findall(
        r"\b([A-Z][a-z]+ [A-Z][a-z]+),\s*(CEO|CFO|COO|President|Vice President|VP|Plant Manager|Director|Manager)\b",
        text,
    )
    return unique_preserving_order([f"{name}|{title}" for name, title in matches])


def _website_contacts(
    *,
    company_name: str,
    company_name_normalized: str,
    source_name: str,
    source_url: str,
    article_url: str,
    article_html: str,
    settings: OpportunitySettings,
) -> list[dict[str, Any]]:
    parser = LinkParser(article_url)
    parser.feed(article_html)

    source_host = urlparse(article_url).netloc.lower()
    candidate_links: list[tuple[str, str]] = []
    for href, text in parser.links:
        host = urlparse(href).netloc.lower()
        if not host or host == source_host:
            continue
        candidate_links.append((href, text))

    contacts: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()

    def add_contact(contact_type: str, contact_value: str, *, name: str = "", title: str = "", contact_source_url: str = source_url) -> None:
        key = (contact_type, contact_value)
        if not contact_value or key in seen_pairs:
            return
        seen_pairs.add(key)
        contacts.append(
            {
                "company_name": company_name,
                "company_name_normalized": company_name_normalized,
                "contact_type": contact_type,
                "contact_value": contact_value,
                "name": name,
                "title": title,
                "source_name": source_name,
                "source_url": contact_source_url,
            }
        )

    for email in _emails(article_html):
        add_contact("email", email)
    for phone in _phones(article_html):
        add_contact("phone", phone)

    website_url = ""
    contact_page_url = ""
    for href, text in candidate_links:
        lowered = f"{href} {text}".lower()
        if not website_url:
            website_url = href
        if "contact" in lowered:
            contact_page_url = href
        if website_url and contact_page_url:
            break

    if website_url:
        add_contact("website", website_url, contact_source_url=website_url)
    if contact_page_url:
        add_contact("contact_page", contact_page_url, contact_source_url=contact_page_url)

    person_tokens = _contact_people(strip_html(article_html))
    for token in person_tokens[:3]:
        name, title = token.split("|", 1)
        add_contact("person", name, name=name, title=title)

    if website_url:
        try:
            company_html = _fetch_text(website_url, settings=settings, accept="text/html,application/xhtml+xml")
            for email in _emails(company_html)[:2]:
                add_contact("email", email, contact_source_url=website_url)
            for phone in _phones(company_html)[:2]:
                add_contact("phone", phone, contact_source_url=website_url)
            company_parser = LinkParser(website_url)
            company_parser.feed(company_html)
            for href, text in company_parser.links:
                lowered = f"{href} {text}".lower()
                if "contact" in lowered:
                    add_contact("contact_page", href, contact_source_url=href)
                    break
        except Exception as exc:
            logging.warning("Contact enrichment skipped for %s (%s): %s", company_name, website_url, exc)

    return contacts


def run_opportunity_rss_ingest(
    config: PipelineConfig,
    *,
    store: SalesIntelStore | None = None,
    export_snapshots: bool = True,
) -> dict[str, int]:
    settings = _load_settings(config)
    target_store = store or SalesIntelStore.from_config(config)
    target_store.initialize()

    raw_count = 0
    accepted_count = 0
    review_count = 0
    article_fetches = 0

    for feed in FEEDS:
        try:
            candidates = _resolved_feed_candidates(feed, settings)
        except Exception as exc:
            logging.warning("Skipping feed %s due to fetch failure: %s", feed.name, exc)
            continue

        if not candidates:
            logging.warning("No RSS/Atom payloads resolved for feed %s", feed.name)
            continue

        for feed_url, xml_text in candidates:
            try:
                items = _parse_items(feed, feed_url, xml_text, settings=settings)
            except Exception as exc:
                logging.warning("Feed parse failed for %s (%s): %s", feed.name, feed_url, exc)
                continue

            for item in items:
                raw_count += 1
                raw_id = target_store.upsert_raw_feed_item(
                    {
                        "id": stable_id(item.url),
                        "feed_name": item.feed_name,
                        "feed_url": item.feed_url,
                        "item_title": item.title,
                        "item_summary": item.summary,
                        "item_url": item.url,
                        "published_at": item.published_at,
                        "raw_payload": json_dumps(item.raw_payload),
                    }
                )

                classified = classify_feed_item(item, settings=settings)
                if not classified:
                    continue

                classified = dict(classified)
                classified["raw_item_id"] = raw_id
                target_store.upsert_event(classified)

                contacts: list[dict[str, Any]] = []
                if (
                    classified["review_status"] == "accepted"
                    and article_fetches < settings.article_fetch_limit
                ):
                    try:
                        article_html = _fetch_text(
                            item.url,
                            settings=settings,
                            accept="text/html,application/xhtml+xml",
                        )
                        article_fetches += 1
                        contacts = _website_contacts(
                            company_name=classified["company_name"],
                            company_name_normalized=classified["company_name_normalized"],
                            source_name=classified["source_name"],
                            source_url=classified["source_url"],
                            article_url=item.url,
                            article_html=article_html,
                            settings=settings,
                        )
                    except Exception as exc:
                        logging.warning("Article enrichment failed for %s: %s", item.url, exc)

                target_store.replace_event_contacts(event_id=classified["id"], contacts=contacts)

                if classified["review_status"] == "accepted":
                    accepted_count += 1
                else:
                    review_count += 1

    target_store.rebuild_intersections()
    if export_snapshots:
        target_store.export_snapshots()

    return {
        "raw_items": raw_count,
        "accepted_events": accepted_count,
        "review_events": review_count,
    }
