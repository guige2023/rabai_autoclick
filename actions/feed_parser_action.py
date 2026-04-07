"""
RSS/Atom Feed Parser Action Module.

Parses RSS 2.0, Atom 1.0, and JSON Feed formats.
Handles feed discovery, item extraction, and feed normalization.

Example:
    >>> from feed_parser_action import FeedParser
    >>> parser = FeedParser()
    >>> items = parser.parse("https://example.com/feed.xml")
    >>> normalized = parser.normalize(items, "https://example.com")
"""
from __future__ import annotations

import re
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class FeedItem:
    """Normalized feed item."""
    id: str
    title: str
    link: str
    description: str = ""
    content: str = ""
    author: str = ""
    published: Optional[datetime] = None
    updated: Optional[datetime] = None
    categories: list[str] = field(default_factory=list)
    enclosure_url: str = ""
    enclosure_type: str = ""


@dataclass
class FeedInfo:
    """Feed metadata."""
    title: str
    link: str
    description: str = ""
    language: str = ""
    items: list[FeedItem] = field(default_factory=list)


class FeedParser:
    """Parse RSS, Atom, and JSON feeds."""

    def parse(self, xml_content: str) -> FeedInfo:
        """
        Auto-detect feed format and parse.

        Returns:
            FeedInfo with metadata and items
        """
        if xml_content.strip().startswith("{"):
            return self.parse_json_feed(xml_content)
        elif "<rss" in xml_content[:200].lower():
            return self.parse_rss(xml_content)
        elif "<feed" in xml_content[:200].lower():
            return self.parse_atom(xml_content)
        else:
            return self.parse_rss(xml_content)

    def parse_rss(self, xml: str) -> FeedInfo:
        """Parse RSS 2.0 feed."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(xml)

        title_sel = CSSSelector("channel > title")
        title_elem = title_sel.match(doc.root)
        title = parser.get_text(title_elem).strip() if title_elem else ""

        link_sel = CSSSelector("channel > link")
        link_elem = link_sel.match(doc.root)
        link = parser.get_text(link_elem).strip() if link_elem else ""

        desc_sel = CSSSelector("channel > description")
        desc_elem = desc_sel.match(doc.root)
        description = parser.get_text(desc_elem).strip() if desc_elem else ""

        lang_sel = CSSSelector("channel > language")
        lang_elem = lang_sel.match(doc.root)
        language = parser.get_text(lang_elem).strip() if lang_elem else ""

        item_sel = CSSSelector("item")
        item_elems = item_sel.match_all(doc.root)
        items = [self._parse_rss_item(item, parser) for item in item_elems]

        return FeedInfo(title=title, link=link, description=description, language=language, items=items)

    def _parse_rss_item(self, elem, parser) -> FeedItem:
        def text(selector: str) -> str:
            sel = CSSSelector(selector)
            e = sel.match(elem)
            return parser.get_text(e).strip() if e else ""

        def attr(selector: str, attr_name: str) -> str:
            sel = CSSSelector(selector)
            e = sel.match(elem)
            return e.get_attribute(attr_name) if e else ""

        item_id = attr("guid", "isPermaLink") or text("guid") or text("link")
        pub_text = text("pubDate")
        published = self._parse_date(pub_text) if pub_text else None

        enclosure_url = attr("enclosure", "url")
        enclosure_type = attr("enclosure", "type")

        categories = []
        for cat_sel in CSSSelector("category").match_all(elem):
            cat_text = parser.get_text(cat_sel).strip()
            if cat_text:
                categories.append(cat_text)

        return FeedItem(
            id=item_id,
            title=text("title"),
            link=text("link"),
            description=text("description"),
            content=text("content\\:encoded") or text("content"),
            author=text("author") or text("dc\\:creator"),
            published=published,
            categories=categories,
            enclosure_url=enclosure_url,
            enclosure_type=enclosure_type,
        )

    def parse_atom(self, xml: str) -> FeedInfo:
        """Parse Atom 1.0 feed."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(xml)

        title_sel = CSSSelector("feed > title")
        title_elem = title_sel.match(doc.root)
        title = parser.get_text(title_elem).strip() if title_elem else ""

        link_sel = CSSSelector("feed > link[rel='alternate']")
        link_elem = link_sel.match(doc.root)
        link = link_elem.get_attribute("href") if link_elem else ""
        if not link:
            link_elem2 = CSSSelector("feed > link")
            link_elem2 = link_elem2.match(doc.root)
            link = link_elem2.get_attribute("href") if link_elem2 else ""

        desc_sel = CSSSelector("feed > subtitle")
        desc_elem = desc_sel.match(doc.root)
        description = parser.get_text(desc_elem).strip() if desc_elem else ""

        lang_sel = CSSSelector("feed > language")
        lang_elem = lang_sel.match(doc.root)
        language = parser.get_text(lang_elem).strip() if lang_elem else ""

        entry_sel = CSSSelector("entry")
        entry_elems = entry_sel.match_all(doc.root)
        items = [self._parse_atom_entry(entry, parser) for entry in entry_elems]

        return FeedInfo(title=title, link=link, description=description, language=language, items=items)

    def _parse_atom_entry(self, elem, parser) -> FeedItem:
        def text(selector: str) -> str:
            sel = CSSSelector(selector)
            e = sel.match(elem)
            return parser.get_text(e).strip() if e else ""

        def link(selector: str) -> str:
            sel = CSSSelector(selector)
            e = sel.match(elem)
            return e.get_attribute("href") if e else ""

        item_id = text("id") or text("link")
        pub_text = text("published") or text("updated")
        published = self._parse_date(pub_text) if pub_text else None

        author_sel = CSSSelector("author name")
        author_elem = author_sel.match(elem)
        author = parser.get_text(author_elem).strip() if author_elem else ""

        categories = []
        for cat_sel in CSSSelector("category").match_all(elem):
            term = cat_sel.get_attribute("term")
            if term:
                categories.append(term)

        enclosure_url = link("link[rel='enclosure']")
        enclosure_type_elem = CSSSelector("link[rel='enclosure']").match(elem)
        enclosure_type = enclosure_type_elem.get_attribute("type") if enclosure_type_elem else ""

        return FeedItem(
            id=item_id,
            title=text("title"),
            link=link("link[rel='alternate']") or link("link"),
            description=text("summary"),
            content=text("content"),
            author=author,
            published=published,
            categories=categories,
            enclosure_url=enclosure_url,
            enclosure_type=enclosure_type,
        )

    def parse_json_feed(self, json_str: str) -> FeedInfo:
        """Parse JSON Feed (https://jsonfeed.org/)."""
        import json
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            return FeedInfo(title="", link="")

        title = data.get("title", "")
        link = data.get("home_page_url", "") or data.get("feed_url", "")
        description = data.get("description", "")
        language = data.get("language", "")

        items = []
        for item_data in data.get("items", []):
            pub_text = item_data.get("date_published") or item_data.get("date_modified", "")
            published = self._parse_date(pub_text) if pub_text else None

            enclosure = item_data.get("attachments", [{}])[0] if item_data.get("attachments") else {}
            enclosure_url = enclosure.get("url", "")
            enclosure_type = enclosure.get("mime_type", "")

            items.append(FeedItem(
                id=item_data.get("id", ""),
                title=item_data.get("title", ""),
                link=item_data.get("url", ""),
                description=item_data.get("summary", ""),
                content=item_data.get("content_html", "") or item_data.get("content_text", ""),
                author=item_data.get("authors", [{}])[0].get("name", "") if item_data.get("authors") else "",
                published=published,
                categories=[c.get("slug", "") for c in item_data.get("tags", [])],
                enclosure_url=enclosure_url,
                enclosure_type=enclosure_type,
            ))

        return FeedInfo(title=title, link=link, description=description, language=language, items=items)

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        date_str = date_str.strip()
        formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                pass
        try:
            return datetime.fromtimestamp(time.mktime(time.strptime(date_str[:25], "%a, %d %b %Y %H:%M:%S")))
        except Exception:
            return None

    def discover_feeds(self, html: str) -> list[dict[str, str]]:
        """Discover feed URLs from HTML page."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        feeds: list[dict[str, str]] = []

        for sel_str in ["link[type='application/rss+xml']", "link[type='application/atom+xml']", "link[type='application/json']"]:
            sel = CSSSelector(sel_str)
            for elem in sel.match_all(doc.root):
                href = elem.get_attribute("href")
                feed_type = elem.get_attribute("type", "").split("+")[0].split("/")[-1]
                title = elem.get_attribute("title") or feed_type
                if href:
                    feeds.append({"href": href, "type": feed_type, "title": title})
        return feeds

    def normalize(self, items: list[FeedItem], base_url: str = "") -> list[dict[str, Any]]:
        """Convert FeedItems to plain dictionaries."""
        result: list[dict[str, Any]] = []
        for item in items:
            record: dict[str, Any] = {
                "id": item.id,
                "title": item.title,
                "link": item.link,
                "description": item.description,
                "content": item.content,
                "author": item.author,
                "published": item.published.isoformat() if item.published else None,
                "categories": item.categories,
            }
            if base_url and item.link and not item.link.startswith("http"):
                record["link"] = urllib.parse.urljoin(base_url, item.link)
            result.append(record)
        return result


if __name__ == "__main__":
    parser = FeedParser()
    rss_xml = """<?xml version="1.0"?>
    <rss version="2.0">
    <channel>
        <title>Test Feed</title>
        <link>https://example.com</link>
        <description>A test feed</description>
        <item>
            <title>First Post</title>
            <link>https://example.com/post1</link>
            <guid>post-1</guid>
            <pubDate>Wed, 01 Jan 2025 12:00:00 +0000</pubDate>
        </item>
    </channel>
    </rss>"""
    feed = parser.parse(rss_xml)
    print(f"Feed: {feed.title}")
    print(f"Items: {len(feed.items)}")
    for item in feed.items:
        print(f"  - {item.title}: {item.link}")
