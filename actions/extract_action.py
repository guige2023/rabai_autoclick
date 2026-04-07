"""
Content Extraction Action Module.

Extracts structured data from web pages, feeds, and documents.
Supports article extraction, metadata parsing, and entity recognition.

Example:
    >>> from extract_action import ContentExtractor
    >>> extractor = ContentExtractor()
    >>> article = extractor.extract_article(html_content)
    >>> metadata = extractor.extract_metadata(html_content)
"""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ExtractedArticle:
    """Extracted article content and metadata."""
    title: str
    content: str
    text: str
    author: Optional[str] = None
    published_date: Optional[str] = None
    description: Optional[str] = None
    image: Optional[str] = None
    url: Optional[str] = None
    domain: Optional[str] = None
    language: Optional[str] = None
    tags: list[str] = None


@dataclass
class ExtractedMetadata:
    """Page metadata container."""
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[str] = None
    author: Optional[str] = None
    og_image: Optional[str] = None
    og_title: Optional[str] = None
    og_description: Optional[str] = None
    twitter_card: Optional[str] = None
    twitter_image: Optional[str] = None
    canonical_url: Optional[str] = None


class ContentExtractor:
    """Extract structured content from HTML pages."""

    ARTICLE_SELECTORS = [
        ("article",),
        ("[itemprop='articleBody']",),
        (".post-content",),
        (".entry-content",),
        (".article-content",),
        (".content-body",),
        (".story-body",),
        ("#article-body",),
        (".post-body",),
        ("main",),
    ]

    TITLE_SELECTORS = [
        "h1",
        "[itemprop='headline']",
        ".article-title",
        ".post-title",
        ".entry-title",
        "article h1",
    ]

    def __init__(self):
        self._html = ""

    def extract_article(self, html: str, url: Optional[str] = None) -> ExtractedArticle:
        """
        Extract main article content from HTML.

        Args:
            html: Full HTML content
            url: Source URL for resolving relative links

        Returns:
            ExtractedArticle with content and metadata
        """
        self._html = html

        title = self._extract_title()
        content = self._extract_content()
        text = self._strip_html(content)
        author = self._extract_author()
        published = self._extract_date()
        description = self._extract_description()
        image = self._extract_main_image(url)
        domain = urllib.parse.urlparse(url).netloc if url else None

        return ExtractedArticle(
            title=title,
            content=content,
            text=text,
            author=author,
            published_date=published,
            description=description,
            image=image,
            url=url,
            domain=domain,
            language=self._detect_language(text),
        )

    def extract_metadata(self, html: str, url: Optional[str] = None) -> ExtractedMetadata:
        """Extract OpenGraph, Twitter Card, and meta tags."""
        from html_parser_action import HTMLParserAction, CSSSelector

        parser = HTMLParserAction()
        doc = parser.parse_string(html)

        def get_meta(name: str) -> Optional[str]:
            sel = CSSSelector(f"meta[name='{name}']")
            elem = sel.match(doc.root)
            return elem.get_attribute("content") if elem else None

        def get_og(name: str) -> Optional[str]:
            sel = CSSSelector(f"meta[property='og:{name}']")
            elem = sel.match(doc.root)
            return elem.get_attribute("content") if elem else None

        def get_twitter(name: str) -> Optional[str]:
            sel = CSSSelector(f"meta[name='twitter:{name}']")
            elem = sel.match(doc.root)
            return elem.get_attribute("content") if elem else None

        sel_canonical = CSSSelector("link[rel='canonical']")
        canonical_elem = sel_canonical.match(doc.root)
        canonical = canonical_elem.get_attribute("href") if canonical_elem else None

        return ExtractedMetadata(
            title=get_meta("title") or get_og("title"),
            description=get_meta("description"),
            keywords=get_meta("keywords"),
            author=get_meta("author"),
            og_image=get_og("image"),
            og_title=get_og("title"),
            og_description=get_og("description"),
            twitter_card=get_meta("twitter:card"),
            twitter_image=get_twitter("image"),
            canonical_url=canonical,
        )

    def _extract_title(self) -> str:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_str in self.TITLE_SELECTORS:
            sel = CSSSelector(sel_str)
            elem = sel.match(doc.root)
            if elem:
                text = parser.get_text(elem).strip()
                if text:
                    return text

        sel = CSSSelector("title")
        elem = sel.match(doc.root)
        if elem:
            return parser.get_text(elem).strip()
        return ""

    def _extract_content(self) -> str:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_tuple in self.ARTICLE_SELECTORS:
            sel_str = sel_tuple[0]
            sel = CSSSelector(sel_str)
            elems = sel.match_all(doc.root)
            for elem in elems:
                text = parser.get_text(elem).strip()
                if len(text) > 200:
                    return self._clean_content(elem, parser)
        return ""

    def _clean_content(self, elem, parser) -> str:
        from html_parser_action import HTMLElement

        def clean_node(node: HTMLElement) -> str:
            tag = node.tag.lower()
            if tag in ("script", "style", "nav", "footer", "header", "aside", "noscript"):
                return ""
            text = node.text or ""
            for child in node.children:
                text += clean_node(child)
            text += node.text or ""
            if tag in ("p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li"):
                text += "\n"
            return text

        return clean_node(elem).strip()

    def _strip_html(self, html_content: str) -> str:
        from html_parser_action import HTMLParserAction
        parser = HTMLParserAction()
        return parser.strip_tags(html_content)

    def _extract_author(self) -> Optional[str]:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_str in ["[itemprop='author']", ".author", ".byline", "[rel='author']"]:
            sel = CSSSelector(sel_str)
            elem = sel.match(doc.root)
            if elem:
                text = parser.get_text(elem).strip()
                if text:
                    text = re.sub(r"^by\s+", "", text, flags=re.IGNORECASE)
                    return text
        return None

    def _extract_date(self) -> Optional[str]:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_str in ["[itemprop='datePublished']", "time[datetime]", ".published", ".post-date"]:
            sel = CSSSelector(sel_str)
            elem = sel.match(doc.root)
            if elem:
                date = elem.get_attribute("datetime") or elem.get_attribute("content") or parser.get_text(elem)
                if date:
                    return date.strip()
        return None

    def _extract_description(self) -> Optional[str]:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_str in ["meta[name='description']", "meta[property='og:description']"]:
            sel = CSSSelector(sel_str)
            elem = sel.match(doc.root)
            if elem:
                content = elem.get_attribute("content")
                if content:
                    return content.strip()
        return None

    def _extract_main_image(self, url: Optional[str] = None) -> Optional[str]:
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(self._html)

        for sel_str in ["meta[property='og:image']", "[itemprop='image']", "article img", ".post-thumbnail img"]:
            sel = CSSSelector(sel_str)
            elem = sel.match(doc.root)
            if elem:
                src = elem.get_attribute("src") or elem.get_attribute("content") or elem.get_attribute("href")
                if src:
                    if url and not src.startswith("http"):
                        src = urllib.parse.urljoin(url, src)
                    return src
        return None

    def _detect_language(self, text: str) -> Optional[str]:
        if not text:
            return None
        try:
            import langdetect
            return langdetect.detect(text[:500])
        except Exception:
            return None

    def extract_links(self, html: str, base_url: str = "") -> list[dict[str, str]]:
        """Extract all links with text and href."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector("a")
        links: list[dict[str, str]] = []
        for elem in sel.match_all(doc.root):
            href = elem.get_attribute("href")
            if href and not href.startswith(("javascript:", "#", "mailto:")):
                if base_url and not href.startswith("http"):
                    href = urllib.parse.urljoin(base_url, href)
                text = parser.get_text(elem).strip()
                links.append({"href": href, "text": text})
        return links

    def extract_images(self, html: str, base_url: str = "") -> list[dict[str, str]]:
        """Extract all images with src and alt text."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector("img")
        images: list[dict[str, str]] = []
        for elem in sel.match_all(doc.root):
            src = elem.get_attribute("src") or elem.get_attribute("data-src") or ""
            if src:
                if base_url and not src.startswith("http"):
                    src = urllib.parse.urljoin(base_url, src)
                alt = elem.get_attribute("alt") or ""
                images.append({"src": src, "alt": alt})
        return images

    def extract_structured_data(self, html: str) -> list[dict[str, Any]]:
        """Extract JSON-LD structured data from page."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector("script[type='application/ld+json']")
        results: list[dict[str, Any]] = []
        for elem in sel.match_all(doc.root):
            import json
            text = parser.get_text(elem)
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)
            except json.JSONDecodeError:
                pass
        return results


if __name__ == "__main__":
    extractor = ContentExtractor()
    html = """
    <html><head><title>Test Article</title>
    <meta name='author' content='John Doe'>
    <meta name='description' content='A test article'>
    <meta property='og:image' content='https://example.com/image.jpg'>
    </head><body>
    <article>
        <h1>My Article Title</h1>
        <p>This is the article content with some text.</p>
    </article>
    </body></html>
    """
    article = extractor.extract_article(html, "https://example.com/article")
    print(f"Title: {article.title}")
    print(f"Author: {article.author}")
    print(f"Text: {article.text[:100]}")
