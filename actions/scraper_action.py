"""Web scraping action module for RabAI AutoClick.

Provides web scraping operations:
- FetchAction: HTTP GET requests with headers and query params
- ParseHtmlAction: Parse HTML content with CSS selectors
- ExtractLinksAction: Extract all links from HTML
- ExtractImagesAction: Extract all images from HTML
- ScrapeTableAction: Extract table data from HTML
- FollowLinksAction: Recursively follow and scrape links
"""

import re
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FetchAction(BaseAction):
    """HTTP GET request action."""
    action_type = "fetch"
    display_name = "HTTP获取"
    description = "执行HTTP GET请求获取网页内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.error

            url = params.get("url")
            if not url:
                return ActionResult(success=False, message="URL is required")

            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            req = urllib.request.Request(url)
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read().decode("utf-8", errors="replace")
                    return ActionResult(
                        success=True,
                        message="Fetched successfully",
                        data={
                            "content": content,
                            "status_code": response.status,
                            "headers": dict(response.headers),
                            "url": response.url
                        }
                    )
            except urllib.error.HTTPError as e:
                return ActionResult(success=False, message=f"HTTP error: {e.code}")
            except urllib.error.URLError as e:
                return ActionResult(success=False, message=f"URL error: {e.reason}")
            except Exception as e:
                return ActionResult(success=False, message=f"Error: {str(e)}")

        except ImportError:
            return ActionResult(success=False, message="urllib not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Unexpected error: {str(e)}")


class ParseHtmlAction(BaseAction):
    """Parse HTML with CSS selectors."""
    action_type = "parse_html"
    display_name = "解析HTML"
    description = "使用CSS选择器解析HTML内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            from html.parser import HTMLParser

            html = params.get("html", "")
            selector = params.get("selector", "")
            attribute = params.get("attribute", None)

            if not html:
                return ActionResult(success=False, message="HTML content is required")

            class SelectorParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.results = []
                    self.current_tag = None
                    self.current_attrs = {}
                    self.current_content = ""
                    self.selector = ""
                    self.target_attribute = None

                def handle_starttag(self, tag, attrs):
                    self.current_tag = tag
                    self.current_attrs = dict(attrs)
                    self.current_content = ""

                def handle_data(self, data):
                    self.current_content += data

                def handle_endtag(self, tag):
                    if self.current_tag == tag:
                        if self.matches_selector(tag, self.current_attrs):
                            if self.target_attribute:
                                val = self.current_attrs.get(self.target_attribute, "")
                                if val:
                                    self.results.append(val)
                            else:
                                self.results.append(self.current_content.strip())
                    self.current_tag = None

                def matches_selector(self, tag, attrs):
                    if not self.selector:
                        return True
                    if self.selector.startswith("."):
                        class_name = self.selector[1:]
                        return any(cls == class_name for cls in attrs.get("class", "").split())
                    elif self.selector.startswith("#"):
                        return attrs.get("id", "") == self.selector[1:]
                    elif self.selector.startswith("["):
                        match = re.match(r"\[(\w+)(?:='(.*?)')?\]", self.selector)
                        if match:
                            key, val = match.groups()
                            return attrs.get(key, "") == (val or "")
                        return False
                    else:
                        return tag == self.selector

            parser = SelectorParser()
            parser.selector = selector
            parser.target_attribute = attribute
            parser.feed(html)

            return ActionResult(
                success=True,
                message=f"Found {len(parser.results)} matches",
                data={"results": parser.results, "count": len(parser.results)}
            )

        except ImportError:
            return ActionResult(success=False, message="HTML parser not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Parse error: {str(e)}")


class ExtractLinksAction(BaseAction):
    """Extract all links from HTML."""
    action_type = "extract_links"
    display_name = "提取链接"
    description = "从HTML中提取所有链接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            base_url = params.get("base_url", "")
            filter_pattern = params.get("filter_pattern", "")

            if not html:
                return ActionResult(success=False, message="HTML content is required")

            href_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
            links = []

            for match in href_pattern.finditer(html):
                href = match.group(1)
                if href.startswith(("http://", "https://", "//")):
                    if href.startswith("//"):
                        full_url = "https:" + href
                    else:
                        full_url = href
                elif href.startswith("/"):
                    if base_url:
                        parsed = urlparse(base_url)
                        full_url = f"{parsed.scheme}://{parsed.netloc}{href}"
                    else:
                        full_url = href
                elif base_url:
                    full_url = urljoin(base_url, href)
                else:
                    full_url = href

                if filter_pattern:
                    if not re.search(filter_pattern, full_url):
                        continue

                links.append(full_url)

            return ActionResult(
                success=True,
                message=f"Extracted {len(links)} links",
                data={"links": links, "count": len(links)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {str(e)}")


class ExtractImagesAction(BaseAction):
    """Extract all images from HTML."""
    action_type = "extract_images"
    display_name = "提取图片"
    description = "从HTML中提取所有图片URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            base_url = params.get("base_url", "")

            if not html:
                return ActionResult(success=False, message="HTML content is required")

            img_pattern = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
            images = []

            for match in img_pattern.finditer(html):
                src = match.group(1)
                if src.startswith(("http://", "https://", "//")):
                    if src.startswith("//"):
                        full_url = "https:" + src
                    else:
                        full_url = src
                elif src.startswith("/"):
                    if base_url:
                        parsed = urlparse(base_url)
                        full_url = f"{parsed.scheme}://{parsed.netloc}{src}"
                    else:
                        full_url = src
                elif base_url:
                    full_url = urljoin(base_url, src)
                else:
                    full_url = src

                images.append(full_url)

            return ActionResult(
                success=True,
                message=f"Extracted {len(images)} images",
                data={"images": images, "count": len(images)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {str(e)}")


class ScrapeTableAction(BaseAction):
    """Extract table data from HTML."""
    action_type = "scrape_table"
    display_name = "提取表格"
    description = "从HTML中提取表格数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            table_index = params.get("table_index", 0)

            if not html:
                return ActionResult(success=False, message="HTML content is required")

            from html.parser import HTMLParser

            class TableParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.tables = []
                    self.current_table = []
                    self.current_row = []
                    self.current_cell = ""
                    self.in_table = False
                    self.in_row = False
                    self.in_cell = False
                    self.tables_found = 0

                def handle_starttag(self, tag, attrs):
                    if tag == "table":
                        self.in_table = True
                        self.current_table = []
                        self.tables_found += 1
                    elif tag == "tr" and self.in_table:
                        self.in_row = True
                        self.current_row = []
                    elif tag in ("td", "th") and self.in_row:
                        self.in_cell = True
                        self.current_cell = ""

                def handle_data(self, data):
                    if self.in_cell:
                        self.current_cell += data

                def handle_endtag(self, tag):
                    if tag in ("td", "th") and self.in_cell:
                        self.current_row.append(self.current_cell.strip())
                        self.in_cell = False
                    elif tag == "tr" and self.in_row:
                        if self.current_row:
                            self.current_table.append(self.current_row)
                        self.in_row = False
                    elif tag == "table" and self.in_table:
                        if self.current_table:
                            self.tables.append(self.current_table)
                        self.in_table = False

            parser = TableParser()
            parser.feed(html)

            if table_index >= len(parser.tables):
                return ActionResult(
                    success=False,
                    message=f"Table index {table_index} not found, found {len(parser.tables)} tables"
                )

            table = parser.tables[table_index]
            headers = table[0] if table else []
            rows = table[1:] if len(table) > 1 else []

            return ActionResult(
                success=True,
                message=f"Extracted table with {len(rows)} rows",
                data={
                    "headers": headers,
                    "rows": rows,
                    "row_count": len(rows),
                    "column_count": len(headers)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Table scrape error: {str(e)}")


class FollowLinksAction(BaseAction):
    """Recursively follow and scrape links."""
    action_type = "follow_links"
    display_name = "跟踪链接"
    description = "递归跟踪链接并抓取内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.error

            start_url = params.get("url", "")
            max_depth = params.get("max_depth", 2)
            filter_pattern = params.get("filter_pattern", "")
            delay = params.get("delay", 1.0)

            if not start_url:
                return ActionResult(success=False, message="URL is required")

            visited = set()
            results = []

            def fetch_url(url):
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=15) as response:
                        return response.read().decode("utf-8", errors="replace"), response.url
                except Exception:
                    return None, None

            def scrape(url, depth):
                if depth > max_depth or url in visited:
                    return
                visited.add(url)

                content, final_url = fetch_url(url)
                if not content:
                    return

                href_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
                links = []
                for match in href_pattern.finditer(content):
                    href = match.group(1)
                    if href.startswith(("http://", "https://")):
                        if filter_pattern and not re.search(filter_pattern, href):
                            continue
                        links.append(href)

                results.append({
                    "url": final_url,
                    "depth": depth,
                    "links_found": len(links),
                    "links": links[:10]
                })

                for link in links[:5]:
                    time.sleep(delay)
                    scrape(link, depth + 1)

            scrape(start_url, 0)

            return ActionResult(
                success=True,
                message=f"Scraped {len(results)} pages",
                data={"pages": results, "total_pages": len(results), "visited_count": len(visited)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Follow links error: {str(e)}")
