"""Web Scraper action module for RabAI AutoClick.

Provides web scraping operations:
- ScrapeHTMLAction: Scrape HTML pages
- ScrapeAPIAction: Scrape API endpoints
- ScrapeJSONAction: Scrape JSON data
- ScrapePaginationAction: Scrape paginated content
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScrapeHTMLAction(BaseAction):
    """Scrape HTML pages."""
    action_type = "scrape_html"
    display_name = "HTML抓取"
    description = "抓取HTML页面"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML scraping."""
        url = params.get('url', '')
        selectors = params.get('selectors', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'scraped_data')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests
            from bs4 import BeautifulSoup

            resolved_url = context.resolve_value(url) if context else url

            response = requests.get(resolved_url, headers=headers, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')

            results = {}
            for key, selector in selectors.items():
                elements = soup.select(selector)
                if len(elements) == 1:
                    results[key] = elements[0].get_text(strip=True)
                else:
                    results[key] = [el.get_text(strip=True) for el in elements]

            result = {
                'url': resolved_url,
                'data': results,
                'status_code': response.status_code,
            }

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"Scraped {len(results)} fields from HTML"
            )
        except ImportError:
            return ActionResult(success=False, message="requests/beautifulsoup4 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"HTML scrape error: {e}")


class ScrapeJSONAction(BaseAction):
    """Scrape JSON data."""
    action_type = "scrape_json"
    display_name = "JSON抓取"
    description = "抓取JSON数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON scraping."""
        url = params.get('url', '')
        path = params.get('path', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'scraped_data')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import requests

            resolved_url = context.resolve_value(url) if context else url

            response = requests.get(resolved_url, headers=headers, timeout=30)
            data = response.json()

            if path:
                for key in path.split('.'):
                    if key.isdigit():
                        data = data[int(key)]
                    elif key in data:
                        data = data[key]
                    else:
                        data = None
                        break

            result = {
                'url': resolved_url,
                'data': data,
                'status_code': response.status_code,
            }

            return ActionResult(
                success=response.ok,
                data={output_var: result},
                message=f"Scraped JSON from {resolved_url}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON scrape error: {e}")


class ScrapePaginationAction(BaseAction):
    """Scrape paginated content."""
    action_type = "scrape_pagination"
    display_name = "分页抓取"
    description = "抓取分页内容"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pagination scraping."""
        base_url = params.get('base_url', '')
        page_param = params.get('page_param', 'page')
        max_pages = params.get('max_pages', 10)
        extract_func = params.get('extract_func', 'text')
        output_var = params.get('output_var', 'scraped_data')

        if not base_url:
            return ActionResult(success=False, message="base_url is required")

        try:
            import requests

            resolved_base = context.resolve_value(base_url) if context else base_url
            resolved_max = context.resolve_value(max_pages) if context else max_pages

            all_results = []
            for page in range(1, resolved_max + 1):
                page_url = f"{resolved_base}?{page_param}={page}"
                response = requests.get(page_url, timeout=30)

                if not response.ok:
                    break

                all_results.append({'page': page, 'status': response.status_code, 'content': response.text[:500]})

            result = {
                'pages_scraped': len(all_results),
                'results': all_results,
            }

            return ActionResult(
                success=len(all_results) > 0,
                data={output_var: result},
                message=f"Scraped {len(all_results)} pages"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pagination scrape error: {e}")
