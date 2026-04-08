"""
HTML and web scraping utilities - parsing, manipulation, sanitization, extraction.
"""
from typing import Any, Dict, List, Optional, Tuple
import re
import html.entities
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _unescape_html(text: str) -> str:
    def fixup(m: re.Match) -> str:
        s = m.group(0)
        if s.startswith("&#"):
            try:
                if s.startswith("&#x"):
                    return chr(int(s[2:-1], 16))
                return chr(int(s[2:-1]))
            except ValueError:
                return s
        entity = html.entities.entitydefs.get(s[1:-1], s)
        return entity
    return re.sub(r"&[^;]+;", fixup, text)


def _strip_tags(html_text: str) -> str:
    clean = re.sub(r"<[^>]+>", "", html_text)
    return _unescape_html(clean)


def _extract_links(html_text: str) -> List[Dict[str, str]]:
    pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]*)</a>', re.IGNORECASE)
    return [{"url": match.group(1), "text": match.group(2).strip()} for match in pattern.finditer(html_text)]


def _extract_images(html_text: str) -> List[str]:
    pattern = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
    return [m.group(1) for m in pattern.finditer(html_text)]


def _extract_meta(html_text: str) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    title = re.search(r"<title>([^<]+)</title>", html_text, re.IGNORECASE)
    if title:
        meta["title"] = title.group(1).strip()
    desc = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
    if desc:
        meta["description"] = desc.group(1)
    keywords = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
    if keywords:
        meta["keywords"] = keywords.group(1)
    og_title = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
    if og_title:
        meta["og_title"] = og_title.group(1)
    return meta


def _sanitize_html(html_text: str, allowed_tags: Optional[List[str]] = None) -> str:
    allowed = allowed_tags or ["p", "br", "b", "i", "em", "strong", "a", "ul", "ol", "li"]
    allowed_pattern = "|".join(allowed)
    clean = re.sub(r"<!DOCTYPE[^>]*>", "", html_text, flags=re.IGNORECASE)
    clean = re.sub(r"<!--.*?-->", "", clean, flags=re.DOTALL)
    clean = re.sub(r"<script[^>]*>.*?</script>", "", clean, flags=re.IGNORECASE | re.DOTALL)
    clean = re.sub(r"<style[^>]*>.*?</style>", "", clean, flags=re.IGNORECASE | re.DOTALL)
    clean = re.sub(r"<[^>]+>", lambda m: m.group(0) if re.match(rf"</?({allowed_pattern})\b", m.group(0), re.IGNORECASE) else "", clean)
    return clean.strip()


def _build_table(data: List[Dict[str, Any]], headers: Optional[List[str]] = None) -> str:
    if not data:
        return "<table></table>"
    cols = headers or list(data[0].keys())
    rows = ["<table>", "<thead>", "<tr>"]
    for h in cols:
        rows.append(f"<th>{h}</th>")
    rows.extend(["</tr>", "</thead>", "<tbody>"])
    for row in data:
        rows.append("<tr>")
        for c in cols:
            rows.append(f"<td>{row.get(c, '')}</td>")
        rows.append("</tr>")
    rows.extend(["</tbody>", "</table>"])
    return "\n".join(rows)


class HTMLAction(BaseAction):
    """HTML parsing and manipulation operations.

    Provides parsing, tag stripping, link/image extraction, sanitization, table building.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "strip_tags")
        html_text = params.get("html", "")
        url = params.get("url", "")

        try:
            if operation == "strip_tags":
                result = _strip_tags(html_text)
                return {"success": True, "text": result}

            elif operation == "unescape":
                result = _unescape_html(html_text)
                return {"success": True, "text": result}

            elif operation == "extract_links":
                links = _extract_links(html_text)
                return {"success": True, "links": links, "count": len(links)}

            elif operation == "extract_images":
                images = _extract_images(html_text)
                return {"success": True, "images": images, "count": len(images)}

            elif operation == "extract_meta":
                meta = _extract_meta(html_text)
                return {"success": True, "meta": meta}

            elif operation == "sanitize":
                allowed = params.get("allowed_tags")
                result = _sanitize_html(html_text, allowed)
                return {"success": True, "sanitized": result}

            elif operation == "build_table":
                data = params.get("data", [])
                headers = params.get("headers")
                table = _build_table(data, headers)
                return {"success": True, "table": table}

            elif operation == "build_list":
                items = params.get("items", [])
                ordered = params.get("ordered", False)
                tag = "ol" if ordered else "ul"
                rows = [f"<{tag}>"]
                for item in items:
                    rows.append(f"<li>{item}</li>")
                rows.append(f"</{tag}>")
                return {"success": True, "html": "\n".join(rows)}

            elif operation == "build_link":
                href = params.get("href", "")
                text = params.get("text", href)
                return {"success": True, "html": f'<a href="{href}">{text}</a>'}

            elif operation == "build_image":
                src = params.get("src", "")
                alt = params.get("alt", "")
                return {"success": True, "html": f'<img src="{src}" alt="{alt}">'}

            elif operation == "word_count":
                text = _strip_tags(html_text)
                words = len(text.split())
                chars = len(text)
                return {"success": True, "words": words, "characters": chars}

            elif operation == "truncate":
                text = _strip_tags(html_text)
                length = int(params.get("length", 100))
                suffix = params.get("suffix", "...")
                if len(text) <= length:
                    return {"success": True, "text": text}
                truncated = text[:length].rsplit(" ", 1)[0]
                return {"success": True, "text": truncated + suffix}

            elif operation == "highlight":
                text = _strip_tags(html_text)
                term = params.get("term", "")
                if not term:
                    return {"success": False, "error": "term required"}
                pattern = re.compile(f"({re.escape(term)})", re.IGNORECASE)
                highlighted = pattern.sub(r"<mark>\\1</mark>", text)
                return {"success": True, "highlighted": highlighted}

            elif operation == "autolink":
                text = params.get("text", html_text)
                urls = re.findall(r"https?://\S+", text)
                result = text
                for url in urls:
                    result = result.replace(url, f'<a href="{url}">{url}</a>')
                return {"success": True, "html": result, "links_found": len(urls)}

            elif operation == "minify":
                minified = re.sub(r"\s+", " ", html_text)
                minified = re.sub(r">\s+<", "><", minified)
                return {"success": True, "minified": minified.strip()}

            elif operation == "extract_by_selector":
                tag = params.get("tag", "div")
                class_name = params.get("class")
                id_name = params.get("id")
                pattern = tag
                if id_name:
                    pattern = f"{tag}#${id_name}"
                elif class_name:
                    pattern = f'{tag}.{class_name.replace(" ", ".")}'
                regex = f"<{pattern}[^>]*>(.*?)</{tag}>"
                matches = re.findall(regex, html_text, re.DOTALL | re.IGNORECASE)
                return {"success": True, "matches": [_strip_tags(m).strip() for m in matches], "count": len(matches)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"HTMLAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for HTML operations."""
    return HTMLAction().execute(context, params)
