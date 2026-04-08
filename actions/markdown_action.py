"""
Markdown utilities - parsing, rendering, table conversion, link extraction, document generation.
"""
from typing import Any, Dict, List, Optional
import re
import logging
import html

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_headings(text: str) -> List[Dict[str, Any]]:
    pattern = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
    results = []
    for match in pattern.finditer(text):
        results.append({"level": len(match.group(1)), "text": match.group(2).strip(), "line": text[:match.start()].count("\n") + 1})
    return results


def _parse_code_blocks(text: str) -> List[Dict[str, str]]:
    pattern = re.compile(r"```(\w*)\n(.*?)```", re.DOTALL)
    results = []
    for match in pattern.finditer(text):
        results.append({"language": match.group(1), "code": match.group(2)})
    return results


def _extract_links(text: str) -> List[Dict[str, str]]:
    pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
    return [{"text": m.group(1), "url": m.group(2)} for m in pattern.finditer(text)]


def _extract_images(text: str) -> List[Dict[str, str]]:
    pattern = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
    return [{"alt": m.group(1), "url": m.group(2)} for m in pattern.finditer(text)]


def _markdown_to_html(text: str) -> str:
    html_out = text
    html_out = re.sub(r"^######\s+(.+)$", r"<h6>\1</h6>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^#####\s+(.+)$", r"<h5>\1</h5>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^####\s+(.+)$", r"<h4>\1</h4>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^###\s+(.+)$", r"<h3>\1</h3>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^##\s+(.+)$", r"<h2>\1</h2>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^#\s+(.+)$", r"<h1>\1</h1>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html_out)
    html_out = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html_out)
    html_out = re.sub(r"~~(.+?)~~", r"<del>\1</del>", html_out)
    html_out = re.sub(r"`([^`]+)`", r"<code>\1</code>", html_out)
    html_out = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', html_out)
    html_out = re.sub(r"^>\s+(.+)$", r"<blockquote>\1</blockquote>", html_out, flags=re.MULTILINE)
    html_out = re.sub(r"^- ", r"<li>", html_out)
    html_out = re.sub(r"(?<!<li>)\n", r"\n", html_out)
    return html_out


def _table_to_markdown(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def _parse_table(text: str) -> Optional[Dict[str, Any]]:
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    if not lines:
        return None
    rows = []
    for line in lines:
        cells = [c.strip() for c in re.split(r"\|", line)]
        cells = [c for c in cells if c and c != "---"]
        if cells:
            rows.append(cells)
    if not rows:
        return None
    return {"headers": rows[0], "rows": rows[1:] if len(rows) > 1 else []}


def _count_words(text: str) -> Dict[str, int]:
    clean = re.sub(r"[#*`~\[\]()>-]", "", text)
    words = clean.split()
    return {"words": len(words), "chars": len(clean), "lines": len(text.split("\n"))}


class MarkdownAction(BaseAction):
    """Markdown operations.

    Provides parsing, rendering, table conversion, link/image extraction.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "render")
        text = params.get("text", "")

        try:
            if operation == "render":
                html_out = _markdown_to_html(text)
                return {"success": True, "html": html_out}

            elif operation == "parse_headings":
                headings = _parse_headings(text)
                return {"success": True, "headings": headings, "count": len(headings)}

            elif operation == "parse_code_blocks":
                blocks = _parse_code_blocks(text)
                return {"success": True, "blocks": blocks, "count": len(blocks)}

            elif operation == "extract_links":
                links = _extract_links(text)
                return {"success": True, "links": links, "count": len(links)}

            elif operation == "extract_images":
                images = _extract_images(text)
                return {"success": True, "images": images, "count": len(images)}

            elif operation == "parse_table":
                result = _parse_table(text)
                if result:
                    return {"success": True, **result}
                return {"success": False, "error": "Could not parse table"}

            elif operation == "build_table":
                headers = params.get("headers", [])
                rows = params.get("rows", [])
                table = _table_to_markdown(headers, rows)
                return {"success": True, "table": table}

            elif operation == "extract_toc":
                headings = _parse_headings(text)
                toc = []
                for h in headings:
                    indent = "  " * (h["level"] - 1)
                    toc.append(f"{indent}- [{h['text']}](#{h['text'].lower().replace(' ', '-')})")
                return {"success": True, "toc": "\n".join(toc), "count": len(toc)}

            elif operation == "word_count":
                counts = _count_words(text)
                return {"success": True, **counts}

            elif operation == "strip_markdown":
                stripped = re.sub(r"[#*`~\[\]()>-_]", "", text)
                stripped = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", stripped)
                stripped = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", stripped)
                return {"success": True, "text": stripped}

            elif operation == "blockquote":
                content = params.get("content", text)
                cite = params.get("cite", "")
                lines = [f"> {line}" for line in content.split("\n")]
                if cite:
                    lines.append(f"\n> — *{cite}*")
                return {"success": True, "blockquote": "\n".join(lines)}

            elif operation == "build_link":
                text_val = params.get("text", "")
                url = params.get("url", "")
                return {"success": True, "markdown": f"[{text_val}]({url})"}

            elif operation == "build_image":
                alt = params.get("alt", "")
                url = params.get("url", "")
                return {"success": True, "markdown": f"![{alt}]({url})"}

            elif operation == "build_task_list":
                items = params.get("items", [])
                checked = params.get("checked", [])
                lines = []
                for i, item in enumerate(items):
                    checked_mark = "x" if i in checked else " "
                    lines.append(f"- [{checked_mark}] {item}")
                return {"success": True, "markdown": "\n".join(lines)}

            elif operation == "escape_html":
                escaped = html.escape(text)
                return {"success": True, "escaped": escaped}

            elif operation == "unescape_html":
                unescaped = html.unescape(text)
                return {"success": True, "unescaped": unescaped}

            elif operation == "summary":
                headings = _parse_headings(text)
                word_counts = _count_words(text)
                links = _extract_links(text)
                images = _extract_images(text)
                return {
                    "success": True,
                    "headings": len(headings),
                    "words": word_counts["words"],
                    "chars": word_counts["chars"],
                    "lines": word_counts["lines"],
                    "links": len(links),
                    "images": len(images),
                }

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"MarkdownAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for markdown operations."""
    return MarkdownAction().execute(context, params)
