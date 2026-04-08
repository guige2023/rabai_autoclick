"""
URL utilities - parsing, building, encoding, validation, query parameter manipulation.
"""
from typing import Any, Dict, List, Optional
import urllib.parse as urlparse
import re
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_query(qs: str) -> Dict[str, str]:
    return dict(urlparse.parse_qsl(qs, keep_blank_values=True))


def _build_query(params: Dict[str, Any]) -> str:
    return urlparse.urlencode(params, doseq=True)


class URLAction(BaseAction):
    """URL operations.

    Provides parsing, building, query manipulation, encoding, validation.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse")
        url = params.get("url", "")
        text = params.get("text", "")

        try:
            if operation == "parse":
                if not url:
                    return {"success": False, "error": "url required"}
                parsed = urlparse.urlparse(url)
                return {
                    "success": True,
                    "scheme": parsed.scheme,
                    "netloc": parsed.netloc,
                    "hostname": parsed.hostname or "",
                    "port": parsed.port,
                    "path": parsed.path,
                    "params": parsed.params,
                    "query": dict(urlparse.parse_qsl(parsed.query)),
                    "fragment": parsed.fragment,
                    "username": parsed.username or "",
                    "domain": parsed.netloc.split(":")[0] if parsed.netloc else "",
                }

            elif operation == "build":
                scheme = params.get("scheme", "https")
                host = params.get("host", "")
                path = params.get("path", "")
                query_params = params.get("query", {})
                fragment = params.get("fragment", "")
                if not host:
                    return {"success": False, "error": "host required"}
                port = params.get("port")
                netloc = host if not port else f"{host}:{port}"
                query = _build_query(query_params) if query_params else ""
                result = urlparse.urlunparse((scheme, netloc, path, "", query, fragment))
                return {"success": True, "url": result}

            elif operation == "encode":
                if not text:
                    return {"success": False, "error": "text required"}
                encoded = urlparse.quote(text)
                return {"success": True, "encoded": encoded}

            elif operation == "decode":
                if not text:
                    return {"success": False, "error": "text required"}
                decoded = urlparse.unquote(text)
                return {"success": True, "decoded": decoded}

            elif operation == "add_query":
                if not url:
                    return {"success": False, "error": "url required"}
                key = params.get("key", "")
                value = params.get("value", "")
                parsed = urlparse.urlparse(url)
                query = dict(urlparse.parse_qsl(parsed.query))
                query[key] = value
                new_query = _build_query(query)
                reconstructed = urlparse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                return {"success": True, "url": reconstructed}

            elif operation == "remove_query":
                if not url:
                    return {"success": False, "error": "url required"}
                key = params.get("key", "")
                parsed = urlparse.urlparse(url)
                query = dict(urlparse.parse_qsl(parsed.query))
                removed = query.pop(key, None)
                new_query = _build_query(query)
                reconstructed = urlparse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                return {"success": True, "url": reconstructed, "removed": removed is not None}

            elif operation == "get_query":
                if not url:
                    return {"success": False, "error": "url required"}
                parsed = urlparse.urlparse(url)
                query = dict(urlparse.parse_qsl(parsed.query))
                key = params.get("key", "")
                if key:
                    value = query.get(key)
                    return {"success": True, "key": key, "value": value, "found": key in query}
                return {"success": True, "query": query}

            elif operation == "validate":
                if not url:
                    return {"success": False, "error": "url required"}
                pattern = re.compile(
                    r"^https?://"
                    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
                    r"localhost|"
                    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
                    r"(?::\d+)?"
                    r"(?:/?|[/?]\S+)$", re.IGNORECASE)
                valid = bool(pattern.match(url))
                return {"success": True, "valid": valid, "url": url}

            elif operation == "extract_domain":
                if not url:
                    return {"success": False, "error": "url required"}
                parsed = urlparse.urlparse(url)
                domain = parsed.netloc.split(":")[0]
                return {"success": True, "domain": domain, "url": url}

            elif operation == "is_absolute":
                if not url:
                    return {"success": False, "error": "url required"}
                parsed = urlparse.urlparse(url)
                return {"success": True, "absolute": bool(parsed.scheme and parsed.netloc), "url": url}

            elif operation == "join":
                base = url or params.get("base", "")
                if not base:
                    return {"success": False, "error": "base url required"}
                rel = params.get("relative", "")
                joined = urlparse.urljoin(base, rel)
                return {"success": True, "url": joined, "base": base, "relative": rel}

            elif operation == "path_segments":
                if not url:
                    return {"success": False, "error": "url required"}
                parsed = urlparse.urlparse(url)
                segments = [s for s in parsed.path.split("/") if s]
                return {"success": True, "segments": segments, "count": len(segments), "path": parsed.path}

            elif operation == "encode_component":
                if not text:
                    return {"success": False, "error": "text required"}
                safe = params.get("safe", "")
                encoded = urlparse.quote(text, safe=safe)
                return {"success": True, "encoded": encoded}

            elif operation == "decode_component":
                if not text:
                    return {"success": False, "error": "text required"}
                decoded = urlparse.unquote(text)
                return {"success": True, "decoded": decoded}

            elif operation == "urlencode":
                params_dict = params.get("params", {})
                doseq = params.get("doseq", True)
                encoded = urlparse.urlencode(params_dict, doseq=doseq)
                return {"success": True, "encoded": encoded}

            elif operation == "parse_qs":
                if not text:
                    return {"success": False, "error": "query string required"}
                parsed = urlparse.parse_qs(text, keep_blank_values=True)
                return {"success": True, "params": {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"URLAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for URL operations."""
    return URLAction().execute(context, params)
