"""
HTTP Request Action Module.

Provides HTTP request handling with methods: GET, POST, PUT, DELETE, PATCH.
Supports headers, query params, body, timeout, and redirect handling.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class HTTPRequestResult:
    """Result of an HTTP request."""
    status_code: int
    headers: dict[str, str]
    body: Any
    elapsed_ms: float
    error: Optional[str] = None


class HTTPRequestAction(BaseAction):
    """Perform HTTP requests with configurable method, headers, and body."""

    def __init__(self) -> None:
        super().__init__("http_request")

    def execute(self, context: dict, params: dict) -> HTTPRequestResult:
        """
        Execute HTTP request.

        Args:
            context: Execution context (may contain auth tokens)
            params: Request parameters:
                - method: HTTP method (GET, POST, PUT, DELETE, PATCH)
                - url: Target URL
                - headers: Optional request headers
                - params: Optional query parameters
                - body: Optional request body
                - timeout: Request timeout in seconds (default: 30)
                - allow_redirects: Whether to follow redirects (default: True)

        Returns:
            HTTPRequestResult with status, headers, body, and elapsed time
        """
        import time
        import json
        import urllib.request
        import urllib.parse
        import urllib.error

        method = params.get("method", "GET").upper()
        url = params.get("url", "")
        headers = params.get("headers", {})
        query_params = params.get("params", {})
        body = params.get("body")
        timeout = params.get("timeout", 30)
        allow_redirects = params.get("allow_redirects", True)

        if not url:
            return HTTPRequestResult(
                status_code=0,
                headers={},
                body=None,
                elapsed_ms=0,
                error="URL is required"
            )

        if query_params:
            url += "?" + urllib.parse.urlencode(query_params)

        start_time = time.time()
        try:
            req = urllib.request.Request(url, method=method, headers=headers)
            if body is not None:
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode("utf-8")
                    req.add_header("Content-Type", "application/json")
                elif isinstance(body, str):
                    body = body.encode("utf-8")
                req.data = body

            class RedirectHandler(urllib.request.HTTPRedirectHandler):
                def __init__(self2):
                    pass
                def redirect_request(self2, req, fp, code, msg, headers, newurl):
                    if not allow_redirects:
                        return None
                    return urllib.request.Request(newurl, headers=req.headers, origin_req_host=req.origin_req_host)

            opener = urllib.request.build_opener(RedirectHandler())
            with opener.open(req, timeout=timeout) as response:
                response_body = response.read()
                content_type = response.headers.get("Content-Type", "")
                result_body = response_body.decode("utf-8")
                if "application/json" in content_type:
                    result_body = json.loads(result_body)

                elapsed_ms = (time.time() - start_time) * 1000
                return HTTPRequestResult(
                    status_code=response.status,
                    headers=dict(response.headers),
                    body=result_body,
                    elapsed_ms=elapsed_ms
                )
        except urllib.error.HTTPError as e:
            elapsed_ms = (time.time() - start_time) * 1000
            try:
                error_body = json.loads(e.read().decode("utf-8"))
            except Exception:
                error_body = e.read().decode("utf-8")
            return HTTPRequestResult(
                status_code=e.code,
                headers={},
                body=error_body,
                elapsed_ms=elapsed_ms,
                error=f"HTTP Error {e.code}"
            )
        except urllib.error.URLError as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return HTTPRequestResult(
                status_code=0,
                headers={},
                body=None,
                elapsed_ms=elapsed_ms,
                error=f"URL Error: {e.reason}"
            )
        except TimeoutError:
            elapsed_ms = (time.time() - start_time) * 1000
            return HTTPRequestResult(
                status_code=0,
                headers={},
                body=None,
                elapsed_ms=elapsed_ms,
                error="Request timed out"
            )
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            return HTTPRequestResult(
                status_code=0,
                headers={},
                body=None,
                elapsed_ms=elapsed_ms,
                error=f"Unexpected error: {str(e)}"
            )
