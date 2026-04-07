"""HTTP client action module for RabAI AutoClick.

Provides HTTP client operations:
- HttpClientGetAction: HTTP GET with query params
- HttpClientPostAction: HTTP POST with body
- HttpClientHeadersAction: Custom headers handling
- HttpClientAuthAction: Authentication handling
- HttpClientRedirectAction: Redirect handling
- HttpClientTimeoutAction: Timeout handling
"""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
import base64
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HttpClientGetAction(BaseAction):
    """HTTP GET request."""
    action_type = "http_client_get"
    display_name = "HTTP GET客户端"
    description = "HTTP GET请求客户端"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            query_params = params.get("params", {})
            headers = params.get("headers", {})
            follow_redirects = params.get("follow_redirects", True)
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="url is required")

            if query_params:
                encoded = urllib.parse.urlencode(query_params)
                separator = "&" if "?" in url else "?"
                url = url + separator + encoded

            req = urllib.request.Request(url)
            req.add_header("User-Agent", "RabAIAutoClick/1.0")
            for k, v in headers.items():
                req.add_header(k, v)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read()
                    content_type = response.headers.get("Content-Type", "")

                    if "application/json" in content_type:
                        data = json.loads(content.decode("utf-8"))
                    else:
                        data = content.decode("utf-8", errors="replace")

                    return ActionResult(
                        success=True,
                        message=f"GET {url} -> {response.status}",
                        data={
                            "status": response.status,
                            "content": data,
                            "headers": dict(response.headers),
                            "url": response.url
                        }
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(
                    success=False,
                    message=f"HTTP {e.code}: {e.reason}",
                    data={"status": e.code, "body": body}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"GET error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class HttpClientPostAction(BaseAction):
    """HTTP POST request."""
    action_type = "http_client_post"
    display_name = "HTTP POST客户端"
    description = "HTTP POST请求客户端"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            body = params.get("body", {})
            json_body = params.get("json")
            headers = params.get("headers", {})
            content_type = params.get("content_type", "json")
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="url is required")

            if json_body is not None:
                body_data = json.dumps(json_body).encode("utf-8")
                req_content_type = "application/json"
            elif isinstance(body, dict):
                body_data = json.dumps(body).encode("utf-8")
                req_content_type = "application/json"
            elif isinstance(body, str):
                body_data = body.encode("utf-8")
                req_content_type = "text/plain"
            else:
                body_data = b""
                req_content_type = "application/octet-stream"

            req = urllib.request.Request(url, data=body_data, method="POST")
            req.add_header("User-Agent", "RabAIAutoClick/1.0")
            req.add_header("Content-Type", req_content_type)
            for k, v in headers.items():
                req.add_header(k, v)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read()
                    content_type_header = response.headers.get("Content-Type", "")

                    if "application/json" in content_type_header:
                        data = json.loads(content.decode("utf-8"))
                    else:
                        data = content.decode("utf-8", errors="replace")

                    return ActionResult(
                        success=True,
                        message=f"POST {url} -> {response.status}",
                        data={"status": response.status, "content": data}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(
                    success=False,
                    message=f"HTTP {e.code}: {e.reason}",
                    data={"status": e.code, "body": body}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"POST error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class HttpClientHeadersAction(BaseAction):
    """Custom headers handling."""
    action_type = "http_client_headers"
    display_name = "HTTP Headers处理"
    description = "HTTP自定义Headers处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            headers = params.get("headers", {})
            method = params.get("method", "GET").upper()

            if not url:
                return ActionResult(success=False, message="url is required")

            req = urllib.request.Request(url, method=method)
            for k, v in headers.items():
                req.add_header(k, v)

            return ActionResult(
                success=True,
                message=f"Prepared {method} request with {len(headers)} headers",
                data={"headers": dict(req.headers), "method": method, "url": url}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Headers error: {str(e)}")


class HttpClientAuthAction(BaseAction):
    """Authentication handling."""
    action_type = "http_client_auth"
    display_name = "HTTP认证"
    description = "HTTP认证处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            auth_type = params.get("auth_type", "bearer")
            username = params.get("username", "")
            password = params.get("password", "")
            token = params.get("token", "")
            api_key = params.get("api_key", "")
            api_key_header = params.get("api_key_header", "X-API-Key")

            if not url:
                return ActionResult(success=False, message="url is required")

            req = urllib.request.Request(url)

            if auth_type == "bearer" and token:
                req.add_header("Authorization", f"Bearer {token}")

            elif auth_type == "basic" and username:
                credentials = f"{username}:{password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                req.add_header("Authorization", f"Basic {encoded}")

            elif auth_type == "api_key" and api_key:
                req.add_header(api_key_header, api_key)

            elif auth_type == "digest":
                pass

            headers = dict(req.headers)

            return ActionResult(
                success=True,
                message=f"Added {auth_type} auth to request",
                data={"auth_type": auth_type, "has_auth": True, "headers": headers}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")


class HttpClientRedirectAction(BaseAction):
    """Redirect handling."""
    action_type = "http_client_redirect"
    display_name = "HTTP重定向"
    description = "HTTP重定向处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            max_redirects = params.get("max_redirects", 5)
            follow_redirects = params.get("follow_redirects", True)
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="url is required")

            redirect_chain = [url]
            current_url = url

            class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None

            if not follow_redirects:
                opener = urllib.request.build_opener(NoRedirectHandler)
            else:
                opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)

            req = opener.request = urllib.request.Request(url)
            req.add_header("User-Agent", "RabAIAutoClick/1.0")

            try:
                response = opener.open(req, timeout=timeout)
                final_url = response.url
                status = response.status
                response.close()

                return ActionResult(
                    success=True,
                    message=f"Request to {url} -> {final_url} ({status})",
                    data={
                        "final_url": final_url,
                        "status": status,
                        "redirect_count": len(redirect_chain) - 1,
                        "redirect_chain": redirect_chain
                    }
                )

            except urllib.error.HTTPError as e:
                return ActionResult(
                    success=False,
                    message=f"HTTP {e.code}: {e.reason}",
                    data={"status": e.code, "redirect_chain": redirect_chain}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Redirect error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class HttpClientTimeoutAction(BaseAction):
    """Timeout handling."""
    action_type = "http_client_timeout"
    display_name = "HTTP超时处理"
    description = "HTTP超时处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            connect_timeout = params.get("connect_timeout", 5)
            read_timeout = params.get("read_timeout", 30)
            retry_count = params.get("retry_count", 0)
            retry_delay = params.get("retry_delay", 1)

            if not url:
                return ActionResult(success=False, message="url is required")

            timeout = (connect_timeout, read_timeout)
            last_error = None

            for attempt in range(retry_count + 1):
                try:
                    req = urllib.request.Request(url)
                    req.add_header("User-Agent", "RabAIAutoClick/1.0")

                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        content = response.read()
                        return ActionResult(
                            success=True,
                            message=f"Request succeeded on attempt {attempt + 1}",
                            data={
                                "status": response.status,
                                "content_length": len(content),
                                "attempt": attempt + 1
                            }
                        )

                except urllib.error.HTTPError as e:
                    last_error = f"HTTP {e.code}: {e.reason}"
                    if e.code < 500:
                        return ActionResult(
                            success=False,
                            message=f"Request failed with {last_error}",
                            data={"status": e.code, "attempt": attempt + 1}
                        )

                except Exception as e:
                    last_error = str(e)

                if attempt < retry_count:
                    time.sleep(retry_delay * (attempt + 1))

            return ActionResult(
                success=False,
                message=f"Request failed after {retry_count + 1} attempts: {last_error}",
                data={"attempts": retry_count + 1, "last_error": last_error}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout error: {str(e)}")
