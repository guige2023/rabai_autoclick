"""API client action module for RabAI AutoClick.

Provides API operations:
- RestApiCallAction: Generic REST API calls
- GraphQLQueryAction: GraphQL queries
- OAuthTokenAction: OAuth token management
- ApiPaginationAction: Paginate through API results
- ApiRateLimitAction: Handle rate limiting
- WebhookTriggerAction: Trigger webhooks
"""

import hashlib
import hmac
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RestApiCallAction(BaseAction):
    """Generic REST API call."""
    action_type = "rest_api_call"
    display_name = "REST API调用"
    description = "通用REST API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body", None)
            auth = params.get("auth", None)
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="url is required")

            if auth:
                auth_type = auth.get("type", "bearer")
                if auth_type == "bearer":
                    headers["Authorization"] = f"Bearer {auth.get('token', '')}"
                elif auth_type == "basic":
                    import base64
                    credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                    encoded = base64.b64encode(credentials.encode()).decode()
                    headers["Authorization"] = f"Basic {encoded}"
                elif auth_type == "api_key":
                    key_name = auth.get("key_name", "X-API-Key")
                    headers[key_name] = auth.get("key", "")

            req = urllib.request.Request(url, method=method)

            for key, value in headers.items():
                if key.lower() not in ("content-length", "content-type") or method != "GET":
                    req.add_header(key, value)

            if body:
                if isinstance(body, dict):
                    body = json.dumps(body).encode("utf-8")
                    req.add_header("Content-Type", "application/json")
                elif isinstance(body, str):
                    body = body.encode("utf-8")
                req.data = body

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
                        message=f"{method} request successful",
                        data={"status_code": response.status, "content": data}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                try:
                    error_data = json.loads(body)
                except:
                    error_data = body
                return ActionResult(
                    success=False,
                    message=f"HTTP {e.code}: {e.reason}",
                    data={"status_code": e.code, "body": error_data}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class GraphQLQueryAction(BaseAction):
    """Execute GraphQL queries."""
    action_type = "graphql_query"
    display_name = "GraphQL查询"
    description = "执行GraphQL查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            query = params.get("query", "")
            variables = params.get("variables", None)
            headers = params.get("headers", {})
            operation_name = params.get("operation_name", None)
            timeout = params.get("timeout", 30)

            if not endpoint or not query:
                return ActionResult(success=False, message="endpoint and query are required")

            headers["Content-Type"] = "application/json"

            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            if operation_name:
                payload["operationName"] = operation_name

            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(endpoint, data=body, method="POST")
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = json.loads(response.read().decode("utf-8"))

                    if "errors" in content:
                        return ActionResult(
                            success=False,
                            message=f"GraphQL errors: {content['errors']}",
                            data={"errors": content["errors"]}
                        )

                    return ActionResult(
                        success=True,
                        message="GraphQL query successful",
                        data={"data": content.get("data")}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Query failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class OAuthTokenAction(BaseAction):
    """OAuth token management."""
    action_type = "oauth_token"
    display_name = "OAuth令牌"
    description = "OAuth令牌管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_type = params.get("action", "get")
            token_url = params.get("token_url", "")
            client_id = params.get("client_id", "")
            client_secret = params.get("client_secret", "")
            grant_type = params.get("grant_type", "client_credentials")
            refresh_token = params.get("refresh_token", "")
            scope = params.get("scope", "")

            if action_type == "get":
                if not token_url or not client_id:
                    return ActionResult(success=False, message="token_url and client_id required")

                data = {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": grant_type
                }

                if grant_type == "refresh_token" and refresh_token:
                    data["refresh_token"] = refresh_token
                elif scope:
                    data["scope"] = scope

                body = urllib.parse.urlencode(data).encode("utf-8")
                req = urllib.request.Request(token_url, data=body, method="POST")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = json.loads(response.read().decode("utf-8"))

                        return ActionResult(
                            success=True,
                            message="Token obtained",
                            data={
                                "access_token": content.get("access_token"),
                                "refresh_token": content.get("refresh_token"),
                                "expires_in": content.get("expires_in"),
                                "token_type": content.get("token_type", "Bearer")
                            }
                        )
                except urllib.error.HTTPError as e:
                    return ActionResult(success=False, message=f"Token error: {e.code}")
                except Exception as e:
                    return ActionResult(success=False, message=f"Token failed: {str(e)}")

            elif action_type == "refresh":
                if not token_url or not refresh_token:
                    return ActionResult(success=False, message="token_url and refresh_token required")

                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret
                }

                body = urllib.parse.urlencode(data).encode("utf-8")
                req = urllib.request.Request(token_url, data=body, method="POST")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = json.loads(response.read().decode("utf-8"))
                        return ActionResult(
                            success=True,
                            message="Token refreshed",
                            data={"access_token": content.get("access_token")}
                        )
                except Exception as e:
                    return ActionResult(success=False, message=f"Refresh failed: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ApiPaginationAction(BaseAction):
    """Paginate through API results."""
    action_type = "api_pagination"
    display_name = "API分页"
    description = "分页获取API结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("url", "")
            method = params.get("method", "GET")
            params_list = params.get("params", [])
            page_param = params.get("page_param", "page")
            limit_param = params.get("limit_param", "limit")
            max_pages = params.get("max_pages", 10)
            total_field = params.get("total_field", "total")
            data_field = params.get("data_field", "data")
            headers = params.get("headers", {})

            if not base_url:
                return ActionResult(success=False, message="url is required")

            all_results = []
            page = 1
            total = None

            while page <= max_pages:
                url = base_url
                if method == "GET":
                    separator = "?" if "?" not in base_url else "&"
                    url = f"{base_url}{separator}{page_param}={page}&{limit_param}={params_list.get(limit_param, 100)}"

                req = urllib.request.Request(url, method=method)
                for key, value in headers.items():
                    req.add_header(key, value)

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = json.loads(response.read().decode("utf-8"))

                        if isinstance(content, dict):
                            if data_field and data_field in content:
                                items = content[data_field]
                            else:
                                items = [v for v in content.values() if isinstance(v, list)]
                                items = items[0] if items else []

                            if total is None and total_field in content:
                                total = content[total_field]

                            all_results.extend(items if isinstance(items, list) else [items])
                        elif isinstance(content, list):
                            all_results.extend(content)

                    if total and len(all_results) >= total:
                        break

                    page += 1

                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        break
                    return ActionResult(success=False, message=f"HTTP {e.code}")
                except Exception as e:
                    return ActionResult(success=False, message=f"Pagination error: {str(e)}")

            return ActionResult(
                success=True,
                message=f"Fetched {len(all_results)} results across {page} pages",
                data={"results": all_results, "total_fetched": len(all_results), "pages_fetched": page}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ApiRateLimitAction(BaseAction):
    """Handle API rate limiting."""
    action_type = "api_rate_limit"
    display_name = "API限流处理"
    description = "处理API速率限制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_type = params.get("action", "check")
            requests_per_window = params.get("requests_per_window", 60)
            window_seconds = params.get("window_seconds", 60)

            if action_type == "check":
                current_time = time.time()
                window_key = int(current_time / window_seconds)

                request_counts = getattr(self, "_rate_limit_counts", {})

                current_count = request_counts.get(window_key, 0)

                if current_count >= requests_per_window:
                    retry_after = window_seconds - (current_time % window_seconds)
                    return ActionResult(
                        success=False,
                        message=f"Rate limit exceeded",
                        data={
                            "allowed": False,
                            "current_count": current_count,
                            "limit": requests_per_window,
                            "retry_after": retry_after
                        }
                    )
                else:
                    request_counts[window_key] = current_count + 1
                    self._rate_limit_counts = request_counts
                    return ActionResult(
                        success=True,
                        message="Request allowed",
                        data={
                            "allowed": True,
                            "current_count": current_count + 1,
                            "remaining": requests_per_window - current_count - 1
                        }
                    )

            elif action_type == "wait":
                wait_time = params.get("wait_time", 1)
                time.sleep(wait_time)
                return ActionResult(success=True, message=f"Waited {wait_time}s")

            elif action_type == "reset":
                self._rate_limit_counts = {}
                return ActionResult(success=True, message="Rate limit reset")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class WebhookTriggerAction(BaseAction):
    """Trigger webhooks."""
    action_type = "webhook_trigger"
    display_name = "触发Webhook"
    description = "触发Webhook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "POST").upper()
            payload = params.get("payload", {})
            headers = params.get("headers", {})
            secret = params.get("secret", "")

            if not url:
                return ActionResult(success=False, message="url is required")

            body = json.dumps(payload).encode("utf-8")

            if secret:
                import base64
                import hmac
                signature = hmac.new(secret.encode(), body, hashlib.sha256).digest()
                headers["X-Signature-256"] = "sha256=" + base64.b64encode(signature).decode()

            req = urllib.request.Request(url, data=body, method=method)
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=15) as response:
                    content = response.read().decode("utf-8", errors="replace")
                    return ActionResult(
                        success=True,
                        message="Webhook triggered",
                        data={"status_code": response.status, "response": content}
                    )
            except urllib.error.HTTPError as e:
                return ActionResult(
                    success=False,
                    message=f"Webhook failed: {e.code}",
                    data={"status_code": e.code}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Webhook error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
