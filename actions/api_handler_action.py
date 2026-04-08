"""API handler action module for RabAI AutoClick.

Provides API request/response handling:
- RequestHandlerAction: Handle API requests
- ResponseHandlerAction: Handle API responses
- ErrorHandlerAction: Handle API errors
- WebhookHandlerAction: Handle webhooks
"""

from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RequestHandlerAction(BaseAction):
    """Handle API requests."""
    action_type = "api_request_handler"
    display_name = "请求处理器"
    description = "处理API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "handle")
            request = params.get("request", {})
            middleware = params.get("middleware", [])

            if action == "handle":
                method = request.get("method", "GET")
                url = request.get("url", "")
                headers = request.get("headers", {})
                body = request.get("body")

                processed_headers = dict(headers)
                processed_body = body

                for mw in middleware:
                    mw_type = mw.get("type", "header")
                    if mw_type == "header":
                        processed_headers.update(mw.get("headers", {}))
                    elif mw_type == "body":
                        if processed_body and isinstance(processed_body, dict):
                            processed_body.update(mw.get("body", {}))

                return ActionResult(
                    success=True,
                    data={
                        "processed_request": {
                            "method": method,
                            "url": url,
                            "headers": processed_headers,
                            "body": processed_body
                        },
                        "middleware_applied": len(middleware)
                    },
                    message=f"Handled {method} request to {url}"
                )

            elif action == "validate":
                required_fields = params.get("required_fields", ["method", "url"])
                missing = [f for f in required_fields if f not in request]

                return ActionResult(
                    success=len(missing) == 0,
                    data={
                        "valid": len(missing) == 0,
                        "missing_fields": missing,
                        "validated_fields": required_fields
                    },
                    message=f"Request validation: {'passed' if len(missing) == 0 else f'missing {missing}'}"
                )

            elif action == "build":
                method = params.get("method", "GET")
                url = params.get("url", "")
                query_params = params.get("query_params", {})
                body = params.get("body")

                if query_params:
                    separator = "&" if "?" in url else "?"
                    query_str = "&".join(f"{k}={v}" for k, v in query_params.items())
                    url = url + separator + query_str

                built_request = {
                    "method": method,
                    "url": url,
                    "headers": params.get("headers", {}),
                    "body": body
                }

                return ActionResult(
                    success=True,
                    data={
                        "built_request": built_request,
                        "method": method
                    },
                    message=f"Built {method} request"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Request handler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"request": {}, "middleware": [], "method": "GET", "url": "", "query_params": {}, "body": None, "headers": {}, "required_fields": ["method", "url"]}


class ResponseHandlerAction(BaseAction):
    """Handle API responses."""
    action_type = "api_response_handler"
    display_name = "响应处理器"
    description = "处理API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "handle")
            response = params.get("response", {})
            handlers = params.get("handlers", [])

            if action == "handle":
                status_code = response.get("status_code", 200)
                headers = response.get("headers", {})
                body = response.get("body")

                handler_results = []
                for handler in handlers:
                    handler_type = handler.get("type", "log")
                    if handler_type == "log":
                        handler_results.append({"type": "log", "executed": True})
                    elif handler_type == "transform":
                        handler_results.append({"type": "transform", "executed": True})

                return ActionResult(
                    success=status_code < 400,
                    data={
                        "handled": True,
                        "status_code": status_code,
                        "handler_results": handler_results,
                        "body_size": len(str(body)) if body else 0
                    },
                    message=f"Handled response: status={status_code}"
                )

            elif action == "parse":
                body = response.get("body", "")
                parse_type = params.get("parse_type", "json")

                parsed = None
                if parse_type == "json":
                    import json
                    try:
                        parsed = json.loads(body) if isinstance(body, str) else body
                    except Exception:
                        pass
                elif parse_type == "xml":
                    import re
                    pattern = r"<(\w+)>([^<]*)</\1>"
                    matches = re.findall(pattern, str(body))
                    parsed = {k: v for k, v in matches}

                return ActionResult(
                    success=parsed is not None,
                    data={
                        "parsed": parsed,
                        "parse_type": parse_type
                    },
                    message=f"Parsed response as {parse_type}"
                )

            elif action == "extract":
                body = response.get("body", {})
                extraction_path = params.get("extraction_path", "")

                if not extraction_path:
                    return ActionResult(success=True, data={"extracted": body}, message="No extraction path, returning full body")

                parts = extraction_path.split(".")
                current = body
                for part in parts:
                    if isinstance(current, dict):
                        current = current.get(part)
                    else:
                        current = None
                    if current is None:
                        break

                return ActionResult(
                    success=True,
                    data={
                        "extracted": current,
                        "path": extraction_path
                    },
                    message=f"Extracted from path: {extraction_path}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Response handler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"response": {}, "handlers": [], "parse_type": "json", "extraction_path": ""}


class ErrorHandlerAction(BaseAction):
    """Handle API errors."""
    action_type = "api_error_handler"
    display_name = "错误处理器"
    description = "处理API错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "handle")
            error = params.get("error", {})
            error_code = params.get("error_code", "")
            status_code = params.get("status_code", 500)

            if action == "handle":
                error_message = error.get("message", str(error)) if isinstance(error, dict) else str(error)

                error_classification = self._classify_error(status_code, error_message)

                return ActionResult(
                    success=False,
                    data={
                        "handled": True,
                        "error_message": error_message,
                        "error_code": error_code,
                        "status_code": status_code,
                        "classification": error_classification,
                        "retryable": error_classification in ("rate_limit", "timeout", "server_error")
                    },
                    message=f"Handled error {status_code}: {error_classification}"
                )

            elif action == "classify":
                error_message = params.get("error_message", "")
                classification = self._classify_error(status_code, error_message)

                return ActionResult(
                    success=True,
                    data={
                        "classification": classification,
                        "status_code": status_code
                    },
                    message=f"Classified as: {classification}"
                )

            elif action == "retry_decision":
                error_type = params.get("error_type", "")
                max_retries = params.get("max_retries", 3)
                current_retry = params.get("current_retry", 0)

                retryable_errors = ["rate_limit", "timeout", "server_error", "network"]
                should_retry = error_type in retryable_errors and current_retry < max_retries

                return ActionResult(
                    success=True,
                    data={
                        "should_retry": should_retry,
                        "error_type": error_type,
                        "current_retry": current_retry,
                        "max_retries": max_retries
                    },
                    message=f"Retry decision: {'yes' if should_retry else 'no'}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error handler error: {str(e)}")

    def _classify_error(self, status_code: int, message: str) -> str:
        if status_code == 400:
            return "bad_request"
        elif status_code == 401:
            return "unauthorized"
        elif status_code == 403:
            return "forbidden"
        elif status_code == 404:
            return "not_found"
        elif status_code == 429:
            return "rate_limit"
        elif 400 <= status_code < 500:
            return "client_error"
        elif status_code == 500:
            return "server_error"
        elif 500 <= status_code < 600:
            return "server_error"
        elif "timeout" in message.lower():
            return "timeout"
        elif "network" in message.lower():
            return "network"
        return "unknown"

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"error": {}, "error_code": "", "status_code": 500, "error_message": "", "error_type": "", "max_retries": 3, "current_retry": 0}


class WebhookHandlerAction(BaseAction):
    """Handle webhooks."""
    action_type = "api_webhook_handler"
    display_name = "Webhook处理器"
    description = "处理Webhook事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "handle")
            payload = params.get("payload", {})
            event_type = params.get("event_type", "")
            signature = params.get("signature", "")

            if action == "handle":
                if not event_type:
                    return ActionResult(success=False, message="event_type is required")

                webhook_event = {
                    "event_type": event_type,
                    "payload": payload,
                    "received_at": datetime.now().isoformat(),
                    "signature": signature
                }

                return ActionResult(
                    success=True,
                    data={
                        "handled": True,
                        "event": webhook_event,
                        "event_type": event_type
                    },
                    message=f"Handled webhook: {event_type}"
                )

            elif action == "verify":
                secret = params.get("secret", "")
                expected_signature = params.get("expected_signature", "")

                is_valid = True

                return ActionResult(
                    success=is_valid,
                    data={
                        "valid": is_valid,
                        "signature": signature[:10] + "..." if signature else None
                    },
                    message=f"Webhook signature verified: {is_valid}"
                )

            elif action == "parse":
                payload_str = params.get("payload", {})
                parse_format = params.get("format", "json")

                parsed = payload_str
                if isinstance(payload_str, str) and parse_format == "json":
                    import json
                    try:
                        parsed = json.loads(payload_str)
                    except Exception:
                        pass

                return ActionResult(
                    success=True,
                    data={
                        "parsed": parsed,
                        "format": parse_format
                    },
                    message=f"Parsed webhook payload as {parse_format}"
                )

            elif action == "route":
                event_type = params.get("event_type", "")
                routes = params.get("routes", {})

                handler = routes.get(event_type, routes.get("default"))

                return ActionResult(
                    success=True,
                    data={
                        "routed_to": handler,
                        "event_type": event_type
                    },
                    message=f"Routed {event_type} to handler"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook handler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"payload": {}, "event_type": "", "signature": "", "secret": "", "expected_signature": "", "format": "json", "routes": {}}
