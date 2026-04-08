"""JSON-RPC client action module for RabAI AutoClick.

Provides JSON-RPC 2.0 operations:
- JsonRpcCallAction: Generic JSON-RPC 2.0 calls
- JsonRpcBatchAction: Batch JSON-RPC requests
- JsonRpcNotificationAction: Fire-and-forget notifications
- JsonRpcSubscriptionAction: Subscribe to server notifications
"""

import json
import uuid
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional, Union


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonRpcCallAction(BaseAction):
    """Execute a JSON-RPC 2.0 request."""
    action_type = "jsonrpc_call"
    display_name = "JSON-RPC调用"
    description = "执行JSON-RPC 2.0请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "")
            args = params.get("args", [])
            kwargs = params.get("kwargs", {})
            headers = params.get("headers", {"Content-Type": "application/json"})
            timeout = params.get("timeout", 30)
            auth = params.get("auth", None)

            if not url:
                return ActionResult(success=False, message="url is required")
            if not method:
                return ActionResult(success=False, message="method is required")

            request_id = str(uuid.uuid4())
            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": args if args else kwargs,
                "id": request_id
            }

            if auth:
                auth_type = auth.get("type", "bearer")
                if auth_type == "bearer":
                    headers["Authorization"] = f"Bearer {auth.get('token', '')}"
                elif auth_type == "basic":
                    import base64
                    credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                    encoded = base64.b64encode(credentials.encode()).decode()
                    headers["Authorization"] = f"Basic {encoded}"

            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    result_data = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                error_body = e.read().decode("utf-8") if e.fp else ""
                return ActionResult(
                    success=False,
                    message=f"HTTP error {e.code}: {e.reason}",
                    data={"http_code": e.code, "error_body": error_body}
                )
            except urllib.error.URLError as e:
                return ActionResult(success=False, message=f"URL error: {e.reason}")

            if "error" in result_data:
                return ActionResult(
                    success=False,
                    message=f"JSON-RPC error: {result_data['error']}",
                    data=result_data
                )

            return ActionResult(success=True, message="JSON-RPC call successful", data=result_data)

        except Exception as e:
            return ActionResult(success=False, message=f"JSON-RPC call failed: {str(e)}")


class JsonRpcBatchAction(BaseAction):
    """Execute multiple JSON-RPC requests as a batch."""
    action_type = "jsonrpc_batch"
    display_name = "JSON-RPC批量调用"
    description = "批量执行多个JSON-RPC请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            calls = params.get("calls", [])
            headers = params.get("headers", {"Content-Type": "application/json"})
            timeout = params.get("timeout", 60)
            auth = params.get("auth", None)

            if not url:
                return ActionResult(success=False, message="url is required")
            if not calls:
                return ActionResult(success=False, message="calls list is required")

            batch = []
            for idx, call in enumerate(calls):
                batch.append({
                    "jsonrpc": "2.0",
                    "method": call.get("method", ""),
                    "params": call.get("params", []),
                    "id": call.get("id", idx + 1)
                })

            if auth:
                auth_type = auth.get("type", "bearer")
                if auth_type == "bearer":
                    headers["Authorization"] = f"Bearer {auth.get('token', '')}"
                elif auth_type == "basic":
                    import base64
                    credentials = f"{auth.get('username', '')}:{auth.get('password', '')}"
                    encoded = base64.b64encode(credentials.encode()).decode()
                    headers["Authorization"] = f"Basic {encoded}"

            data = json.dumps(batch).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    result_data = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                error_body = e.read().decode("utf-8") if e.fp else ""
                return ActionResult(
                    success=False,
                    message=f"HTTP error {e.code}: {e.reason}",
                    data={"http_code": e.code, "error_body": error_body}
                )
            except urllib.error.URLError as e:
                return ActionResult(success=False, message=f"URL error: {e.reason}")

            results = []
            errors = []
            for item in result_data:
                if "error" in item:
                    errors.append(item)
                else:
                    results.append(item)

            return ActionResult(
                success=len(errors) == 0,
                message=f"Batch complete: {len(results)} succeeded, {len(errors)} failed",
                data={"results": results, "errors": errors, "total": len(result_data)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"JSON-RPC batch failed: {str(e)}")


class JsonRpcNotificationAction(BaseAction):
    """Send a JSON-RPC notification (no response expected)."""
    action_type = "jsonrpc_notification"
    display_name = "JSON-RPC通知"
    description = "发送JSON-RPC通知（无响应）"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "")
            args = params.get("args", [])
            headers = params.get("headers", {"Content-Type": "application/json"})
            timeout = params.get("timeout", 10)

            if not url:
                return ActionResult(success=False, message="url is required")
            if not method:
                return ActionResult(success=False, message="method is required")

            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": args
            }

            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    return ActionResult(success=True, message="Notification sent")
            except urllib.error.HTTPError:
                return ActionResult(success=True, message="Notification sent (error response expected for notifications)")
            except urllib.error.URLError:
                return ActionResult(success=False, message="Failed to send notification")

        except Exception as e:
            return ActionResult(success=False, message=f"JSON-RPC notification failed: {str(e)}")


class JsonRpcSubscriptionAction(BaseAction):
    """Subscribe to JSON-RPC server notifications."""
    action_type = "jsonrpc_subscription"
    display_name = "JSON-RPC订阅"
    description = "订阅JSON-RPC服务器通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            subscription_id = params.get("subscription_id", "")
            unsubscribe_method = params.get("unsubscribe_method", "unsubscribe")
            max_duration = params.get("max_duration", 300)
            callback = params.get("callback", None)

            if not url:
                return ActionResult(success=False, message="url is required")
            if not subscription_id:
                return ActionResult(success=False, message="subscription_id is required")

            import time
            start_time = time.time()
            messages = []

            while (time.time() - start_time) < max_duration:
                try:
                    unsub_payload = {
                        "jsonrpc": "2.0",
                        "method": unsubscribe_method,
                        "params": [subscription_id],
                        "id": str(uuid.uuid4())
                    }
                    data = json.dumps(unsub_payload).encode("utf-8")
                    headers = {"Content-Type": "application/json"}
                    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=5) as response:
                        result = json.loads(response.read().decode("utf-8"))
                        if "result" in result:
                            messages.append(result["result"])
                except Exception:
                    break

            return ActionResult(
                success=True,
                message=f"Subscription ended, received {len(messages)} messages",
                data={"messages": messages, "subscription_id": subscription_id}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"JSON-RPC subscription failed: {str(e)}")
