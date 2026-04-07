"""HTTP requests action module for RabAI AutoClick.

Provides HTTP operations:
- GetRequestAction: HTTP GET with params and headers
- PostRequestAction: HTTP POST with JSON/form data
- PutRequestAction: HTTP PUT request
- DeleteRequestAction: HTTP DELETE request
- PatchRequestAction: HTTP PATCH request
- BatchRequestAction: Execute multiple HTTP requests
- DownloadFileAction: Download file from URL
"""

import json
import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GetRequestAction(BaseAction):
    """HTTP GET request with query parameters."""
    action_type = "http_get"
    display_name = "HTTP GET"
    description = "执行HTTP GET请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.parse
            import urllib.error

            url = params.get("url", "")
            query_params = params.get("params", {})
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="URL is required")

            if query_params:
                encoded_params = urllib.parse.urlencode(query_params)
                url = url + ("?" + encoded_params if "?" not in url else "&" + encoded_params)

            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; RabAIAutoClick/1.0)")
            for key, value in headers.items():
                req.add_header(key, value)

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
                        message="GET request successful",
                        data={
                            "status_code": response.status,
                            "content": data,
                            "headers": dict(response.headers),
                            "content_type": content_type
                        }
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except ImportError:
            return ActionResult(success=False, message="urllib not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PostRequestAction(BaseAction):
    """HTTP POST request with JSON or form data."""
    action_type = "http_post"
    display_name = "HTTP POST"
    description = "执行HTTP POST请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.parse
            import urllib.error

            url = params.get("url", "")
            data = params.get("data", {})
            json_data = params.get("json")
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="URL is required")

            if json_data is not None:
                body = json.dumps(json_data).encode("utf-8")
                content_type = "application/json"
            elif data:
                body = urllib.parse.urlencode(data).encode("utf-8")
                content_type = "application/x-www-form-urlencoded"
            else:
                body = b""

            req = urllib.request.Request(url, data=body)
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; RabAIAutoClick/1.0)")
            req.add_header("Content-Type", content_type)
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read()
                    content_type = response.headers.get("Content-Type", "")

                    if "application/json" in content_type:
                        result = json.loads(content.decode("utf-8"))
                    else:
                        result = content.decode("utf-8", errors="replace")

                    return ActionResult(
                        success=True,
                        message="POST request successful",
                        data={"status_code": response.status, "content": result}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except ImportError:
            return ActionResult(success=False, message="urllib not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PutRequestAction(BaseAction):
    """HTTP PUT request."""
    action_type = "http_put"
    display_name = "HTTP PUT"
    description = "执行HTTP PUT请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.parse
            import urllib.error

            url = params.get("url", "")
            data = params.get("data", {})
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="URL is required")

            if data:
                body = json.dumps(data).encode("utf-8") if isinstance(data, dict) else str(data).encode("utf-8")
            else:
                body = b""

            req = urllib.request.Request(url, data=body, method="PUT")
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; RabAIAutoClick/1.0)")
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read().decode("utf-8", errors="replace")
                    return ActionResult(
                        success=True,
                        message="PUT request successful",
                        data={"status_code": response.status, "content": content}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DeleteRequestAction(BaseAction):
    """HTTP DELETE request."""
    action_type = "http_delete"
    display_name = "HTTP DELETE"
    description = "执行HTTP DELETE请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.error

            url = params.get("url", "")
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="URL is required")

            req = urllib.request.Request(url, method="DELETE")
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; RabAIAutoClick/1.0)")
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read().decode("utf-8", errors="replace")
                    return ActionResult(
                        success=True,
                        message="DELETE request successful",
                        data={"status_code": response.status, "content": content}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PatchRequestAction(BaseAction):
    """HTTP PATCH request."""
    action_type = "http_patch"
    display_name = "HTTP PATCH"
    description = "执行HTTP PATCH请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.error

            url = params.get("url", "")
            data = params.get("data", {})
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="URL is required")

            if data:
                body = json.dumps(data).encode("utf-8") if isinstance(data, dict) else str(data).encode("utf-8")
            else:
                body = b""

            req = urllib.request.Request(url, data=body, method="PATCH")
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; RabAIAutoClick/1.0)")
            req.add_header("Content-Type", "application/json")
            for key, value in headers.items():
                req.add_header(key, value)

            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    content = response.read().decode("utf-8", errors="replace")
                    return ActionResult(
                        success=True,
                        message="PATCH request successful",
                        data={"status_code": response.status, "content": content}
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}", data={"body": body})
            except Exception as e:
                return ActionResult(success=False, message=f"Request failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class BatchRequestAction(BaseAction):
    """Execute multiple HTTP requests in batch."""
    action_type = "http_batch"
    display_name = "批量HTTP请求"
    description = "批量执行多个HTTP请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            requests_config = params.get("requests", [])
            max_concurrent = params.get("max_concurrent", 3)
            delay = params.get("delay", 0.5)

            if not requests_config:
                return ActionResult(success=False, message="No requests configured")

            results = []
            for i, req_config in enumerate(requests_config):
                method = req_config.get("method", "GET").upper()
                url = req_config.get("url", "")
                headers = req_config.get("headers", {})
                data = req_config.get("data", {})

                try:
                    if method == "GET":
                        action = GetRequestAction()
                    elif method == "POST":
                        action = PostRequestAction()
                    elif method == "PUT":
                        action = PutRequestAction()
                    elif method == "DELETE":
                        action = DeleteRequestAction()
                    elif method == "PATCH":
                        action = PatchRequestAction()
                    else:
                        results.append({"index": i, "success": False, "message": f"Unknown method: {method}"})
                        continue

                    result = action.execute(context, {"url": url, "headers": headers, "data": data})
                    results.append({
                        "index": i,
                        "success": result.success,
                        "message": result.message,
                        "data": result.data
                    })

                    if i < len(requests_config) - 1:
                        time.sleep(delay)

                except Exception as e:
                    results.append({"index": i, "success": False, "message": str(e)})

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=True,
                message=f"Batch completed: {success_count}/{len(results)} successful",
                data={"results": results, "success_count": success_count, "total": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {str(e)}")


class DownloadFileAction(BaseAction):
    """Download file from URL."""
    action_type = "download_file"
    display_name = "下载文件"
    description = "从URL下载文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import urllib.request
            import urllib.error
            os.makedirs = os.makedirs
            path = os.path

            url = params.get("url", "")
            output_path = params.get("output_path", "")
            filename = params.get("filename", "")
            timeout = params.get("timeout", 60)

            if not url:
                return ActionResult(success=False, message="URL is required")

            if not output_path:
                output_path = "/tmp"

            try:
                with urllib.request.urlopen(url, timeout=timeout) as response:
                    content = response.read()
                    content_disposition = response.headers.get("Content-Disposition", "")
                    if not filename:
                        match = content_disposition and re.search(r'filename="?([^";]+)"?', content_disposition)
                        filename = match.group(1) if match else url.split("/")[-1].split("?")[0]

                    if not filename:
                        return ActionResult(success=False, message="Could not determine filename")

                    filepath = path.join(output_path, filename)
                    os.makedirs(path.dirname(filepath) if path.dirname(filepath) else ".", exist_ok=True)

                    with open(filepath, "wb") as f:
                        f.write(content)

                    return ActionResult(
                        success=True,
                        message=f"Downloaded {len(content)} bytes",
                        data={
                            "filepath": filepath,
                            "filename": filename,
                            "size": len(content),
                            "content_type": response.headers.get("Content-Type", "")
                        }
                    )
            except urllib.error.HTTPError as e:
                return ActionResult(success=False, message=f"HTTP {e.code}: {e.reason}")
            except Exception as e:
                return ActionResult(success=False, message=f"Download failed: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
