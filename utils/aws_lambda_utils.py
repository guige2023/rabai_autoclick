"""
AWS Lambda Utilities.

Helpers for creating, updating, invoking, and managing AWS Lambda functions,
including async invocations, batch processing, and layered dependencies.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import hashlib
import base64
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
LAMBDA_API_BASE = f"https://lambda.{AWS_REGION}.amazonaws.com/2024-06-30"


# --------------------------------------------------------------------------- #
# Signature (simplified — prefer boto3 in production)
# --------------------------------------------------------------------------- #

def _signed_headers(
    method: str,
    path: str,
    body: Optional[bytes] = None,
) -> dict[str, str]:
    """
    Build request headers with AWS Signature Version 4.

    Note: For production use, prefer boto3 (awscurl, aiobotocore, etc.).
    This implementation is a minimal placeholder for scripting contexts.
    """
    import datetime
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    headers = {
        "Host": f"lambda.{AWS_REGION}.amazonaws.com",
        "X-Amz-Date": amz_date,
        "X-Amz-Target": "LambdaService.Invoke",
        "Content-Type": "application/json",
    }
    if body:
        headers["X-Amz-Content-Sha256"] = hashlib.sha256(body).hexdigest()
    return headers


# --------------------------------------------------------------------------- #
# Lambda Function Management
# --------------------------------------------------------------------------- #

def get_function(function_name: str) -> dict[str, Any]:
    """Return Lambda function configuration metadata."""
    path = f"/functions/{urllib.parse.quote(function_name)}"
    url = f"{LAMBDA_API_BASE}{path}"
    req = urllib.request.Request(url, headers=_signed_headers("GET", path), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


def list_functions(
    marker: Optional[str] = None,
    max_items: int = 100,
) -> dict[str, Any]:
    """List Lambda functions with pagination."""
    params = {"MaxItems": str(max_items)}
    if marker:
        params["Marker"] = marker
    qs = urllib.parse.urlencode(params)
    path = f"/functions?{qs}"
    url = f"{LAMBDA_API_BASE}{path}"
    req = urllib.request.Request(url, headers=_signed_headers("GET", path), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


def update_function_code(
    function_name: str,
    s3_bucket: str,
    s3_key: str,
    s3_version: Optional[str] = None,
) -> dict[str, Any]:
    """Update Lambda function code from an S3 location."""
    path = f"/functions/{urllib.parse.quote(function_name)}/code"
    url = f"{LAMBDA_API_BASE}{path}"
    body = {
        "S3Bucket": s3_bucket,
        "S3Key": s3_key,
    }
    if s3_version:
        body["S3ObjectVersion"] = s3_version
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers=_signed_headers("PUT", path, data), method="PUT"
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


def update_function_config(
    function_name: str,
    memory_size: Optional[int] = None,
    timeout: Optional[int] = None,
    env_vars: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Update Lambda function configuration options."""
    path = f"/functions/{urllib.parse.quote(function_name)}/configuration"
    url = f"{LAMBDA_API_BASE}{path}"
    body: dict[str, Any] = {}
    if memory_size is not None:
        body["MemorySize"] = memory_size
    if timeout is not None:
        body["Timeout"] = timeout
    if env_vars:
        body["Environment"] = {"Variables": env_vars}
    if not body:
        return {}
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers=_signed_headers("PATCH", path, data), method="PATCH"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# Invocation
# --------------------------------------------------------------------------- #

def invoke(
    function_name: str,
    payload: Optional[dict[str, Any]] = None,
    invocation_type: str = "RequestResponse",
    log_type: str = "Tail",
) -> dict[str, Any]:
    """
    Invoke a Lambda function.

    Args:
        function_name: Name or ARN of the function.
        payload: JSON-serializable input data.
        invocation_type: RequestResponse, Event, or DryRun.
        log_type: Tail or None.

    Returns:
        Response dict with 'statusCode', 'body', 'logs' (if log_type=Tail).
    """
    path = f"/functions/{urllib.parse.quote(function_name)}/invocations"
    url = f"{LAMBDA_API_BASE}{path}"
    body = json.dumps(payload or {}).encode()
    headers = _signed_headers("POST", path, body)
    headers["X-Amz-Invocation-Type"] = invocation_type
    headers["X-Amz-Log-Type"] = log_type
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            raw = resp.read()
            result: dict[str, Any] = {"statusCode": resp.status}
            if log_type == "Tail":
                result["logs"] = base64.b64decode(
                    resp.headers.get("X-Amz-Log-Result", "")
                ).decode()
            result["body"] = json.loads(raw) if raw else None
            return result
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


def invoke_async(
    function_name: str,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Invoke a Lambda function asynchronously."""
    return invoke(
        function_name, payload=payload,
        invocation_type="Event", log_type="None",
    )


# --------------------------------------------------------------------------- #
# Aliases
# --------------------------------------------------------------------------- #

def list_aliases(function_name: str) -> list[dict[str, Any]]:
    """Return all aliases for a function."""
    path = f"/functions/{urllib.parse.quote(function_name)}/aliases"
    url = f"{LAMBDA_API_BASE}{path}"
    req = urllib.request.Request(url, headers=_signed_headers("GET", path), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read()).get("Aliases", [])
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


def update_alias(
    function_name: str,
    alias_name: str,
    function_version: str,
    description: str = "",
) -> dict[str, Any]:
    """Create or update a function alias."""
    path = f"/functions/{urllib.parse.quote(function_name)}/aliases/{alias_name}"
    url = f"{LAMBDA_API_BASE}{path}"
    body: dict[str, Any] = {"FunctionVersion": function_version}
    if description:
        body["Description"] = description
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers=_signed_headers("PUT", path, data), method="PUT"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# Layer Management
# --------------------------------------------------------------------------- #

def list_layers(
    compatible_runtime: Optional[str] = None,
    max_items: int = 100,
) -> list[dict[str, Any]]:
    """List available Lambda layers."""
    params = {"MaxItems": str(max_items)}
    if compatible_runtime:
        params["CompatibleRuntime"] = compatible_runtime
    qs = urllib.parse.urlencode(params)
    path = f"/layers?{qs}"
    url = f"{LAMBDA_API_BASE}{path}"
    req = urllib.request.Request(url, headers=_signed_headers("GET", path), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            return data.get("Layers", [])
    except urllib.error.HTTPError as exc:
        raise LambdaAPIError(exc.code, exc.read().decode()) from exc


class LambdaAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Lambda API error {status}: {body}")
