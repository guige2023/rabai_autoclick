"""AWS Lambda action module for RabAI AutoClick.

Provides serverless function management via AWS Lambda API
for deploying, invoking, and monitoring Lambda functions.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import hashlib
import hmac
import base64
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LambdaAction(BaseAction):
    """AWS Lambda API integration for serverless operations.

    Supports function invocation, listing, configuration management,
    and async invocation handling.

    Args:
        config: Lambda configuration containing region, access_key,
                secret_key, and function_name
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.region = self.config.get("region", "us-east-1")
        self.function_name = self.config.get("function_name", "")
        self.access_key = self.config.get("access_key", "")
        self.secret_key = self.config.get("secret_key", "")
        self.endpoint = self.config.get(
            "endpoint",
            f"https://lambda.{self.region}.amazonaws.com"
        )

    def _sign(self, key: bytes, msg: str) -> bytes:
        """Create HMAC signature."""
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _get_signature_key(self, key: str, date_stamp: str, region: str) -> bytes:
        """Get signing key."""
        k_date = self._sign(("AWS4" + key).encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, region)
        k_service = self._sign(k_region, "lambda")
        k_signing = self._sign(k_service, "aws4_request")
        return k_signing

    def _make_request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make signed HTTP request to Lambda."""
        service = "lambda"
        host = f"lambda.{self.region}.amazonaws.com"

        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        payload_json = json.dumps(payload) if payload else ""
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        headers = {
            "Host": host,
            "X-Amz-Date": amz_date,
            "X-Amz-Target": "LambdaService.Invoke",
            "Content-Type": "application/json",
        }

        if method == "POST" and payload:
            headers["Content-Length"] = str(len(payload_json))

        sorted_headers = sorted(headers.items())
        canonical_headers = "\n".join(f"{k}:{v}" for k, v in sorted_headers) + "\n"
        signed_headers = ";".join(k.lower() for k, v in sorted_headers)

        canonical_request = "\n".join([
            method, endpoint, "",
            canonical_headers, signed_headers, payload_hash
        ])

        credential_scope = f"{date_stamp}/{self.region}/{service}/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amz_date, credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        ])

        signing_key = self._get_signature_key(self.secret_key, date_stamp, self.region)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        authorization = (
            f"AWS4-HMAC-SHA256 "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        headers["Authorization"] = authorization

        url = f"{self.endpoint}{endpoint}"
        body = payload_json.encode("utf-8") if payload else None
        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=60) as response:
                result = response.read().decode("utf-8")
                if result:
                    return json.loads(result)
                return {"success": True}
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def invoke(
        self,
        payload: Optional[Dict] = None,
        invocation_type: str = "RequestResponse",
        function_name: Optional[str] = None,
    ) -> ActionResult:
        """Invoke a Lambda function.

        Args:
            payload: Input data for the function
            invocation_type: RequestResponse, Event, or DryRun
            function_name: Function name (uses config default)

        Returns:
            ActionResult with function response
        """
        func_name = function_name or self.function_name
        if not func_name:
            return ActionResult(success=False, error="Missing function_name")

        endpoint = f"/2015-03-31/functions/{func_name}/invocations"

        headers = {
            "X-Amz-Invocation-Type": invocation_type,
            "Content-Type": "application/json",
        }

        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        host = f"lambda.{self.region}.amazonaws.com"

        headers["Host"] = host
        headers["X-Amz-Date"] = amz_date

        payload_json = json.dumps(payload) if payload else "{}"
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        sorted_headers = sorted(headers.items())
        canonical_headers = "\n".join(f"{k}:{v}" for k, v in sorted_headers) + "\n"
        signed_headers = ";".join(k.lower() for k, v in sorted_headers)

        canonical_request = "\n".join([
            "POST", endpoint, "",
            canonical_headers, signed_headers, payload_hash
        ])

        credential_scope = f"{date_stamp}/{self.region}/lambda/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amz_date, credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        ])

        signing_key = self._get_signature_key(self.secret_key, date_stamp, self.region)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        authorization = (
            f"AWS4-HMAC-SHA256 "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        headers["Authorization"] = authorization

        url = f"{self.endpoint}{endpoint}"
        req = Request(
            url,
            data=payload_json.encode("utf-8"),
            headers=headers,
            method="POST",
        )

        try:
            with urlopen(req, timeout=60) as response:
                result = response.read().decode("utf-8")
                return ActionResult(
                    success=True,
                    data={"response": json.loads(result) if result else {}},
                )
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
            )
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def list_functions(self) -> ActionResult:
        """List all Lambda functions.

        Returns:
            ActionResult with functions list
        """
        result = self._make_request("GET", "/2015-03-31/functions")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        functions = result.get("Functions", [])
        return ActionResult(success=True, data={"functions": functions})

    def get_function(self, function_name: Optional[str] = None) -> ActionResult:
        """Get function configuration.

        Args:
            function_name: Function name (uses config default)

        Returns:
            ActionResult with function configuration
        """
        func_name = function_name or self.function_name
        if not func_name:
            return ActionResult(success=False, error="Missing function_name")

        result = self._make_request("GET", f"/2015-03-31/functions/{func_name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Lambda operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "invoke": self.invoke,
            "list_functions": self.list_functions,
            "get_function": self.get_function,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
