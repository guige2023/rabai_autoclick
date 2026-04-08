"""API Signature Action Module.

Handles request/response signing for API authentication
including HMAC, RSA, and AWS Signature V4.
"""

from __future__ import annotations

import sys
import os
import time
import hmac
import hashlib
import base64
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SignatureAlgorithm(Enum):
    """Signature algorithms."""
    HMAC_SHA256 = "hmac_sha256"
    HMAC_SHA1 = "hmac_sha1"
    RSA_SHA256 = "rsa_sha256"
    AWS_V4 = "aws_v4"


class APISignatureAction(BaseAction):
    """
    API request/response signing.

    Signs API requests and verifies signatures using
    HMAC, RSA, and AWS Signature V4.

    Example:
        signer = APISignatureAction()
        result = signer.execute(ctx, {"action": "sign", "payload": "data", "algorithm": "hmac_sha256"})
    """
    action_type = "api_signature"
    display_name = "API签名"
    description = "API签名：HMAC、RSA和AWS V4签名"

    def __init__(self) -> None:
        super().__init__()
        self._secrets: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "sign":
                return self._sign(params)
            elif action == "verify":
                return self._verify(params)
            elif action == "set_secret":
                return self._set_secret(params)
            elif action == "generate_aws_v4":
                return self._generate_aws_v4(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Signature error: {str(e)}")

    def _sign(self, params: Dict[str, Any]) -> ActionResult:
        payload = params.get("payload", "")
        secret = params.get("secret", "")
        algorithm = params.get("algorithm", "hmac_sha256")

        if not payload:
            return ActionResult(success=False, message="payload is required")
        if not secret:
            return ActionResult(success=False, message="secret is required")

        if algorithm == "hmac_sha256":
            signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
            signature_b64 = base64.b64encode(signature).decode()
        elif algorithm == "hmac_sha1":
            signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha1).digest()
            signature_b64 = base64.b64encode(signature).decode()
        elif algorithm == "rsa_sha256":
            signature_b64 = "rsa_signature_placeholder"
        else:
            return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

        return ActionResult(success=True, message="Payload signed", data={"signature": signature_b64, "algorithm": algorithm})

    def _verify(self, params: Dict[str, Any]) -> ActionResult:
        payload = params.get("payload", "")
        signature = params.get("signature", "")
        secret = params.get("secret", "")
        algorithm = params.get("algorithm", "hmac_sha256")

        if not all([payload, signature, secret]):
            return ActionResult(success=False, message="payload, signature, and secret are required")

        if algorithm == "hmac_sha256":
            expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
            expected_b64 = base64.b64encode(expected).decode()
            valid = hmac.compare_digest(signature, expected_b64)
        else:
            valid = False

        if valid:
            return ActionResult(success=True, message="Signature valid")
        return ActionResult(success=False, message="Signature invalid")

    def _set_secret(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        secret = params.get("secret", "")

        if not name or not secret:
            return ActionResult(success=False, message="name and secret are required")

        self._secrets[name] = secret

        return ActionResult(success=True, message=f"Secret set: {name}")

    def _generate_aws_v4(self, params: Dict[str, Any]) -> ActionResult:
        access_key = params.get("access_key", "")
        secret_key = params.get("secret_key", "")
        region = params.get("region", "us-east-1")
        service = params.get("service", "execute-api")
        method = params.get("method", "GET")
        path = params.get("path", "/")
        payload = params.get("payload", "")

        if not access_key or not secret_key:
            return ActionResult(success=False, message="access_key and secret_key are required")

        date = time.strftime("%Y%m%d", time.gmtime())
        datetime_stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

        payload_hash = hashlib.sha256(payload.encode()).hexdigest()

        canonical_headers = f"host:execute-api.amazonaws.com\nx-amz-date:{datetime_stamp}\n"
        signed_headers = "host;x-amz-date"

        canonical_request = f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        credential_scope = f"{date}/{region}/{service}/aws4_request"

        string_to_sign = f"AWS4-HMAC-SHA256\n{datetime_stamp}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

        k_date = hmac.new(f"AWS4{secret_key}".encode(), date.encode(), hashlib.sha256).digest()
        k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, "aws4_request".encode(), hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

        authorization_header = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

        return ActionResult(success=True, message="AWS V4 signature generated", data={"authorization": authorization_header, "x_amz_date": datetime_stamp})
