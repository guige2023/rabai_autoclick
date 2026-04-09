"""
API request signer action for request authentication.

Provides HMAC, AWS Signature v4, and JWT token signing.
"""

from typing import Any, Dict, Optional
import hashlib
import hmac
import time
import base64
import json


class APIRequestSignerAction:
    """API request signing for authentication."""

    def __init__(
        self,
        secret_key: str = "",
        algorithm: str = "hmac_sha256",
    ) -> None:
        """
        Initialize request signer.

        Args:
            secret_key: Secret key for signing
            algorithm: Signing algorithm
        """
        self.secret_key = secret_key
        self.algorithm = algorithm

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute signing operation.

        Args:
            params: Dictionary containing:
                - operation: 'sign', 'verify', 'generate_token'
                - request: Request to sign
                - secret_key: Override secret key
                - token_payload: Payload for token generation

        Returns:
            Dictionary with signed request or verification result
        """
        operation = params.get("operation", "sign")

        if operation == "sign":
            return self._sign_request(params)
        elif operation == "verify":
            return self._verify_signature(params)
        elif operation == "generate_token":
            return self._generate_token(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _sign_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Sign API request."""
        request = params.get("request", {})
        secret_key = params.get("secret_key", self.secret_key)
        signing_type = params.get("signing_type", "hmac")

        if not secret_key:
            return {"success": False, "error": "Secret key is required"}

        method = request.get("method", "GET")
        path = request.get("path", "/")
        headers = request.get("headers", {})
        body = request.get("body", "")

        if signing_type == "hmac":
            signature = self._sign_hmac(method, path, headers, body, secret_key)
        elif signing_type == "aws_v4":
            signature = self._sign_aws_v4(method, path, headers, body, secret_key, params)
        elif signing_type == "simple":
            signature = self._sign_simple(method, path, headers, body, secret_key)
        else:
            return {"success": False, "error": f"Unknown signing type: {signing_type}"}

        return {
            "success": True,
            "signature": signature,
            "signed_headers": {
                "X-Signature": signature,
                "X-Timestamp": str(int(time.time())),
            },
        }

    def _sign_hmac(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: str,
        secret_key: str,
    ) -> str:
        """Generate HMAC-SHA256 signature."""
        string_to_sign = f"{method}\n{path}\n{json.dumps(headers, sort_keys=True)}\n{body}"
        signature = hmac.new(
            secret_key.encode(), string_to_sign.encode(), hashlib.sha256
        ).hexdigest()
        return signature

    def _sign_aws_v4(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: str,
        secret_key: str,
        params: dict[str, Any],
    ) -> str:
        """Generate AWS Signature Version 4."""
        region = params.get("region", "us-east-1")
        service = params.get("service", "execute-api")
        timestamp = params.get("timestamp", int(time.time()))

        date = time.strftime("%Y%m%d", time.gmtime(timestamp))
        datetime = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(timestamp))

        canonical_uri = path
        canonical_querystring = ""
        canonical_headers = "\n".join(f"{k.lower()}:{v}" for k, v in headers.items()) + "\n"
        signed_headers = ";".join(k.lower() for k in headers.keys())

        payload_hash = hashlib.sha256(body.encode()).hexdigest()

        canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

        credential_scope = f"{date}/{region}/{service}/aws4_request"
        string_to_sign = f"AWS4-HMAC-SHA256\n{datetime}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

        k_date = hmac.new(f"AWS4{secret_key}".encode(), date.encode(), hashlib.sha256).digest()
        k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()

        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

        return f"AWS4-HMAC-SHA256 Credential={secret_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

    def _sign_simple(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: str,
        secret_key: str,
    ) -> str:
        """Generate simple MD5-based signature."""
        string_to_sign = f"{method}:{path}:{body}"
        signature = hashlib.md5(f"{secret_key}{string_to_sign}".encode()).hexdigest()
        return signature

    def _verify_signature(self, params: dict[str, Any]) -> dict[str, Any]:
        """Verify request signature."""
        request = params.get("request", {})
        provided_signature = params.get("signature", "")
        secret_key = params.get("secret_key", self.secret_key)

        signed_result = self._sign_request({**params, "secret_key": secret_key})
        expected_signature = signed_result.get("signature", "")

        is_valid = hmac.compare_digest(provided_signature, expected_signature)

        return {"success": True, "valid": is_valid}

    def _generate_token(self, params: dict[str, Any]) -> dict[str, Any]:
        """Generate JWT-like token."""
        payload = params.get("token_payload", {})
        secret_key = params.get("secret_key", self.secret_key)
        expires_in = params.get("expires_in", 3600)

        if not secret_key:
            return {"success": False, "error": "Secret key is required"}

        header = {"alg": "HS256", "typ": "JWT"}
        payload["exp"] = int(time.time()) + expires_in
        payload["iat"] = int(time.time())

        header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip("=")
        payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")

        signature_input = f"{header_b64}.{payload_b64}"
        signature = hmac.new(secret_key.encode(), signature_input.encode(), hashlib.sha256)
        signature_b64 = base64.urlsafe_b64encode(signature.digest()).decode().rstrip("=")

        token = f"{signature_input}.{signature_b64}"

        return {"success": True, "token": token, "expires_in": expires_in}
