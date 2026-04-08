"""
API Request Signer Action Module.

Signs API requests with HMAC, AWS Signature v4, OAuth 1.0a,
and other cryptographic signature schemes.

Author: RabAi Team
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import os
import time
import urllib.parse
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SignatureAlgorithm(Enum):
    """Supported signature algorithms."""
    HMAC_SHA256 = "hmac_sha256"
    HMAC_SHA1 = "hmac_sha1"
    AWS_V4 = "aws_v4"
    OAUTH1 = "oauth1"
    PLAINTEXT = "plaintext"


@dataclass
class SigningCredentials:
    """API signing credentials."""
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    session_token: Optional[str] = None
    region: str = "us-east-1"
    service: str = "execute-api"


@dataclass
class SignedRequest:
    """A signed HTTP request."""
    url: str
    method: str
    headers: Dict[str, str]
    body: Optional[bytes]
    signature: str
    signature_type: str


class ApiRequestSignerAction(BaseAction):
    """API request signer action.
    
    Signs HTTP requests with various cryptographic signature
    schemes for authenticated API access.
    """
    action_type = "api_request_signer"
    display_name = "API请求签名"
    description = "API请求加密签名"
    
    def __init__(self):
        super().__init__()
        self._credentials: Dict[str, SigningCredentials] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sign an API request.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - url: The request URL
                - method: HTTP method
                - headers: Request headers
                - body: Request body
                - algorithm: Signature algorithm to use
                - credentials: Signing credentials dict
                - credential_name: Named credential set to use
                
        Returns:
            ActionResult with signed request details.
        """
        start_time = time.time()
        
        url = params.get("url", "")
        method = params.get("method", "GET")
        headers = params.get("headers", {})
        body = params.get("body")
        algorithm = SignatureAlgorithm(params.get("algorithm", "hmac_sha256"))
        credentials = self._get_credentials(params)
        
        if not url:
            return ActionResult(
                success=False,
                message="Missing required parameter: url",
                duration=time.time() - start_time
            )
        
        try:
            if algorithm == SignatureAlgorithm.HMAC_SHA256:
                result = self._sign_hmac_sha256(url, method, headers, body, credentials)
            elif algorithm == SignatureAlgorithm.HMAC_SHA1:
                result = self._sign_hmac_sha1(url, method, headers, body, credentials)
            elif algorithm == SignatureAlgorithm.AWS_V4:
                result = self._sign_aws_v4(url, method, headers, body, credentials)
            elif algorithm == SignatureAlgorithm.OAUTH1:
                result = self._sign_oauth1(url, method, headers, body, credentials)
            elif algorithm == SignatureAlgorithm.PLAINTEXT:
                result = self._sign_plaintext(url, method, headers, body, credentials)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown signature algorithm: {algorithm}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Request signed with {algorithm.value}",
                data={
                    "url": result.url,
                    "method": result.method,
                    "headers": dict(result.headers),
                    "signature": result.signature,
                    "signature_type": result.signature_type
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Signing failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_credentials(self, params: Dict[str, Any]) -> SigningCredentials:
        """Get signing credentials from params or stored credentials."""
        cred_name = params.get("credential_name")
        if cred_name and cred_name in self._credentials:
            return self._credentials[cred_name]
        
        cred_dict = params.get("credentials", {})
        return SigningCredentials(
            access_key=cred_dict.get("access_key"),
            secret_key=cred_dict.get("secret_key"),
            session_token=cred_dict.get("session_token"),
            region=cred_dict.get("region", "us-east-1"),
            service=cred_dict.get("service", "execute-api")
        )
    
    def store_credentials(self, name: str, credentials: SigningCredentials) -> None:
        """Store credentials for later use."""
        self._credentials[name] = credentials
    
    def _sign_hmac_sha256(
        self, url: str, method: str, headers: Dict, body: Any, credentials: SigningCredentials
    ) -> SignedRequest:
        """Sign request with HMAC-SHA256."""
        secret = credentials.secret_key or ""
        
        body_bytes = self._encode_body(body)
        
        string_to_sign = f"{method}\n{url}\n{body_bytes.decode('utf-8') if body_bytes else ''}"
        signature = hmac.new(
            secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
        new_headers = dict(headers)
        new_headers["X-Signature"] = signature
        new_headers["X-Timestamp"] = str(int(time.time()))
        
        return SignedRequest(
            url=url, method=method, headers=new_headers,
            body=body_bytes, signature=signature, signature_type="HMAC-SHA256"
        )
    
    def _sign_hmac_sha1(
        self, url: str, method: str, headers: Dict, body: Any, credentials: SigningCredentials
    ) -> SignedRequest:
        """Sign request with HMAC-SHA1."""
        secret = credentials.secret_key or ""
        
        body_bytes = self._encode_body(body)
        
        string_to_sign = f"{method}\n{url}\n{body_bytes.decode('utf-8') if body_bytes else ''}"
        signature = hmac.new(
            secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha1
        ).hexdigest()
        
        new_headers = dict(headers)
        new_headers["Authorization"] = f"HMAC-SHA1 {signature}"
        
        return SignedRequest(
            url=url, method=method, headers=new_headers,
            body=body_bytes, signature=signature, signature_type="HMAC-SHA1"
        )
    
    def _sign_aws_v4(
        self, url: str, method: str, headers: Dict, body: Any, credentials: SigningCredentials
    ) -> SignedRequest:
        """Sign request with AWS Signature Version 4."""
        access_key = credentials.access_key or ""
        secret_key = credentials.secret_key or ""
        region = credentials.region
        service = credentials.service
        
        parsed_url = urllib.parse.urlparse(url)
        host = parsed_url.netloc
        path = parsed_url.path or "/"
        query = parsed_url.query
        
        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        
        new_headers = dict(headers)
        new_headers["Host"] = host
        new_headers["X-Amz-Date"] = amz_date
        
        if credentials.session_token:
            new_headers["X-Amz-Security-Token"] = credentials.session_token
        
        body_bytes = self._encode_body(body)
        payload_hash = hashlib.sha256(body_bytes if body_bytes else b"").hexdigest()
        new_headers["X-Amz-Content-Sha256"] = payload_hash
        
        signed_headers = ";".join(["host", "x-amz-date"])
        
        canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
        
        canonical_request = f"{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        hashed_canonical = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical}"
        
        k_date = hmac.new(f"AWS4{secret_key}".encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
        k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        
        authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        new_headers["Authorization"] = authorization
        
        return SignedRequest(
            url=url, method=method, headers=new_headers,
            body=body_bytes, signature=signature, signature_type="AWSV4"
        )
    
    def _sign_oauth1(
        self, url: str, method: str, headers: Dict, body: Any, credentials: SigningCredentials
    ) -> SignedRequest:
        """Sign request with OAuth 1.0a."""
        import secrets
        import uuid
        
        access_key = credentials.access_key or ""
        secret_key = credentials.secret_key or ""
        
        oauth_params = OrderedDict([
            ("oauth_consumer_key", access_key),
            ("oauth_signature_method", "HMAC-SHA1"),
            ("oauth_timestamp", str(int(time.time()))),
            ("oauth_nonce", secrets.token_hex(16)),
            ("oauth_version", "1.0")
        ])
        
        body_bytes = self._encode_body(body)
        
        if body_bytes:
            parsed_url = urllib.parse.urlparse(url)
            if parsed_url.query:
                query_params = urllib.parse.parse_qsl(parsed_url.query)
                for k, v in query_params:
                    oauth_params[k] = v
        
        sorted_params = sorted(oauth_params.items())
        param_string = "&".join(f"{urllib.parse.quote(str(k), '')}={urllib.parse.quote(str(v), '')}" for k, v in sorted_params)
        
        base_string = "&".join([
            method.upper(),
            urllib.parse.quote(url, ""),
            urllib.parse.quote(param_string, "")
        ])
        
        signing_key = f"{urllib.parse.quote(secret_key, '')}&"
        signature = base64.b64encode(
            hmac.new(signing_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha1).digest()
        ).decode("utf-8")
        
        oauth_params["oauth_signature"] = signature
        
        oauth_header = ", ".join(
            f'{urllib.parse.quote(str(k), "")}="{urllib.parse.quote(str(v), "")}"'
            for k, v in oauth_params.items()
        )
        
        new_headers = dict(headers)
        new_headers["Authorization"] = f"OAuth {oauth_header}"
        
        return SignedRequest(
            url=url, method=method, headers=new_headers,
            body=body_bytes, signature=signature, signature_type="OAuth1.0a"
        )
    
    def _sign_plaintext(
        self, url: str, method: str, headers: Dict, body: Any, credentials: SigningCredentials
    ) -> SignedRequest:
        """Plaintext signature (no actual signing, for testing)."""
        secret = credentials.secret_key or ""
        body_bytes = self._encode_body(body)
        
        new_headers = dict(headers)
        new_headers["X-Api-Key"] = credentials.access_key or ""
        
        return SignedRequest(
            url=url, method=method, headers=new_headers,
            body=body_bytes, signature=secret, signature_type="Plaintext"
        )
    
    def _encode_body(self, body: Any) -> Optional[bytes]:
        """Encode request body to bytes."""
        if body is None:
            return None
        if isinstance(body, bytes):
            return body
        if isinstance(body, dict):
            return json.dumps(body).encode("utf-8")
        return str(body).encode("utf-8")
    
    def verify_signature(
        self, url: str, method: str, headers: Dict, body: Any, signature: str, algorithm: str
    ) -> bool:
        """Verify a request signature."""
        try:
            credentials = SigningCredentials()
            if algorithm == "hmac_sha256":
                result = self._sign_hmac_sha256(url, method, headers, body, credentials)
                return hmac.compare_digest(result.signature, signature)
            elif algorithm == "hmac_sha1":
                result = self._sign_hmac_sha1(url, method, headers, body, credentials)
                return hmac.compare_digest(result.signature, signature)
            return False
        except Exception:
            return False
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate signing parameters."""
        if "url" not in params:
            return False, "Missing required parameter: url"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["url"]
