"""
API Request Signing Module.

Implements request signing for API authentication (HMAC-SHA256, AWS Signature, etc.).
Supports multiple signing algorithms and timestamp validation.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode, urlparse

import logging

logger = logging.getLogger(__name__)


@dataclass
class SigningConfig:
    """Configuration for request signing."""
    
    algorithm: str = "hmac-sha256"
    secret_key: str = ""
    timestamp_ttl: int = 300  # 5 minutes
    include_headers: Tuple[str, ...] = ("host", "content-type", "date")
    signature_prefix: str = "sig"
    timestamp_header: str = "X-Signature-Timestamp"
    signature_header: str = "X-Signature"
    

@dataclass
class SignedRequest:
    """Container for a signed request."""
    
    url: str
    method: str
    headers: Dict[str, str]
    body: Optional[bytes]
    signature: str
    timestamp: int
    algorithm: str
    
    
class RequestSigner:
    """
    Handles API request signing with multiple algorithm support.
    
    Example:
        signer = RequestSigner(SigningConfig(
            algorithm="hmac-sha256",
            secret_key="my-secret-key"
        ))
        signed = signer.sign_request("https://api.example.com/endpoint", "POST", 
                                      headers={"Content-Type": "application/json"},
                                      body=b'{"data": "value"}')
    """
    
    SUPPORTED_ALGORITHMS = {
        "hmac-sha256": "sha256",
        "hmac-sha512": "sha512",
        "aws-sig-v4": "aws-sig-v4",
    }
    
    def __init__(self, config: SigningConfig) -> None:
        """
        Initialize the request signer.
        
        Args:
            config: Signing configuration containing secret key and options.
            
        Raises:
            ValueError: If algorithm is not supported.
        """
        if config.algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported algorithm: {config.algorithm}. "
                f"Supported: {list(self.SUPPORTED_ALGORITHMS.keys())}"
            )
        self.config = config
        self._sign_functions: Dict[str, Callable[..., str]] = {
            "hmac-sha256": self._sign_hmac_sha256,
            "hmac-sha512": self._sign_hmac_sha512,
            "aws-sig-v4": self._sign_aws_sig_v4,
        }
        
    def sign_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        timestamp: Optional[int] = None,
    ) -> SignedRequest:
        """
        Sign an HTTP request.
        
        Args:
            url: The request URL.
            method: HTTP method (GET, POST, etc.).
            headers: Request headers.
            body: Request body bytes.
            timestamp: Unix timestamp (defaults to current time).
            
        Returns:
            SignedRequest object containing the signature and headers.
        """
        headers = headers or {}
        timestamp = timestamp or int(time.time())
        
        if self.config.algorithm == "aws-sig-v4":
            signature, signed_headers = self._sign_aws_sig_v4(
                url, method, headers, body, timestamp
            )
        else:
            signature = self._sign_functions[self.config.algorithm](
                url, method, headers, body, timestamp
            )
            signed_headers = self.config.include_headers
            
        # Build signature header
        if self.config.algorithm == "aws-sig-v4":
            auth_header = signature
        else:
            auth_header = f"{self.config.algorithm_prefix} {signature}"
            
        # Add signature headers
        signed_headers_dict = dict(headers)
        signed_headers_dict[self.config.timestamp_header] = str(timestamp)
        signed_headers_dict[self.config.signature_header] = signature
        
        return SignedRequest(
            url=url,
            method=method,
            headers=signed_headers_dict,
            body=body,
            signature=signature,
            timestamp=timestamp,
            algorithm=self.config.algorithm,
        )
        
    def _sign_hmac_sha256(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timestamp: int,
    ) -> str:
        """Generate HMAC-SHA256 signature."""
        string_to_sign = self._build_string_to_sign(url, method, headers, body, timestamp)
        signature = hmac.new(
            self.config.secret_key.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signature
        
    def _sign_hmac_sha512(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timestamp: int,
    ) -> str:
        """Generate HMAC-SHA512 signature."""
        string_to_sign = self._build_string_to_sign(url, method, headers, body, timestamp)
        signature = hmac.new(
            self.config.secret_key.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        return signature
        
    def _sign_aws_sig_v4(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timestamp: int,
    ) -> Tuple[str, Tuple[str, ...]]:
        """Generate AWS Signature Version 4."""
        parsed_url = urlparse(url)
        host = headers.get("host", parsed_url.netloc)
        date = time.strftime("%Y%m%d", time.gmtime(timestamp))
        datetime = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(timestamp))
        
        # Create canonical request
        canonical_uri = parsed_url.path or "/"
        canonical_querystring = parsed_url.query
        payload_hash = hashlib.sha256(body or b"").hexdigest()
        
        canonical_headers = f"host:{host}\nx-amz-date:{datetime}\n"
        signed_headers = "host;x-amz-date"
        
        canonical_request = "\n".join([
            method,
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])
        
        # Create string to sign
        credential_scope = f"{date}/us-east-1/execute-api/aws4_request"
        hashed_canonical = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            datetime,
            credential_scope,
            hashed_canonical,
        ])
        
        # Calculate signature
        k_date = hmac.new(f"AWS4{self.config.secret_key}".encode("utf-8"), date.encode("utf-8"), hashlib.sha256).digest()
        k_region = hmac.new(k_date, b"us-east-1", hashlib.sha256).digest()
        k_service = hmac.new(k_region, b"execute-api", hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        
        # Build authorization header
        auth_header = (
            f"AWS4-HMAC-SHA256 Credential={self.config.secret_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        
        return auth_header, (signed_headers,)
        
    def _build_string_to_sign(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timestamp: int,
    ) -> str:
        """Build the canonical string to sign."""
        parsed = urlparse(url)
        path = parsed.path or "/"
        query = parsed.query or ""
        
        # Sort and normalize headers
        sorted_headers = sorted(
            [(k.lower(), v) for k, v in headers.items() if k.lower() in self.config.include_headers],
            key=lambda x: x[0]
        )
        canonical_headers = "\n".join(f"{k}:{v}" for k, v in sorted_headers)
        
        # Body hash
        body_hash = hashlib.sha256(body or b"").hexdigest()
        
        return "\n".join([
            method.upper(),
            path,
            query,
            canonical_headers,
            str(timestamp),
            body_hash,
        ])
        
    def verify_signature(
        self,
        request: SignedRequest,
        expected_body: Optional[bytes] = None,
    ) -> bool:
        """
        Verify a signed request.
        
        Args:
            request: The signed request to verify.
            expected_body: Optional body for verification.
            
        Returns:
            True if signature is valid and timestamp is within TTL.
        """
        # Check timestamp freshness
        current_time = int(time.time())
        if abs(current_time - request.timestamp) > self.config.timestamp_ttl:
            logger.warning(f"Signature timestamp expired: {request.timestamp}")
            return False
            
        # Recalculate signature
        recalculated = self._sign_functions[request.algorithm](
            request.url,
            request.method,
            {k: v for k, v in request.headers.items() 
             if k.lower() not in (self.config.timestamp_header, self.config.signature_header)},
            request.body or expected_body,
            request.timestamp,
        )
        
        return hmac.compare_digest(recalculated, request.signature)
        

class SigningError(Exception):
    """Raised when signing fails."""
    pass


class VerificationError(Exception):
    """Raised when signature verification fails."""
    pass
