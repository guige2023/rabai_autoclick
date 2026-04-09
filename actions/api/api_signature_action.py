"""API Signature Action Module.

Provides request signing for API operations including
HMAC signatures, OAuth tokens, and API key management.

Example:
    >>> from actions.api.api_signature_action import APISignatureAction
    >>> action = APISignatureAction(secret_key="my_secret")
    >>> signed = action.sign_request({"method": "GET", "url": "/api"})
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
import base64
import threading
import uuid


class SignatureAlgorithm(Enum):
    """Signature algorithm types."""
    HMAC_SHA256 = "hmac_sha256"
    HMAC_SHA512 = "hmac_sha512"
    PLAINTEXT = "plaintext"


class SignatureComponent(Enum):
    """Components to include in signature."""
    METHOD = "method"
    URL = "url"
    TIMESTAMP = "timestamp"
    BODY = "body"
    HEADERS = "headers"
    NONCE = "nonce"


@dataclass
class SignatureConfig:
    """Configuration for request signing.
    
    Attributes:
        algorithm: Signature algorithm
        include_components: Components to include
        timestamp_tolerance: Timestamp tolerance in seconds
        nonce_ttl: Nonce time-to-live in seconds
        header_prefix: Prefix for signature headers
    """
    algorithm: SignatureAlgorithm = SignatureAlgorithm.HMAC_SHA256
    include_components: list = field(default_factory=lambda: [
        SignatureComponent.METHOD,
        SignatureComponent.URL,
        SignatureComponent.TIMESTAMP,
    ])
    timestamp_tolerance: float = 300.0
    nonce_ttl: float = 60.0
    header_prefix: str = "X-Signature"


@dataclass
class SignedRequest:
    """Signed request with signature details.
    
    Attributes:
        headers: Request headers including signature
        signature: The signature value
        timestamp: Request timestamp
        nonce: Request nonce
    """
    headers: Dict[str, str]
    signature: str
    timestamp: float
    nonce: str
    body_hash: Optional[str] = None


class APISignatureAction:
    """Request signer for API operations.
    
    Provides cryptographic signing of API requests using
    HMAC algorithms with various component combinations.
    
    Attributes:
        config: Signature configuration
        secret_key: Secret key for signing
        _used_nonces: Set of used nonces
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        secret_key: str,
        config: Optional[SignatureConfig] = None,
    ) -> None:
        """Initialize signature action.
        
        Args:
            secret_key: Secret key for signing
            config: Signature configuration
        """
        self.secret_key = secret_key
        self.config = config or SignatureConfig()
        self._used_nonces: Dict[str, float] = {}
        self._lock = threading.RLock()
    
    def sign_request(
        self,
        request: Dict[str, Any],
        body: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> SignedRequest:
        """Sign an API request.
        
        Args:
            request: Request dictionary with method, url, etc.
            body: Request body string
            headers: Additional headers
        
        Returns:
            SignedRequest with signature headers
        """
        timestamp = time.time()
        nonce = self._generate_nonce()
        headers = headers or {}
        
        body_hash = None
        if body and SignatureComponent.BODY in self.config.include_components:
            body_hash = self._hash_body(body)
        
        string_to_sign = self._build_string_to_sign(
            request, timestamp, nonce, body_hash
        )
        
        signature = self._compute_signature(string_to_sign)
        
        prefix = self.config.header_prefix
        signature_headers = {
            f"{prefix}-Algorithm": self.config.algorithm.value,
            f"{prefix}-Timestamp": str(int(timestamp)),
            f"{prefix}-Nonce": nonce,
            f"{prefix}-Signature": signature,
        }
        
        if body_hash:
            signature_headers[f"{prefix}-Body-Hash"] = body_hash
        
        all_headers = {**headers, **signature_headers}
        
        return SignedRequest(
            headers=all_headers,
            signature=signature,
            timestamp=timestamp,
            nonce=nonce,
            body_hash=body_hash,
        )
    
    def verify_request(
        self,
        request: Dict[str, Any],
        signature: str,
        timestamp: float,
        nonce: str,
        body: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Verify a signed API request.
        
        Args:
            request: Request dictionary
            signature: Signature to verify
            timestamp: Request timestamp
            nonce: Request nonce
            body: Request body
            headers: Request headers
        
        Returns:
            True if signature is valid
        """
        if not self._validate_timestamp(timestamp):
            return False
        
        if not self._validate_nonce(nonce):
            return False
        
        body_hash = None
        if body and SignatureComponent.BODY in self.config.include_components:
            body_hash = self._hash_body(body)
        
        string_to_sign = self._build_string_to_sign(
            request, timestamp, nonce, body_hash
        )
        
        expected_signature = self._compute_signature(string_to_sign)
        
        return hmac.compare_digest(signature, expected_signature)
    
    def _build_string_to_sign(
        self,
        request: Dict[str, Any],
        timestamp: float,
        nonce: str,
        body_hash: Optional[str],
    ) -> str:
        """Build string to sign.
        
        Args:
            request: Request dictionary
            timestamp: Timestamp
            nonce: Nonce
            body_hash: Body hash
        
        Returns:
            String to sign
        """
        parts = []
        
        for component in self.config.include_components:
            if component == SignatureComponent.METHOD:
                parts.append(request.get("method", "GET").upper())
            elif component == SignatureComponent.URL:
                parts.append(request.get("url", "/"))
            elif component == SignatureComponent.TIMESTAMP:
                parts.append(str(int(timestamp)))
            elif component == SignatureComponent.BODY:
                if body_hash:
                    parts.append(body_hash)
            elif component == SignatureComponent.NONCE:
                parts.append(nonce)
        
        return "\n".join(parts)
    
    def _compute_signature(self, string_to_sign: str) -> str:
        """Compute signature from string.
        
        Args:
            string_to_sign: String to sign
        
        Returns:
            Signature string
        """
        if self.config.algorithm == SignatureAlgorithm.HMAC_SHA256:
            digest = hashlib.sha256
        elif self.config.algorithm == SignatureAlgorithm.HMAC_SHA512:
            digest = hashlib.sha512
        else:
            return string_to_sign
        
        signature = hmac.new(
            self.secret_key.encode(),
            string_to_sign.encode(),
            digest,
        ).digest()
        
        return base64.b64encode(signature).decode()
    
    def _hash_body(self, body: str) -> str:
        """Hash request body.
        
        Args:
            body: Request body
        
        Returns:
            Body hash
        """
        return hashlib.sha256(body.encode()).hexdigest()
    
    def _generate_nonce(self) -> str:
        """Generate unique nonce.
        
        Returns:
            Nonce string
        """
        return str(uuid.uuid4())
    
    def _validate_timestamp(self, timestamp: float) -> bool:
        """Validate request timestamp.
        
        Args:
            timestamp: Request timestamp
        
        Returns:
            True if valid
        """
        current_time = time.time()
        return abs(current_time - timestamp) <= self.config.timestamp_tolerance
    
    def _validate_nonce(self, nonce: str) -> bool:
        """Validate nonce (prevent replay).
        
        Args:
            nonce: Nonce to validate
        
        Returns:
            True if valid
        """
        with self._lock:
            current_time = time.time()
            
            if nonce in self._used_nonces:
                return False
            
            self._used_nonces[nonce] = current_time
            
            expired = [
                n for n, t in self._used_nonces.items()
                if current_time - t > self.config.nonce_ttl
            ]
            for n in expired:
                del self._used_nonces[n]
            
            return True
    
    def sign_url(
        self,
        url: str,
        expires_in: Optional[float] = None,
    ) -> str:
        """Sign a URL with query parameters.
        
        Args:
            url: URL to sign
            expires_in: Expiration time in seconds
        
        Returns:
            Signed URL
        """
        import urllib.parse
        
        parsed = urllib.parse.urlparse(url)
        
        expires = str(int(time.time() + (expires_in or self.config.timestamp_tolerance)))
        
        string_to_sign = f"{parsed.path}\n{expires}"
        signature = self._compute_signature(string_to_sign)
        
        params = urllib.parse.parse_qsl(parsed.query)
        params.extend([
            ("expires", expires),
            ("signature", signature),
        ])
        
        new_query = urllib.parse.urlencode(params)
        new_url = parsed._replace(query=new_query).geturl()
        
        return new_url
