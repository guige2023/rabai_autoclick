"""API signature action for request signing and verification.

Implements HMAC and other signature schemes for API
request authentication and integrity verification.
"""

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SignatureAlgorithm(Enum):
    """Supported signature algorithms."""
    HMAC_SHA256 = "hmac_sha256"
    HMAC_SHA512 = "hmac_sha512"
    AWS_V4 = "aws_v4"
    PLAINTEXT = "plaintext"


@dataclass
class SignatureConfig:
    """Configuration for request signing."""
    algorithm: SignatureAlgorithm
    secret_key: str
    include_timestamp: bool = True
    include_headers: list[str] = None
    timestamp_tolerance_seconds: int = 300


@dataclass
class SignedRequest:
    """A signed API request."""
    method: str
    url: str
    headers: dict[str, str]
    body: Optional[str]
    signature: str
    timestamp: float


@dataclass
class VerificationResult:
    """Result of signature verification."""
    valid: bool
    error: Optional[str] = None
    timestamp_age_seconds: float = 0.0


class APISignatureAction:
    """Sign and verify API requests.

    Example:
        >>> signer = APISignatureAction(algorithm=SignatureAlgorithm.HMAC_SHA256)
        >>> signed = signer.sign_request("GET", "/api/data", {}, None, "secret")
        >>> result = signer.verify_request(signed, "secret")
    """

    def __init__(
        self,
        algorithm: SignatureAlgorithm = SignatureAlgorithm.HMAC_SHA256,
        secret_key: str = "",
    ) -> None:
        self.algorithm = algorithm
        self._secret_key = secret_key

    def sign_request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: Optional[str],
        secret_key: Optional[str] = None,
        timestamp: Optional[float] = None,
    ) -> SignedRequest:
        """Sign an API request.

        Args:
            method: HTTP method.
            url: Request URL.
            headers: Request headers.
            body: Request body.
            secret_key: Secret key (uses default if not provided).
            timestamp: Request timestamp.

        Returns:
            Signed request object.
        """
        key = secret_key or self._secret_key
        ts = timestamp or time.time()

        headers = headers.copy()
        if self.algorithm != SignatureAlgorithm.PLAINTEXT:
            headers["X-Timestamp"] = str(int(ts))

        string_to_sign = self._build_string_to_sign(method, url, headers, body, ts)
        signature = self._compute_signature(string_to_sign, key)

        headers["X-Signature"] = signature

        return SignedRequest(
            method=method,
            url=url,
            headers=headers,
            body=body,
            signature=signature,
            timestamp=ts,
        )

    def verify_request(
        self,
        signed_request: SignedRequest,
        secret_key: Optional[str] = None,
        tolerance_seconds: int = 300,
    ) -> VerificationResult:
        """Verify a signed request.

        Args:
            signed_request: Signed request to verify.
            secret_key: Secret key (uses default if not provided).
            tolerance_seconds: Max age of timestamp.

        Returns:
            Verification result.
        """
        key = secret_key or self._secret_key

        if self.algorithm != SignatureAlgorithm.PLAINTEXT:
            ts_str = signed_request.headers.get("X-Timestamp")
            if not ts_str:
                return VerificationResult(valid=False, error="Missing timestamp")

            try:
                ts = float(ts_str)
            except ValueError:
                return VerificationResult(valid=False, error="Invalid timestamp")

            age = time.time() - ts
            if age > tolerance_seconds or age < -60:
                return VerificationResult(
                    valid=False,
                    error=f"Timestamp out of range: {age}s",
                    timestamp_age_seconds=age,
                )

        provided_sig = signed_request.headers.get("X-Signature", "")
        if not provided_sig:
            return VerificationResult(valid=False, error="Missing signature")

        expected_sig = self._compute_signature(
            self._build_string_to_sign(
                signed_request.method,
                signed_request.url,
                signed_request.headers,
                signed_request.body,
                signed_request.timestamp,
            ),
            key,
        )

        if not hmac.compare_digest(provided_sig, expected_sig):
            return VerificationResult(valid=False, error="Signature mismatch")

        return VerificationResult(
            valid=True,
            timestamp_age_seconds=time.time() - signed_request.timestamp,
        )

    def _build_string_to_sign(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: Optional[str],
        timestamp: float,
    ) -> str:
        """Build the string to sign.

        Args:
            method: HTTP method.
            url: Request URL.
            headers: Request headers.
            body: Request body.
            timestamp: Request timestamp.

        Returns:
            String to sign.
        """
        if self.algorithm == SignatureAlgorithm.HMAC_SHA256:
            parts = [
                method.upper(),
                url,
                str(int(timestamp)),
            ]
            if body:
                parts.append(self._hash_body(body))
            return "\n".join(parts)

        elif self.algorithm == SignatureAlgorithm.HMAC_SHA512:
            parts = [
                method.upper(),
                url,
                str(int(timestamp)),
            ]
            if body:
                parts.append(self._hash_body_sha512(body))
            return "\n".join(parts)

        elif self.algorithm == SignatureAlgorithm.AWS_V4:
            return self._build_aws_v4_string(method, url, headers, body, timestamp)

        return f"{method}:{url}:{body or ''}"

    def _compute_signature(self, string_to_sign: str, secret_key: str) -> str:
        """Compute signature for string.

        Args:
            string_to_sign: String to sign.
            secret_key: Secret key.

        Returns:
            Signature.
        """
        key_bytes = secret_key.encode("utf-8")
        data_bytes = string_to_sign.encode("utf-8")

        if self.algorithm in (SignatureAlgorithm.HMAC_SHA256, SignatureAlgorithm.AWS_V4):
            return hmac.new(key_bytes, data_bytes, hashlib.sha256).hexdigest()
        elif self.algorithm == SignatureAlgorithm.HMAC_SHA512:
            return hmac.new(key_bytes, data_bytes, hashlib.sha512).hexdigest()

        return string_to_sign

    def _hash_body(self, body: str) -> str:
        """Hash request body.

        Args:
            body: Request body.

        Returns:
            Body hash (SHA256 hex).
        """
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _hash_body_sha512(self, body: str) -> str:
        """Hash request body with SHA512.

        Args:
            body: Request body.

        Returns:
            Body hash (SHA512 hex).
        """
        return hashlib.sha512(body.encode("utf-8")).hexdigest()

    def _build_aws_v4_string(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: Optional[str],
        timestamp: float,
    ) -> str:
        """Build AWS V4 signing string.

        Args:
            method: HTTP method.
            url: Request URL.
            headers: Request headers.
            body: Request body.
            timestamp: Request timestamp.

        Returns:
            AWS V4 canonical request string.
        """
        date = time.strftime("%Y%m%d", time.gmtime(timestamp))
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(timestamp))

        canonical_uri = url.split("?")[0] if "?" in url else url
        canonical_querystring = url.split("?")[1] if "?" in url else ""

        signed_headers = ";".join([
            h.lower() for h in sorted(headers.keys())
        ])

        payload_hash = self._hash_body(body) if body else self._hash_body("")

        canonical_headers = "\n".join([
            f"{h.lower()}:{headers[h].strip()}"
            for h in sorted(headers.keys())
        ]) + "\n"

        canonical_request = "\n".join([
            method.upper(),
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        return canonical_request
