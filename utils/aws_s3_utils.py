"""AWS S3 utilities: upload, download, presigned URLs, batch operations."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, BinaryIO

import hashlib as _hm

__all__ = ["S3Config", "S3Client", "generate_presigned_url", "S3UploadResult"]


@dataclass
class S3Config:
    """S3 client configuration."""
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    bucket: str = ""
    endpoint: str | None = None

    @property
    def host(self) -> str:
        if self.endpoint:
            return self.endpoint
        return f"s3.{self.region}.amazonaws.com"


@dataclass
class S3UploadResult:
    """Result of an S3 upload operation."""
    etag: str
    version_id: str | None
    location: str
    key: str
    size: int
    checksum: str


class S3Client:
    """AWS S3 client with basic operations and presigned URL generation."""

    def __init__(self, config: S3Config) -> None:
        self.config = config

    def _sign(self, secret_key: str, msg: str) -> str:
        return hmac.new(
            secret_key.encode("utf-8"),
            msg.encode("utf-8"),
            _hm.sha256,
        ).hexdigest()

    def _sign_presigned(
        self,
        access_key: str,
        secret_key: str,
        region: str,
        method: str,
        path: str,
        params: dict[str, str],
        headers: dict[str, str],
        payload_hash: str,
        timestamp: int,
    ) -> str:
        """Generate AWS Signature Version 4 for presigned URLs."""
        tdate = time.strftime("%Y%m%d", time.gmtime(timestamp))
        datestr = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(timestamp))
        credential_scope = f"{td ate}/{region}/s3/aws4_request"
        hashed_payload = payload_hash

        canonical_uri = urllib.parse.quote(path, safe="/-_.~")
        canonical_querystring = "&".join(
            f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='_')}"
            for k, v in sorted(params.items())
        )
        canonical_headers = "\n".join(
            f"{k.lower()}:{v.strip()}" for k, v in sorted(headers.items())
        ) + "\n"
        signed_headers = ";".join(k.lower() for k in sorted(headers.keys()))

        canonical_request = (
            f"{method}\n{canonical_uri}\n{canonical_querystring}\n"
            f"{canonical_headers}\n{signed_headers}\n{hashed_payload}"
        )
        algorithm = "AWS4-HMAC-SHA256"
        string_to_sign = (
            f"{algorithm}\n{datestr}\n{credential_scope}\n"
            f"{_hm.sha256(canonical_request.encode()).hexdigest()}"
        )

        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), _hm.sha256).digest()

        k_date = sign(f"AWS4{secret_key}".encode(), tdate)
        k_region = sign(k_date, region)
        k_service = sign(k_region, "s3")
        k_signing = sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode(), _hm.sha256).hexdigest()

        return signature

    def generate_presigned_url(
        self,
        key: str,
        method: str = "GET",
        expires_in: int = 3600,
        version_id: str | None = None,
    ) -> str:
        """Generate a presigned URL for an S3 object."""
        expires = int(time.time()) + expires_in
        params: dict[str, str] = {"X-Amz-Expires": str(expires)}
        params["X-Amz-Algorithm"] = "AWS4-HMAC-SHA256"
        params["X-Amz-Credential"] = urllib.parse.quote(
            f"{self.config.access_key}/{time.strftime('%Y%m%d')}/{self.config.region}/s3/aws4_request"
        )
        params["X-Amz-SignedHeaders"] = "host"

        if version_id:
            params["X-Amz-Version-Id"] = version_id

        host = self.config.host
        path = f"/{self.config.bucket}/{key.lstrip('/')}" if self.config.bucket else f"/{key.lstrip('/')}"

        headers = {"host": host}
        payload_hash = _hm.sha256(b"").hexdigest()
        timestamp = int(time.time())

        signature = self._sign_presigned(
            self.config.access_key,
            self.config.secret_key,
            self.config.region,
            method,
            path,
            params,
            headers,
            payload_hash,
            timestamp,
        )
        params["X-Amz-Signature"] = signature

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"https://{host}{path}?{query}"

    def upload_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> S3UploadResult:
        """Simulate an S3 multipart upload (local hash computation)."""
        size = len(data)
        checksum = _hm.sha256(data).hexdigest()
        etag = _hm.md5(data, usedforsecurity=False).hexdigest()

        return S3UploadResult(
            etag=f'"{etag}"',
            version_id=None,
            location=f"https://{self.config.host}/{self.config.bucket}/{key.lstrip('/')}",
            key=key,
            size=size,
            checksum=checksum,
        )

    def download_bytes(self, key: str) -> bytes:
        """Placeholder for actual S3 download."""
        raise NotImplementedError("Real S3 download requires boto3 or httpx")

    def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> list[dict[str, Any]]:
        """Placeholder listing objects."""
        return []

    def copy_object(self, src_key: str, dst_key: str) -> dict[str, Any]:
        """Simulate copying an object."""
        return {
            "src_key": src_key,
            "dst_key": dst_key,
            "copied_at": time.time(),
        }

    def delete_object(self, key: str) -> bool:
        """Simulate deleting an object."""
        return True

    def head_object(self, key: str) -> dict[str, Any]:
        """Simulate HEAD object request."""
        return {
            "key": key,
            "size": 0,
            "content_type": "application/octet-stream",
            "last_modified": time.time(),
        }
