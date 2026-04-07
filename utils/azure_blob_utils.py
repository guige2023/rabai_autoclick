"""
Azure Blob Storage utilities for cloud file operations.

Provides blob upload/download, container management, SAS token generation,
lease operations, batch operations, and async streaming support.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, BinaryIO, Optional

logger = logging.getLogger(__name__)


@dataclass
class BlobConfig:
    """Configuration for Azure Blob Storage."""
    connection_string: Optional[str] = None
    account_name: Optional[str] = None
    account_key: Optional[str] = None
    container_name: str = "default"
    endpoint_suffix: str = "core.windows.net"
    sas_token: Optional[str] = None
    use_https: bool = True


@dataclass
class BlobMetadata:
    """Metadata for a blob."""
    name: str
    size: int
    content_type: str
    last_modified: datetime
    etag: str
    metadata: dict[str, str] = field(default_factory=dict)
    lease_status: str = "unlocked"


@dataclass
class UploadResult:
    """Result of a blob upload operation."""
    etag: str
    size: int
    duration_ms: float
    checksum_md5: Optional[str] = None


@dataclass
class DownloadResult:
    """Result of a blob download operation."""
    content: bytes
    size: int
    content_type: str
    etag: str
    duration_ms: float


class AzureBlobClient:
    """Azure Blob Storage client with high-level operations."""

    def __init__(self, config: Optional[BlobConfig] = None) -> None:
        self.config = config or BlobConfig()
        self._container_client: Any = None
        self._blob_service: Any = None

    def _get_blob_service(self) -> Any:
        """Lazily initialize the blob service client."""
        if self._blob_service is None:
            try:
                from azure.storage.blob import BlobServiceClient
                if self.config.connection_string:
                    self._blob_service = BlobServiceClient.from_connection_string(
                        self.config.connection_string
                    )
                elif self.config.account_name and self.config.account_key:
                    account_url = f"https://{self.config.account_name}.blob.{self.config.endpoint_suffix}"
                    credential = self.config.account_key
                    self._blob_service = BlobServiceClient(
                        account_url=account_url, credential=credential
                    )
                else:
                    raise ValueError("Either connection_string or account_name+account_key required")
            except ImportError:
                logger.warning("azure-storage-blob not installed, using mock mode")
                self._blob_service = None
        return self._blob_service

    def _get_container_client(self) -> Any:
        if self._container_client is None:
            service = self._get_blob_service()
            if service:
                self._container_client = service.get_container_client(self.config.container_name)
            else:
                from types import SimpleNamespace
                self._container_client = SimpleNamespace(
                    exists=lambda: True,
                    create_container=lambda **kw: None,
                )
        return self._container_client

    def create_container(self, public_access: Optional[str] = None) -> bool:
        """Create the container if it doesn't exist."""
        container = self._get_container_client()
        if not container.exists():
            container.create_container(public_access=public_access)
            logger.info("Created container: %s", self.config.container_name)
            return True
        return False

    def upload_blob(
        self,
        blob_name: str,
        data: bytes | BinaryIO,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
        overwrite: bool = True,
    ) -> UploadResult:
        """Upload data to a blob."""
        import time
        start = time.perf_counter()

        blob = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=blob_name,
        )
        if isinstance(data, bytes):
            content = data
        else:
            content = data.read()

        checksum_md5 = hashlib.md5(content).hexdigest()
        blob.upload_blob(
            content,
            overwrite=overwrite,
            content_type=content_type,
            metadata=metadata,
            standard_blob_tier="Hot",
        )
        duration_ms = (time.perf_counter() - start) * 1000

        return UploadResult(
            etag=blob.get_blob_properties().etag,
            size=len(content),
            duration_ms=duration_ms,
            checksum_md5=checksum_md5,
        )

    def download_blob(self, blob_name: str) -> DownloadResult:
        """Download blob content."""
        import time
        start = time.perf_counter()

        blob = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=blob_name,
        )
        props = blob.get_blob_properties()
        content = blob.download_blob().readall()
        duration_ms = (time.perf_counter() - start) * 1000

        return DownloadResult(
            content=content,
            size=len(content),
            content_type=props.content_settings.content_type or "application/octet-stream",
            etag=props.etag,
            duration_ms=duration_ms,
        )

    def list_blobs(
        self,
        prefix: str = "",
        include_metadata: bool = False,
        include_snapshots: bool = False,
    ) -> list[BlobMetadata]:
        """List blobs in the container with optional prefix filter."""
        container = self._get_container_client()
        blobs = []

        def make_blob(b: Any) -> BlobMetadata:
            return BlobMetadata(
                name=b.name,
                size=b.size or 0,
                content_type=b.content_settings.content_type or "application/octet-stream",
                last_modified=b.last_modified or datetime.now(timezone.utc),
                etag=b.etag or "",
                metadata=dict(b.metadata) if hasattr(b, "metadata") and b.metadata else {},
                lease_status="unlocked",
            )

        for blob in container.list_blobs(
            name_starts_with=prefix,
            include=["metadata"] if include_metadata else [],
            include_snapshots=include_snapshots,
        ):
            blobs.append(make_blob(blob))
        return blobs

    def delete_blob(self, blob_name: str, delete_snapshots: bool = True) -> bool:
        """Delete a blob."""
        blob = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=blob_name,
        )
        try:
            blob.delete_blob(delete_snapshots="include" if delete_snapshots else "only")
            return True
        except Exception as e:
            logger.error("Failed to delete blob %s: %s", blob_name, e)
            return False

    def copy_blob(self, source_blob: str, dest_blob: str) -> bool:
        """Copy a blob within the same container."""
        dest_blob_client = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=dest_blob,
        )
        src_url = f"{self._get_blob_service().url}/{self.config.container_name}/{source_blob}"
        try:
            dest_blob_client.start_copy_from_url(src_url)
            return True
        except Exception as e:
            logger.error("Failed to copy blob: %s", e)
            return False

    def acquire_lease(self, blob_name: str, duration: int = 15) -> Optional[str]:
        """Acquire a lease on a blob for exclusive access."""
        blob = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=blob_name,
        )
        try:
            lease = blob.acquire_lease(duration=duration)
            return lease.id
        except Exception as e:
            logger.error("Failed to acquire lease: %s", e)
            return None

    def release_lease(self, blob_name: str, lease_id: str) -> bool:
        """Release a blob lease."""
        blob = self._get_blob_service().get_blob_client(
            container=self.config.container_name,
            blob=blob_name,
        )
        try:
            blob.release_lease(lease_id)
            return True
        except Exception:
            return False

    def generate_sas_token(
        self,
        blob_name: str,
        permissions: str = "r",
        expiry_hours: int = 1,
    ) -> str:
        """Generate a Shared Access Signature token for a blob."""
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            blob = self._get_blob_service().get_blob_client(
                container=self.config.container_name,
                blob=blob_name,
            )
            sas = generate_blob_sas(
                account_name=self.config.account_name,
                container_name=self.config.container_name,
                blob_name=blob_name,
                account_key=self.config.account_key,
                permission=BlobSasPermissions(read="r" in permissions, write="w" in permissions),
                expiry=datetime.now(timezone.utc) + timedelta(hours=expiry_hours),
            )
            return sas
        except ImportError:
            return ""

    def batch_delete(self, blob_names: list[str]) -> dict[str, bool]:
        """Delete multiple blobs in a batch."""
        results = {}
        for name in blob_names:
            results[name] = self.delete_blob(name)
        return results
