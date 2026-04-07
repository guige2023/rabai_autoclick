"""Google Cloud utilities: GCS operations, GCE management, and BigQuery queries."""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any

__all__ = [
    "GCSConfig",
    "GCSClient",
    "GCEConfig",
    "GCEClient",
    "BigQueryConfig",
    "BigQueryClient",
]


@dataclass
class GCSConfig:
    """Google Cloud Storage configuration."""

    project_id: str = ""
    bucket_name: str = ""
    credentials_path: str | None = None


@dataclass
class GCEConfig:
    """Google Compute Engine configuration."""

    project_id: str = ""
    zone: str = "us-central1-a"
    credentials_path: str | None = None


@dataclass
class BigQueryConfig:
    """BigQuery configuration."""

    project_id: str = ""
    dataset: str = ""
    credentials_path: str | None = None


class GCSClient:
    """Google Cloud Storage client wrapper."""

    def __init__(self, config: GCSConfig | None = None) -> None:
        self.config = config or GCSConfig()

    def _get_client(self) -> Any:
        try:
            from google.cloud import storage
            kwargs = {"project": self.config.project_id}
            if self.config.credentials_path:
                kwargs["credentials"] = self.config.credentials_path
            return storage.Client(**kwargs)
        except ImportError:
            return None

    def upload_bytes(
        self,
        data: bytes,
        destination: str,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """Upload bytes to a GCS object."""
        client = self._get_client()
        if client is None:
            return False
        bucket = client.bucket(self.config.bucket_name)
        blob = bucket.blob(destination)
        blob.upload_from_string(data, content_type=content_type)
        return True

    def download_bytes(self, source: str) -> bytes | None:
        """Download bytes from a GCS object."""
        client = self._get_client()
        if client is None:
            return None
        bucket = client.bucket(self.config.bucket_name)
        blob = bucket.blob(source)
        return blob.download_as_bytes()

    def list_blobs(self, prefix: str = "", max_results: int = 100) -> list[str]:
        """List blobs in the bucket with optional prefix."""
        client = self._get_client()
        if client is None:
            return []
        bucket = client.bucket(self.config.bucket_name)
        blobs = bucket.list_blobs(prefix=prefix, max_results=max_results)
        return [b.name for b in blobs]

    def delete_blob(self, source: str) -> bool:
        """Delete a blob from GCS."""
        client = self._get_client()
        if client is None:
            return False
        bucket = client.bucket(self.config.bucket_name)
        blob = bucket.blob(source)
        blob.delete()
        return True

    def generate_signed_url(
        self,
        source: str,
        expiration_minutes: int = 60,
    ) -> str | None:
        """Generate a signed URL for a GCS object."""
        client = self._get_client()
        if client is None:
            return None
        bucket = client.bucket(self.config.bucket_name)
        blob = bucket.blob(source)
        url = blob.generate_signed_url(
            expiration=time.time() + expiration_minutes * 60,
            version="v4",
        )
        return url


class GCEClient:
    """Google Compute Engine client wrapper."""

    def __init__(self, config: GCEConfig | None = None) -> None:
        self.config = config or GCEConfig()

    def _get_client(self) -> Any:
        try:
            from google.cloud import compute_v1
            return compute_v1
        except ImportError:
            return None

    def list_instances(self) -> list[dict[str, Any]]:
        """List all GCE instances in the project and zone."""
        compute = self._get_client()
        if compute is None:
            return []
        manager = compute.InstancesClient()
        request = compute.ListInstancesRequest(
            project=self.config.project_id,
            zone=self.config.zone,
        )
        response = manager.list(request=request)
        return [
            {
                "name": i.name,
                "status": i.status,
                "machine_type": i.machine_type,
                "internal_ip": i.network_interfaces[0].network_iap.tunnel_intelligence_tunnel_properties.udpg_transport_protocol if hasattr(i, "network_interfaces") and i.network_interfaces else None,
            }
            for i in response.items
        ]

    def start_instance(self, instance_name: str) -> bool:
        """Start a GCE instance."""
        compute = self._get_client()
        if compute is None:
            return False
        manager = compute.InstancesClient()
        request = compute.StartInstanceRequest(
            project=self.config.project_id,
            zone=self.config.zone,
            instance=instance_name,
        )
        manager.start(request=request)
        return True

    def stop_instance(self, instance_name: str) -> bool:
        """Stop a GCE instance."""
        compute = self._get_client()
        if compute is None:
            return False
        manager = compute.InstancesClient()
        request = compute.StopInstanceRequest(
            project=self.config.project_id,
            zone=self.config.zone,
            instance=instance_name,
        )
        manager.stop(request=request)
        return True


class BigQueryClient:
    """BigQuery client wrapper for running queries."""

    def __init__(self, config: BigQueryConfig | None = None) -> None:
        self.config = config or BigQueryConfig()

    def _get_client(self) -> Any:
        try:
            from google.cloud import bigquery
            kwargs = {"project": self.config.project_id}
            if self.config.credentials_path:
                kwargs["credentials"] = self.config.credentials_path
            return bigquery.Client(**kwargs)
        except ImportError:
            return None

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> list[dict[str, Any]]:
        """Execute a BigQuery SQL query and return results."""
        client = self._get_client()
        if client is None:
            return []
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", str(v))
                for k, v in params.items()
            ]
        query_job = client.query(sql, job_config=job_config)
        return [dict(row) for row in query_job.result(timeout=timeout)]

    def insert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
    ) -> bool:
        """Insert rows into a BigQuery table."""
        client = self._get_client()
        if client is None:
            return False
        table_ref = f"{self.config.project_id}.{self.config.dataset}.{table}"
        errors = client.insert_rows_json(table_ref, rows)
        return len(errors) == 0
