"""Cloudflare utilities: DNS management, CDN purging, and worker deployment."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

__all__ = [
    "CloudflareConfig",
    "CloudflareDNS",
    "CloudflareCDN",
    "CloudflareWorker",
]


@dataclass
class CloudflareConfig:
    """Cloudflare API configuration."""

    api_email: str = ""
    api_key: str = ""
    zone_id: str = ""
    account_id: str = ""


class CloudflareDNS:
    """Cloudflare DNS management."""

    BASE_URL = "https://api.cloudflare.com/client/v4"

    def __init__(self, config: CloudflareConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated Cloudflare API request."""
        import urllib.request
        import urllib.error

        url = f"{self.BASE_URL}{path}"
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(
            url,
            data=body,
            headers=self._headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return {"success": False, "errors": [e.read().decode()]}

    def list_records(self, name: str | None = None, type_: str | None = None) -> list[dict[str, Any]]:
        """List DNS records."""
        path = f"/zones/{self.config.zone_id}/dns_records"
        params = []
        if name:
            params.append(f"name={name}")
        if type_:
            params.append(f"type={type_}")
        if params:
            path += "?" + "&".join(params)
        result = self._request("GET", path)
        return result.get("result", []) if result.get("success") else []

    def add_record(
        self,
        name: str,
        type_: str,
        content: str,
        ttl: int = 300,
        proxied: bool = False,
    ) -> dict[str, Any]:
        """Add a DNS record."""
        path = f"/zones/{self.config.zone_id}/dns_records"
        data = {
            "name": name,
            "type": type_,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        }
        result = self._request("POST", path, data)
        return result.get("result", {}) if result.get("success") else {}

    def update_record(
        self,
        record_id: str,
        content: str,
        ttl: int = 300,
        proxied: bool = False,
    ) -> dict[str, Any]:
        """Update an existing DNS record."""
        path = f"/zones/{self.config.zone_id}/dns_records/{record_id}"
        data = {"content": content, "ttl": ttl, "proxied": proxied}
        result = self._request("PUT", path, data)
        return result.get("result", {}) if result.get("success") else {}

    def delete_record(self, record_id: str) -> bool:
        """Delete a DNS record."""
        path = f"/zones/{self.config.zone_id}/dns_records/{record_id}"
        result = self._request("DELETE", path)
        return result.get("success", False)


class CloudflareCDN:
    """Cloudflare CDN cache management."""

    def __init__(self, config: CloudflareConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import urllib.request
        import urllib.error

        url = f"{CloudflareDNS.BASE_URL}{path}"
        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=self._headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return {"success": False, "errors": [e.read().decode()]}

    def purge_all(self) -> bool:
        """Purge entire CDN cache."""
        path = f"/zones/{self.config.zone_id}/cache_purge"
        result = self._request("POST", path, {"purge_everything": True})
        return result.get("success", False)

    def purge_files(self, files: list[str]) -> bool:
        """Purge specific files from CDN cache."""
        path = f"/zones/{self.config.zone_id}/cache_purge"
        result = self._request("POST", path, {"files": files})
        return result.get("success", False)

    def purge_by_tag(self, tags: list[str]) -> bool:
        """Purge cache by cache tags."""
        path = f"/zones/{self.config.zone_id}/cache_purge"
        result = self._request("POST", path, {"tags": tags})
        return result.get("success", False)


class CloudflareWorker:
    """Cloudflare Workers deployment."""

    def __init__(self, config: CloudflareConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/javascript",
        }

    def _request(
        self,
        method: str,
        path: str,
        data: str | None = None,
    ) -> dict[str, Any]:
        import urllib.request
        import urllib.error

        url = f"https://api.cloudflare.com/client/v4{path}"
        body = data.encode() if data else None
        req = urllib.request.Request(url, data=body, headers=self._headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return {"success": False, "errors": [e.read().decode()]}

    def deploy_script(self, script_name: str, script_content: str) -> bool:
        """Deploy a Worker script."""
        path = f"/accounts/{self.config.account_id}/workers/scripts/{script_name}"
        result = self._request("PUT", path, script_content)
        return result.get("success", False)

    def get_script(self, script_name: str) -> str | None:
        """Get Worker script content."""
        path = f"/accounts/{self.config.account_id}/workers/scripts/{script_name}"
        result = self._request("GET", path)
        return result.get("result", {}).get("script") if result.get("success") else None

    def delete_script(self, script_name: str) -> bool:
        """Delete a Worker script."""
        path = f"/accounts/{self.config.account_id}/workers/scripts/{script_name}"
        result = self._request("DELETE", path)
        return result.get("success", False)
