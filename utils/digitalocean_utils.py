"""DigitalOcean utilities: Droplet management, DNS, and storage operations."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

__all__ = [
    "DOConfig",
    "DODroplet",
    "DODNS",
    "DOStorage",
]


@dataclass
class DOConfig:
    """DigitalOcean API configuration."""

    api_token: str = ""
    endpoint: str = "https://api.digitalocean.com/v2"


class DODroplet:
    """DigitalOcean Droplet management."""

    BASE_URL = "https://api.digitalocean.com/v2"

    def __init__(self, config: DOConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_token}",
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
            return {"error": e.read().decode()}

    def list(self) -> list[dict[str, Any]]:
        result = self._request("GET", "/droplets")
        return result.get("droplets", [])

    def create(
        self,
        name: str,
        size: str = "s-1vcpu-1gb",
        region: str = "nyc1",
        image: str = "ubuntu-22-04-x64",
        ssh_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        data = {
            "name": name,
            "size": size,
            "region": region,
            "image": image,
            "ssh_keys": ssh_keys or [],
        }
        result = self._request("POST", "/droplets", data)
        return result.get("droplet", {})

    def get(self, droplet_id: int) -> dict[str, Any]:
        result = self._request("GET", f"/droplets/{droplet_id}")
        return result.get("droplet", {})

    def delete(self, droplet_id: int) -> bool:
        result = self._request("DELETE", f"/droplets/{droplet_id}")
        return result.get("status") == "destroyed"

    def power_on(self, droplet_id: int) -> bool:
        result = self._request("POST", f"/droplets/{droplet_id}/actions", {"type": "power_on"})
        return "action" in result

    def power_off(self, droplet_id: int) -> bool:
        result = self._request("POST", f"/droplets/{droplet_id}/actions", {"type": "power_off"})
        return "action" in result

    def reboot(self, droplet_id: int) -> bool:
        result = self._request("POST", f"/droplets/{droplet_id}/actions", {"type": "reboot"})
        return "action" in result

    def snapshots(self, droplet_id: int) -> list[dict[str, Any]]:
        result = self._request("GET", f"/droplets/{droplet_id}/snapshots")
        return result.get("snapshots", [])


class DODNS:
    """DigitalOcean DNS management."""

    def __init__(self, config: DOConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_token}",
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
            return {"error": e.read().decode()}

    def list_records(self, domain: str) -> list[dict[str, Any]]:
        result = self._request("GET", f"/domains/{domain}/records")
        return result.get("domain_records", [])

    def create_record(
        self,
        domain: str,
        name: str,
        type_: str,
        value: str,
        priority: int | None = None,
        port: int | None = None,
        ttl: int = 3600,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "name": name,
            "type": type_,
            "value": value,
            "ttl": ttl,
        }
        if priority is not None:
            data["priority"] = priority
        if port is not None:
            data["port"] = port
        result = self._request("POST", f"/domains/{domain}/records", data)
        return result.get("domain_record", {})

    def delete_record(self, domain: str, record_id: int) -> bool:
        result = self._request("DELETE", f"/domains/{domain}/records/{record_id}")
        return "domain_record" in result or result.get("status") == "destroyed"


class DOStorage:
    """DigitalOcean Block Storage management."""

    def __init__(self, config: DOConfig) -> None:
        self.config = config
        self._headers = {
            "Authorization": f"Bearer {config.api_token}",
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
            return {"error": e.read().decode()}

    def create_volume(
        self,
        name: str,
        region: str = "nyc1",
        size_gigabytes: int = 10,
        description: str = "",
    ) -> dict[str, Any]:
        data = {
            "name": name,
            "region": region,
            "size_gigabytes": size_gigabytes,
            "description": description,
        }
        result = self._request("POST", "/volumes", data)
        return result.get("volume", {})

    def list_volumes(self) -> list[dict[str, Any]]:
        result = self._request("GET", "/volumes")
        return result.get("volumes", [])

    def delete_volume(self, name: str, region: str) -> bool:
        result = self._request(
            "DELETE",
            f"/volumes/{name}?region={region}",
        )
        return "status" in result
