"""Cloudflare action module for RabAI AutoClick.

Provides Cloudflare API operations for DNS management,
Workers serverless functions, and edge caching.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CloudflareAction(BaseAction):
    """Cloudflare API integration for DNS, Workers, and CDN operations.

    Supports DNS record management, zone operations, Workers deployment,
    and cache purging.

    Args:
        config: Cloudflare configuration containing email, api_key, and zone_id
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.email = self.config.get("email", "")
        self.api_key = self.config.get("api_key", "")
        self.zone_id = self.config.get("zone_id", "")
        self.api_base = "https://api.cloudflare.com/client/v4"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Cloudflare API."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = dict(self.headers)
        if self.email:
            headers["X-Auth-Email"] = self.email

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                if not result.get("success", True):
                    return {
                        "error": result.get("errors", [{}])[0].get("message", "Unknown error"),
                        "success": False,
                    }
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def list_zones(self) -> ActionResult:
        """List all zones (domains).

        Returns:
            ActionResult with zones list
        """
        result = self._make_request("GET", "zones")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"zones": result.get("result", [])})

    def list_dns_records(
        self,
        zone_id: Optional[str] = None,
        record_type: Optional[str] = None,
        name: Optional[str] = None,
    ) -> ActionResult:
        """List DNS records for a zone.

        Args:
            zone_id: Zone ID (uses config default)
            record_type: Filter by record type (A, CNAME, etc.)
            name: Filter by record name

        Returns:
            ActionResult with DNS records list
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        endpoint = f"zones/{zid}/dns_records"
        params = []
        if record_type:
            params.append(f"type={record_type}")
        if name:
            params.append(f"name={name}")

        if params:
            endpoint += "?" + "&".join(params)

        result = self._make_request("GET", endpoint)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True, data={"records": result.get("result", [])}
        )

    def create_dns_record(
        self,
        name: str,
        record_type: str,
        content: str,
        zone_id: Optional[str] = None,
        ttl: int = 3600,
        proxied: bool = False,
    ) -> ActionResult:
        """Create a DNS record.

        Args:
            name: Record name
            record_type: Record type (A, AAAA, CNAME, MX, TXT, etc.)
            content: Record content/value
            zone_id: Zone ID
            ttl: Time to live in seconds
            proxied: Whether to enable Cloudflare proxy

        Returns:
            ActionResult with created record
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        data = {
            "name": name,
            "type": record_type,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        }

        result = self._make_request(
            "POST", f"zones/{zid}/dns_records", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("result", {}))

    def update_dns_record(
        self,
        record_id: str,
        name: str,
        record_type: str,
        content: str,
        zone_id: Optional[str] = None,
        ttl: int = 3600,
        proxied: bool = False,
    ) -> ActionResult:
        """Update a DNS record.

        Args:
            record_id: Record ID
            name: Record name
            record_type: Record type
            content: Record content
            zone_id: Zone ID
            ttl: Time to live
            proxied: Cloudflare proxy status

        Returns:
            ActionResult with updated record
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        data = {
            "name": name,
            "type": record_type,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        }

        result = self._make_request(
            "PUT", f"zones/{zid}/dns_records/{record_id}", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("result", {}))

    def delete_dns_record(
        self,
        record_id: str,
        zone_id: Optional[str] = None,
    ) -> ActionResult:
        """Delete a DNS record.

        Args:
            record_id: Record ID
            zone_id: Zone ID

        Returns:
            ActionResult with deletion status
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        result = self._make_request("DELETE", f"zones/{zid}/dns_records/{record_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def purge_cache(
        self,
        zone_id: Optional[str] = None,
        files: Optional[List[str]] = None,
        purge_everything: bool = False,
    ) -> ActionResult:
        """Purge Cloudflare cache.

        Args:
            zone_id: Zone ID
            files: List of specific file URLs to purge
            purge_everything: Purge entire cache

        Returns:
            ActionResult with purge status
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        data = {}
        if purge_everything:
            data["purge_everything"] = True
        elif files:
            data["files"] = files
        else:
            return ActionResult(
                success=False,
                error="Must specify files or purge_everything",
            )

        result = self._make_request("POST", f"zones/{zid}/purge_cache", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"purged": True})

    def get_zone_stats(
        self,
        zone_id: Optional[str] = None,
        period: int = 1,
    ) -> ActionResult:
        """Get zone analytics.

        Args:
            zone_id: Zone ID
            period: Time period in days

        Returns:
            ActionResult with zone statistics
        """
        zid = zone_id or self.zone_id
        if not zid:
            return ActionResult(success=False, error="Missing zone_id")

        result = self._make_request(
            "GET", f"zones/{zid}/analytics/dashboard?since={-24*period}&until=now"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("result", {}))

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Cloudflare operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "list_zones": self.list_zones,
            "list_dns_records": self.list_dns_records,
            "create_dns_record": self.create_dns_record,
            "update_dns_record": self.update_dns_record,
            "delete_dns_record": self.delete_dns_record,
            "purge_cache": self.purge_cache,
            "get_zone_stats": self.get_zone_stats,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
