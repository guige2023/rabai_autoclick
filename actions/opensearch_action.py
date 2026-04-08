"""OpenSearch action module for RabAI AutoClick.

Provides search and analytics operations via OpenSearch API
for distributed search and log analytics.
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


class OpenSearchAction(BaseAction):
    """OpenSearch API integration for search and analytics.

    Supports document CRUD, full-text search, aggregations,
    and index management.

    Args:
        config: OpenSearch configuration containing hosts and auth
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.hosts = self.config.get("hosts", ["http://localhost:9200"])
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.host = self.hosts[0] if isinstance(self.hosts, list) else self.hosts
        self.api_base = f"{self.host}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to OpenSearch."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {"Content-Type": "application/json"}

        if self.username and self.password:
            import base64
            creds = f"{self.username}:{self.password}"
            headers["Authorization"] = f"Basic {base64.b64encode(creds.encode()).decode()}"

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def create_index(
        self,
        index: str,
        mappings: Optional[Dict] = None,
        settings: Optional[Dict] = None,
    ) -> ActionResult:
        """Create an index.

        Args:
            index: Index name
            mappings: Optional field mappings
            settings: Optional index settings

        Returns:
            ActionResult with creation status
        """
        body = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings

        result = self._make_request(
            "PUT", index, data=body if body else None
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"index": index, "created": True})

    def index_document(
        self,
        index: str,
        doc_id: Optional[str],
        document: Dict[str, Any],
    ) -> ActionResult:
        """Index a document.

        Args:
            index: Index name
            doc_id: Optional document ID
            document: Document body

        Returns:
            ActionResult with indexing status
        """
        endpoint = f"{index}/_doc"
        if doc_id:
            endpoint = f"{index}/_doc/{doc_id}"

        result = self._make_request("POST", endpoint, data=document)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "_id": result.get("_id"),
                "result": result.get("result"),
            },
        )

    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict]] = None,
        aggs: Optional[Dict] = None,
    ) -> ActionResult:
        """Search for documents.

        Args:
            index: Index name
            query: OpenSearch query DSL
            size: Number of results
            from_: Result offset
            sort: Sort specification
            aggs: Aggregation definitions

        Returns:
            ActionResult with search results
        """
        body = {"query": query, "size": size, "from": from_}
        if sort:
            body["sort"] = sort
        if aggs:
            body["aggs"] = aggs

        result = self._make_request("POST", f"{index}/_search", data=body)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        hits = result.get("hits", {})
        return ActionResult(
            success=True,
            data={
                "total": hits.get("total", {}).get("value", 0),
                "hits": [h["_source"] for h in hits.get("hits", [])],
                "aggregations": result.get("aggregations", {}),
            },
        )

    def get_document(
        self,
        index: str,
        doc_id: str,
    ) -> ActionResult:
        """Get a document by ID.

        Args:
            index: Index name
            doc_id: Document ID

        Returns:
            ActionResult with document
        """
        result = self._make_request("GET", f"{index}/_doc/{doc_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("_source", {}))

    def delete_document(
        self,
        index: str,
        doc_id: str,
    ) -> ActionResult:
        """Delete a document.

        Args:
            index: Index name
            doc_id: Document ID

        Returns:
            ActionResult with deletion status
        """
        result = self._make_request("DELETE", f"{index}/_doc/{doc_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def bulk(
        self,
        index: str,
        operations: List[Dict[str, Any]],
    ) -> ActionResult:
        """Bulk index/delete documents.

        Args:
            index: Index name
            operations: List of {'index' or 'delete': doc} operations

        Returns:
            ActionResult with bulk results
        """
        lines = []
        for op in operations:
            action = "index" if "index" in op else "delete"
            doc = op[action]
            lines.append(json.dumps({action: {"_index": index, "_id": doc.get("_id")}}))
            if action == "index":
                lines.append(json.dumps(doc))

        body = "\n".join(lines) + "\n"
        headers = {"Content-Type": "application/x-ndjson"}
        url = f"{self.api_base}/_bulk"

        import base64
        req_headers = dict(headers)
        if self.username and self.password:
            creds = f"{self.username}:{self.password}"
            req_headers["Authorization"] = f"Basic {base64.b64encode(creds.encode()).decode()}"

        req = Request(
            url,
            data=body.encode("utf-8"),
            headers=req_headers,
            method="POST",
        )

        try:
            with urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
                return ActionResult(
                    success=True,
                    data={
                        "took": result.get("took"),
                        "errors": result.get("errors", False),
                    },
                )
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(success=False, error=f"HTTP {e.code}: {error_body}")
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute OpenSearch operation."""
        operations = {
            "create_index": self.create_index,
            "index_document": self.index_document,
            "search": self.search,
            "get_document": self.get_document,
            "delete_document": self.delete_document,
            "bulk": self.bulk,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
