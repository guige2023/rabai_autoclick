"""Meilisearch action module for RabAI AutoClick.

Provides lightning-fast search engine operations via Meilisearch API
for full-text search, filtering, and relevance tuning.
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


class MeilisearchAction(BaseAction):
    """Meilisearch API integration for full-text search.

    Supports document indexing, search queries, index management,
    and relevance tuning.

    Args:
        config: Meilisearch configuration containing host and master_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "http://localhost:7700")
        self.master_key = self.config.get("master_key", "")
        self.api_base = self.host
        self.headers = {"Content-Type": "application/json"}
        if self.master_key:
            self.headers["Authorization"] = f"Bearer {self.master_key}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Meilisearch."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = dict(self.headers)
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
        uid: str,
        primary_key: Optional[str] = None,
    ) -> ActionResult:
        """Create an index.

        Args:
            uid: Index unique identifier
            primary_key: Primary key field name

        Returns:
            ActionResult with index info
        """
        data = {"uid": uid}
        if primary_key:
            data["primaryKey"] = primary_key

        result = self._make_request("POST", "indexes", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"index_uid": result.get("uid")})

    def add_documents(
        self,
        index_uid: str,
        documents: List[Dict],
        primary_key: Optional[str] = None,
    ) -> ActionResult:
        """Add or update documents.

        Args:
            index_uid: Index identifier
            documents: List of document dicts
            primary_key: Optional primary key override

        Returns:
            ActionResult with task info
        """
        endpoint = f"indexes/{index_uid}/documents"
        if primary_key:
            endpoint += f"?primaryKey={primary_key}"

        result = self._make_request("POST", endpoint, data=documents)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"task_uid": result.get("taskUid"), "status": result.get("status")},
        )

    def search(
        self,
        index_uid: str,
        query: str,
        limit: int = 20,
        offset: int = 0,
        filter_expr: Optional[str] = None,
        sort: Optional[List[str]] = None,
        attributes_to_retrieve: Optional[List[str]] = None,
    ) -> ActionResult:
        """Search for documents.

        Args:
            index_uid: Index identifier
            query: Search query string
            limit: Maximum results
            offset: Result offset
            filter_expr: Filter expression
            sort: Sort fields
            attributes_to_retrieve: Fields to return

        Returns:
            ActionResult with search results
        """
        data = {
            "q": query,
            "limit": limit,
            "offset": offset,
        }
        if filter_expr:
            data["filter"] = filter_expr
        if sort:
            data["sort"] = sort
        if attributes_to_retrieve:
            data["attributesToRetrieve"] = attributes_to_retrieve

        result = self._make_request("POST", f"indexes/{index_uid}/search", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "hits": result.get("hits", []),
                "total": result.get("estimatedTotalHits"),
                "processing_time_ms": result.get("processingTimeMs"),
            },
        )

    def get_document(
        self,
        index_uid: str,
        document_id: str,
    ) -> ActionResult:
        """Get a document by ID.

        Args:
            index_uid: Index identifier
            document_id: Document ID

        Returns:
            ActionResult with document
        """
        result = self._make_request(
            "GET", f"indexes/{index_uid}/documents/{document_id}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def update_settings(
        self,
        index_uid: str,
        searchable_attributes: Optional[List[str]] = None,
        filterable_attributes: Optional[List[str]] = None,
        sortable_attributes: Optional[List[str]] = None,
        ranking_rules: Optional[List[str]] = None,
    ) -> ActionResult:
        """Update index settings.

        Args:
            index_uid: Index identifier
            searchable_attributes: Fields to search
            filterable_attributes: Fields to filter
            sortable_attributes: Fields to sort
            ranking_rules: Ranking rules order

        Returns:
            ActionResult with task info
        """
        settings = {}
        if searchable_attributes:
            settings["searchableAttributes"] = searchable_attributes
        if filterable_attributes:
            settings["filterableAttributes"] = filterable_attributes
        if sortable_attributes:
            settings["sortableAttributes"] = sortable_attributes
        if ranking_rules:
            settings["rankingRules"] = ranking_rules

        result = self._make_request(
            "PUT", f"indexes/{index_uid}/settings", data=settings
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True, data={"task_uid": result.get("taskUid")}
        )

    def delete_documents(
        self,
        index_uid: str,
        document_ids: Optional[List[str]] = None,
    ) -> ActionResult:
        """Delete documents.

        Args:
            index_uid: Index identifier
            document_ids: List of IDs to delete (all if None)

        Returns:
            ActionResult with task info
        """
        if document_ids:
            result = self._make_request(
                "POST",
                f"indexes/{index_uid}/documents/delete-batch",
                data=document_ids,
            )
        else:
            result = self._make_request(
                "DELETE", f"indexes/{index_uid}/documents"
            )

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True, data={"task_uid": result.get("taskUid")}
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Meilisearch operation."""
        operations = {
            "create_index": self.create_index,
            "add_documents": self.add_documents,
            "search": self.search,
            "get_document": self.get_document,
            "update_settings": self.update_settings,
            "delete_documents": self.delete_documents,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
