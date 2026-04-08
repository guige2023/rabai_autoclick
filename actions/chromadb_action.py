"""Chromadb action module for RabAI AutoClick.

Provides vector database operations via Chroma API for
local AI-powered search and embeddings storage.
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


class ChromaAction(BaseAction):
    """Chroma API integration for local vector storage.

    Supports collection management, document/embedding storage,
    and similarity search.

    Args:
        config: Chroma configuration containing host and port
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8000)
        self.api_base = f"http://{self.host}:{self.port}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Chroma."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data, default=str).encode("utf-8") if data else None
        headers = {"Content-Type": "application/json"}

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

    def create_collection(
        self,
        name: str,
        get_or_create: bool = False,
        metadata: Optional[Dict] = None,
    ) -> ActionResult:
        """Create or get a collection.

        Args:
            name: Collection name
            get_or_create: Return existing if exists
            metadata: Optional collection metadata

        Returns:
            ActionResult with collection info
        """
        data = {
            "name": name,
            "get_or_create": get_or_create,
        }
        if metadata:
            data["metadata"] = metadata

        result = self._make_request("POST", "api/v1/collections", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"id": result.get("id"), "name": result.get("name")},
        )

    def list_collections(self) -> ActionResult:
        """List all collections.

        Returns:
            ActionResult with collections list
        """
        result = self._make_request("GET", "api/v1/collections")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"collections": result})

    def get_collection(self, name: str) -> ActionResult:
        """Get a collection by name.

        Args:
            name: Collection name

        Returns:
            ActionResult with collection data
        """
        result = self._make_request("GET", f"api/v1/collections/{name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def delete_collection(self, name: str) -> ActionResult:
        """Delete a collection.

        Args:
            name: Collection name

        Returns:
            ActionResult with deletion status
        """
        result = self._make_request("DELETE", f"api/v1/collections/{name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def add(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: Optional[List[List[float]]] = None,
        documents: Optional[List[str]] = None,
        metadatas: Optional[List[Dict]] = None,
    ) -> ActionResult:
        """Add items to a collection.

        Args:
            collection_name: Collection name
            ids: List of item IDs
            embeddings: Optional list of vectors
            documents: Optional list of text documents
            metadatas: Optional list of metadata dicts

        Returns:
            ActionResult with add status
        """
        data = {"ids": ids}
        if embeddings:
            data["embeddings"] = embeddings
        if documents:
            data["documents"] = documents
        if metadatas:
            data["metadatas"] = metadatas

        result = self._make_request(
            "POST", f"api/v1/collections/{collection_name}/add", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"added": len(ids)})

    def query(
        self,
        collection_name: str,
        query_embeddings: Optional[List[List[float]]] = None,
        query_texts: Optional[List[str]] = None,
        n_results: int = 10,
        where: Optional[Dict] = None,
    ) -> ActionResult:
        """Query for similar items.

        Args:
            collection_name: Collection name
            query_embeddings: Query vectors
            query_texts: Query texts (uses built-in embeddings if set up)
            n_results: Number of results
            where: Optional metadata filter

        Returns:
            ActionResult with query results
        """
        data = {"n_results": n_results}
        if query_embeddings:
            data["query_embeddings"] = query_embeddings
        if query_texts:
            data["query_texts"] = query_texts
        if where:
            data["where"] = where

        result = self._make_request(
            "POST", f"api/v1/collections/{collection_name}/query", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "ids": result.get("ids", [[]])[0],
                "distances": result.get("distances", [[]])[0],
                "documents": result.get("documents", [[]])[0],
                "metadatas": result.get("metadatas", [[]])[0],
            },
        )

    def get(
        self,
        collection_name: str,
        ids: Optional[List[str]] = None,
        where: Optional[Dict] = None,
        limit: int = 100,
    ) -> ActionResult:
        """Get items from a collection.

        Args:
            collection_name: Collection name
            ids: Optional list of IDs to retrieve
            where: Optional metadata filter
            limit: Maximum items

        Returns:
            ActionResult with items
        """
        data = {"limit": limit}
        if ids:
            data["ids"] = ids
        if where:
            data["where"] = where

        result = self._make_request(
            "POST", f"api/v1/collections/{collection_name}/get", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "ids": result.get("ids", []),
                "documents": result.get("documents", []),
                "metadatas": result.get("metadatas", []),
            },
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Chroma operation."""
        operations = {
            "create_collection": self.create_collection,
            "list_collections": self.list_collections,
            "get_collection": self.get_collection,
            "delete_collection": self.delete_collection,
            "add": self.add,
            "query": self.query,
            "get": self.get,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
