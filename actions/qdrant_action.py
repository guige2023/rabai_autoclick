"""Qdrant action module for RabAI AutoClick.

Provides vector similarity search via Qdrant API for
AI-powered semantic search and RAG applications.
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


class QdrantAction(BaseAction):
    """Qdrant API integration for vector similarity search.

    Supports collection management, point CRUD, search,
    and payload filtering.

    Args:
        config: Qdrant configuration containing url and api_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.url = self.config.get("url", "http://localhost:6333")
        self.api_key = self.config.get("api_key", "")
        self.collection = self.config.get("collection", "")
        self.api_base = self.url

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Qdrant."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["api-key"] = self.api_key

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
        collection_name: str,
        vector_size: int,
        distance: str = "Cosine",
    ) -> ActionResult:
        """Create a collection.

        Args:
            collection_name: Collection name
            vector_size: Dimension of vectors
            distance: Distance metric (Cosine, Euclidean, Dot)

        Returns:
            ActionResult with creation status
        """
        data = {
            "vectors": {
                "size": vector_size,
                "distance": distance,
            }
        }
        result = self._make_request("PUT", f"collections/{collection_name}", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"collection": collection_name, "created": True},
        )

    def list_collections(self) -> ActionResult:
        """List all collections.

        Returns:
            ActionResult with collections list
        """
        result = self._make_request("GET", "collections")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"collections": result.get("collections", [])},
        )

    def delete_collection(self, collection_name: str) -> ActionResult:
        """Delete a collection.

        Args:
            collection_name: Collection name

        Returns:
            ActionResult with deletion status
        """
        result = self._make_request("DELETE", f"collections/{collection_name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def upsert_points(
        self,
        collection_name: str,
        points: List[Dict[str, Any]],
    ) -> ActionResult:
        """Upsert points (vectors with optional payload).

        Args:
            collection_name: Collection name
            points: List of points with id, vector, and optional payload

        Returns:
            ActionResult with upsert status
        """
        result = self._make_request(
            "PUT",
            f"collections/{collection_name}/points",
            data={"points": points},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"upserted": len(points), "status": result.get("status")},
        )

    def search(
        self,
        collection_name: str,
        vector: List[float],
        limit: int = 10,
        score_threshold: Optional[float] = None,
        filter_cond: Optional[Dict] = None,
    ) -> ActionResult:
        """Search for similar vectors.

        Args:
            collection_name: Collection name
            vector: Query vector
            limit: Maximum results
            score_threshold: Minimum similarity score
            filter_cond: Optional filter condition

        Returns:
            ActionResult with search results
        """
        data = {"vector": vector, "limit": limit}
        if score_threshold is not None:
            data["score_threshold"] = score_threshold
        if filter_cond:
            data["filter"] = filter_cond

        result = self._make_request(
            "POST",
            f"collections/{collection_name}/points/search",
            data=data,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"results": result if isinstance(result, list) else result.get("result", [])},
        )

    def get_point(
        self,
        collection_name: str,
        point_id: str,
    ) -> ActionResult:
        """Get a point by ID.

        Args:
            collection_name: Collection name
            point_id: Point ID

        Returns:
            ActionResult with point data
        """
        result = self._make_request(
            "GET",
            f"collections/{collection_name}/points/{point_id}",
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("result", {}))

    def delete_points(
        self,
        collection_name: str,
        point_ids: List[str],
    ) -> ActionResult:
        """Delete points by IDs.

        Args:
            collection_name: Collection name
            point_ids: List of point IDs

        Returns:
            ActionResult with deletion status
        """
        result = self._make_request(
            "POST",
            f"collections/{collection_name}/points/delete",
            data={"points": point_ids},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"deleted": len(point_ids), "status": result.get("status")},
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Qdrant operation."""
        operations = {
            "create_collection": self.create_collection,
            "list_collections": self.list_collections,
            "delete_collection": self.delete_collection,
            "upsert_points": self.upsert_points,
            "search": self.search,
            "get_point": self.get_point,
            "delete_points": self.delete_points,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
