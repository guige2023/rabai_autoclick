"""Pinecone vector database action module for RabAI AutoClick.

Provides managed vector search operations via Pinecone API for
scalable semantic search and RAG applications.
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


class PineconeAction(BaseAction):
    """Pinecone API integration for managed vector search.

    Supports index management, upsert, search, and deletion
    operations on vector embeddings.

    Args:
        config: Pinecone configuration containing api_key, environment, and index_name
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.environment = self.config.get("environment", "")
        self.index_name = self.config.get("index_name", "")
        self.api_base = f"https://{self.index_name}-{self.environment}"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Pinecone."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }
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

    def describe_index(self) -> ActionResult:
        """Describe the current index.

        Returns:
            ActionResult with index info
        """
        result = self._make_request("GET", "describe_index_project")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def upsert_vectors(
        self,
        vectors: List[Dict[str, Any]],
        namespace: str = "",
    ) -> ActionResult:
        """Upsert vectors to the index.

        Args:
            vectors: List of vectors with id, values, and optional metadata
            namespace: Optional namespace

        Returns:
            ActionResult with upsert status
        """
        data = {"vectors": vectors}
        if namespace:
            data["namespace"] = namespace

        result = self._make_request("POST", "vectors/upsert", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"upserted": len(vectors), "status": result.get("upsertedCount")},
        )

    def query(
        self,
        vector: List[float],
        top_k: int = 10,
        namespace: str = "",
        include_values: bool = True,
        include_metadata: bool = True,
        filter_expr: Optional[Dict] = None,
    ) -> ActionResult:
        """Query for similar vectors.

        Args:
            vector: Query vector
            top_k: Number of results
            namespace: Optional namespace
            include_values: Include vector values
            include_metadata: Include metadata
            filter_expr: Optional filter expression

        Returns:
            ActionResult with query results
        """
        data = {
            "vector": vector,
            "topK": top_k,
            "includeValues": include_values,
            "includeMetadata": include_metadata,
        }
        if namespace:
            data["namespace"] = namespace
        if filter_expr:
            data["filter"] = filter_expr

        result = self._make_request("POST", "query", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"matches": result.get("matches", []), "namespace": result.get("namespace")},
        )

    def fetch_vectors(
        self,
        ids: List[str],
        namespace: str = "",
    ) -> ActionResult:
        """Fetch vectors by IDs.

        Args:
            ids: List of vector IDs
            namespace: Optional namespace

        Returns:
            ActionResult with vectors
        """
        data = {"ids": ids}
        if namespace:
            data["namespace"] = namespace

        result = self._make_request("POST", "vectors/fetch", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"vectors": result.get("vectors", {}), "namespace": result.get("namespace")},
        )

    def delete_vectors(
        self,
        ids: List[str],
        namespace: str = "",
        delete_all: bool = False,
    ) -> ActionResult:
        """Delete vectors by IDs.

        Args:
            ids: List of vector IDs
            namespace: Optional namespace
            delete_all: Delete all vectors

        Returns:
            ActionResult with deletion status
        """
        if delete_all:
            data = {"deleteAll": True}
        else:
            data = {"ids": ids}

        if namespace:
            data["namespace"] = namespace

        result = self._make_request("POST", "vectors/delete", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"deleted": len(ids) if not delete_all else "all"},
        )

    def update_vector(
        self,
        id: str,
        values: Optional[List[float]] = None,
        set_metadata: Optional[Dict] = None,
        namespace: str = "",
    ) -> ActionResult:
        """Update a vector.

        Args:
            id: Vector ID
            values: Optional new vector values
            set_metadata: Optional metadata to set
            namespace: Optional namespace

        Returns:
            ActionResult with update status
        """
        data = {"id": id}
        if values:
            data["setValues"] = values
        if set_metadata:
            data["setMetadata"] = set_metadata
        if namespace:
            data["namespace"] = namespace

        result = self._make_request("POST", "vectors/update", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"updated": id})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Pinecone operation."""
        operations = {
            "describe_index": self.describe_index,
            "upsert_vectors": self.upsert_vectors,
            "query": self.query,
            "fetch_vectors": self.fetch_vectors,
            "delete_vectors": self.delete_vectors,
            "update_vector": self.update_vector,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
