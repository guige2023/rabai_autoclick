"""ChromaDB vector database action module for RabAI AutoClick.

Provides ChromaDB operations for embeddings storage and similarity search.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ChromaDBQueryAction(BaseAction):
    """Execute vector queries via ChromaDB."""
    action_type = "chromadb_query"
    display_name = "ChromaDB查询"
    description = "ChromaDB向量数据库查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ChromaDB query.

        Args:
            context: Execution context.
            params: Dict with keys:
                - collection: Collection name
                - query_vector: Query vector
                - query_texts: Alternative: query text (will use embedding function)
                - limit: Max results
                - where_filter: Optional metadata filter
                - persist_directory: ChromaDB data directory

        Returns:
            ActionResult with query results.
        """
        collection_name = params.get('collection', 'default')
        query_vector = params.get('query_vector', [])
        query_texts = params.get('query_texts', [])
        limit = params.get('limit', 10)
        where_filter = params.get('where_filter', None)
        persist_dir = params.get('persist_directory', './chroma_data')

        if not query_vector and not query_texts:
            return ActionResult(success=False, message="query_vector or query_texts is required")

        try:
            import chromadb
        except ImportError:
            return ActionResult(success=False, message="chromadb not installed. Run: pip install chromadb")

        start = time.time()
        try:
            client = chromadb.PersistentClient(path=persist_dir)
            collection = client.get_collection(collection_name)
            if query_texts:
                results = collection.query(
                    query_texts=query_texts,
                    n_results=limit,
                    where=where_filter,
                )
            else:
                results = collection.query(
                    query_embeddings=[query_vector],
                    n_results=limit,
                    where=where_filter,
                )
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Query returned results",
                data={
                    'ids': results.get('ids', [[]])[0],
                    'distances': results.get('distances', [[]])[0],
                    'metadatas': results.get('metadatas', [[]])[0],
                    'documents': results.get('documents', [[]])[0],
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ChromaDB error: {str(e)}")


class ChromaDBInsertAction(BaseAction):
    """Insert vectors into ChromaDB collection."""
    action_type = "chromadb_insert"
    display_name = "ChromaDB插入"
    description = "ChromaDB向量插入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Insert into ChromaDB.

        Args:
            context: Execution context.
            params: Dict with keys:
                - collection: Collection name
                - ids: List of unique IDs
                - embeddings: List of vectors
                - documents: Optional list of document texts
                - metadatas: Optional list of metadata dicts
                - persist_directory: ChromaDB data directory

        Returns:
            ActionResult with insert confirmation.
        """
        collection_name = params.get('collection', 'default')
        ids = params.get('ids', [])
        embeddings = params.get('embeddings', [])
        documents = params.get('documents', [])
        metadatas = params.get('metadatas', [])
        persist_dir = params.get('persist_directory', './chroma_data')

        if not ids:
            return ActionResult(success=False, message="ids list is required")
        if not embeddings and not documents:
            return ActionResult(success=False, message="embeddings or documents is required")

        try:
            import chromadb
        except ImportError:
            return ActionResult(success=False, message="chromadb not installed")

        start = time.time()
        try:
            client = chromadb.PersistentClient(path=persist_dir)
            collection = client.get_or_create_collection(collection_name)
            collection.add(
                ids=ids,
                embeddings=embeddings if embeddings else None,
                documents=documents if documents else None,
                metadatas=metadatas if metadatas else None,
            )
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Inserted {len(ids)} records",
                data={'count': len(ids), 'collection': collection_name}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ChromaDB insert error: {str(e)}")
