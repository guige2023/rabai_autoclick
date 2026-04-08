"""Elasticsearch integration for search and analytics operations.

Handles Elasticsearch operations including indexing, searching,
aggregations, and cluster management.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime

try:
    from elasticsearch import Elasticsearch, NotFoundError
except ImportError:
    Elasticsearch = None
    NotFoundError = None

logger = logging.getLogger(__name__)


@dataclass
class ESConfig:
    """Configuration for Elasticsearch connection."""
    hosts: list[str]
    api_key: Optional[str] = None
    basic_auth: Optional[tuple[str, str]] = None
    timeout: int = 30
    max_retries: int = 3
    retry_on_timeout: bool = True


@dataclass
class ESDocument:
    """Represents an Elasticsearch document."""
    id: Optional[str]
    index: str
    source: dict
    score: Optional[float] = None
    highlights: dict = field(default_factory=dict)


@dataclass
class ESSearchResult:
    """Search result with documents and metadata."""
    total: int
    documents: list[ESDocument]
    took_ms: int
    scroll_id: Optional[str] = None
    aggregations: dict = field(default_factory=dict)


class ElasticsearchAPIError(Exception):
    """Raised when Elasticsearch operations fail."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class ElasticsearchAction:
    """Elasticsearch client for search and analytics operations."""

    def __init__(self, config: ESConfig):
        """Initialize Elasticsearch client with configuration.

        Args:
            config: ESConfig with hosts and auth

        Raises:
            ImportError: If elasticsearch-py is not installed
        """
        if Elasticsearch is None:
            raise ImportError("elasticsearch required: pip install elasticsearch")

        self.config = config
        self._client: Optional[Elasticsearch] = None

    def connect(self) -> None:
        """Establish connection to Elasticsearch.

        Raises:
            ElasticsearchAPIError: On connection failure
        """
        kwargs: dict[str, Any] = {
            "timeout": self.config.timeout,
            "max_retries": self.config.max_retries,
            "retry_on_timeout": self.config.retry_on_timeout
        }

        if self.config.api_key:
            kwargs["api_key"] = self.config.api_key
        elif self.config.basic_auth:
            kwargs["basic_auth"] = self.config.basic_auth

        try:
            self._client = Elasticsearch(hosts=self.config.hosts, **kwargs)
            info = self._client.info()
            logger.info(f"Connected to Elasticsearch: {info['cluster_name']}")

        except Exception as e:
            raise ElasticsearchAPIError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close Elasticsearch connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Disconnected from Elasticsearch")

    @property
    def client(self) -> Elasticsearch:
        """Get ES client, connect if needed."""
        if self._client is None:
            self.connect()
        return self._client

    def index_document(self, index: str, doc: dict,
                       doc_id: Optional[str] = None,
                       refresh: bool = False) -> str:
        """Index a document.

        Args:
            index: Index name
            doc: Document body
            doc_id: Optional document ID
            refresh: Refresh index after indexing

        Returns:
            Document ID
        """
        try:
            kwargs: dict[str, Any] = {"index": index, "document": doc}

            if doc_id:
                kwargs["id"] = doc_id

            if refresh:
                kwargs["refresh"] = refresh

            result = self.client.index(**kwargs)
            return result["_id"]

        except Exception as e:
            raise ElasticsearchAPIError(f"Index failed: {e}")

    def get_document(self, index: str, doc_id: str) -> ESDocument:
        """Get a document by ID.

        Args:
            index: Index name
            doc_id: Document ID

        Returns:
            ESDocument object

        Raises:
            ElasticsearchAPIError: If document not found
        """
        try:
            result = self.client.get(index=index, id=doc_id)
            return ESDocument(
                id=result["_id"],
                index=result["_index"],
                source=result["_source"]
            )

        except NotFoundError:
            raise ElasticsearchAPIError(f"Document not found: {doc_id}", status_code=404)

        except Exception as e:
            raise ElasticsearchAPIError(f"Get failed: {e}")

    def delete_document(self, index: str, doc_id: str,
                        refresh: bool = False) -> bool:
        """Delete a document.

        Args:
            index: Index name
            doc_id: Document ID
            refresh: Refresh index after deletion

        Returns:
            True if deleted
        """
        try:
            self.client.delete(index=index, id=doc_id, refresh=refresh)
            return True

        except NotFoundError:
            return False

        except Exception as e:
            raise ElasticsearchAPIError(f"Delete failed: {e}")

    def update_document(self, index: str, doc_id: str,
                        doc: dict, retry_on_conflict: int = 3) -> bool:
        """Update a document.

        Args:
            index: Index name
            doc_id: Document ID
            doc: Partial document with updated fields
            retry_on_conflict: Number of retries on conflict

        Returns:
            True if updated
        """
        try:
            self.client.update(
                index=index,
                id=doc_id,
                doc=doc,
                retry_on_conflict=retry_on_conflict
            )
            return True

        except NotFoundError:
            raise ElasticsearchAPIError(f"Document not found: {doc_id}", status_code=404)

        except Exception as e:
            raise ElasticsearchAPIError(f"Update failed: {e}")

    def search(self, index: str, query: dict,
               size: int = 10, from_: int = 0,
               sort: Optional[list] = None,
               highlight: Optional[dict] = None,
               aggregations: Optional[dict] = None,
               scroll: Optional[str] = None) -> ESSearchResult:
        """Execute a search query.

        Args:
            index: Index name or pattern
            query: Elasticsearch query DSL
            size: Number of results
            from_: Starting offset
            sort: Sort specification
            highlight: Highlight configuration
            aggregations: Aggregation definitions
            scroll: Scroll context duration

        Returns:
            ESSearchResult with documents and metadata
        """
        try:
            body: dict[str, Any] = {"query": query}

            if sort:
                body["sort"] = sort

            if highlight:
                body["highlight"] = highlight

            if aggregations:
                body["aggs"] = aggregations

            kwargs: dict[str, Any] = {
                "index": index,
                "body": body,
                "size": size,
                "from_": from_
            }

            if scroll:
                kwargs["scroll"] = scroll

            result = self.client.search(**kwargs)

            documents = []
            for hit in result["hits"]["hits"]:
                documents.append(ESDocument(
                    id=hit["_id"],
                    index=hit["_index"],
                    source=hit["_source"],
                    score=hit.get("_score"),
                    highlights=hit.get("highlight", {})
                ))

            total = result["hits"]["total"]
            total_count = total["value"] if isinstance(total, dict) else total

            return ESSearchResult(
                total=total_count,
                documents=documents,
                took_ms=result["took"],
                scroll_id=result.get("_scroll_id"),
                aggregations=result.get("aggregations", {})
            )

        except Exception as e:
            raise ElasticsearchAPIError(f"Search failed: {e}")

    def scroll_search(self, index: str, query: dict,
                       size: int = 100,
                       scroll: str = "2m") -> list[ESDocument]:
        """Execute a scroll search for large result sets.

        Args:
            index: Index name
            query: Elasticsearch query
            size: Batch size per scroll
            scroll: Scroll context duration

        Returns:
            All matching ESDocument objects
        """
        try:
            result = self.search(index, query, size=size, scroll=scroll)
            all_docs = result.documents.copy()

            while result.scroll_id and result.documents:
                result = self.scroll(scroll_id=result.scroll_id, scroll=scroll)
                all_docs.extend(result.documents)

            return all_docs

        except Exception as e:
            raise ElasticsearchAPIError(f"Scroll search failed: {e}")

    def scroll(self, scroll_id: str, scroll: str = "2m") -> ESSearchResult:
        """Scroll through existing search context.

        Args:
            scroll_id: Scroll context ID
            scroll: Scroll duration

        Returns:
            ESSearchResult with next batch
        """
        try:
            result = self.client.scroll(scroll_id=scroll_id, scroll=scroll)

            documents = []
            for hit in result["hits"]["hits"]:
                documents.append(ESDocument(
                    id=hit["_id"],
                    index=hit["_index"],
                    source=hit["_source"],
                    score=hit.get("_score")
                ))

            return ESSearchResult(
                total=len(documents),
                documents=documents,
                took_ms=result.get("took", 0),
                scroll_id=result.get("_scroll_id")
            )

        except Exception as e:
            raise ElasticsearchAPIError(f"Scroll failed: {e}")

    def create_index(self, index: str, mappings: Optional[dict] = None,
                     settings: Optional[dict] = None) -> bool:
        """Create an index with mappings and settings.

        Args:
            index: Index name
            mappings: Index field mappings
            settings: Index settings

        Returns:
            True if created
        """
        try:
            body = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings

            self.client.indices.create(index=index, body=body)
            return True

        except Exception as e:
            raise ElasticsearchAPIError(f"Create index failed: {e}")

    def delete_index(self, index: str) -> bool:
        """Delete an index.

        Args:
            index: Index name

        Returns:
            True if deleted
        """
        try:
            self.client.indices.delete(index=index)
            return True

        except NotFoundError:
            return False

        except Exception as e:
            raise ElasticsearchAPIError(f"Delete index failed: {e}")

    def index_exists(self, index: str) -> bool:
        """Check if index exists.

        Args:
            index: Index name

        Returns:
            True if exists
        """
        try:
            return self.client.indices.exists(index=index)
        except Exception as e:
            raise ElasticsearchAPIError(f"Index exists check failed: {e}")

    def bulk_index(self, index: str, documents: list[dict],
                   id_field: Optional[str] = None,
                   refresh: bool = False) -> dict:
        """Bulk index multiple documents.

        Args:
            index: Index name
            documents: List of document bodies
            id_field: Field to use as document ID
            refresh: Refresh after bulk

        Returns:
            Bulk operation summary
        """
        try:
            from elasticsearch.helpers import bulk

            actions = []
            for doc in documents:
                action: dict[str, Any] = {
                    "_index": index,
                    "_source": doc
                }

                if id_field and id_field in doc:
                    action["_id"] = doc[id_field]

                actions.append(action)

            success, failed = bulk(
                self.client,
                actions,
                refresh=refresh
            )

            return {"success": success, "failed": len(failed) if failed else 0}

        except Exception as e:
            raise ElasticsearchAPIError(f"Bulk index failed: {e}")

    def refresh_index(self, index: str) -> None:
        """Refresh an index to make recent changes searchable.

        Args:
            index: Index name
        """
        try:
            self.client.indices.refresh(index=index)
        except Exception as e:
            raise ElasticsearchAPIError(f"Refresh failed: {e}")

    def get_cluster_health(self) -> dict:
        """Get cluster health status.

        Returns:
            Cluster health info
        """
        try:
            return self.client.cluster.health()
        except Exception as e:
            raise ElasticsearchAPIError(f"Cluster health failed: {e}")

    def count(self, index: str, query: Optional[dict] = None) -> int:
        """Count documents matching query.

        Args:
            index: Index name
            query: Optional query filter

        Returns:
            Document count
        """
        try:
            kwargs: dict[str, Any] = {"index": index}
            if query:
                kwargs["body"] = {"query": query}
            return self.client.count(**kwargs)["count"]
        except Exception as e:
            raise ElasticsearchAPIError(f"Count failed: {e}")
