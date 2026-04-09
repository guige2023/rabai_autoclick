"""API Search Action Module.

Provides full-text and structured search capabilities for API
data with indexing, ranking, filtering, and autocomplete.
"""

import time
import threading
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SearchIndexType(Enum):
    """Search index types."""
    INVERTED = "inverted"
    BTREE = "btree"
    HASH = "hash"
    FUZZY = "fuzzy"


@dataclass
class SearchResult:
    """Individual search result."""
    doc_id: str
    score: float
    highlights: List[str]
    fields: Dict[str, Any]
    rank: int


@dataclass
class SearchQuery:
    """Search query specification."""
    query_string: str
    fields: List[str]
    filters: Dict[str, Any]
    boost: Dict[str, float]
    fuzziness: int
    max_results: int
    offset: int


@dataclass
class SearchIndex:
    """Search index data structure."""
    index_type: SearchIndexType
    documents: Dict[str, Dict[str, Any]]
    inverted_index: Dict[str, List[Tuple[str, float]]]
    created_at: float
    doc_count: int


class ApiSearchAction(BaseAction):
    """API Search Engine.

    Full-text search engine with inverted indexing,
    ranking algorithms, and filter support.
    """
    action_type = "api_search"
    display_name = "API搜索引擎"
    description = "全文搜索引擎，支持索引、排名和过滤"

    _indices: Dict[str, SearchIndex] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute search operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'search', 'index', 'delete', 'bulk_index',
                               'suggest', 'analyze', 'reindex'
                - index_name: str - name of the search index
                - query: str - search query string
                - filters: dict (optional) - field filters
                - fields: list (optional) - fields to search
                - boost: dict (optional) - field boost weights
                - fuzziness: int (optional) - fuzzy match distance
                - max_results: int (optional) - max results to return
                - document: dict (optional) - document to index
                - documents: list (optional) - bulk documents

        Returns:
            ActionResult with search results.
        """
        start_time = time.time()
        operation = params.get('operation', 'search')

        try:
            with self._lock:
                if operation == 'search':
                    return self._search(params, start_time)
                elif operation == 'index':
                    return self._index_document(params, start_time)
                elif operation == 'delete':
                    return self._delete_document(params, start_time)
                elif operation == 'bulk_index':
                    return self._bulk_index(params, start_time)
                elif operation == 'suggest':
                    return self._suggest(params, start_time)
                elif operation == 'analyze':
                    return self._analyze(params, start_time)
                elif operation == 'reindex':
                    return self._reindex(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Search error: {str(e)}",
                duration=time.time() - start_time
            )

    def _search(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute search query."""
        index_name = params.get('index_name', 'default')
        query_str = params.get('query', '')
        fields = params.get('fields', ['*'])
        filters = params.get('filters', {})
        boost = params.get('boost', {})
        fuzziness = params.get('fuzziness', 0)
        max_results = params.get('max_results', 10)
        offset = params.get('offset', 0)

        if index_name not in self._indices:
            return ActionResult(
                success=False,
                message=f"Index '{index_name}' not found",
                duration=time.time() - start_time
            )

        index = self._indices[index_name]
        results: List[SearchResult] = []

        query_terms = query_str.lower().split()

        for doc_id, doc in index.documents.items():
            if filters and not self._apply_filters(doc, filters):
                continue

            score = 0.0
            highlights: List[str] = []

            for term in query_terms:
                for field_name, field_value in doc.items():
                    if fields != ['*'] and field_name not in fields:
                        continue
                    if isinstance(field_value, str):
                        field_val_lower = field_value.lower()
                        if term in field_val_lower:
                            boost_weight = boost.get(field_name, 1.0)
                            score += boost_weight * field_val_lower.count(term)
                            for sentence in field_val_lower.split('.'):
                                if term in sentence:
                                    highlights.append(sentence.strip())

            if score > 0:
                results.append(SearchResult(
                    doc_id=doc_id,
                    score=score,
                    highlights=highlights[:3],
                    fields=doc,
                    rank=len(results) + 1
                ))

        results.sort(key=lambda r: r.score, reverse=True)

        paginated = results[offset:offset + max_results]

        return ActionResult(
            success=True,
            message=f"Found {len(results)} results, returning {len(paginated)}",
            data={
                'total': len(results),
                'returned': len(paginated),
                'offset': offset,
                'results': [
                    {'doc_id': r.doc_id, 'score': r.score, 'highlights': r.highlights, 'fields': r.fields, 'rank': r.rank}
                    for r in paginated
                ]
            },
            duration=time.time() - start_time
        )

    def _index_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Index a single document."""
        index_name = params.get('index_name', 'default')
        document = params.get('document', {})
        doc_id = params.get('doc_id', self._generate_doc_id(document))

        if index_name not in self._indices:
            self._indices[index_name] = SearchIndex(
                index_type=SearchIndexType.INVERTED,
                documents={},
                inverted_index={},
                created_at=time.time(),
                doc_count=0
            )

        index = self._indices[index_name]
        index.documents[doc_id] = document
        index.doc_count = len(index.documents)

        for field_name, field_value in document.items():
            if isinstance(field_value, str):
                for term in field_value.lower().split():
                    if term not in index.inverted_index:
                        index.inverted_index[term] = []
                    index.inverted_index[term].append((doc_id, 1.0))

        return ActionResult(
            success=True,
            message=f"Document {doc_id} indexed",
            data={'doc_id': doc_id, 'index_name': index_name},
            duration=time.time() - start_time
        )

    def _delete_document(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a document from index."""
        index_name = params.get('index_name', 'default')
        doc_id = params.get('doc_id', '')

        if index_name not in self._indices:
            return ActionResult(success=False, message=f"Index '{index_name}' not found", duration=time.time() - start_time)

        index = self._indices[index_name]
        if doc_id in index.documents:
            del index.documents[doc_id]
            index.doc_count = len(index.documents)

        return ActionResult(
            success=True,
            message=f"Document {doc_id} deleted",
            data={'doc_id': doc_id},
            duration=time.time() - start_time
        )

    def _bulk_index(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Bulk index multiple documents."""
        index_name = params.get('index_name', 'default')
        documents = params.get('documents', [])

        indexed_count = 0
        for doc in documents:
            doc_id = doc.get('id', self._generate_doc_id(doc))
            if index_name not in self._indices:
                self._indices[index_name] = SearchIndex(
                    index_type=SearchIndexType.INVERTED,
                    documents={},
                    inverted_index={},
                    created_at=time.time(),
                    doc_count=0
                )
            index = self._indices[index_name]
            index.documents[doc_id] = doc
            indexed_count += 1

        if index_name in self._indices:
            self._indices[index_name].doc_count = len(self._indices[index_name].documents)

        return ActionResult(
            success=True,
            message=f"Bulk indexed {indexed_count} documents",
            data={'indexed_count': indexed_count},
            duration=time.time() - start_time
        )

    def _suggest(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get search suggestions/autocomplete."""
        prefix = params.get('prefix', '').lower()
        index_name = params.get('index_name', 'default')
        max_suggestions = params.get('max_suggestions', 5)

        if index_name not in self._indices:
            return ActionResult(success=False, message=f"Index '{index_name}' not found", duration=time.time() - start_time)

        index = self._indices[index_name]
        suggestions: Dict[str, int] = {}

        for term in index.inverted_index:
            if term.startswith(prefix):
                suggestions[term] = len(index.inverted_index[term])

        sorted_suggestions = sorted(suggestions.items(), key=lambda x: x[1], reverse=True)[:max_suggestions]

        return ActionResult(
            success=True,
            message=f"Found {len(sorted_suggestions)} suggestions",
            data={'suggestions': [{'term': t, 'frequency': f} for t, f in sorted_suggestions]},
            duration=time.time() - start_time
        )

    def _analyze(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Analyze text and return tokens."""
        text = params.get('text', '')
        tokens = text.lower().split()

        return ActionResult(
            success=True,
            message=f"Text analyzed into {len(tokens)} tokens",
            data={'tokens': tokens, 'token_count': len(tokens)},
            duration=time.time() - start_time
        )

    def _reindex(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Rebuild the search index."""
        index_name = params.get('index_name', 'default')

        if index_name not in self._indices:
            return ActionResult(success=False, message=f"Index '{index_name}' not found", duration=time.time() - start_time)

        old_index = self._indices[index_name]
        documents = dict(old_index.documents)

        self._indices[index_name] = SearchIndex(
            index_type=SearchIndexType.INVERTED,
            documents={},
            inverted_index={},
            created_at=time.time(),
            doc_count=0
        )

        for doc_id, doc in documents.items():
            self._indices[index_name].documents[doc_id] = doc
            for field_name, field_value in doc.items():
                if isinstance(field_value, str):
                    for term in field_value.lower().split():
                        if term not in self._indices[index_name].inverted_index:
                            self._indices[index_name].inverted_index[term] = []
                        self._indices[index_name].inverted_index[term].append((doc_id, 1.0))

        self._indices[index_name].doc_count = len(documents)

        return ActionResult(
            success=True,
            message=f"Reindexed {len(documents)} documents",
            data={'doc_count': len(documents)},
            duration=time.time() - start_time
        )

    def _apply_filters(self, doc: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Apply filters to a document."""
        for field_name, filter_value in filters.items():
            if field_name not in doc:
                return False
            if isinstance(filter_value, dict):
                op = filter_value.get('op', 'eq')
                val = filter_value.get('value')
                if op == 'eq' and doc[field_name] != val:
                    return False
                elif op == 'gt' and not (isinstance(doc[field_name], (int, float)) and doc[field_name] > val):
                    return False
                elif op == 'lt' and not (isinstance(doc[field_name], (int, float)) and doc[field_name] < val):
                    return False
                elif op == 'in' and doc[field_name] not in val:
                    return False
            elif doc[field_name] != filter_value:
                return False
        return True

    def _generate_doc_id(self, document: Dict[str, Any]) -> str:
        """Generate a document ID from content hash."""
        content = str(sorted(document.items()))
        return hashlib.md5(content.encode()).hexdigest()[:16]
