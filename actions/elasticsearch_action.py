"""
Elasticsearch search and indexing actions.
"""
from __future__ import annotations

import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin


class ElasticsearchClient:
    """Elasticsearch API client."""

    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        verify_certs: bool = True
    ):
        """
        Initialize Elasticsearch client.

        Args:
            hosts: List of Elasticsearch host URLs.
            username: Username for authentication.
            password: Password for authentication.
            timeout: Request timeout in seconds.
            verify_certs: Verify SSL certificates.
        """
        if hosts is None:
            hosts = ['http://localhost:9200']

        self.hosts = hosts
        self.timeout = timeout

        self.session = requests.Session()

        if username and password:
            self.session.auth = (username, password)

        self.session.verify = verify_certs

    def _get_url(self, path: str) -> str:
        """Build URL for a request."""
        return urljoin(self.hosts[0], path)

    def index_document(
        self,
        index: str,
        doc_type: str,
        document: Dict[str, Any],
        id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Index a document.

        Args:
            index: Index name.
            doc_type: Document type.
            document: Document body.
            id: Optional document ID.

        Returns:
            Indexing result.
        """
        url = f'/{index}/{doc_type}'

        if id:
            url += f'/{id}'

        try:
            response = self.session.put(
                url,
                json=document,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def search(
        self,
        index: str,
        query: Dict[str, Any],
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Search for documents.

        Args:
            index: Index name.
            query: Elasticsearch query DSL.
            size: Number of results.
            from_: Starting offset.
            sort: Sort criteria.

        Returns:
            Search results.
        """
        url = f'/{index}/_search'

        params = {
            'size': size,
            'from': from_,
        }

        body = {'query': query}

        if sort:
            body['sort'] = sort

        try:
            response = self.session.get(
                url,
                json=body,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def get_document(
        self,
        index: str,
        doc_type: str,
        doc_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a document by ID.

        Args:
            index: Index name.
            doc_type: Document type.
            doc_id: Document ID.

        Returns:
            Document or None.
        """
        url = f'/{index}/{doc_type}/{doc_id}'

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return None

    def delete_document(
        self,
        index: str,
        doc_type: str,
        doc_id: str
    ) -> Dict[str, Any]:
        """
        Delete a document.

        Args:
            index: Index name.
            doc_type: Document type.
            doc_id: Document ID.

        Returns:
            Deletion result.
        """
        url = f'/{index}/{doc_type}/{doc_id}'

        try:
            response = self.session.delete(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create an index.

        Args:
            index: Index name.
            mappings: Index mappings.
            settings: Index settings.

        Returns:
            Creation result.
        """
        url = f'/{index}'

        body = {}
        if mappings:
            body['mappings'] = mappings
        if settings:
            body['settings'] = settings

        try:
            response = self.session.put(url, json=body, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def delete_index(self, index: str) -> Dict[str, Any]:
        """
        Delete an index.

        Args:
            index: Index name.

        Returns:
            Deletion result.
        """
        url = f'/{index}'

        try:
            response = self.session.delete(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

    def get_cluster_health() -> Dict[str, Any]:
        """Get cluster health."""
        url = '/_cluster/health'

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}


def search_all(
    es_url: str,
    index: str,
    query: str,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Perform a simple search.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        query: Query string.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        List of matching documents.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/{index}/_search'

    body = {
        'query': {
            'query_string': {'query': query}
        }
    }

    try:
        response = session.post(url, json=body, timeout=30)
        response.raise_for_status()
        data = response.json()

        hits = data.get('hits', {}).get('hits', [])
        return [hit['_source'] for hit in hits]
    except Exception:
        return []


def match_query(
    es_url: str,
    index: str,
    field: str,
    value: str,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Perform a match query.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        field: Field to search.
        value: Value to match.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        List of matching documents.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/{index}/_search'

    body = {
        'query': {
            'match': {field: value}
        }
    }

    try:
        response = session.post(url, json=body, timeout=30)
        response.raise_for_status()
        data = response.json()

        hits = data.get('hits', {}).get('hits', [])
        return [hit['_source'] for hit in hits]
    except Exception:
        return []


def bulk_index(
    es_url: str,
    index: str,
    doc_type: str,
    documents: List[Dict[str, Any]],
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Bulk index documents.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        doc_type: Document type.
        documents: List of documents to index.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        Bulk indexing result.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/_bulk'

    lines = []
    for doc in documents:
        lines.append({'index': {'_index': index, '_type': doc_type}})
        lines.append(doc)

    import json
    body = '\n'.join(json.dumps(line) for line in lines) + '\n'

    try:
        response = session.post(
            url,
            data=body.encode('utf-8'),
            headers={'Content-Type': 'application/x-ndjson'},
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {'error': str(e)}


def create_mapping(
    es_url: str,
    index: str,
    doc_type: str,
    properties: Dict[str, Dict[str, Any]],
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create an index with mapping.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        doc_type: Document type.
        properties: Field properties.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        Creation result.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/{index}'

    body = {
        'mappings': {
            doc_type: {
                'properties': properties
            }
        }
    }

    try:
        response = session.put(url, json=body, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {'error': str(e)}


def count_documents(
    es_url: str,
    index: str,
    query: Optional[Dict[str, Any]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> int:
    """
    Count documents in an index.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        query: Optional query to filter.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        Document count.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/{index}/_count'

    body = {}
    if query:
        body['query'] = query

    try:
        response = session.post(url, json=body, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('count', 0)
    except Exception:
        return 0


def get_index_stats(
    es_url: str,
    index: str,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get index statistics.

    Args:
        es_url: Elasticsearch URL.
        index: Index name.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        Index statistics.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/{index}/_stats'

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {'error': str(e)}


def list_indices(
    es_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> List[str]:
    """
    List all indices.

    Args:
        es_url: Elasticsearch URL.
        username: Optional auth username.
        password: Optional auth password.

    Returns:
        List of index names.
    """
    session = requests.Session()
    if username and password:
        session.auth = (username, password)

    url = f'{es_url.rstrip("/")}/_cat/indices?h=index'

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return [line.strip() for line in response.text.splitlines() if line.strip()]
    except Exception:
        return []
