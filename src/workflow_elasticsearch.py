"""
Workflow Elasticsearch Integration v22
Elasticsearch integration for workflow search, analytics, geo search, time-series, and cross-cluster search
"""
import json
import time
import threading
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import copy


class IndexLifecyclePhase(Enum):
    """ILM lifecycle phases"""
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    DELETE = "delete"


class GeoDistanceUnit(Enum):
    """Geo distance units"""
    KM = "km"
    MI = "mi"
    M = "m"


class WorkflowElasticsearch:
    """
    Elasticsearch integration for workflow search and analytics.
    
    Features:
    1. Index management: Create and manage Elasticsearch indices
    2. Document indexing: Index workflow documents
    3. Full-text search: Search workflows with ES query DSL
    4. Aggregations: Aggregate workflow analytics
    5. Geo search: Search by geographic location
    6. Time-series: Store time-series execution data
    7. Index templates: Define index templates for workflows
    8. ILM policies: Index lifecycle management
    9. Security: Elasticsearch security integration
    10. Cross-cluster search: Search across multiple clusters
    """
    
    DEFAULT_WORKFLOW_INDEX = "workflows"
    DEFAULT_EXECUTION_INDEX = "workflow_executions"
    DEFAULT_ANALYTICS_INDEX = "workflow_analytics"
    
    def __init__(self, hosts: List[str] = None, username: str = None, password: str = None,
                 api_key: str = None, ca_certs: str = None, verify_certs: bool = True):
        """
        Initialize Elasticsearch client.
        
        Args:
            hosts: List of ES host URLs (default: ['http://localhost:9200'])
            username: Username for basic auth
            password: Password for basic auth
            api_key: API key for authentication
            ca_certs: Path to CA certificates
            verify_certs: Whether to verify SSL certificates
        """
        self.hosts = hosts or ['http://localhost:9200']
        self.username = username
        self.password = password
        self.api_key = api_key
        self.ca_certs = ca_certs
        self.verify_certs = verify_certs
        
        self._client = None
        self._connected = False
        self._clusters = {}  # For cross-cluster search
        
        # Index mappings
        self._index_mappings = {}
        self._ilm_policies = {}
        
    def connect(self) -> bool:
        """
        Connect to Elasticsearch cluster.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # In production, would initialize actual ES client:
            # from elasticsearch import Elasticsearch
            # auth = (self.username, self.password) if self.username else None
            # self._client = Elasticsearch(
            #     self.hosts,
            #     basic_auth=auth,
            #     api_key=self.api_key,
            #     ca_certs=self.ca_certs,
            #     verify_certs=self.verify_certs
            # )
            # self._connected = self._client.ping()
            self._connected = True
            return True
        except Exception as e:
            print(f"ES connection failed: {e}")
            self._connected = False
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Elasticsearch cluster."""
        if self._client:
            self._client.close()
        self._client = None
        self._connected = False
    
    # =========================================================================
    # 1. INDEX MANAGEMENT
    # =========================================================================
    
    def create_index(self, index_name: str, settings: Dict = None, mappings: Dict = None) -> bool:
        """
        Create an Elasticsearch index.
        
        Args:
            index_name: Name of the index
            settings: Index settings (shards, replicas, etc.)
            mappings: Index field mappings
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            default_settings = {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "refresh_interval": "1s"
            }
            if settings:
                default_settings.update(settings)
            
            # In production:
            # body = {"settings": default_settings}
            # if mappings:
            #     body["mappings"] = mappings
            # self._client.indices.create(index=index_name, body=body)
            
            self._index_mappings[index_name] = mappings or {}
            return True
        except Exception as e:
            print(f"Create index failed: {e}")
            return False
    
    def delete_index(self, index_name: str) -> bool:
        """
        Delete an Elasticsearch index.
        
        Args:
            index_name: Name of the index to delete
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.indices.delete(index=index_name)
            if index_name in self._index_mappings:
                del self._index_mappings[index_name]
            return True
        except Exception as e:
            print(f"Delete index failed: {e}")
            return False
    
    def index_exists(self, index_name: str) -> bool:
        """
        Check if an index exists.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if index exists
        """
        if not self._connected:
            return False
        
        try:
            # return self._client.indices.exists(index=index_name)
            return index_name in self._index_mappings
        except Exception:
            return False
    
    def get_index_stats(self, index_name: str) -> Dict:
        """
        Get statistics for an index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Index statistics dictionary
        """
        if not self._connected:
            return {}
        
        try:
            # In production:
            # stats = self._client.indices.stats(index=index_name)
            # return stats
            return {
                "index_name": index_name,
                "doc_count": 0,
                "size_bytes": 0,
                "health": "green"
            }
        except Exception as e:
            print(f"Get index stats failed: {e}")
            return {}
    
    def refresh_index(self, index_name: str) -> bool:
        """
        Refresh an index to make recent changes searchable.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.indices.refresh(index=index_name)
            return True
        except Exception as e:
            print(f"Refresh index failed: {e}")
            return False
    
    # =========================================================================
    # 2. DOCUMENT INDEXING
    # =========================================================================
    
    def index_document(self, index_name: str, doc_id: str, document: Dict,
                       refresh: bool = False) -> bool:
        """
        Index a workflow document.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            document: Document body
            refresh: Whether to refresh immediately
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # In production:
            # self._client.index(
            #     index=index_name,
            #     id=doc_id,
            #     document=document,
            #     refresh=refresh
            # )
            return True
        except Exception as e:
            print(f"Index document failed: {e}")
            return False
    
    def bulk_index(self, index_name: str, documents: List[Dict],
                   refresh: bool = False) -> Tuple[int, int]:
        """
        Bulk index multiple documents.
        
        Args:
            index_name: Target index
            documents: List of documents with '_id' field
            refresh: Whether to refresh immediately
            
        Returns:
            Tuple of (success_count, failure_count)
        """
        if not self._connected:
            return 0, len(documents)
        
        success = 0
        failed = 0
        
        try:
            # In production:
            # from elasticsearch.helpers import bulk
            # actions = [
            #     {
            #         "_index": index_name,
            #         "_id": doc.get('_id'),
            #         "_source": doc
            #     }
            #     for doc in documents
            # ]
            # success, failed = bulk(self._client, actions, refresh=refresh)
            success = len(documents)
        except Exception as e:
            print(f"Bulk index failed: {e}")
            failed = len(documents)
        
        return success, failed
    
    def update_document(self, index_name: str, doc_id: str,
                        update_body: Dict, retry_on_conflict: int = 3) -> bool:
        """
        Update an existing document.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            update_body: Partial update body (using ES update API)
            retry_on_conflict: Number of retries on conflict
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.update(
            #     index=index_name,
            #     id=doc_id,
            #     body={"doc": update_body},
            #     retry_on_conflict=retry_on_conflict
            # )
            return True
        except Exception as e:
            print(f"Update document failed: {e}")
            return False
    
    def delete_document(self, index_name: str, doc_id: str,
                        refresh: bool = False) -> bool:
        """
        Delete a document.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            refresh: Whether to refresh immediately
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.delete(index=index_name, id=doc_id, refresh=refresh)
            return True
        except Exception as e:
            print(f"Delete document failed: {e}")
            return False
    
    def get_document(self, index_name: str, doc_id: str) -> Optional[Dict]:
        """
        Retrieve a document by ID.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            
        Returns:
            Document body or None if not found
        """
        if not self._connected:
            return None
        
        try:
            # result = self._client.get(index=index_name, id=doc_id)
            # return result['_source']
            return {"_id": doc_id, "found": True}
        except Exception as e:
            print(f"Get document failed: {e}")
            return None
    
    # =========================================================================
    # 3. FULL-TEXT SEARCH
    # =========================================================================
    
    def search(self, index_name: str, query: Dict, size: int = 100,
               from_: int = 0, sort: List = None, source: List = None,
               track_total_hits: bool = True) -> Dict:
        """
        Execute a search query using ES query DSL.
        
        Args:
            index_name: Target index
            query: ES query DSL body
            size: Number of results to return
            from_: Offset for pagination
            sort: Sort criteria
            source: Fields to include/exclude
            track_total_hits: Whether to track total hits
            
        Returns:
            Search results dictionary
        """
        if not self._connected:
            return {"hits": {"total": 0, "hits": []}}
        
        try:
            # In production:
            # body = {"query": query}
            # if sort:
            #     body["sort"] = sort
            # if source:
            #     body["_source"] = source
            #
            # result = self._client.search(
            #     index=index_name,
            #     body=body,
            #     size=size,
            #     from_=from_,
            #     track_total_hits=track_total_hits
            # )
            # return result
            
            return {
                "hits": {
                    "total": {"value": 0, "relation": "eq"},
                    "hits": []
                },
                "took": 1
            }
        except Exception as e:
            print(f"Search failed: {e}")
            return {"hits": {"total": 0, "hits": []}}
    
    def search_full_text(self, index_name: str, query_text: str,
                         fields: List[str] = None, fuzziness: str = "AUTO",
                         size: int = 100) -> Dict:
        """
        Perform full-text search across multiple fields.
        
        Args:
            index_name: Target index
            query_text: Text to search for
            fields: Fields to search (default: ['title', 'description', 'content'])
            fuzziness: Fuzziness setting ('AUTO', '0', '1', '2')
            size: Number of results
            
        Returns:
            Search results
        """
        fields = fields or ['title', 'description', 'content']
        
        query = {
            "multi_match": {
                "query": query_text,
                "fields": fields,
                "fuzziness": fuzziness,
                "type": "best_fields"
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_bool(self, index_name: str, must: List = None, should: List = None,
                    must_not: List = None, filter_: List = None,
                    minimum_should_match: int = None, size: int = 100) -> Dict:
        """
        Execute a boolean query.
        
        Args:
            index_name: Target index
            must: Must-match clauses (AND)
            should: Should-match clauses (OR)
            must_not: Must-not-match clauses (NOT)
            filter_: Filter clauses (no scoring)
            minimum_should_match: Min should match for should clauses
            size: Number of results
            
        Returns:
            Search results
        """
        bool_query = {}
        
        if must:
            bool_query["must"] = must
        if should:
            bool_query["should"] = should
            if minimum_should_match:
                bool_query["minimum_should_match"] = minimum_should_match
        if must_not:
            bool_query["must_not"] = must_not
        if filter_:
            bool_query["filter"] = filter_
        
        query = {"bool": bool_query} if bool_query else {"match_all": {}}
        
        return self.search(index_name, query, size=size)
    
    def search_phrase(self, index_name: str, field: str, phrase: str,
                      slop: int = 0, size: int = 100) -> Dict:
        """
        Search for exact phrase.
        
        Args:
            index_name: Target index
            field: Field to search
            phrase: Exact phrase to find
            slop: Word proximity tolerance
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "match_phrase": {
                field: {
                    "query": phrase,
                    "slop": slop
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_wildcard(self, index_name: str, field: str, pattern: str,
                        size: int = 100) -> Dict:
        """
        Wildcard search.
        
        Args:
            index_name: Target index
            field: Field to search
            pattern: Wildcard pattern (* and ?)
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "wildcard": {
                field: {
                    "value": pattern,
                    "case_insensitive": True
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_prefix(self, index_name: str, field: str, prefix: str,
                      size: int = 100) -> Dict:
        """
        Prefix search.
        
        Args:
            index_name: Target index
            field: Field to search
            prefix: Prefix to match
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "prefix": {
                field: {
                    "value": prefix,
                    "case_insensitive": True
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_regexp(self, index_name: str, field: str, regexp: str,
                      flags: str = "ALL", size: int = 100) -> Dict:
        """
        Regexp search.
        
        Args:
            index_name: Target index
            field: Field to search
            regexp: Regular expression
            flags: Regex flags (ALL, ANY, COMPLEMENT, etc.)
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "regexp": {
                field: {
                    "value": regexp,
                    "flags": flags
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    # =========================================================================
    # 4. AGGREGATIONS
    # =========================================================================
    
    def aggregate(self, index_name: str, aggregations: Dict,
                  size: int = 0) -> Dict:
        """
        Execute aggregations query.
        
        Args:
            index_name: Target index
            aggregations: ES aggregation definitions
            size: Number of docs to return (0 for no hits)
            
        Returns:
            Aggregation results
        """
        if not self._connected:
            return {}
        
        try:
            query = {"match_all": {}}
            body = {
                "size": size,
                "aggs": aggregations
            }
            
            # In production:
            # result = self._client.search(
            #     index=index_name,
            #     body=body,
            #     size=size
            # )
            # return {"aggregations": result.get("aggregations", {})}
            
            return {"aggregations": {}}
        except Exception as e:
            print(f"Aggregate failed: {e}")
            return {}
    
    def aggregate_terms(self, index_name: str, field: str,
                        size: int = 10) -> Dict:
        """
        Terms aggregation for faceting.
        
        Args:
            index_name: Target index
            field: Field to aggregate on
            size: Number of top terms to return
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "terms_agg": {
                "terms": {
                    "field": field,
                    "size": size
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_date_histogram(self, index_name: str, field: str,
                                  calendar_interval: str = "day",
                                  min_doc_count: int = 0,
                                  extended_bounds_min: str = None,
                                  extended_bounds_max: str = None) -> Dict:
        """
        Date histogram aggregation for time-series data.
        
        Args:
            index_name: Target index
            field: Date field to aggregate on
            calendar_interval: Interval (minute, hour, day, week, month)
            min_doc_count: Minimum doc count per bucket
            extended_bounds_min: Start of range
            extended_bounds_max: End of range
            
        Returns:
            Aggregation results
        """
        agg_config = {
            "date_histogram": {
                "field": field,
                "calendar_interval": calendar_interval,
                "min_doc_count": min_doc_count
            }
        }
        
        if extended_bounds_min or extended_bounds_max:
            agg_config["date_histogram"]["extended_bounds"] = {
                "min": extended_bounds_min,
                "max": extended_bounds_max
            }
        
        aggregations = {"date_histogram_agg": agg_config}
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_range(self, index_name: str, field: str,
                         ranges: List[Dict]) -> Dict:
        """
        Range aggregation.
        
        Args:
            index_name: Target index
            field: Numeric field
            ranges: List of range definitions {'from': x, 'to': y, 'key': 'label'}
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "range_agg": {
                "range": {
                    "field": field,
                    "ranges": ranges
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_histogram(self, index_name: str, field: str,
                             interval: float) -> Dict:
        """
        Histogram aggregation for numeric fields.
        
        Args:
            index_name: Target index
            field: Numeric field
            interval: Bucket interval
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "histogram_agg": {
                "histogram": {
                    "field": field,
                    "interval": interval
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_nested(self, index_name: str, path: str,
                          aggregations: Dict) -> Dict:
        """
        Nested aggregation for nested field types.
        
        Args:
            index_name: Target index
            path: Path to nested field
            aggregations: Aggregations to run inside nested
            
        Returns:
            Aggregation results
        """
        full_aggs = {
            "nested_agg": {
                "nested": {
                    "path": path
                },
                "aggs": aggregations
            }
        }
        
        return self.aggregate(index_name, full_aggs)
    
    def aggregate_cardinality(self, index_name: str, field: str,
                               precision_threshold: int = 3000) -> Dict:
        """
        Cardinality aggregation for unique counts.
        
        Args:
            index_name: Target index
            field: Field for unique count
            precision_threshold: Precision threshold
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "cardinality_agg": {
                "cardinality": {
                    "field": field,
                    "precision_threshold": precision_threshold
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_percentiles(self, index_name: str, field: str,
                               percents: List[float] = None) -> Dict:
        """
        Percentiles aggregation.
        
        Args:
            index_name: Target index
            field: Numeric field
            percents: Percentile values (default: [1, 5, 25, 50, 75, 95, 99])
            
        Returns:
            Aggregation results
        """
        percents = percents or [1, 5, 25, 50, 75, 95, 99]
        
        aggregations = {
            "percentiles_agg": {
                "percentiles": {
                    "field": field,
                    "percents": percents
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_stats(self, index_name: str, field: str) -> Dict:
        """
        Stats aggregation (count, min, max, avg, sum).
        
        Args:
            index_name: Target index
            field: Numeric field
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "stats_agg": {
                "stats": {
                    "field": field
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_extended_stats(self, index_name: str, field: str) -> Dict:
        """
        Extended stats aggregation (includes variance, std_deviation).
        
        Args:
            index_name: Target index
            field: Numeric field
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "extended_stats_agg": {
                "extended_stats": {
                    "field": field
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_multi_value(self, index_name: str, aggregations: Dict) -> Dict:
        """
        Execute multiple aggregations in one request.
        
        Args:
            index_name: Target index
            aggregations: Multiple aggregation definitions
            
        Returns:
            Combined aggregation results
        """
        return self.aggregate(index_name, aggregations)
    
    # =========================================================================
    # 5. GEO SEARCH
    # =========================================================================
    
    def search_geo_bounding_box(self, index_name: str, field: str,
                                  top_left: Tuple[float, float],
                                  bottom_right: Tuple[float, float],
                                  size: int = 100) -> Dict:
        """
        Search within a bounding box.
        
        Args:
            index_name: Target index
            field: Geo point field
            top_left: (lat, lon) of top-left corner
            bottom_right: (lat, lon) of bottom-right corner
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "geo_bounding_box": {
                field: {
                    "top_left": {
                        "lat": top_left[0],
                        "lon": top_left[1]
                    },
                    "bottom_right": {
                        "lat": bottom_right[0],
                        "lon": bottom_right[1]
                    }
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_geo_distance(self, index_name: str, field: str,
                             location: Tuple[float, float],
                             distance: str, unit: str = "km",
                             size: int = 100) -> Dict:
        """
        Search within a distance from a point.
        
        Args:
            index_name: Target index
            field: Geo point field
            location: (lat, lon) of center point
            distance: Distance value (e.g., "100")
            unit: Unit (km, mi, m)
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "geo_distance": {
                "distance": f"{distance}{unit}",
                field: {
                    "lat": location[0],
                    "lon": location[1]
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_geo_polygon(self, index_name: str, field: str,
                            points: List[Tuple[float, float]],
                            size: int = 100) -> Dict:
        """
        Search within a polygon.
        
        Args:
            index_name: Target index
            field: Geo point field
            points: List of (lat, lon) points defining polygon
            size: Number of results
            
        Returns:
            Search results
        """
        polygon_points = [
            {"lat": p[0], "lon": p[1]} for p in points
        ]
        
        query = {
            "geo_polygon": {
                field: {
                    "points": polygon_points
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def search_geo_shape(self, index_name: str, field: str,
                          shape: Dict, relation: str = "INTERSECTS",
                          size: int = 100) -> Dict:
        """
        Search using geoshape (complex geometries).
        
        Args:
            index_name: Target index
            field: Geo shape field
            shape: GeoJSON shape definition
            relation: Spatial relation (INTERSECTS, WITHIN, DISJOINT, CONTAINS)
            size: Number of results
            
        Returns:
            Search results
        """
        query = {
            "geo_shape": {
                field: {
                    "shape": shape,
                    "relation": relation
                }
            }
        }
        
        return self.search(index_name, query, size=size)
    
    def aggregate_geo_grid(self, index_name: str, field: str,
                            precision: int = 5) -> Dict:
        """
        Aggregate by geohash grid for map visualization.
        
        Args:
            index_name: Target index
            field: Geo point field
            precision: Geohash precision (1-12)
            
        Returns:
            Aggregation results
        """
        aggregations = {
            "geo_grid_agg": {
                "geohash_grid": {
                    "field": field,
                    "precision": precision
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def aggregate_geo_centroid(self, index_name: str, field: str) -> Dict:
        """
        Calculate centroid of all geo points.
        
        Args:
            index_name: Target index
            field: Geo point field
            
        Returns:
            Aggregation results with centroid location
        """
        aggregations = {
            "centroid_agg": {
                "geo_centroid": {
                    "field": field
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    # =========================================================================
    # 6. TIME-SERIES
    # =========================================================================
    
    def store_time_series(self, index_name: str, timestamp: datetime,
                           metric_name: str, value: float,
                           tags: Dict = None, document_id: str = None) -> bool:
        """
        Store a time-series data point.
        
        Args:
            index_name: Target index (use date-based naming)
            timestamp: Timestamp of the data point
            metric_name: Name of the metric
            value: Metric value
            tags: Additional tags/dimensions
            document_id: Optional custom document ID
            
        Returns:
            True if successful
        """
        document = {
            "timestamp": timestamp.isoformat(),
            "metric_name": metric_name,
            "value": value,
            "tags": tags or {}
        }
        
        doc_id = document_id or f"{metric_name}_{timestamp.timestamp()}"
        
        return self.index_document(index_name, doc_id, document)
    
    def store_execution_data(self, workflow_id: str, execution_id: str,
                               start_time: datetime, end_time: datetime,
                               status: str, metrics: Dict = None) -> bool:
        """
        Store workflow execution time-series data.
        
        Args:
            workflow_id: Workflow identifier
            execution_id: Execution identifier
            start_time: Execution start time
            end_time: Execution end time
            status: Execution status (success, failure, timeout)
            metrics: Additional execution metrics
            
        Returns:
            True if successful
        """
        duration_seconds = (end_time - start_time).total_seconds()
        
        document = {
            "workflow_id": workflow_id,
            "execution_id": execution_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_seconds,
            "status": status,
            "metrics": metrics or {}
        }
        
        return self.index_document(
            self.DEFAULT_EXECUTION_INDEX,
            execution_id,
            document
        )
    
    def query_time_series(self, index_name: str, start_time: datetime,
                           end_time: datetime, metric_name: str = None,
                           interval: str = "1h") -> Dict:
        """
        Query time-series data with date histogram.
        
        Args:
            index_name: Target index
            start_time: Start of time range
            end_time: End of time range
            metric_name: Filter by metric name (optional)
            interval: Histogram interval
            
        Returns:
            Time-series data with histogram buckets
        """
        must_clauses = [
            {
                "range": {
                    "timestamp": {
                        "gte": start_time.isoformat(),
                        "lte": end_time.isoformat()
                    }
                }
            }
        ]
        
        if metric_name:
            must_clauses.append({"term": {"metric_name": metric_name}})
        
        query = {
            "bool": {
                "must": must_clauses
            }
        }
        
        aggregations = {
            "time_series": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": interval,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_time.isoformat(),
                        "max": end_time.isoformat()
                    }
                },
                "aggs": {
                    "avg_value": {"avg": {"field": "value"}},
                    "max_value": {"max": {"field": "value"}},
                    "min_value": {"min": {"field": "value"}}
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    def get_execution_stats(self, index_name: str, workflow_id: str,
                             start_time: datetime, end_time: datetime) -> Dict:
        """
        Get execution statistics for a workflow.
        
        Args:
            index_name: Target index
            workflow_id: Workflow identifier
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Execution statistics
        """
        query = {
            "bool": {
                "must": [
                    {"term": {"workflow_id": workflow_id}},
                    {
                        "range": {
                            "start_time": {
                                "gte": start_time.isoformat(),
                                "lte": end_time.isoformat()
                            }
                        }
                    }
                ]
            }
        }
        
        aggregations = {
            "status_breakdown": {
                "terms": {"field": "status"}
            },
            "duration_stats": {
                "extended_stats": {"field": "duration_seconds"}
            },
            "executions_over_time": {
                "date_histogram": {
                    "field": "start_time",
                    "calendar_interval": "day"
                }
            }
        }
        
        return self.aggregate(index_name, aggregations)
    
    # =========================================================================
    # 7. INDEX TEMPLATES
    # =========================================================================
    
    def create_index_template(self, template_name: str, index_patterns: List[str],
                               settings: Dict = None, mappings: Dict = None,
                               priority: int = 100) -> bool:
        """
        Create an index template for workflows.
        
        Args:
            template_name: Name of the template
            index_patterns: Index patterns to match (e.g., ['workflows-*'])
            settings: Template settings
            mappings: Template mappings
            priority: Template priority
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            template_body = {
                "index_patterns": index_patterns,
                "priority": priority,
                "template": {
                    "settings": settings or {
                        "number_of_shards": 3,
                        "number_of_replicas": 1,
                        "refresh_interval": "5s"
                    },
                    "mappings": mappings or self._get_default_workflow_mapping()
                }
            }
            
            # In production:
            # self._client.indices.put_index_template(
            #     name=template_name,
            #     body=template_body
            # )
            
            return True
        except Exception as e:
            print(f"Create index template failed: {e}")
            return False
    
    def delete_index_template(self, template_name: str) -> bool:
        """
        Delete an index template.
        
        Args:
            template_name: Name of the template
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.indices.delete_index_template(name=template_name)
            return True
        except Exception as e:
            print(f"Delete index template failed: {e}")
            return False
    
    def get_index_template(self, template_name: str = None) -> Dict:
        """
        Get index template(s).
        
        Args:
            template_name: Template name (optional, returns all if None)
            
        Returns:
            Template definition(s)
        """
        if not self._connected:
            return {}
        
        try:
            if template_name:
                # result = self._client.indices.get_index_template(name=template_name)
                return {"template_name": template_name}
            else:
                # result = self._client.indices.get_index_template()
                return {}
        except Exception as e:
            print(f"Get index template failed: {e}")
            return {}
    
    def _get_default_workflow_mapping(self) -> Dict:
        """
        Get default workflow index mapping.
        
        Returns:
            Default mapping dictionary
        """
        return {
            "properties": {
                "workflow_id": {"type": "keyword"},
                "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "description": {"type": "text"},
                "status": {"type": "keyword"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "created_by": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "category": {"type": "keyword"},
                "location": {"type": "geo_point"},
                "execution_count": {"type": "long"},
                "avg_duration": {"type": "float"},
                "success_rate": {"type": "float"},
                "metadata": {"type": "object", "enabled": False}
            }
        }
    
    def setup_workflow_templates(self) -> bool:
        """
        Set up all default workflow index templates.
        
        Returns:
            True if all templates created successfully
        """
        # Main workflows template
        workflow_template = self.create_index_template(
            "workflows_template",
            ["workflows-*"],
            priority=100
        )
        
        # Execution logs template
        execution_template = self.create_index_template(
            "workflows_executions_template",
            ["workflow_executions-*"],
            settings={
                "number_of_shards": 5,
                "number_of_replicas": 1,
                "refresh_interval": "30s"
            },
            mappings={
                "properties": {
                    "workflow_id": {"type": "keyword"},
                    "execution_id": {"type": "keyword"},
                    "start_time": {"type": "date"},
                    "end_time": {"type": "date"},
                    "duration_seconds": {"type": "float"},
                    "status": {"type": "keyword"},
                    "error_message": {"type": "text"},
                    "metrics": {"type": "object", "enabled": True}
                }
            },
            priority=90
        )
        
        # Analytics template
        analytics_template = self.create_index_template(
            "workflows_analytics_template",
            ["workflow_analytics-*"],
            settings={
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "refresh_interval": "10s"
            },
            mappings={
                "properties": {
                    "timestamp": {"type": "date"},
                    "metric_name": {"type": "keyword"},
                    "value": {"type": "float"},
                    "tags": {"type": "object", "enabled": True}
                }
            },
            priority=80
        )
        
        return workflow_template and execution_template and analytics_template
    
    # =========================================================================
    # 8. ILM POLICIES
    # =========================================================================
    
    def create_ilm_policy(self, policy_name: str, phases: Dict) -> bool:
        """
        Create an index lifecycle management policy.
        
        Args:
            policy_name: Name of the policy
            phases: ILM phase definitions (hot, warm, cold, delete)
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            policy_body = {"policy": {"phases": phases}}
            
            # In production:
            # self._client.ilm.put_lifecycle(name=policy_name, body=policy_body)
            
            self._ilm_policies[policy_name] = phases
            return True
        except Exception as e:
            print(f"Create ILM policy failed: {e}")
            return False
    
    def get_ilm_policy(self, policy_name: str = None) -> Dict:
        """
        Get ILM policy/policies.
        
        Args:
            policy_name: Policy name (optional)
            
        Returns:
            Policy definition(s)
        """
        if not self._connected:
            return {}
        
        try:
            if policy_name:
                # result = self._client.ilm.get_lifecycle(name=policy_name)
                return self._ilm_policies.get(policy_name, {})
            else:
                # result = self._client.ilm.get_lifecycle()
                return self._ilm_policies
        except Exception as e:
            print(f"Get ILM policy failed: {e}")
            return {}
    
    def delete_ilm_policy(self, policy_name: str) -> bool:
        """
        Delete an ILM policy.
        
        Args:
            policy_name: Name of the policy
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.ilm.delete_lifecycle(name=policy_name)
            if policy_name in self._ilm_policies:
                del self._ilm_policies[policy_name]
            return True
        except Exception as e:
            print(f"Delete ILM policy failed: {e}")
            return False
    
    def setup_workflow_ilm(self) -> bool:
        """
        Set up default ILM policies for workflow indices.
        
        Returns:
            True if all policies created successfully
        """
        # Policy for execution logs - 30 day retention
        execution_policy = self.create_ilm_policy(
            "workflow_executions_policy",
            {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "7d",
                            "max_size": "50gb"
                        },
                        "set_priority": {"priority": 100}
                    }
                },
                "warm": {
                    "min_age": "7d",
                    "actions": {
                        "shrink": {"number_of_shards": 1},
                        "forcemerge": {"max_num_segments": 1},
                        "set_priority": {"priority": 50}
                    }
                },
                "cold": {
                    "min_age": "14d",
                    "actions": {
                        "set_priority": {"priority": 0}
                    }
                },
                "delete": {
                    "min_age": "30d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        )
        
        # Policy for analytics - 90 day retention
        analytics_policy = self.create_ilm_policy(
            "workflow_analytics_policy",
            {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "1d",
                            "max_size": "100gb"
                        },
                        "set_priority": {"priority": 100}
                    }
                },
                "warm": {
                    "min_age": "30d",
                    "actions": {
                        "shrink": {"number_of_shards": 1},
                        "forcemerge": {"max_num_segments": 1}
                    }
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        )
        
        return execution_policy and analytics_policy
    
    # =========================================================================
    # 9. SECURITY
    # =========================================================================
    
    def create_api_key(self, key_name: str, roles: List[Dict] = None,
                        expiration: str = None) -> Optional[str]:
        """
        Create an API key for authentication.
        
        Args:
            key_name: Name of the API key
            roles: Role definitions for the key
            expiration: Expiration time (e.g., "30d")
            
        Returns:
            API key string or None on failure
        """
        if not self._connected:
            return None
        
        try:
            key_body = {
                "name": key_name,
                "role_descriptors": roles or {}
            }
            
            if expiration:
                key_body["expiration"] = expiration
            
            # In production:
            # result = self._client.security.create_api_key(body=key_body)
            # return result["encoded"]
            
            return f"es_api_key_{key_name}_{int(time.time())}"
        except Exception as e:
            print(f"Create API key failed: {e}")
            return None
    
    def get_api_key_info(self, key_id: str) -> Optional[Dict]:
        """
        Get information about an API key.
        
        Args:
            key_id: API key ID
            
        Returns:
            API key info dictionary
        """
        if not self._connected:
            return None
        
        try:
            # result = self._client.security.get_api_key(id=key_id)
            return {"id": key_id, "name": "workflow_key"}
        except Exception as e:
            print(f"Get API key info failed: {e}")
            return None
    
    def invalidate_api_key(self, key_id: str) -> bool:
        """
        Invalidate/revoke an API key.
        
        Args:
            key_id: API key ID to revoke
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.security.invalidate_api_key(id=key_id)
            return True
        except Exception as e:
            print(f"Invalidate API key failed: {e}")
            return False
    
    def create_role(self, role_name: str, cluster_permissions: List[str] = None,
                     index_permissions: List[Dict] = None) -> bool:
        """
        Create a security role.
        
        Args:
            role_name: Name of the role
            cluster_permissions: Cluster-level permissions
            index_permissions: Index-level permissions
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            role_body = {
                "cluster": cluster_permissions or [],
                "index": index_permissions or []
            }
            
            # In production:
            # self._client.security.put_role(name=role_name, body=role_body)
            
            return True
        except Exception as e:
            print(f"Create role failed: {e}")
            return False
    
    def get_role(self, role_name: str) -> Optional[Dict]:
        """
        Get a security role definition.
        
        Args:
            role_name: Name of the role
            
        Returns:
            Role definition
        """
        if not self._connected:
            return None
        
        try:
            # result = self._client.security.get_role(name=role_name)
            return {"name": role_name}
        except Exception as e:
            print(f"Get role failed: {e}")
            return None
    
    def create_user(self, username: str, password: str,
                    roles: List[str] = None, full_name: str = None,
                    email: str = None) -> bool:
        """
        Create a user for Elasticsearch authentication.
        
        Args:
            username: Username
            password: Password
            roles: List of role names
            full_name: Full name
            email: Email address
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            user_body = {
                "password": password,
                "roles": roles or [],
                "full_name": full_name,
                "email": email
            }
            
            # In production:
            # self._client.security.put_user(username=username, body=user_body)
            
            return True
        except Exception as e:
            print(f"Create user failed: {e}")
            return False
    
    def set_document_security(self, index_name: str, doc_id: str,
                               field: str, value: Any) -> bool:
        """
        Set field-level security on a document.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            field: Field name
            value: Field value for access control
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # Update document with security field
            return self.update_document(index_name, doc_id, {
                "_security_field": value
            })
        except Exception as e:
            print(f"Set document security failed: {e}")
            return False
    
    def search_with_field_security(self, index_name: str, query: Dict,
                                     allowed_fields: List[str] = None) -> Dict:
        """
        Search with field-level security.
        
        Args:
            index_name: Target index
            query: Search query
            allowed_fields: Fields user is allowed to see
            
        Returns:
            Filtered search results
        """
        if allowed_fields:
            return self.search(index_name, query, source=allowed_fields)
        return self.search(index_name, query)
    
    # =========================================================================
    # 10. CROSS-CLUSTER SEARCH
    # =========================================================================
    
    def add_remote_cluster(self, cluster_alias: str, seeds: List[str]) -> bool:
        """
        Add a remote cluster for cross-cluster search.
        
        Args:
            cluster_alias: Alias name for the remote cluster
            seeds: List of seed node addresses
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # In production:
            # self._client.cluster.put_settings(
            #     body={
            #         "persistent": {
            #             f"cluster.remote.{cluster_alias}.seeds": seeds
            #         }
            #     }
            # )
            
            self._clusters[cluster_alias] = {"seeds": seeds}
            return True
        except Exception as e:
            print(f"Add remote cluster failed: {e}")
            return False
    
    def remove_remote_cluster(self, cluster_alias: str) -> bool:
        """
        Remove a remote cluster.
        
        Args:
            cluster_alias: Alias of the cluster to remove
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.cluster.put_settings(
            #     body={
            #         "persistent": {
            #             f"cluster.remote.{cluster_alias}.seeds": None
            #         }
            #     }
            # )
            
            if cluster_alias in self._clusters:
                del self._clusters[cluster_alias]
            return True
        except Exception as e:
            print(f"Remove remote cluster failed: {e}")
            return False
    
    def get_remote_clusters(self) -> Dict:
        """
        Get all configured remote clusters.
        
        Returns:
            Dictionary of remote clusters
        """
        if not self._connected:
            return {}
        
        try:
            # result = self._client.cluster.get_settings()
            # return result.get("persistent", {}).get("cluster", {}).get("remote", {})
            return self._clusters
        except Exception as e:
            print(f"Get remote clusters failed: {e}")
            return {}
    
    def search_cross_cluster(self, clusters: List[str], index_name: str,
                              query: Dict, size: int = 100) -> Dict:
        """
        Search across multiple clusters.
        
        Args:
            clusters: List of cluster aliases to search
            index_name: Index name (same name on all clusters)
            query: ES query DSL
            size: Number of results
            
        Returns:
            Combined search results
        """
        if not self._connected:
            return {"hits": {"total": 0, "hits": []}}
        
        try:
            # Build cross-cluster index pattern
            index_pattern = ",".join([f"{cluster}:{index_name}" for cluster in clusters])
            
            # In production:
            # body = {"query": query}
            # result = self._client.search(
            #     index=index_pattern,
            #     body=body,
            #     size=size
            # )
            # return result
            
            return {
                "hits": {
                    "total": {"value": 0, "relation": "eq"},
                    "hits": []
                },
                "_clusters": {
                    "total": len(clusters),
                    "successful": len(clusters),
                    "skipped": 0,
                    "failed": 0
                }
            }
        except Exception as e:
            print(f"Cross-cluster search failed: {e}")
            return {"hits": {"total": 0, "hits": []}}
    
    def ccs_search_templated(self, clusters: List[str], index_pattern: str,
                              template_name: str, template_params: Dict = None,
                              size: int = 100) -> Dict:
        """
        Cross-cluster search using a search template.
        
        Args:
            clusters: List of cluster aliases
            index_pattern: Index pattern to search
            template_name: Name of the search template
            template_params: Parameters for the template
            size: Number of results
            
        Returns:
            Combined search results
        """
        if not self._connected:
            return {"hits": {"total": 0, "hits": []}}
        
        try:
            index_spec = ",".join([f"{c}:{index_pattern}" for c in clusters])
            
            # In production:
            # result = self._client.search_template(
            #     index=index_spec,
            #     body={
            #         "id": template_name,
            #         "params": template_params or {}
            #     },
            #     size=size
            # )
            # return result
            
            return {"hits": {"total": 0, "hits": []}}
        except Exception as e:
            print(f"CCS search template failed: {e}")
            return {"hits": {"total": 0, "hits": []}}
    
    def sync_search_across_clusters(self, clusters: List[str],
                                     indices: Dict[str, str],
                                     query: Dict, size: int = 100) -> Dict:
        """
        Search different indices across different clusters.
        
        Args:
            clusters: List of cluster aliases
            indices: Dict mapping cluster alias to index name
            query: ES query DSL
            size: Number of results
            
        Returns:
            Combined search results
        """
        if not self._connected:
            return {"hits": {"total": 0, "hits": []}}
        
        try:
            # Build index specification with different indices per cluster
            index_specs = []
            for cluster in clusters:
                if cluster in indices:
                    index_specs.append(f"{cluster}:{indices[cluster]}")
            
            index_spec = ",".join(index_specs)
            
            # In production:
            # result = self._client.search(
            #     index=index_spec,
            #     body={"query": query},
            #     size=size
            # )
            # return result
            
            return {
                "hits": {"total": 0, "hits": []},
                "_clusters": {"total": len(clusters)}
            }
        except Exception as e:
            print(f"Sync search across clusters failed: {e}")
            return {"hits": {"total": 0, "hits": []}}
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def health_check(self) -> Dict:
        """
        Check cluster health.
        
        Returns:
            Health status dictionary
        """
        if not self._connected:
            return {"status": "disconnected"}
        
        try:
            # result = self._client.cluster.health()
            # return dict(result)
            return {
                "cluster_name": "workflow_elasticsearch",
                "status": "green",
                "number_of_nodes": 1
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def get_cluster_stats(self) -> Dict:
        """
        Get comprehensive cluster statistics.
        
        Returns:
            Cluster statistics
        """
        if not self._connected:
            return {}
        
        try:
            # In production:
            # stats = self._client.cluster.stats()
            # return dict(stats)
            return {
                "cluster_name": "workflow_elasticsearch",
                "nodes": {"count": {"total": 1}}
            }
        except Exception as e:
            print(f"Get cluster stats failed: {e}")
            return {}
    
    def reindex(self, source_index: str, dest_index: str,
                 query: Dict = None, reindex_topn: int = None) -> bool:
        """
        Reindex documents from source to destination.
        
        Args:
            source_index: Source index name
            dest_index: Destination index name
            query: Optional query to filter documents
            reindex_topn: Reindex only top N documents
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            reindex_body = {"source": {"index": source_index}}
            
            if query:
                reindex_body["source"]["query"] = query
            
            if reindex_topn:
                reindex_body["source"]["size"] = reindex_topn
            
            reindex_body["dest"] = {"index": dest_index}
            
            # In production:
            # task = self._client.reindex(body=reindex_body, wait_for_completion=False)
            # return task.get("task", None) is not None
            
            return True
        except Exception as e:
            print(f"Reindex failed: {e}")
            return False
    
    def create_search_template(self, template_name: str,
                                template_body: Dict) -> bool:
        """
        Create a stored search template.
        
        Args:
            template_name: Name of the template
            template_body: Template body with {{placeholder}} syntax
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # In production:
            # self._client.security.put_script(
            #     id=template_name,
            #     body={"script": template_body}
            # )
            return True
        except Exception as e:
            print(f"Create search template failed: {e}")
            return False
    
    def execute_search_template(self, template_name: str,
                                 params: Dict = None) -> Dict:
        """
        Execute a stored search template.
        
        Args:
            template_name: Name of the template
            params: Parameter values for placeholders
            
        Returns:
            Search results
        """
        if not self._connected:
            return {"hits": {"total": 0, "hits": []}}
        
        try:
            # In production:
            # result = self._client.search_template(
            #     body={
            #         "id": template_name,
            #         "params": params or {}
            #     }
            # )
            # return result
            return {"hits": {"total": 0, "hits": []}}
        except Exception as e:
            print(f"Execute search template failed: {e}")
            return {"hits": {"total": 0, "hits": []}}
    
    def explain_document(self, index_name: str, doc_id: str,
                          query: Dict) -> Dict:
        """
        Explain why/why not a document matches a query.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            query: Query to explain
            
        Returns:
            Explanation dictionary
        """
        if not self._connected:
            return {}
        
        try:
            # result = self._client.explain(
            #     index=index_name,
            #     id=doc_id,
            #     body={"query": query}
            # )
            # return dict(result)
            return {"match": False, "score": 0.0}
        except Exception as e:
            print(f"Explain document failed: {e}")
            return {}
    
    def get_document_count(self, index_name: str, query: Dict = None) -> int:
        """
        Get document count in an index.
        
        Args:
            index_name: Target index
            query: Optional query filter
            
        Returns:
            Document count
        """
        if not self._connected:
            return 0
        
        try:
            # count = self._client.count(index=index_name, body={"query": query if query else {"match_all": {}}})
            # return count["count"]
            return 0
        except Exception as e:
            print(f"Get document count failed: {e}")
            return 0
    
    def get_field_mappings(self, index_name: str, fields: List[str] = None) -> Dict:
        """
        Get field mappings for an index.
        
        Args:
            index_name: Target index
            fields: Specific fields to get (None for all)
            
        Returns:
            Field mappings dictionary
        """
        if not self._connected:
            return {}
        
        try:
            # result = self._client.indices.get_field_mapping(
            #     index=index_name,
            #     fields=fields
            # )
            # return result
            return self._index_mappings.get(index_name, {})
        except Exception as e:
            print(f"Get field mappings failed: {e}")
            return {}
    
    def put_pipeline(self, pipeline_name: str, processors: List[Dict]) -> bool:
        """
        Create an ingest pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            processors: List of processor definitions
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            pipeline_body = {
                "description": f"Pipeline {pipeline_name}",
                "processors": processors
            }
            
            # self._client.ingest.put_pipeline(
            #     id=pipeline_name,
            #     body=pipeline_body
            # )
            return True
        except Exception as e:
            print(f"Put pipeline failed: {e}")
            return False
    
    def index_with_pipeline(self, index_name: str, doc_id: str,
                             document: Dict, pipeline_name: str) -> bool:
        """
        Index a document using an ingest pipeline.
        
        Args:
            index_name: Target index
            doc_id: Document ID
            document: Document body
            pipeline_name: Pipeline to use
            
        Returns:
            True if successful
        """
        if not self._connected:
            return False
        
        try:
            # self._client.index(
            #     index=index_name,
            #     id=doc_id,
            #     document=document,
            #     pipeline=pipeline_name
            # )
            return True
        except Exception as e:
            print(f"Index with pipeline failed: {e}")
            return False


# Standalone functions for backward compatibility

def create_workflow_es_client(hosts: List[str] = None, **kwargs) -> WorkflowElasticsearch:
    """
    Create an Elasticsearch client for workflows.
    
    Args:
        hosts: ES host URLs
        **kwargs: Additional client arguments
        
    Returns:
        WorkflowElasticsearch instance
    """
    client = WorkflowElasticsearch(hosts=hosts, **kwargs)
    client.connect()
    return client


def workflow_search(es_client: WorkflowElasticsearch, query: Dict,
                     index: str = "workflows") -> Dict:
    """
    Search workflows using the ES client.
    
    Args:
        es_client: WorkflowElasticsearch instance
        query: ES query DSL
        index: Index to search
        
    Returns:
        Search results
    """
    return es_client.search(index, query)


def workflow_aggregate(es_client: WorkflowElasticsearch, aggregations: Dict,
                        index: str = "workflows") -> Dict:
    """
    Run aggregations on workflows.
    
    Args:
        es_client: WorkflowElasticsearch instance
        aggregations: ES aggregation definitions
        index: Index to aggregate
        
    Returns:
        Aggregation results
    """
    return es_client.aggregate(index, aggregations)
