"""
Tests for Workflow Elasticsearch Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_elasticsearch import (
    WorkflowElasticsearch,
    IndexLifecyclePhase,
    GeoDistanceUnit,
)


class TestWorkflowElasticsearch(unittest.TestCase):
    """Test WorkflowElasticsearch class"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch(
            hosts=['http://localhost:9200'],
            username='elastic',
            password='test123'
        )

    def test_init(self):
        """Test Elasticsearch initialization"""
        self.assertEqual(self.es.hosts, ['http://localhost:9200'])
        self.assertEqual(self.es.username, 'elastic')
        self.assertEqual(self.es.password, 'test123')
        self.assertFalse(self.es._connected)

    def test_default_hosts(self):
        """Test default hosts"""
        es = WorkflowElasticsearch()
        self.assertEqual(es.hosts, ['http://localhost:9200'])

    def test_connect_success(self):
        """Test successful connection"""
        result = self.es.connect()
        self.assertTrue(result)
        self.assertTrue(self.es._connected)

    def test_disconnect(self):
        """Test disconnection"""
        self.es.connect()
        self.es.disconnect()
        self.assertFalse(self.es._connected)
        self.assertIsNone(self.es._client)


class TestIndexManagement(unittest.TestCase):
    """Test index management functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()

    def test_create_index(self):
        """Test creating an index"""
        result = self.es.create_index("test_index")
        self.assertTrue(result)
        self.assertTrue(self.es.index_exists("test_index"))

    def test_create_index_with_settings(self):
        """Test creating index with custom settings"""
        settings = {"number_of_shards": 5, "number_of_replicas": 2}
        result = self.es.create_index("test_index_2", settings=settings)
        self.assertTrue(result)

    def test_create_index_with_mappings(self):
        """Test creating index with field mappings"""
        mappings = {
            "properties": {
                "workflow_id": {"type": "keyword"},
                "content": {"type": "text"}
            }
        }
        result = self.es.create_index("test_index_3", mappings=mappings)
        self.assertTrue(result)
        self.assertIn("test_index_3", self.es._index_mappings)

    def test_create_index_not_connected(self):
        """Test creating index when not connected"""
        es = WorkflowElasticsearch()
        result = es.create_index("test_index")
        self.assertFalse(result)

    def test_delete_index(self):
        """Test deleting an index"""
        self.es.create_index("to_delete")
        result = self.es.delete_index("to_delete")
        self.assertTrue(result)
        self.assertFalse(self.es.index_exists("to_delete"))

    def test_delete_index_not_connected(self):
        """Test deleting index when not connected"""
        es = WorkflowElasticsearch()
        result = es.delete_index("test_index")
        self.assertFalse(result)

    def test_index_exists(self):
        """Test checking if index exists"""
        self.es.create_index("existing_index")
        self.assertTrue(self.es.index_exists("existing_index"))
        self.assertFalse(self.es.index_exists("nonexistent_index"))

    def test_get_index_stats(self):
        """Test getting index statistics"""
        self.es.create_index("stats_index")
        stats = self.es.get_index_stats("stats_index")
        self.assertIsInstance(stats, dict)


class TestDocumentIndexing(unittest.TestCase):
    """Test document indexing functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()
        self.es.create_index("docs_index")

    def test_index_document(self):
        """Test indexing a document"""
        doc = {
            "workflow_id": "wf_001",
            "workflow_name": "Test Workflow",
            "content": "Test content"
        }
        result = self.es.index_document("docs_index", "doc_001", doc)
        self.assertTrue(result)

    def test_index_document_with_id(self):
        """Test indexing a document with custom ID"""
        doc = {
            "workflow_id": "wf_002",
            "workflow_name": "Another Workflow"
        }
        result = self.es.index_document("docs_index", "custom_id", doc)
        self.assertTrue(result)

    def test_index_document_not_connected(self):
        """Test indexing document when not connected"""
        es = WorkflowElasticsearch()
        result = es.index_document("docs_index", "doc_id", {"test": "doc"})
        self.assertFalse(result)

    def test_bulk_index(self):
        """Test bulk indexing"""
        docs = [
            {"workflow_id": "wf_001", "content": "Content 1"},
            {"workflow_id": "wf_002", "content": "Content 2"},
            {"workflow_id": "wf_003", "content": "Content 3"}
        ]
        result = self.es.bulk_index("docs_index", docs)
        self.assertTrue(result)

    def test_get_document(self):
        """Test getting a document"""
        doc = {"workflow_id": "wf_001", "content": "Test"}
        self.es.index_document("docs_index", "test_doc", doc)
        retrieved = self.es.get_document("docs_index", "test_doc")
        self.assertIsInstance(retrieved, dict)

    def test_update_document(self):
        """Test updating a document"""
        doc = {"workflow_id": "wf_001", "content": "Original"}
        self.es.index_document("docs_index", "update_doc", doc)
        result = self.es.update_document("docs_index", "update_doc", {"content": "Updated"})
        self.assertTrue(result)

    def test_delete_document(self):
        """Test deleting a document"""
        doc = {"workflow_id": "wf_001", "content": "To delete"}
        self.es.index_document("docs_index", "delete_doc", doc)
        result = self.es.delete_document("docs_index", "delete_doc")
        self.assertTrue(result)


class TestFullTextSearch(unittest.TestCase):
    """Test full-text search functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()
        self.es.create_index("search_index")

    def test_search(self):
        """Test basic search"""
        self.es.index_document("search_index", "doc_001", {"content": "Hello world test"})
        results = self.es.search("search_index", {"query": "hello"})
        self.assertIsInstance(results, dict)

    def test_search_with_filters(self):
        """Test search with filters"""
        results = self.es.search("search_index", {"query": "test"}, filters={"status": "active"})
        self.assertIsInstance(results, dict)

    def test_search_pagination(self):
        """Test search with pagination"""
        results = self.es.search("search_index", {"query": "test"}, from_=0, size=10)
        self.assertIsInstance(results, dict)

    def test_search_not_connected(self):
        """Test search when not connected"""
        es = WorkflowElasticsearch()
        results = es.search("search_index", {"query": "test"})
        self.assertFalse(results)


class TestAggregations(unittest.TestCase):
    """Test aggregation functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()
        self.es.create_index("agg_index")

    def test_aggregate(self):
        """Test aggregation"""
        result = self.es.aggregate("agg_index", "workflow_category", "terms")
        self.assertIsInstance(result, dict)

    def test_aggregate_with_filters(self):
        """Test aggregation with filters"""
        result = self.es.aggregate(
            "agg_index",
            "status",
            "terms",
            filters={"category": "automation"}
        )
        self.assertIsInstance(result, dict)

    def test_aggregate_not_connected(self):
        """Test aggregation when not connected"""
        es = WorkflowElasticsearch()
        result = es.aggregate("agg_index", "field", "terms")
        self.assertFalse(result)


class TestGeoSearch(unittest.TestCase):
    """Test geo search functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()
        self.es.create_index("geo_index")

    def test_search_geo_distance(self):
        """Test geo distance search"""
        result = self.es.search_geo_distance(
            "geo_index",
            "location",
            lat=40.7128,
            lon=-74.0060,
            distance="10km"
        )
        self.assertIsInstance(result, dict)

    def test_search_geo_distance_not_connected(self):
        """Test geo distance search when not connected"""
        es = WorkflowElasticsearch()
        result = es.search_geo_distance("geo_index", "location", 40.0, -74.0, "5km")
        self.assertFalse(result)


class TestTimeSeries(unittest.TestCase):
    """Test time-series functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()
        self.es.create_index("ts_index")

    def test_index_time_series(self):
        """Test indexing time-series data"""
        result = self.es.index_time_series(
            "ts_index",
            {"metric": 42.0, "status": "success"},
            timestamp=datetime.now()
        )
        self.assertTrue(result)

    def test_get_time_series(self):
        """Test getting time-series data"""
        self.es.index_time_series(
            "ts_index",
            {"metric": 42.0},
            timestamp=datetime.now()
        )
        result = self.es.get_time_series(
            "ts_index",
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now()
        )
        self.assertIsInstance(result, dict)


class TestILM(unittest.TestCase):
    """Test index lifecycle management"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()

    def test_create_ilm_policy(self):
        """Test creating ILM policy"""
        result = self.es.create_ilm_policy(
            "test_policy",
            phases={
                IndexLifecyclePhase.HOT: {"min_age": "0ms"},
                IndexLifecyclePhase.DELETE: {"min_age": "30d"}
            }
        )
        self.assertTrue(result)
        self.assertIn("test_policy", self.es._ilm_policies)

    def test_apply_ilm_policy(self):
        """Test applying ILM policy to index"""
        self.es.create_index("ilm_index")
        result = self.es.apply_ilm_policy("ilm_index", "test_policy")
        self.assertTrue(result)


class TestCrossCluster(unittest.TestCase):
    """Test cross-cluster search"""

    def setUp(self):
        """Set up test fixtures"""
        self.es = WorkflowElasticsearch()
        self.es.connect()

    def test_add_cluster(self):
        """Test adding a remote cluster"""
        result = self.es.add_cluster("remote_cluster", ["http://remote:9200"])
        self.assertTrue(result)
        self.assertIn("remote_cluster", self.es._clusters)

    def test_search_cross_cluster(self):
        """Test cross-cluster search"""
        self.es.add_cluster("remote", ["http://remote:9200"])
        self.es.create_index("cc_index")
        result = self.es.search_cross_cluster("remote", "cc_index", "test query")
        self.assertIsInstance(result, dict)


class TestIndexLifecyclePhase(unittest.TestCase):
    """Test IndexLifecyclePhase enum"""

    def test_phase_values(self):
        """Test ILM phase values"""
        self.assertEqual(IndexLifecyclePhase.HOT.value, "hot")
        self.assertEqual(IndexLifecyclePhase.WARM.value, "warm")
        self.assertEqual(IndexLifecyclePhase.COLD.value, "cold")
        self.assertEqual(IndexLifecyclePhase.DELETE.value, "delete")


class TestGeoDistanceUnit(unittest.TestCase):
    """Test GeoDistanceUnit enum"""

    def test_unit_values(self):
        """Test geo distance unit values"""
        self.assertEqual(GeoDistanceUnit.KM.value, "km")
        self.assertEqual(GeoDistanceUnit.MI.value, "mi")
        self.assertEqual(GeoDistanceUnit.M.value, "m")


class TestDefaultIndices(unittest.TestCase):
    """Test default index constants"""

    def test_default_indices(self):
        """Test default index names"""
        self.assertEqual(WorkflowElasticsearch.DEFAULT_WORKFLOW_INDEX, "workflows")
        self.assertEqual(WorkflowElasticsearch.DEFAULT_EXECUTION_INDEX, "workflow_executions")
        self.assertEqual(WorkflowElasticsearch.DEFAULT_ANALYTICS_INDEX, "workflow_analytics")


if __name__ == '__main__':
    unittest.main()
