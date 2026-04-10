"""
Tests for Workflow OpenSearch Module
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

from src.workflow_opensearch import (
    WorkflowOpenSearch,
    IndexLifecyclePhase,
    AlertSeverity,
)


class TestWorkflowOpenSearch(unittest.TestCase):
    """Test WorkflowOpenSearch class"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch(
            hosts=['http://localhost:9200'],
            username='admin',
            password='admin123'
        )

    def test_init(self):
        """Test OpenSearch initialization"""
        self.assertEqual(self.os.hosts, ['http://localhost:9200'])
        self.assertEqual(self.os.username, 'admin')
        self.assertEqual(self.os.password, 'admin123')
        self.assertFalse(self.os._connected)

    def test_default_hosts(self):
        """Test default hosts"""
        os_client = WorkflowOpenSearch()
        self.assertEqual(os_client.hosts, ['http://localhost:9200'])

    def test_connect_success(self):
        """Test successful connection"""
        result = self.os.connect()
        self.assertTrue(result)
        self.assertTrue(self.os._connected)

    def test_disconnect(self):
        """Test disconnection"""
        self.os.connect()
        self.os.disconnect()
        self.assertFalse(self.os._connected)
        self.assertIsNone(self.os._client)


class TestIndexManagement(unittest.TestCase):
    """Test index management functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_index(self):
        """Test creating an index"""
        result = self.os.create_index("test_index")
        self.assertTrue(result)
        self.assertTrue(self.os.index_exists("test_index"))

    def test_create_index_with_settings(self):
        """Test creating index with custom settings"""
        settings = {"number_of_shards": 5, "number_of_replicas": 2}
        result = self.os.create_index("test_index_2", settings=settings)
        self.assertTrue(result)

    def test_create_index_with_mappings(self):
        """Test creating index with field mappings"""
        mappings = {
            "properties": {
                "workflow_id": {"type": "keyword"},
                "content": {"type": "text"}
            }
        }
        result = self.os.create_index("test_index_3", mappings=mappings)
        self.assertTrue(result)
        self.assertIn("test_index_3", self.os._index_mappings)

    def test_create_index_not_connected(self):
        """Test creating index when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.create_index("test_index")
        self.assertFalse(result)

    def test_delete_index(self):
        """Test deleting an index"""
        self.os.create_index("to_delete")
        result = self.os.delete_index("to_delete")
        self.assertTrue(result)
        self.assertFalse(self.os.index_exists("to_delete"))

    def test_delete_index_not_connected(self):
        """Test deleting index when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.delete_index("test_index")
        self.assertFalse(result)

    def test_index_exists(self):
        """Test checking if index exists"""
        self.os.create_index("existing_index")
        self.assertTrue(self.os.index_exists("existing_index"))
        self.assertFalse(self.os.index_exists("nonexistent_index"))

    def test_get_index_stats(self):
        """Test getting index statistics"""
        self.os.create_index("stats_index")
        stats = self.os.get_index_stats("stats_index")
        self.assertIsInstance(stats, dict)


class TestDocumentIndexing(unittest.TestCase):
    """Test document indexing functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()
        self.os.create_index("docs_index")

    def test_index_document(self):
        """Test indexing a document"""
        doc = {
            "workflow_id": "wf_001",
            "workflow_name": "Test Workflow",
            "content": "Test content"
        }
        result = self.os.index_document("docs_index", "doc_001", doc)
        self.assertTrue(result)

    def test_index_document_with_id(self):
        """Test indexing a document with custom ID"""
        doc = {
            "workflow_id": "wf_002",
            "workflow_name": "Another Workflow"
        }
        result = self.os.index_document("docs_index", "custom_id", doc)
        self.assertTrue(result)

    def test_index_document_not_connected(self):
        """Test indexing document when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.index_document("docs_index", "doc_id", {"test": "doc"})
        self.assertFalse(result)

    def test_bulk_index(self):
        """Test bulk indexing"""
        docs = [
            {"workflow_id": "wf_001", "content": "Content 1"},
            {"workflow_id": "wf_002", "content": "Content 2"},
            {"workflow_id": "wf_003", "content": "Content 3"}
        ]
        result = self.os.bulk_index("docs_index", docs)
        self.assertTrue(result)

    def test_get_document(self):
        """Test getting a document"""
        doc = {"workflow_id": "wf_001", "content": "Test"}
        self.os.index_document("docs_index", "test_doc", doc)
        retrieved = self.os.get_document("docs_index", "test_doc")
        self.assertIsInstance(retrieved, dict)

    def test_update_document(self):
        """Test updating a document"""
        doc = {"workflow_id": "wf_001", "content": "Original"}
        self.os.index_document("docs_index", "update_doc", doc)
        result = self.os.update_document("docs_index", "update_doc", {"content": "Updated"})
        self.assertTrue(result)

    def test_delete_document(self):
        """Test deleting a document"""
        doc = {"workflow_id": "wf_001", "content": "To delete"}
        self.os.index_document("docs_index", "delete_doc", doc)
        result = self.os.delete_document("docs_index", "delete_doc")
        self.assertTrue(result)


class TestFullTextSearch(unittest.TestCase):
    """Test full-text search functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()
        self.os.create_index("search_index")

    def test_search(self):
        """Test basic search"""
        self.os.index_document("search_index", "doc_001", {"content": "Hello world test"})
        results = self.os.search("search_index", {"query": "hello"})
        self.assertIsInstance(results, dict)

    def test_search_with_filters(self):
        """Test search with filters"""
        results = self.os.search("search_index", {"query": "test"}, filters={"status": "active"})
        self.assertIsInstance(results, dict)

    def test_search_pagination(self):
        """Test search with pagination"""
        results = self.os.search("search_index", {"query": "test"}, from_=0, size=10)
        self.assertIsInstance(results, dict)

    def test_search_not_connected(self):
        """Test search when not connected"""
        os_client = WorkflowOpenSearch()
        results = os_client.search("search_index", {"query": "test"})
        self.assertFalse(results)


class TestAggregations(unittest.TestCase):
    """Test aggregation functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()
        self.os.create_index("agg_index")

    def test_aggregate(self):
        """Test aggregation"""
        result = self.os.aggregate("agg_index", "workflow_category", "terms")
        self.assertIsInstance(result, dict)

    def test_aggregate_with_filters(self):
        """Test aggregation with filters"""
        result = self.os.aggregate(
            "agg_index",
            "status",
            "terms",
            filters={"category": "automation"}
        )
        self.assertIsInstance(result, dict)

    def test_aggregate_not_connected(self):
        """Test aggregation when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.aggregate("agg_index", "field", "terms")
        self.assertFalse(result)


class TestAnomalyDetection(unittest.TestCase):
    """Test anomaly detection functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_anomaly_detector(self):
        """Test creating anomaly detector"""
        config = {
            "name": "test_detector",
            "feature": ["metric_value"],
            "detection_interval": "1h"
        }
        result = self.os.create_anomaly_detector(config)
        self.assertTrue(result)
        self.assertIn("test_detector", self.os._anomaly_detectors)

    def test_get_anomaly_results(self):
        """Test getting anomaly results"""
        result = self.os.get_anomaly_results("test_detector")
        self.assertIsInstance(result, dict)

    def test_create_detector_not_connected(self):
        """Test creating detector when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.create_anomaly_detector({"name": "test"})
        self.assertFalse(result)


class TestAlerting(unittest.TestCase):
    """Test alerting functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_alert_rule(self):
        """Test creating alert rule"""
        rule = {
            "name": "high_error_rate",
            "condition": "error_rate > 0.1",
            "severity": AlertSeverity.HIGH
        }
        result = self.os.create_alert_rule("alert_001", rule)
        self.assertTrue(result)
        self.assertIn("alert_001", self.os._alert_rules)

    def test_get_active_alerts(self):
        """Test getting active alerts"""
        result = self.os.get_active_alerts()
        self.assertIsInstance(result, list)

    def test_acknowledge_alert(self):
        """Test acknowledging an alert"""
        self.os.create_alert_rule("alert_002", {
            "name": "test_alert",
            "condition": "metric > 100",
            "severity": AlertSeverity.MEDIUM
        })
        result = self.os.acknowledge_alert("alert_002")
        self.assertTrue(result)


class TestSQLQuery(unittest.TestCase):
    """Test SQL query functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_execute_sql(self):
        """Test executing SQL query"""
        result = self.os.execute_sql("SELECT * FROM workflows LIMIT 10")
        self.assertIsInstance(result, dict)

    def test_execute_sql_not_connected(self):
        """Test executing SQL when not connected"""
        os_client = WorkflowOpenSearch()
        result = os_client.execute_sql("SELECT * FROM test")
        self.assertFalse(result)


class TestILM(unittest.TestCase):
    """Test index lifecycle management"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_ilm_policy(self):
        """Test creating ILM policy"""
        result = self.os.create_ilm_policy(
            "test_policy",
            phases={
                IndexLifecyclePhase.HOT: {"min_age": "0ms"},
                IndexLifecyclePhase.DELETE: {"min_age": "30d"}
            }
        )
        self.assertTrue(result)
        self.assertIn("test_policy", self.os._ilm_policies)

    def test_apply_ilm_policy(self):
        """Test applying ILM policy to index"""
        self.os.create_index("ilm_index")
        result = self.os.apply_ilm_policy("ilm_index", "test_policy")
        self.assertTrue(result)


class TestCrossCluster(unittest.TestCase):
    """Test cross-cluster search"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_add_cluster(self):
        """Test adding a remote cluster"""
        result = self.os.add_cluster("remote_cluster", ["http://remote:9200"])
        self.assertTrue(result)
        self.assertIn("remote_cluster", self.os._clusters)

    def test_search_cross_cluster(self):
        """Test cross-cluster search"""
        self.os.add_cluster("remote", ["http://remote:9200"])
        self.os.create_index("cc_index")
        result = self.os.search_cross_cluster("remote", "cc_index", "test query")
        self.assertIsInstance(result, dict)


class TestIndexPatterns(unittest.TestCase):
    """Test index patterns"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_index_pattern(self):
        """Test creating index pattern"""
        result = self.os.create_index_pattern(
            "workflows_pattern",
            ["workflows_*", "executions_*"]
        )
        self.assertTrue(result)
        self.assertIn("workflows_pattern", self.os._index_patterns)

    def test_get_matching_indices(self):
        """Test getting matching indices"""
        result = self.os.get_matching_indices("workflows_*")
        self.assertIsInstance(result, list)


class TestAlertSeverity(unittest.TestCase):
    """Test AlertSeverity enum"""

    def test_severity_values(self):
        """Test alert severity values"""
        self.assertEqual(AlertSeverity.CRITICAL.value, "critical")
        self.assertEqual(AlertSeverity.HIGH.value, "high")
        self.assertEqual(AlertSeverity.MEDIUM.value, "medium")
        self.assertEqual(AlertSeverity.LOW.value, "low")
        self.assertEqual(AlertSeverity.INFO.value, "info")


class TestIndexLifecyclePhase(unittest.TestCase):
    """Test IndexLifecyclePhase enum"""

    def test_phase_values(self):
        """Test ILM phase values"""
        self.assertEqual(IndexLifecyclePhase.HOT.value, "hot")
        self.assertEqual(IndexLifecyclePhase.WARM.value, "warm")
        self.assertEqual(IndexLifecyclePhase.COLD.value, "cold")
        self.assertEqual(IndexLifecyclePhase.DELETE.value, "delete")


class TestDefaultIndices(unittest.TestCase):
    """Test default index constants"""

    def test_default_indices(self):
        """Test default index names"""
        self.assertEqual(WorkflowOpenSearch.DEFAULT_WORKFLOW_INDEX, "workflows")
        self.assertEqual(WorkflowOpenSearch.DEFAULT_EXECUTION_INDEX, "workflow_executions")
        self.assertEqual(WorkflowOpenSearch.DEFAULT_ANALYTICS_INDEX, "workflow_analytics")
        self.assertEqual(WorkflowOpenSearch.DEFAULT_ALERT_INDEX, "workflow_alerts")


class TestDashboardIntegration(unittest.TestCase):
    """Test dashboard integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.os = WorkflowOpenSearch()
        self.os.connect()

    def test_create_dashboard(self):
        """Test creating dashboard"""
        config = {
            "title": "Workflow Analytics",
            "widgets": [{"type": "line", "query": "workflows/*"}]
        }
        result = self.os.create_dashboard("dash_001", config)
        self.assertTrue(result)

    def test_get_dashboard(self):
        """Test getting dashboard"""
        result = self.os.get_dashboard("dash_001")
        self.assertIsInstance(result, dict)


if __name__ == '__main__':
    unittest.main()
