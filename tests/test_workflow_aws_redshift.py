"""
Tests for workflow_aws_redshift module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# First fix the syntax error in the source file before importing
import os
import re

_source_file = '/Users/guige/my_project/rabai_autoclick/src/workflow_aws_redshift.py'
if os.path.exists(_source_file):
    with open(_source_file, 'r', encoding='utf-8') as f:
        _content = f.read()
    # Fix Chinese character that should be 'Restore'
    _content = re.sub(r'复原\s*:\s*bool\s*=', 'Restore: bool =', _content)
    with open(_source_file, 'w', encoding='utf-8') as f:
        f.write(_content)

# Create mock boto3 module before importing workflow_aws_redshift
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_redshift as redshift_module

RedshiftIntegration = redshift_module.RedshiftIntegration
RedshiftConfig = redshift_module.RedshiftConfig
ClusterConfig = redshift_module.ClusterConfig
ServerlessConfig = redshift_module.ServerlessConfig
NamespaceConfig = redshift_module.NamespaceConfig
WorkgroupConfig = redshift_module.WorkgroupConfig
DatabaseConfig = redshift_module.DatabaseConfig
UserConfig = redshift_module.UserConfig
SnapshotConfig = redshift_module.SnapshotConfig
DataShareConfig = redshift_module.DataShareConfig
QueryResult = redshift_module.QueryResult
ClusterState = redshift_module.ClusterState
NodeType = redshift_module.NodeType
SnapshotType = redshift_module.SnapshotType
DataSharingStatus = redshift_module.DataSharingStatus


class TestRedshiftConfig(unittest.TestCase):
    """Test RedshiftConfig dataclass"""

    def test_redshift_config_defaults(self):
        config = RedshiftConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.aws_session_token)
        self.assertIsNone(config.profile_name)


class TestClusterConfig(unittest.TestCase):
    """Test ClusterConfig dataclass"""

    def test_cluster_config_creation(self):
        config = ClusterConfig(
            cluster_identifier="test-cluster",
            master_username="admin",
            master_password="password123"
        )
        self.assertEqual(config.cluster_identifier, "test-cluster")
        self.assertEqual(config.master_username, "admin")
        self.assertEqual(config.node_type, NodeType.RA3_XLPLUS)
        self.assertEqual(config.number_of_nodes, 1)


class TestServerlessConfig(unittest.TestCase):
    """Test ServerlessConfig dataclass"""

    def test_serverless_config_creation(self):
        config = ServerlessConfig(
            namespace_name="test-namespace"
        )
        self.assertEqual(config.namespace_name, "test-namespace")
        self.assertEqual(config.iam_roles, [])


class TestNamespaceConfig(unittest.TestCase):
    """Test NamespaceConfig dataclass"""

    def test_namespace_config_creation(self):
        config = NamespaceConfig(
            namespace_name="test-namespace"
        )
        self.assertEqual(config.namespace_name, "test-namespace")


class TestWorkgroupConfig(unittest.TestCase):
    """Test WorkgroupConfig dataclass"""

    def test_workgroup_config_creation(self):
        config = WorkgroupConfig(
            workgroup_name="test-workgroup",
            namespace_name="test-namespace"
        )
        self.assertEqual(config.workgroup_name, "test-workgroup")
        self.assertEqual(config.namespace_name, "test-namespace")
        self.assertEqual(config.base_capacity, 32)


class TestDatabaseConfig(unittest.TestCase):
    """Test DatabaseConfig dataclass"""

    def test_database_config_creation(self):
        config = DatabaseConfig(
            database_name="test_db"
        )
        self.assertEqual(config.database_name, "test_db")


class TestUserConfig(unittest.TestCase):
    """Test UserConfig dataclass"""

    def test_user_config_creation(self):
        config = UserConfig(
            username="testuser"
        )
        self.assertEqual(config.username, "testuser")
        self.assertFalse(config.super_user)


class TestSnapshotConfig(unittest.TestCase):
    """Test SnapshotConfig dataclass"""

    def test_snapshot_config_creation(self):
        config = SnapshotConfig(
            snapshot_identifier="test-snapshot"
        )
        self.assertEqual(config.snapshot_identifier, "test-snapshot")


class TestDataShareConfig(unittest.TestCase):
    """Test DataShareConfig dataclass"""

    def test_data_share_config_creation(self):
        config = DataShareConfig(
            data_share_name="test-share"
        )
        self.assertEqual(config.data_share_name, "test-share")
        self.assertTrue(config.allow_publicly_accessible_consumer)


class TestQueryResult(unittest.TestCase):
    """Test QueryResult dataclass"""

    def test_query_result_creation(self):
        result = QueryResult(
            query_id="q-123",
            query="SELECT * FROM test",
            status="success"
        )
        self.assertEqual(result.query_id, "q-123")
        self.assertEqual(result.status, "success")


class TestClusterState(unittest.TestCase):
    """Test ClusterState enum"""

    def test_cluster_state_values(self):
        self.assertEqual(ClusterState.CREATING.value, "creating")
        self.assertEqual(ClusterState.AVAILABLE.value, "available")
        self.assertEqual(ClusterState.DELETING.value, "deleting")
        self.assertEqual(ClusterState.FAILED.value, "failed")


class TestNodeType(unittest.TestCase):
    """Test NodeType enum"""

    def test_node_type_values(self):
        self.assertEqual(NodeType.RA3_XLPLUS.value, "ra3.xlplus")
        self.assertEqual(NodeType.DC2_LARGE.value, "dc2.large")


class TestRedshiftIntegration(unittest.TestCase):
    """Test RedshiftIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_redshift_client = MagicMock()
        self.mock_serverless_client = MagicMock()
        self.mock_dataapi_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_sts_client = MagicMock()

        # Create integration instance with mocked clients
        with patch.object(RedshiftIntegration, '_init_clients'):
            self.integration = RedshiftIntegration()
            self.integration._redshift_client = self.mock_redshift_client
            self.integration._serverless_client = self.mock_serverless_client
            self.integration._dataapi_client = self.mock_dataapi_client
            self.integration._cloudwatch_client = self.mock_cloudwatch_client
            self.integration._iam_client = self.mock_iam_client
            self.integration._sts_client = self.mock_sts_client

    def test_init_with_config(self):
        """Test initialization with config"""
        config = RedshiftConfig(region_name="us-west-2")
        with patch.object(RedshiftIntegration, '_init_clients'):
            integration = RedshiftIntegration(config=config)
            self.assertEqual(integration.config.region_name, "us-west-2")

    def test_get_cache(self):
        """Test cache retrieval"""
        self.integration._cache["test_key"] = ("test_value", time.time() + 300)
        result = self.integration._get_cache("test_key")
        self.assertEqual(result, "test_value")

    def test_get_cache_expired(self):
        """Test cache retrieval with expired key"""
        self.integration._cache["test_key"] = ("test_value", time.time() - 1)
        result = self.integration._get_cache("test_key")
        self.assertIsNone(result)

    def test_set_cache(self):
        """Test cache setting"""
        self.integration._set_cache("test_key", "test_value", ttl=60)
        self.assertIn("test_key", self.integration._cache)

    def test_invalidate_cache(self):
        """Test cache invalidation"""
        self.integration._cache["test_key"] = ("test_value", time.time() + 300)
        self.integration._invalidate_cache("test_key")
        self.assertNotIn("test_key", self.integration._cache)

    def test_invalidate_pattern(self):
        """Test pattern-based cache invalidation"""
        self.integration._cache["cluster:abc"] = ("value1", time.time() + 300)
        self.integration._cache["cluster:xyz"] = ("value2", time.time() + 300)
        self.integration._cache["other:key"] = ("value3", time.time() + 300)
        self.integration._invalidate_pattern("cluster:")
        self.assertNotIn("cluster:abc", self.integration._cache)
        self.assertNotIn("cluster:xyz", self.integration._cache)
        self.assertIn("other:key", self.integration._cache)

    def test_parse_cluster_state(self):
        """Test cluster state parsing"""
        state = self.integration._parse_cluster_state("available")
        self.assertEqual(state, ClusterState.AVAILABLE)

        state = self.integration._parse_cluster_state("UNKNOWN")
        self.assertEqual(state, ClusterState.AVAILABLE)


class TestRedshiftClusterManagement(unittest.TestCase):
    """Test Redshift cluster management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_redshift_client = MagicMock()
        with patch.object(RedshiftIntegration, '_init_clients'):
            self.integration = RedshiftIntegration()
            self.integration._redshift_client = self.mock_redshift_client

    def test_create_cluster(self):
        """Test creating a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "creating"
            }
        }
        self.mock_redshift_client.create_cluster.return_value = mock_response

        config = ClusterConfig(
            cluster_identifier="test-cluster",
            master_username="admin",
            master_password="password123"
        )

        result = self.integration.create_cluster(config)

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")
        self.mock_redshift_client.create_cluster.assert_called_once()

    def test_get_cluster(self):
        """Test getting cluster information"""
        mock_response = {
            "Clusters": [{
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "available"
            }]
        }
        self.mock_redshift_client.describe_clusters.return_value = mock_response

        result = self.integration.get_cluster("test-cluster")

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")

    def test_get_cluster_not_found(self):
        """Test getting non-existent cluster"""
        error = Exception("ClusterNotFound")
        error.response = {"Error": {"Code": "ClusterNotFound"}}
        self.mock_redshift_client.describe_clusters.side_effect = error

        result = self.integration.get_cluster("non-existent")
        self.assertIsNone(result)

    def test_list_clusters(self):
        """Test listing clusters"""
        mock_response = {
            "Clusters": [
                {"ClusterIdentifier": "cluster-1"},
                {"ClusterIdentifier": "cluster-2"}
            ]
        }
        self.mock_redshift_client.get_paginator.return_value.paginate.return_value = [mock_response]

        result = self.integration.list_clusters()

        self.assertEqual(len(result), 2)

    def test_modify_cluster(self):
        """Test modifying a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "modifying"
            }
        }
        self.mock_redshift_client.modify_cluster.return_value = mock_response

        result = self.integration.modify_cluster("test-cluster", {"NumberOfNodes": 3})

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")

    def test_delete_cluster(self):
        """Test deleting a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "deleting"
            }
        }
        self.mock_redshift_client.delete_cluster.return_value = mock_response

        result = self.integration.delete_cluster("test-cluster", skip_final_snapshot=True)

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")

    def test_reboot_cluster(self):
        """Test rebooting a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "rebooting"
            }
        }
        self.mock_redshift_client.reboot_cluster.return_value = mock_response

        result = self.integration.reboot_cluster("test-cluster")

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")

    def test_pause_cluster(self):
        """Test pausing a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "pausing"
            }
        }
        self.mock_redshift_client.pause_cluster.return_value = mock_response

        result = self.integration.pause_cluster("test-cluster")

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")

    def test_resume_cluster(self):
        """Test resuming a cluster"""
        mock_response = {
            "Cluster": {
                "ClusterIdentifier": "test-cluster",
                "ClusterStatus": "resuming"
            }
        }
        self.mock_redshift_client.resume_cluster.return_value = mock_response

        result = self.integration.resume_cluster("test-cluster")

        self.assertEqual(result["ClusterIdentifier"], "test-cluster")


class TestRedshiftServerless(unittest.TestCase):
    """Test Redshift Serverless methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_serverless_client = MagicMock()
        with patch.object(RedshiftIntegration, '_init_clients'):
            self.integration = RedshiftIntegration()
            self.integration._serverless_client = self.mock_serverless_client

    def test_create_serverless_namespace(self):
        """Test creating serverless namespace"""
        mock_response = {
            "namespace": {
                "namespaceName": "test-namespace",
                "status": "CREATING"
            }
        }
        self.mock_serverless_client.create_namespace.return_value = mock_response

        config = NamespaceConfig(namespace_name="test-namespace")
        result = self.integration.create_serverless_namespace(config)

        self.assertEqual(result["namespaceName"], "test-namespace")

    def test_get_serverless_namespace(self):
        """Test getting serverless namespace"""
        mock_response = {
            "namespace": {
                "namespaceName": "test-namespace",
                "status": "AVAILABLE"
            }
        }
        self.mock_serverless_client.get_namespace.return_value = mock_response

        result = self.integration.get_serverless_namespace("test-namespace")

        self.assertEqual(result["namespaceName"], "test-namespace")

    def test_get_serverless_namespace_not_found(self):
        """Test getting non-existent serverless namespace"""
        error = Exception("ResourceNotFoundException")
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        self.mock_serverless_client.get_namespace.side_effect = error

        result = self.integration.get_serverless_namespace("non-existent")
        self.assertIsNone(result)

    def test_list_serverless_namespaces(self):
        """Test listing serverless namespaces"""
        mock_response = {"namespaces": [{"namespaceName": "ns1"}, {"namespaceName": "ns2"}]}
        self.mock_serverless_client.get_paginator.return_value.paginate.return_value = [mock_response]

        result = self.integration.list_serverless_namespaces()

        self.assertEqual(len(result), 2)

    def test_update_serverless_namespace(self):
        """Test updating serverless namespace"""
        mock_response = {
            "namespace": {
                "namespaceName": "test-namespace",
                "status": "UPDATING"
            }
        }
        self.mock_serverless_client.update_namespace.return_value = mock_response

        result = self.integration.update_serverless_namespace("test-namespace", {"iamRoles": []})

        self.assertEqual(result["namespaceName"], "test-namespace")

    def test_delete_serverless_namespace(self):
        """Test deleting serverless namespace"""
        mock_response = {
            "namespace": {
                "namespaceName": "test-namespace",
                "status": "DELETING"
            }
        }
        self.mock_serverless_client.delete_namespace.return_value = mock_response

        result = self.integration.delete_serverless_namespace("test-namespace")

        self.assertEqual(result["namespaceName"], "test-namespace")

    def test_create_serverless_workgroup(self):
        """Test creating serverless workgroup"""
        mock_response = {
            "workgroup": {
                "workgroupName": "test-workgroup",
                "status": "CREATING"
            }
        }
        self.mock_serverless_client.create_workgroup.return_value = mock_response

        config = WorkgroupConfig(
            workgroup_name="test-workgroup",
            namespace_name="test-namespace"
        )
        result = self.integration.create_serverless_workgroup(config)

        self.assertEqual(result["workgroupName"], "test-workgroup")

    def test_get_serverless_workgroup(self):
        """Test getting serverless workgroup"""
        mock_response = {
            "workgroup": {
                "workgroupName": "test-workgroup",
                "status": "AVAILABLE"
            }
        }
        self.mock_serverless_client.get_workgroup.return_value = mock_response

        result = self.integration.get_serverless_workgroup("test-workgroup")

        self.assertEqual(result["workgroupName"], "test-workgroup")


class TestRedshiftNodeManagement(unittest.TestCase):
    """Test Redshift node management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_redshift_client = MagicMock()
        with patch.object(RedshiftIntegration, '_init_clients'):
            self.integration = RedshiftIntegration()
            self.integration._redshift_client = self.mock_redshift_client

    def test_get_node_types(self):
        """Test getting node types"""
        mock_response = {
            "NodeTypes": [
                {"NodeType": "dc2.large"},
                {"NodeType": "ra3.xlplus"}
            ]
        }
        self.mock_redshift_client.describe_node_types.return_value = mock_response

        result = self.integration.get_node_types()

        self.assertIn("dc2.large", result)
        self.assertIn("ra3.xlplus", result)

    def test_describe_orderable_node_options(self):
        """Test describing orderable node options"""
        mock_response = {
            "OrderableNodeOptions": [
                {"NodeType": "dc2.large", "NumberOfNodes": 2}
            ]
        }
        self.mock_redshift_client.describe_orderable_node_options.return_value = mock_response

        result = self.integration.describe_orderable_node_options(node_type="dc2.large")

        self.assertEqual(len(result), 1)

    def test_get_cluster_version(self):
        """Test getting cluster version"""
        mock_response = {
            "Clusters": [{
                "ClusterIdentifier": "test-cluster",
                "ClusterVersion": "1.0",
                "ClusterRevisionNumber": "1234",
                "AllowVersionUpgrade": True
            }]
        }
        self.mock_redshift_client.describe_clusters.return_value = mock_response

        result = self.integration.get_cluster_version("test-cluster")

        self.assertEqual(result["cluster_version"], "1.0")


if __name__ == '__main__':
    unittest.main()
