"""
Tests for workflow_aws_keyspaces module
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

# Create mock boto3 module before importing workflow_aws_keyspaces
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_keyspaces as _keyspaces_module

# Extract classes
KeyspacesIntegration = _keyspaces_module.KeyspacesIntegration
KeyspaceState = _keyspaces_module.KeyspaceState
TableState = _keyspaces_module.TableState
ReplicationStrategy = _keyspaces_module.ReplicationStrategy
CapacityMode = _keyspaces_module.CapacityMode
ImportTaskStatus = _keyspaces_module.ImportTaskStatus
RestoreTaskStatus = _keyspaces_module.RestoreTaskStatus
SparkConnectorStatus = _keyspaces_module.SparkConnectorStatus
KeyspacesConfig = _keyspaces_module.KeyspacesConfig
KeyspaceConfig = _keyspaces_module.KeyspaceConfig
TableConfig = _keyspaces_module.TableConfig
ColumnDefinition = _keyspaces_module.ColumnDefinition
ImportConfig = _keyspaces_module.ImportConfig
RestoreConfig = _keyspaces_module.RestoreConfig
SparkConnectorConfig = _keyspaces_module.SparkConnectorConfig
CloudWatchConfig = _keyspaces_module.CloudWatchConfig


class TestEnums(unittest.TestCase):
    """Test enum classes"""

    def test_keyspace_state_values(self):
        self.assertEqual(KeyspaceState.CREATING.value, "creating")
        self.assertEqual(KeyspaceState.ACTIVE.value, "active")
        self.assertEqual(KeyspaceState.DELETING.value, "deleting")

    def test_table_state_values(self):
        self.assertEqual(TableState.CREATING.value, "CREATING")
        self.assertEqual(TableState.ACTIVE.value, "ACTIVE")

    def test_replication_strategy_values(self):
        self.assertEqual(ReplicationStrategy.SINGLE_REGION.value, "SINGLE_REGION")
        self.assertEqual(ReplicationStrategy.MULTI_REGION.value, "MULTI_REGION")

    def test_capacity_mode_values(self):
        self.assertEqual(CapacityMode.ON_DEMAND.value, "ON_DEMAND")
        self.assertEqual(CapacityMode.PROVISIONED.value, "PROVISIONED")


class TestKeyspacesConfig(unittest.TestCase):
    """Test KeyspacesConfig dataclass"""

    def test_default_config(self):
        config = KeyspacesConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)

    def test_custom_config(self):
        config = KeyspacesConfig(
            region_name="us-west-2",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "test_key")


class TestKeyspaceConfig(unittest.TestCase):
    """Test KeyspaceConfig dataclass"""

    def test_default_keyspace_config(self):
        config = KeyspaceConfig(keyspace_name="test-keyspace")
        self.assertEqual(config.keyspace_name, "test-keyspace")
        self.assertEqual(config.capacity_mode, "PROVISIONED")
        self.assertEqual(config.throughput, 1000)

    def test_custom_keyspace_config(self):
        config = KeyspaceConfig(
            keyspace_name="my-keyspace",
            replication_settings={"regionName": "us-east-1"},
            point_in_time_recovery=True,
            capacity_mode="ON_DEMAND"
        )
        self.assertEqual(config.keyspace_name, "my-keyspace")
        self.assertEqual(config.point_in_time_recovery, True)


class TestTableConfig(unittest.TestCase):
    """Test TableConfig dataclass"""

    def test_table_config_creation(self):
        config = TableConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            partition_key_columns=["pk1"],
            clustering_key_columns=["ck1"],
            default_time_to_live=3600
        )
        self.assertEqual(config.table_name, "test-table")
        self.assertEqual(config.default_time_to_live, 3600)


class TestColumnDefinition(unittest.TestCase):
    """Test ColumnDefinition dataclass"""

    def test_column_definition_creation(self):
        col = ColumnDefinition(name="id", column_type="uuid")
        self.assertEqual(col.name, "id")
        self.assertEqual(col.column_type, "uuid")


class TestImportConfig(unittest.TestCase):
    """Test ImportConfig dataclass"""

    def test_import_config_creation(self):
        config = ImportConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            s3_bucket="my-bucket",
            s3_prefix="imports/",
            iam_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        self.assertEqual(config.s3_bucket, "my-bucket")
        self.assertEqual(config.compression_type, "NONE")


class TestRestoreConfig(unittest.TestCase):
    """Test RestoreConfig dataclass"""

    def test_restore_config_creation(self):
        config = RestoreConfig(
            source_keyspace_name="source-ks",
            source_table_name="source-table",
            target_keyspace_name="target-ks",
            target_table_name="target-table",
            restore_timestamp="2024-01-01T00:00:00Z"
        )
        self.assertEqual(config.source_keyspace_name, "source-ks")


class TestSparkConnectorConfig(unittest.TestCase):
    """Test SparkConnectorConfig dataclass"""

    def test_default_spark_config(self):
        config = SparkConnectorConfig()
        self.assertEqual(config.spark_master_url, "local[*]")
        self.assertEqual(config.app_name, "keyspaces-spark-connector")

    def test_custom_spark_config(self):
        config = SparkConnectorConfig(
            spark_master_url="spark://master:7077",
            app_name="my-app",
            keyspaces_connection_timeout_ms=20000
        )
        self.assertEqual(config.spark_master_url, "spark://master:7077")


class TestKeyspacesIntegration(unittest.TestCase):
    """Test KeyspacesIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_init(self):
        """Test KeyspacesIntegration initialization"""
        ks = KeyspacesIntegration(KeyspacesConfig(region_name="us-west-2"))
        self.assertEqual(ks.config.region_name, "us-west-2")


class TestKeyspaceManagement(unittest.TestCase):
    """Test keyspace management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_create_keyspace(self):
        """Test creating a keyspace"""
        self.mock_client.create_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace",
                "keyspaceArn": "arn:aws:cassandra:us-east-1:123456789012:keyspace/test-keyspace"
            }
        }

        config = KeyspaceConfig(keyspace_name="test-keyspace")
        result = self.keyspaces.create_keyspace(config, wait_for_completion=False)
        self.assertEqual(result["keyspace_name"], "test-keyspace")

    def test_create_keyspace_with_replication(self):
        """Test creating a keyspace with replication settings"""
        self.mock_client.create_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace"
            }
        }

        config = KeyspaceConfig(
            keyspace_name="test-keyspace",
            replication_settings={"regionName": "us-east-1"}
        )
        result = self.keyspaces.create_keyspace(config, wait_for_completion=False)
        self.assertEqual(result["keyspace_name"], "test-keyspace")

    def test_get_keyspace(self):
        """Test getting a keyspace"""
        self.mock_client.get_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace",
                "keyspaceArn": "arn:aws:cassandra:us-east-1:123456789012:keyspace/test-keyspace",
                "replicationSpecification": {"regionName": "us-east-1"}
            }
        }

        result = self.keyspaces.get_keyspace("test-keyspace")
        self.assertEqual(result["keyspace_name"], "test-keyspace")

    def test_list_keyspaces(self):
        """Test listing keyspaces"""
        self.mock_client.list_keyspaces.return_value = {
            "keyspaces": [
                {"keyspaceName": "ks-1"},
                {"keyspaceName": "ks-2"}
            ]
        }

        result = self.keyspaces.list_keyspaces()
        self.assertEqual(len(result), 2)

    def test_delete_keyspace(self):
        """Test deleting a keyspace"""
        self.mock_client.delete_keyspace.return_value = {}

        result = self.keyspaces.delete_keyspace("test-keyspace")
        self.assertIn("ResponseMetadata", result)


class TestTableManagement(unittest.TestCase):
    """Test table management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_create_table(self):
        """Test creating a table"""
        self.mock_client.create_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "resourceArn": "arn:aws:cassandra:us-east-1:123456789012:table/test-keyspace/test-table"
            }
        }

        config = TableConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            partition_key_columns=["pk1"],
            clustering_key_columns=["ck1"]
        )
        result = self.keyspaces.create_table(config, wait_for_completion=False)
        self.assertEqual(result["table_name"], "test-table")

    def test_create_table_with_ttl(self):
        """Test creating a table with TTL"""
        self.mock_client.create_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "defaultTimeToLive": 3600
            }
        }

        config = TableConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            partition_key_columns=["pk1"],
            default_time_to_live=3600
        )
        result = self.keyspaces.create_table(config, wait_for_completion=False)
        self.assertEqual(result["default_time_to_live"], 3600)

    def test_get_table(self):
        """Test getting a table"""
        self.mock_client.get_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "status": "ACTIVE"
            }
        }

        result = self.keyspaces.get_table("test-keyspace", "test-table")
        self.assertEqual(result["table_name"], "test-table")

    def test_list_tables(self):
        """Test listing tables"""
        self.mock_client.list_tables.return_value = {
            "tables": [
                {"keyspaceName": "test-keyspace", "tableName": "table-1"},
                {"keyspaceName": "test-keyspace", "tableName": "table-2"}
            ]
        }

        result = self.keyspaces.list_tables("test-keyspace")
        self.assertEqual(len(result), 2)

    def test_delete_table(self):
        """Test deleting a table"""
        self.mock_client.delete_table.return_value = {}

        result = self.keyspaces.delete_table("test-keyspace", "test-table")
        self.assertIn("ResponseMetadata", result)

    def test_update_table_ttl(self):
        """Test updating table TTL"""
        self.mock_client.update_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "defaultTimeToLive": 7200
            }
        }

        result = self.keyspaces.update_table_ttl("test-keyspace", "test-table", 7200)
        self.assertIn("table", result)


class TestPointInTimeRecovery(unittest.TestCase):
    """Test PITR methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_enable_pitr_keyspace(self):
        """Test enabling PITR for keyspace"""
        self.mock_client.create_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace"
            }
        }

        config = KeyspaceConfig(keyspace_name="test-keyspace", point_in_time_recovery=True)
        result = self.keyspaces.create_keyspace(config, wait_for_completion=False)
        self.assertEqual(result["point_in_time_recovery"], True)

    def test_enable_pitr_table(self):
        """Test enabling PITR for table"""
        self.mock_client.create_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table"
            }
        }

        config = TableConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            partition_key_columns=["pk1"],
            point_in_time_recovery=True
        )
        result = self.keyspaces.create_table(config, wait_for_completion=False)
        self.assertTrue(result.get("point_in_time_recovery"))

    def test_get_pitr_status_keyspace(self):
        """Test getting PITR status for keyspace"""
        self.mock_client.get_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace"
            }
        }

        result = self.keyspaces.get_keyspace("test-keyspace")
        self.assertIn("keyspace_name", result)


class TestDataImport(unittest.TestCase):
    """Test data import methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_import_data(self):
        """Test importing data from S3"""
        self.mock_client.create_import_task.return_value = {
            "importTask": {
                "id": "import-123",
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "status": "IN_PROGRESS"
            }
        }

        config = ImportConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            s3_bucket="my-bucket",
            s3_prefix="imports/",
            iam_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        result = self.keyspaces.import_data(config)
        self.assertIn("importTask", result)

    def test_get_import_task(self):
        """Test getting import task status"""
        self.mock_client.get_import_task.return_value = {
            "importTask": {
                "id": "import-123",
                "status": "COMPLETED"
            }
        }

        result = self.keyspaces.get_import_task("import-123")
        self.assertEqual(result["importTask"]["status"], "COMPLETED")

    def test_list_import_tasks(self):
        """Test listing import tasks"""
        self.mock_client.list_import_tasks.return_value = {
            "importTasks": [
                {"id": "import-1", "status": "COMPLETED"},
                {"id": "import-2", "status": "IN_PROGRESS"}
            ]
        }

        result = self.keyspaces.list_import_tasks("test-keyspace", "test-table")
        self.assertEqual(len(result), 2)


class TestPointInTimeRestore(unittest.TestCase):
    """Test PITR restore methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_restore_table(self):
        """Test restoring a table from PITR"""
        self.mock_client.create_restore_job.return_value = {
            "restoreJob": {
                "id": "restore-123",
                "sourceKeyspaceName": "source-ks",
                "sourceTableName": "source-table",
                "targetKeyspaceName": "target-ks",
                "targetTableName": "target-table",
                "status": "IN_PROGRESS"
            }
        }

        config = RestoreConfig(
            source_keyspace_name="source-ks",
            source_table_name="source-table",
            target_keyspace_name="target-ks",
            target_table_name="target-table",
            restore_timestamp="2024-01-01T00:00:00Z"
        )
        result = self.keyspaces.restore_table(config)
        self.assertIn("restoreJob", result)

    def test_get_restore_job(self):
        """Test getting restore job status"""
        self.mock_client.get_restore_job.return_value = {
            "restoreJob": {
                "id": "restore-123",
                "status": "COMPLETED"
            }
        }

        result = self.keyspaces.get_restore_job("restore-123")
        self.assertEqual(result["restoreJob"]["status"], "COMPLETED")


class TestEncryption(unittest.TestCase):
    """Test encryption methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_create_table_with_encryption(self):
        """Test creating a table with encryption"""
        self.mock_client.create_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "encryptionSpecification": {
                    "encryptionType": "AWS_OWNED_KMS_KEY"
                }
            }
        }

        config = TableConfig(
            keyspace_name="test-keyspace",
            table_name="test-table",
            partition_key_columns=["pk1"],
            encryption_settings={
                "encryption_type": "AWS_OWNED_KMS_KEY"
            }
        )
        result = self.keyspaces.create_table(config, wait_for_completion=False)
        self.assertIn("encryption_specification", result)


class TestTTL(unittest.TestCase):
    """Test TTL methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_update_table_ttl(self):
        """Test updating table TTL"""
        self.mock_client.update_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "defaultTimeToLive": 86400
            }
        }

        result = self.keyspaces.update_table_ttl("test-keyspace", "test-table", 86400)
        self.assertIn("table", result)

    def test_disable_ttl(self):
        """Test disabling TTL"""
        self.mock_client.update_table.return_value = {
            "table": {
                "keyspaceName": "test-keyspace",
                "tableName": "test-table",
                "defaultTimeToLive": 0
            }
        }

        result = self.keyspaces.update_table_ttl("test-keyspace", "test-table", 0)
        self.assertIn("table", result)


class TestMultiRegionReplication(unittest.TestCase):
    """Test multi-region replication methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_create_keyspace_multi_region(self):
        """Test creating a multi-region keyspace"""
        self.mock_client.create_keyspace.return_value = {
            "keyspace": {
                "keyspaceName": "test-keyspace",
                "replicationSpecification": {
                    "regionName": ["us-east-1", "us-west-2"]
                }
            }
        }

        config = KeyspaceConfig(
            keyspace_name="test-keyspace",
            replication_settings={
                "regionName": ["us-east-1", "us-west-2"]
            }
        )
        result = self.keyspaces.create_keyspace(config, wait_for_completion=False)
        self.assertIn("replication_settings", result)


class TestSparkConnector(unittest.TestCase):
    """Test Spark connector methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_get_spark_connector_config(self):
        """Test getting Spark connector configuration"""
        result = self.keyspaces.get_spark_connector_config()
        self.assertIn("spark_master_url")

    def test_validate_spark_connector(self):
        """Test validating Spark connector"""
        config = SparkConnectorConfig(
            spark_master_url="spark://master:7077",
            app_name="test-app"
        )
        result = self.keyspaces.validate_spark_connector(config)
        self.assertTrue(result)


class TestCloudWatchMonitoring(unittest.TestCase):
    """Test CloudWatch monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_client
            self.keyspaces = KeyspacesIntegration(KeyspacesConfig(region_name="us-east-1"))
            self.keyspaces._client = self.mock_client

    def test_get_metric_summary(self):
        """Test getting metric summary"""
        result = self.keyspaces.get_metric_summary("test-keyspace", "test-table")
        self.assertIn("metrics")

    def test_get_wcu_scu_metrics(self):
        """Test getting WCU/SCU metrics"""
        result = self.keyspaces.get_wcu_scu_metrics("test-keyspace", "test-table")
        self.assertIn("metrics")

    def test_get_storage_metrics(self):
        """Test getting storage metrics"""
        result = self.keyspaces.get_storage_metrics("test-keyspace", "test-table")
        self.assertIn("metrics")


if __name__ == '__main__':
    unittest.main()
