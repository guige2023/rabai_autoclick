"""
Tests for workflow_aws_rds module
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

# Create mock boto3 module before importing workflow_aws_rds
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

# Now we can import the module
from src.workflow_aws_rds import (
    RDSIntegration,
    DBEngine,
    DBInstanceState,
    BackupStrategy,
    StorageType,
    RDSConfig,
    DBInstanceConfig,
    ReadReplicaConfig,
    SnapshotConfig,
    ParameterGroupConfig,
)


class TestDBEngine(unittest.TestCase):
    """Test DBEngine enum"""

    def test_db_engine_values(self):
        self.assertEqual(DBEngine.MYSQL.value, "mysql")
        self.assertEqual(DBEngine.POSTGRESQL.value, "postgres")
        self.assertEqual(DBEngine.MARIADB.value, "mariadb")
        self.assertEqual(DBEngine.ORACLE.value, "oracle-ee")
        self.assertEqual(DBEngine.ORACLE_SE.value, "oracle-se")
        self.assertEqual(DBEngine.ORACLE_SE1.value, "oracle-se1")
        self.assertEqual(DBEngine.ORACLE_SE2.value, "oracle-se2")
        self.assertEqual(DBEngine.SQLSERVER_EE.value, "sqlserver-ee")
        self.assertEqual(DBEngine.SQLSERVER_EX.value, "sqlserver-ex")
        self.assertEqual(DBEngine.SQLSERVER_SE.value, "sqlserver-se")
        self.assertEqual(DBEngine.SQLSERVER_WEB.value, "sqlserver-web")
        self.assertEqual(DBEngine.AURORA_MYSQL.value, "aurora-mysql")
        self.assertEqual(DBEngine.AURORA_POSTGRESQL.value, "aurora-postgresql")
        self.assertEqual(DBEngine.AURORA.value, "aurora")


class TestDBInstanceState(unittest.TestCase):
    """Test DBInstanceState enum"""

    def test_db_instance_state_values(self):
        self.assertEqual(DBInstanceState.CREATING.value, "creating")
        self.assertEqual(DBInstanceState.AVAILABLE.value, "available")
        self.assertEqual(DBInstanceState.DELETING.value, "deleting")
        self.assertEqual(DBInstanceState.DELETED.value, "deleted")
        self.assertEqual(DBInstanceState.MODIFYING.value, "modifying")
        self.assertEqual(DBInstanceState.REBOOTING.value, "rebooting")
        self.assertEqual(DBInstanceState.FAILING.value, "failing")
        self.assertEqual(DBInstanceState.FAILED.value, "failed")
        self.assertEqual(DBInstanceState.BACKING_UP.value, "backing-up")
        self.assertEqual(DBInstanceState.STARTING.value, "starting")
        self.assertEqual(DBInstanceState.STOPPING.value, "stopping")
        self.assertEqual(DBInstanceState.STOPPED.value, "stopped")


class TestBackupStrategy(unittest.TestCase):
    """Test BackupStrategy enum"""

    def test_backup_strategy_values(self):
        self.assertEqual(BackupStrategy.DAILY.value, "daily")
        self.assertEqual(BackupStrategy.WEEKLY.value, "weekly")
        self.assertEqual(BackupStrategy.MONTHLY.value, "monthly")
        self.assertEqual(BackupStrategy.CUSTOM.value, "custom")


class TestStorageType(unittest.TestCase):
    """Test StorageType enum"""

    def test_storage_type_values(self):
        self.assertEqual(StorageType.STANDARD.value, "standard")
        self.assertEqual(StorageType.PIOPS.value, "piops")
        self.assertEqual(StorageType.GP2.value, "gp2")
        self.assertEqual(StorageType.GP3.value, "gp3")
        self.assertEqual(StorageType.IO1.value, "io1")
        self.assertEqual(StorageType.IO2.value, "io2")


class TestRDSConfig(unittest.TestCase):
    """Test RDSConfig dataclass"""

    def test_rds_config_defaults(self):
        config = RDSConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.aws_session_token)
        self.assertIsNone(config.profile_name)

    def test_rds_config_full(self):
        config = RDSConfig(
            region_name="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_session_token="session-token",
            profile_name="my-profile"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "AKIAIOSFODNN7EXAMPLE")
        self.assertEqual(config.profile_name, "my-profile")


class TestDBInstanceConfig(unittest.TestCase):
    """Test DBInstanceConfig dataclass"""

    def test_db_instance_config_defaults(self):
        config = DBInstanceConfig(
            db_instance_identifier="my-db-instance",
            master_username="admin",
            master_password="password123"
        )
        self.assertEqual(config.db_instance_identifier, "my-db-instance")
        self.assertEqual(config.db_instance_class, "db.t3.micro")
        self.assertEqual(config.engine, DBEngine.POSTGRESQL)
        self.assertEqual(config.master_username, "admin")
        self.assertEqual(config.allocated_storage, 20)
        self.assertEqual(config.multi_az, False)
        self.assertEqual(config.backup_retention_period, 7)

    def test_db_instance_config_full(self):
        config = DBInstanceConfig(
            db_instance_identifier="prod-db",
            db_instance_class="db.r5.large",
            engine=DBEngine.MYSQL,
            engine_version="8.0.35",
            master_username="dbadmin",
            master_password="SecurePass123!",
            allocated_storage=100,
            max_allocated_storage=500,
            storage_type=StorageType.GP3,
            iops=3000,
            multi_az=True,
            availability_zone="us-east-1a",
            preferred_availability_zone="us-east-1b",
            db_name="mydb",
            enable_encryption=True,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/1234abcd-12ab-34cd-56ef-1234567890ab",
            enable_performance_insights=True,
            performance_insights_retention_period=14,
            enable_cloudwatch_logs_exports=["error", "general"],
            publicly_accessible=True,
            tags={"Environment": "production", "Application": "myapp"}
        )
        self.assertEqual(config.db_instance_class, "db.r5.large")
        self.assertEqual(config.engine, DBEngine.MYSQL)
        self.assertEqual(config.engine_version, "8.0.35")
        self.assertEqual(config.multi_az, True)
        self.assertEqual(config.enable_encryption, True)
        self.assertEqual(config.enable_performance_insights, True)
        self.assertEqual(config.tags["Environment"], "production")


class TestReadReplicaConfig(unittest.TestCase):
    """Test ReadReplicaConfig dataclass"""

    def test_read_replica_config_defaults(self):
        config = ReadReplicaConfig(
            db_instance_identifier="my-replica",
            source_db_instance_identifier="my-source-db"
        )
        self.assertEqual(config.db_instance_identifier, "my-replica")
        self.assertEqual(config.source_db_instance_identifier, "my-source-db")
        self.assertEqual(config.db_instance_class, "db.t3.micro")
        self.assertFalse(config.enable_encryption)

    def test_read_replica_config_full(self):
        config = ReadReplicaConfig(
            db_instance_identifier="prod-replica",
            source_db_instance_identifier="prod-source",
            db_instance_class="db.r5.xlarge",
            engine=DBEngine.MYSQL,
            availability_zone="us-west-2a",
            enable_encryption=True,
            kms_key_id="arn:aws:kms:us-west-2:123456789:key/5678efgh-56ab-78cd-90ef-567890abcdef",
            tags={"Environment": "production"}
        )
        self.assertEqual(config.db_instance_class, "db.r5.xlarge")
        self.assertEqual(config.engine, DBEngine.MYSQL)
        self.assertTrue(config.enable_encryption)


class TestSnapshotConfig(unittest.TestCase):
    """Test SnapshotConfig dataclass"""

    def test_snapshot_config_creation(self):
        config = SnapshotConfig(
            db_snapshot_identifier="my-snapshot-20240101",
            db_instance_identifier="my-db-instance"
        )
        self.assertEqual(config.db_snapshot_identifier, "my-snapshot-20240101")
        self.assertEqual(config.db_instance_identifier, "my-db-instance")
        self.assertEqual(config.tags, {})

    def test_snapshot_config_with_tags(self):
        config = SnapshotConfig(
            db_snapshot_identifier="prod-snapshot",
            db_instance_identifier="prod-db",
            tags={"Backup": "automated", "Environment": "production"}
        )
        self.assertEqual(config.tags["Backup"], "automated")


class TestParameterGroupConfig(unittest.TestCase):
    """Test ParameterGroupConfig dataclass"""

    def test_parameter_group_config_creation(self):
        config = ParameterGroupConfig(
            db_parameter_group_name="my-parameter-group",
            db_parameter_group_family="postgres14",
            description="Custom parameter group for my application"
        )
        self.assertEqual(config.db_parameter_group_name, "my-parameter-group")
        self.assertEqual(config.db_parameter_group_family, "postgres14")
        self.assertEqual(config.description, "Custom parameter group for my application")


class TestRDSIntegration(unittest.TestCase):
    """Test RDSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_rds_client = MagicMock()
        self.mock_cw_client = MagicMock()

        # Create integration instance with mocked clients
        self.integration = RDSIntegration()
        self.integration._rds_client = self.mock_rds_client
        self.integration._cw_client = self.mock_cw_client

    def test_initialization(self):
        """Test RDSIntegration initialization"""
        integration = RDSIntegration(config=RDSConfig(region_name="eu-west-1"))
        self.assertEqual(integration.config.region_name, "eu-west-1")
        self.assertIsNotNone(integration._lock)
        self.assertEqual(integration._cache, {})

    def test_create_instance_success(self):
        """Test successful instance creation"""
        self.mock_rds_client.create_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "my-new-db",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "postgres",
                "EngineVersion": "14.7",
                "MasterUsername": "admin",
                "DBInstanceStatus": "creating",
                "AllocatedStorage": 20,
                "MultiAZ": False,
                "StorageEncrypted": False
            }
        }

        config = DBInstanceConfig(
            db_instance_identifier="my-new-db",
            master_username="admin",
            master_password="password123",
            engine=DBEngine.POSTGRESQL,
            engine_version="14.7"
        )
        result = self.integration.create_instance(config)

        self.assertEqual(result["DBInstanceIdentifier"], "my-new-db")
        self.assertEqual(result["DBInstanceStatus"], "creating")
        self.mock_rds_client.create_db_instance.assert_called_once()

    def test_create_instance_with_encryption(self):
        """Test instance creation with encryption"""
        self.mock_rds_client.create_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "encrypted-db",
                "DBInstanceClass": "db.r5.large",
                "Engine": "mysql",
                "DBInstanceStatus": "creating",
                "StorageEncrypted": True,
                "KmsKeyId": "arn:aws:kms:us-east-1:123456789:key/1234abcd"
            }
        }

        config = DBInstanceConfig(
            db_instance_identifier="encrypted-db",
            db_instance_class="db.r5.large",
            engine=DBEngine.MYSQL,
            master_username="admin",
            master_password="password123",
            enable_encryption=True,
            kms_key_id="arn:aws:kms:us-east-1:123456789:key/1234abcd"
        )
        result = self.integration.create_instance(config)

        self.assertTrue(result["StorageEncrypted"])

    def test_get_instance(self):
        """Test getting instance information"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "my-db",
                    "DBInstanceClass": "db.t3.micro",
                    "Engine": "postgres",
                    "DBInstanceStatus": "available",
                    "Endpoint": {
                        "Address": "my-db.xyz.us-east-1.rds.amazonaws.com",
                        "Port": 5432
                    },
                    "AllocatedStorage": 20,
                    "MultiAZ": False
                }
            ]
        }

        result = self.integration.get_instance("my-db")

        self.assertIsNotNone(result)
        self.assertEqual(result["DBInstanceIdentifier"], "my-db")
        self.assertEqual(result["DBInstanceStatus"], "available")
        self.mock_rds_client.describe_db_instances.assert_called_once_with(
            DBInstanceIdentifier="my-db"
        )

    def test_get_instance_caching(self):
        """Test that get_instance uses caching"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "cached-db",
                    "DBInstanceClass": "db.t3.micro",
                    "Engine": "postgres",
                    "DBInstanceStatus": "available"
                }
            ]
        }

        # First call should hit the API
        result1 = self.integration.get_instance("cached-db", use_cache=True)

        # Second call should use cache
        result2 = self.integration.get_instance("cached-db", use_cache=True)

        # Should only call describe once due to caching
        self.mock_rds_client.describe_db_instances.assert_called_once()

    def test_get_instance_not_found(self):
        """Test getting non-existent instance"""
        from botocore.exceptions import ClientError

        def raise_not_found(*args, **kwargs):
            raise ClientError(
                {"Error": {"Code": "DBInstanceNotFound", "Message": "Instance not found"}},
                "DescribeDBInstances"
            )

        self.mock_rds_client.describe_db_instances.side_effect = raise_not_found

        result = self.integration.get_instance("non-existent-db")

        self.assertIsNone(result)

    def test_list_instances(self):
        """Test listing all instances"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "db-1",
                    "DBInstanceClass": "db.t3.micro",
                    "Engine": "postgres",
                    "DBInstanceStatus": "available"
                },
                {
                    "DBInstanceIdentifier": "db-2",
                    "DBInstanceClass": "db.r5.large",
                    "Engine": "mysql",
                    "DBInstanceStatus": "available"
                }
            ]
        }

        result = self.integration.list_instances()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["DBInstanceIdentifier"], "db-1")
        self.assertEqual(result[1]["DBInstanceIdentifier"], "db-2")

    def test_list_instances_by_engine(self):
        """Test listing instances filtered by engine"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "mysql-prod",
                    "DBInstanceClass": "db.r5.large",
                    "Engine": "mysql",
                    "DBInstanceStatus": "available"
                }
            ]
        }

        result = self.integration.list_instances(engine=DBEngine.MYSQL)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Engine"], "mysql")

    def test_delete_instance(self):
        """Test instance deletion"""
        self.mock_rds_client.delete_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "to-delete",
                "DBInstanceStatus": "deleting"
            }
        }

        result = self.integration.delete_instance("to-delete")

        self.assertEqual(result["DBInstanceStatus"], "deleting")
        self.mock_rds_client.delete_db_instance.assert_called_once_with(
            DBInstanceIdentifier="to-delete"
        )

    def test_start_instance(self):
        """Test starting an instance"""
        self.mock_rds_client.start_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "stopped-db",
                "DBInstanceStatus": "starting"
            }
        }

        result = self.integration.start_instance("stopped-db")

        self.assertEqual(result["DBInstanceStatus"], "starting")
        self.mock_rds_client.start_db_instance.assert_called_once_with(
            DBInstanceIdentifier="stopped-db"
        )

    def test_stop_instance(self):
        """Test stopping an instance"""
        self.mock_rds_client.stop_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "running-db",
                "DBInstanceStatus": "stopping"
            }
        }

        result = self.integration.stop_instance("running-db")

        self.assertEqual(result["DBInstanceStatus"], "stopping")
        self.mock_rds_client.stop_db_instance.assert_called_once_with(
            DBInstanceIdentifier="running-db"
        )

    def test_reboot_instance(self):
        """Test rebooting an instance"""
        self.mock_rds_client.reboot_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "my-db",
                "DBInstanceStatus": "rebooting"
            }
        }

        result = self.integration.reboot_instance("my-db")

        self.assertEqual(result["DBInstanceStatus"], "rebooting")

    def test_modify_instance(self):
        """Test modifying an instance"""
        self.mock_rds_client.modify_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "my-db",
                "DBInstanceClass": "db.r5.large",
                "DBInstanceStatus": "modifying"
            }
        }

        result = self.integration.modify_instance(
            db_instance_identifier="my-db",
            db_instance_class="db.r5.large",
            allocated_storage=100,
            multi_az=True
        )

        self.assertEqual(result["DBInstanceClass"], "db.r5.large")

    def test_create_read_replica(self):
        """Test creating a read replica"""
        self.mock_rds_client.create_db_instance_read_replica.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "my-replica",
                "SourceDBInstanceIdentifier": "my-source",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "postgres",
                "DBInstanceStatus": "creating"
            }
        }

        config = ReadReplicaConfig(
            db_instance_identifier="my-replica",
            source_db_instance_identifier="my-source"
        )
        result = self.integration.create_read_replica(config)

        self.assertEqual(result["DBInstanceIdentifier"], "my-replica")
        self.assertEqual(result["SourceDBInstanceIdentifier"], "my-source")

    def test_create_read_replica_cross_region(self):
        """Test creating a cross-region read replica"""
        self.mock_rds_client.create_db_instance_read_replica.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "eu-replica",
                "SourceDBInstanceIdentifier": "us-source",
                "DBInstanceClass": "db.r5.large",
                "Engine": "postgres",
                "DBInstanceStatus": "creating"
            }
        }

        config = ReadReplicaConfig(
            db_instance_identifier="eu-replica",
            source_db_instance_identifier="us-source",
            db_instance_class="db.r5.large",
            source_region="us-east-1"
        )
        result = self.integration.create_read_replica(config)

        self.assertEqual(result["DBInstanceIdentifier"], "eu-replica")

    def test_create_snapshot(self):
        """Test creating a snapshot"""
        self.mock_rds_client.create_db_snapshot.return_value = {
            "DBSnapshot": {
                "DBSnapshotIdentifier": "my-snapshot-20240101",
                "DBInstanceIdentifier": "my-db",
                "SnapshotState": "creating",
                "Engine": "postgres",
                "AllocatedStorage": 20
            }
        }

        config = SnapshotConfig(
            db_snapshot_identifier="my-snapshot-20240101",
            db_instance_identifier="my-db"
        )
        result = self.integration.create_snapshot(config)

        self.assertEqual(result["DBSnapshotIdentifier"], "my-snapshot-20240101")
        self.assertEqual(result["SnapshotState"], "creating")

    def test_list_snapshots(self):
        """Test listing snapshots"""
        self.mock_rds_client.describe_db_snapshots.return_value = {
            "DBSnapshots": [
                {
                    "DBSnapshotIdentifier": "snap-1",
                    "DBInstanceIdentifier": "my-db",
                    "SnapshotState": "available",
                    "Engine": "postgres"
                },
                {
                    "DBSnapshotIdentifier": "snap-2",
                    "DBInstanceIdentifier": "my-db",
                    "SnapshotState": "available",
                    "Engine": "postgres"
                }
            ]
        }

        result = self.integration.list_snapshots(db_instance_identifier="my-db")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["DBSnapshotIdentifier"], "snap-1")

    def test_delete_snapshot(self):
        """Test deleting a snapshot"""
        self.mock_rds_client.delete_db_snapshot.return_value = {
            "DBSnapshot": {
                "DBSnapshotIdentifier": "to-delete",
                "SnapshotState": "deleted"
            }
        }

        result = self.integration.delete_snapshot("to-delete")

        self.assertEqual(result["SnapshotState"], "deleted")

    def test_restore_instance_from_snapshot(self):
        """Test restoring an instance from snapshot"""
        self.mock_rds_client.restore_db_instance_from_db_snapshot.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "restored-db",
                "DBSnapshotIdentifier": "my-snapshot",
                "DBInstanceClass": "db.t3.micro",
                "Engine": "postgres",
                "DBInstanceStatus": "creating"
            }
        }

        result = self.integration.restore_from_snapshot(
            db_instance_identifier="restored-db",
            db_snapshot_identifier="my-snapshot"
        )

        self.assertEqual(result["DBInstanceIdentifier"], "restored-db")
        self.assertEqual(result["DBSnapshotIdentifier"], "my-snapshot")

    def test_create_parameter_group(self):
        """Test creating a parameter group"""
        self.mock_rds_client.create_db_parameter_group.return_value = {
            "DBParameterGroup": {
                "DBParameterGroupName": "my-param-group",
                "DBParameterGroupFamily": "postgres14",
                "Description": "My custom parameter group"
            }
        }

        config = ParameterGroupConfig(
            db_parameter_group_name="my-param-group",
            db_parameter_group_family="postgres14",
            description="My custom parameter group"
        )
        result = self.integration.create_parameter_group(config)

        self.assertEqual(result["DBParameterGroupName"], "my-param-group")

    def test_list_parameter_groups(self):
        """Test listing parameter groups"""
        self.mock_rds_client.describe_db_parameter_groups.return_value = {
            "DBParameterGroups": [
                {
                    "DBParameterGroupName": "default.postgres14",
                    "DBParameterGroupFamily": "postgres14",
                    "Description": "Default parameter group for postgres14"
                },
                {
                    "DBParameterGroupName": "custom.params",
                    "DBParameterGroupFamily": "postgres14",
                    "Description": "Custom parameters"
                }
            ]
        }

        result = self.integration.list_parameter_groups(family="postgres14")

        self.assertEqual(len(result), 2)

    def test_get_parameter_group(self):
        """Test getting parameter group details"""
        self.mock_rds_client.describe_db_parameters.return_value = {
            "Parameters": [
                {
                    "ParameterName": "max_connections",
                    "ParameterValue": "100",
                    "ApplyMethod": "dynamic"
                },
                {
                    "ParameterName": "shared_buffers",
                    "ParameterValue": "256MB",
                    "ApplyMethod": "dynamic"
                }
            ]
        }

        result = self.integration.get_parameter_group("my-param-group")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["ParameterName"], "max_connections")

    def test_modify_parameter_group(self):
        """Test modifying a parameter group"""
        self.mock_rds_client.modify_db_parameter_group.return_value = {
            "DBParameterGroupName": "my-param-group"
        }

        parameters = [
            {"ParameterName": "max_connections", "ParameterValue": "200", "ApplyMethod": "immediate"},
            {"ParameterName": "shared_buffers", "ParameterValue": "512MB", "ApplyMethod": "immediate"}
        ]

        result = self.integration.modify_parameter_group("my-param-group", parameters)

        self.assertEqual(result["DBParameterGroupName"], "my-param-group")

    def test_delete_parameter_group(self):
        """Test deleting a parameter group"""
        self.mock_rds_client.delete_db_parameter_group.return_value = {}

        result = self.integration.delete_parameter_group("my-param-group")

        self.assertIsNone(result)


class TestRDSIntegrationCache(unittest.TestCase):
    """Test RDSIntegration caching behavior"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_rds_client = MagicMock()
        self.integration = RDSIntegration()
        self.integration._rds_client = self.mock_rds_client

    def test_cache_key_generation(self):
        """Test cache key generation"""
        key = self.integration._get_cache_key("instance", "my-db")
        self.assertEqual(key, "instance:my-db")

    def test_cache_invalidation_all(self):
        """Test invalidating all cache entries"""
        self.integration._cache["instance:db1"] = {"value": {}, "timestamp": datetime.now()}
        self.integration._cache["instance:db2"] = {"value": {}, "timestamp": datetime.now()}

        self.integration._invalidate_cache()

        self.assertEqual(len(self.integration._cache), 0)

    def test_cache_invalidation_pattern(self):
        """Test invalidating cache entries by pattern"""
        self.integration._cache["instance:db1"] = {"value": {}, "timestamp": datetime.now()}
        self.integration._cache["instance:db2"] = {"value": {}, "timestamp": datetime.now()}
        self.integration._cache["snapshot:snap1"] = {"value": {}, "timestamp": datetime.now()}

        self.integration._invalidate_cache("instance")

        self.assertEqual(len(self.integration._cache), 1)
        self.assertIn("snapshot:snap1", self.integration._cache)

    def test_cache_ttl_expiry(self):
        """Test that cache entries expire based on TTL"""
        old_time = datetime.now() - timedelta(seconds=61)
        self.integration._cache["instance:old-entry"] = {
            "value": {"DBInstanceIdentifier": "old-db"},
            "timestamp": old_time
        }
        self.integration._cache["instance:new-entry"] = {
            "value": {"DBInstanceIdentifier": "new-db"},
            "timestamp": datetime.now()
        }

        # Old entry should be invalid, new entry should be valid
        self.assertFalse(self.integration._is_cache_valid("instance:old-entry"))
        self.assertTrue(self.integration._is_cache_valid("instance:new-entry"))


class TestRDSIntegrationMonitoring(unittest.TestCase):
    """Test RDSIntegration CloudWatch monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_rds_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.integration = RDSIntegration()
        self.integration._rds_client = self.mock_rds_client
        self.integration._cw_client = self.mock_cw_client

    def test_get_metric_statistics(self):
        """Test getting metric statistics"""
        self.mock_cw_client.get_metric_statistics.return_value = {
            "Label": "DatabaseConnections",
            "Datapoints": [
                {"Timestamp": datetime(2024, 1, 1, 12, 0), "Average": 45.0, "Unit": "Count"},
                {"Timestamp": datetime(2024, 1, 1, 12, 5), "Average": 52.0, "Unit": "Count"}
            ]
        }

        result = self.integration.get_metric_statistics(
            namespace="AWS/RDS",
            metric_name="DatabaseConnections",
            start_time=datetime(2024, 1, 1, 11, 0),
            end_time=datetime(2024, 1, 1, 12, 0),
            period=300
        )

        self.assertEqual(result["Label"], "DatabaseConnections")
        self.assertEqual(len(result["Datapoints"]), 2)

    def test_list_metrics(self):
        """Test listing available metrics"""
        self.mock_cw_client.list_metrics.return_value = {
            "Metrics": [
                {"Namespace": "AWS/RDS", "MetricName": "CPUUtilization"},
                {"Namespace": "AWS/RDS", "MetricName": "DatabaseConnections"},
                {"Namespace": "AWS/RDS", "MetricName": "FreeStorageSpace"}
            ]
        }

        result = self.integration.list_metrics(
            namespace="AWS/RDS",
            db_instance_identifier="my-db"
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["MetricName"], "CPUUtilization")

    def test_put_metric_alarm(self):
        """Test creating a CloudWatch alarm"""
        self.mock_cw_client.put_metric_alarm.return_value = {}

        result = self.integration.put_metric_alarm(
            alarm_name="high-cpu-alarm",
            db_instance_identifier="my-db",
            metric_name="CPUUtilization",
            threshold=80.0,
            comparison_operator="GreaterThanThreshold",
            period=300,
            evaluation_periods=2
        )

        self.assertIsNone(result)
        self.mock_cw_client.put_metric_alarm.assert_called_once()

    def test_get_available_metrics(self):
        """Test getting available RDS metrics"""
        self.mock_cw_client.list_metrics.return_value = {
            "Metrics": [
                {"Namespace": "AWS/RDS", "MetricName": "CPUUtilization"},
                {"Namespace": "AWS/RDS", "MetricName": "DatabaseConnections"},
                {"Namespace": "AWS/RDS", "MetricName": "FreeStorageSpace"},
                {"Namespace": "AWS/RDS", "MetricName": "ReplicaLag"}
            ]
        }

        result = self.integration.get_available_metrics("my-db")

        self.assertIn("CPUUtilization", result)
        self.assertIn("DatabaseConnections", result)
        self.assertIn("FreeStorageSpace", result)


class TestRDSIntegrationDescribe(unittest.TestCase):
    """Test RDSIntegration describe methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_rds_client = MagicMock()
        self.integration = RDSIntegration()
        self.integration._rds_client = self.mock_rds_client

    def test_describe_db_instances(self):
        """Test describing DB instances"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "prod-db-1",
                    "DBInstanceClass": "db.r5.large",
                    "Engine": "postgres",
                    "DBInstanceStatus": "available",
                    "Endpoint": {"Address": "prod-db-1.xyz.us-east-1.rds.amazonaws.com", "Port": 5432}
                }
            ]
        }

        result = self.integration.describe_db_instances(
            db_instance_identifier="prod-db-1"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["DBInstanceIdentifier"], "prod-db-1")

    def test_describe_db_instance_tech_support(self):
        """Test describing DB instance with tech support details"""
        self.mock_rds_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "support-test",
                    "DBInstanceClass": "db.t3.medium",
                    "Engine": "mysql",
                    "DBInstanceStatus": "available",
                    "LicenseModel": "general-public-license",
                    "OptionGroupMemberships": [
                        {"OptionGroupName": "default:mysql-8-0", "Status": "in-sync"}
                    ],
                    "DBSecurityGroups": [
                        {"DBSecurityGroupName": "default", "Status": "active"}
                    ]
                }
            ]
        }

        result = self.integration.describe_db_instance_tech_support(
            db_instance_identifier="support-test"
        )

        self.assertEqual(result[0]["DBInstanceIdentifier"], "support-test")
        self.assertEqual(result[0]["LicenseModel"], "general-public-license")


if __name__ == "__main__":
    unittest.main()
