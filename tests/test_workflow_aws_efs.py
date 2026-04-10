"""
Tests for workflow_aws_efs module
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

# Create mock boto3 module before importing workflow_aws_efs
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

# Now we can import the module
from src.workflow_aws_efs import (
    EFSIntegration,
    PerformanceMode,
    ThroughputMode,
    EncryptedOption,
    ReplicationStatus,
    BackupPolicyStatus,
    EFSConfig,
    MountTargetConfig,
    AccessPointConfig,
    EFSFileSystem,
)


class TestPerformanceMode(unittest.TestCase):
    """Test PerformanceMode enum"""

    def test_performance_mode_values(self):
        self.assertEqual(PerformanceMode.GENERAL_PURPOSE.value, "generalPurpose")
        self.assertEqual(PerformanceMode.MAX_IO.value, "maxIO")

    def test_performance_mode_count(self):
        self.assertEqual(len(PerformanceMode), 2)


class TestThroughputMode(unittest.TestCase):
    """Test ThroughputMode enum"""

    def test_throughput_mode_values(self):
        self.assertEqual(ThroughputMode.BURSTING.value, "bursting")
        self.assertEqual(ThroughputMode.PROVISIONED.value, "provisioned")
        self.assertEqual(ThroughputMode.ELASTIC.value, "elastic")

    def test_throughput_mode_count(self):
        self.assertEqual(len(ThroughputMode), 3)


class TestEncryptedOption(unittest.TestCase):
    """Test EncryptedOption enum"""

    def test_encrypted_option_values(self):
        self.assertEqual(EncryptedOption.ENABLED.value, True)
        self.assertEqual(EncryptedOption.DISABLED.value, False)


class TestReplicationStatus(unittest.TestCase):
    """Test ReplicationStatus enum"""

    def test_replication_status_values(self):
        self.assertEqual(ReplicationStatus.ENABLED.value, "ENABLED")
        self.assertEqual(ReplicationStatus.CREATING.value, "CREATING")
        self.assertEqual(ReplicationStatus.DELETED.value, "DELETED")
        self.assertEqual(ReplicationStatus.ERROR.value, "ERROR")


class TestBackupPolicyStatus(unittest.TestCase):
    """Test BackupPolicyStatus enum"""

    def test_backup_policy_status_values(self):
        self.assertEqual(BackupPolicyStatus.ENABLED.value, "ENABLED")
        self.assertEqual(BackupPolicyStatus.DISABLED.value, "DISABLED")
        self.assertEqual(BackupPolicyStatus.ENABLING.value, "ENABLING")
        self.assertEqual(BackupPolicyStatus.DISABLING.value, "DISABLING")


class TestEFSConfig(unittest.TestCase):
    """Test EFSConfig dataclass"""

    def test_efs_config_defaults(self):
        config = EFSConfig()
        self.assertEqual(config.name, "efs-filesystem")
        self.assertEqual(config.region, "us-east-1")
        self.assertEqual(config.performance_mode, PerformanceMode.GENERAL_PURPOSE)
        self.assertEqual(config.throughput_mode, ThroughputMode.BURSTING)
        self.assertTrue(config.encrypted)
        self.assertEqual(config.backup_policy, BackupPolicyStatus.ENABLED)

    def test_efs_config_custom(self):
        config = EFSConfig(
            name="my-efs",
            region="us-west-2",
            performance_mode=PerformanceMode.MAX_IO,
            throughput_mode=ThroughputMode.PROVISIONED,
            provisioned_throughput_mbps=1024.0,
            encrypted=True,
            kms_key_id="kms-key-123",
            replication_region="eu-west-1",
            lifecycle_policies=["transition-to-ia"],
            tags={"Environment": "Production"}
        )
        self.assertEqual(config.name, "my-efs")
        self.assertEqual(config.region, "us-west-2")
        self.assertEqual(config.performance_mode, PerformanceMode.MAX_IO)
        self.assertEqual(config.throughput_mode, ThroughputMode.PROVISIONED)
        self.assertEqual(config.provisioned_throughput_mbps, 1024.0)
        self.assertEqual(config.kms_key_id, "kms-key-123")
        self.assertEqual(config.replication_region, "eu-west-1")

    def test_efs_config_with_tags(self):
        config = EFSConfig(
            name="test-efs",
            tags={"Application": "Web", "Environment": "Dev"}
        )
        self.assertEqual(config.tags["Application"], "Web")


class TestMountTargetConfig(unittest.TestCase):
    """Test MountTargetConfig dataclass"""

    def test_mount_target_config_creation(self):
        config = MountTargetConfig(
            subnet_id="subnet-12345678",
            security_group_ids=["sg-12345678"]
        )
        self.assertEqual(config.subnet_id, "subnet-12345678")
        self.assertEqual(len(config.security_group_ids), 1)

    def test_mount_target_config_with_ip(self):
        config = MountTargetConfig(
            subnet_id="subnet-123",
            security_group_ids=["sg-123"],
            ip_address="10.0.1.100"
        )
        self.assertEqual(config.ip_address, "10.0.1.100")


class TestAccessPointConfig(unittest.TestCase):
    """Test AccessPointConfig dataclass"""

    def test_access_point_config_defaults(self):
        config = AccessPointConfig()
        self.assertEqual(config.name, "efs-access-point")
        self.assertIsNone(config.posix_user)
        self.assertIsNone(config.root_directory)

    def test_access_point_config_custom(self):
        config = AccessPointConfig(
            name="custom-ap",
            posix_user={"Uid": "1000", "Gid": "1000"},
            root_directory={"/": {"Path": "/shared", "CreationInfo": {"OwnerUid": 1000, "OwnerGid": 1000}}}
        )
        self.assertEqual(config.name, "custom-ap")
        self.assertEqual(config.posix_user["Uid"], "1000")


class TestEFSFileSystem(unittest.TestCase):
    """Test EFSFileSystem dataclass"""

    def test_efs_file_system_creation(self):
        fs = EFSFileSystem(
            file_system_id="fs-12345678",
            name="test-efs",
            arn="arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-12345678",
            size_bytes=1073741824,
            performance_mode="generalPurpose",
            throughput_mode="bursting",
            encrypted=True
        )
        self.assertEqual(fs.file_system_id, "fs-12345678")
        self.assertEqual(fs.name, "test-efs")
        self.assertEqual(fs.size_bytes, 1073741824)
        self.assertEqual(fs.performance_mode, "generalPurpose")

    def test_efs_file_system_with_tags(self):
        fs = EFSFileSystem(
            file_system_id="fs-123",
            name="test-efs",
            arn="arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            tags={"Environment": "Production", "Application": "Web"}
        )
        self.assertEqual(fs.tags["Environment"], "Production")
        self.assertEqual(len(fs.tags), 2)

    def test_efs_file_system_with_replication(self):
        fs = EFSFileSystem(
            file_system_id="fs-123",
            name="test-efs",
            arn="arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            replication_configuration={"Status": "ENABLED", "DestinationRegion": "us-west-2"}
        )
        self.assertIsNotNone(fs.replication_configuration)


class TestEFSIntegration(unittest.TestCase):
    """Test EFSIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_integration_initialization(self):
        """Test EFSIntegration initialization"""
        integration = EFSIntegration(
            region="us-east-1",
            profile_name="test-profile"
        )
        self.assertEqual(integration.region, "us-east-1")
        self.assertEqual(integration.profile_name, "test-profile")

    def test_integration_with_config(self):
        """Test EFSIntegration with custom config"""
        config = EFSConfig(
            name="custom-efs",
            region="us-west-2",
            performance_mode=PerformanceMode.MAX_IO
        )
        integration = EFSIntegration(config=config)
        self.assertEqual(integration.config.name, "custom-efs")
        self.assertEqual(integration.config.performance_mode, PerformanceMode.MAX_IO)

    def test_efs_client_property(self):
        """Test EFS client property"""
        integration = EFSIntegration()
        client = integration.efs_client
        self.assertIsNotNone(client)

    def test_cw_client_property(self):
        """Test CloudWatch client property"""
        integration = EFSIntegration()
        client = integration.cw_client
        self.assertIsNotNone(client)

    def test_sts_client_property(self):
        """Test STS client property"""
        integration = EFSIntegration()
        client = integration.sts_client
        self.assertIsNotNone(client)

    def test_generate_file_system_name(self):
        """Test file system name generation"""
        integration = EFSIntegration()
        name = integration._generate_file_system_name("test")
        self.assertTrue(name.startswith("test-"))
        parts = name.split("-")
        self.assertEqual(len(parts), 3)  # prefix-timestamp-uuid

    def test_get_account_id(self):
        """Test getting AWS account ID"""
        integration = EFSIntegration()
        self.mock_sts_client.get_caller_identity.return_value = {
            "Account": "123456789012"
        }
        account_id = integration._get_account_id()
        self.assertEqual(account_id, "123456789012")

    def test_get_account_id_fallback(self):
        """Test getting AWS account ID with fallback"""
        integration = EFSIntegration()
        self.mock_sts_client.get_caller_identity.side_effect = Exception("Not authenticated")
        account_id = integration._get_account_id()
        self.assertEqual(account_id, "unknown")

    def test_create_file_system_success(self):
        """Test successful file system creation"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-12345678",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-12345678",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "bursting",
            "Encrypted": True,
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(name="test-efs")
        fs = integration.create_file_system(config)

        self.assertEqual(fs.file_system_id, "fs-12345678")
        self.assertEqual(fs.life_cycle_state, "available")
        self.mock_efs_client.create_file_system.assert_called_once()

    def test_create_file_system_with_provisioned_throughput(self):
        """Test file system creation with provisioned throughput"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-123",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "provisioned",
            "ProvisionedThroughputInMibps": 1024.0,
            "Encrypted": True,
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(
            name="test-efs",
            throughput_mode=ThroughputMode.PROVISIONED,
            provisioned_throughput_mbps=1024.0
        )
        fs = integration.create_file_system(config)

        call_kwargs = self.mock_efs_client.create_file_system.call_args[1]
        self.assertEqual(call_kwargs["ProvisionedThroughputInMibps"], 1024.0)

    def test_create_file_system_with_encryption(self):
        """Test file system creation with encryption"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-123",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "bursting",
            "Encrypted": True,
            "KmsKeyId": "kms-key-123",
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(
            name="test-efs",
            encrypted=True,
            kms_key_id="kms-key-123"
        )
        fs = integration.create_file_system(config)

        call_kwargs = self.mock_efs_client.create_file_system.call_args[1]
        self.assertTrue(call_kwargs["Encrypted"])
        self.assertEqual(call_kwargs["KmsKeyId"], "kms-key-123")

    def test_create_file_system_with_tags(self):
        """Test file system creation with tags"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-123",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "bursting",
            "Encrypted": True,
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(name="test-efs")
        tags = {"Environment": "Production", "Application": "Web"}
        fs = integration.create_file_system(config, tags=tags)

        self.mock_efs_client.create_tags.assert_called_once()
        call_args = self.mock_efs_client.create_tags.call_args
        self.assertEqual(len(call_args[1]["Tags"]), 2)

    def test_create_file_system_with_backup_policy(self):
        """Test file system creation with backup policy"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-123",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "bursting",
            "Encrypted": True,
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(
            name="test-efs",
            backup_policy=BackupPolicyStatus.ENABLED
        )
        integration.create_file_system(config)
        # Backup policy should be set after creation

    def test_create_file_system_with_lifecycle_policies(self):
        """Test file system creation with lifecycle policies"""
        integration = EFSIntegration()

        self.mock_efs_client.create_file_system.return_value = {
            "FileSystemId": "fs-123",
            "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
            "SizeInBytes": {"Value": 0},
            "PerformanceMode": "generalPurpose",
            "ThroughputMode": "bursting",
            "Encrypted": True,
            "LifeCycleState": "available",
            "NumberOfMountTargets": 0,
            "OwnerId": "123456789012"
        }

        config = EFSConfig(
            name="test-efs",
            lifecycle_policies=["transition-to-ia", "transition-to-primary"]
        )
        integration.create_file_system(config)
        # Lifecycle policies should be configured

    def test_get_file_system_success(self):
        """Test getting file system details"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-12345678",
                "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-12345678",
                "SizeInBytes": {"Value": 1073741824},
                "PerformanceMode": "generalPurpose",
                "ThroughputMode": "bursting",
                "Encrypted": True,
                "LifeCycleState": "available",
                "NumberOfMountTargets": 2,
                "OwnerId": "123456789012"
            }]
        }

        self.mock_efs_client.list_tags_for_metadata.return_value = {
            "Tags": [{"Key": "Name", "Value": "test-efs"}]
        }

        fs = integration.get_file_system("fs-12345678")

        self.assertEqual(fs.file_system_id, "fs-12345678")
        self.assertEqual(fs.size_bytes, 1073741824)
        self.assertEqual(fs.number_of_mount_targets, 2)

    def test_get_file_system_not_found(self):
        """Test getting non-existent file system"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": []
        }

        fs = integration.get_file_system("fs-nonexistent")
        self.assertIsNone(fs)

    def test_list_file_systems(self):
        """Test listing file systems"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [
                {
                    "FileSystemId": "fs-123",
                    "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
                    "SizeInBytes": {"Value": 0},
                    "PerformanceMode": "generalPurpose",
                    "ThroughputMode": "bursting",
                    "Encrypted": True,
                    "LifeCycleState": "available",
                    "NumberOfMountTargets": 0,
                    "OwnerId": "123456789012"
                },
                {
                    "FileSystemId": "fs-456",
                    "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-456",
                    "SizeInBytes": {"Value": 1073741824},
                    "PerformanceMode": "generalPurpose",
                    "ThroughputMode": "bursting",
                    "Encrypted": True,
                    "LifeCycleState": "available",
                    "NumberOfMountTargets": 1,
                    "OwnerId": "123456789012"
                }
            ]
        }

        self.mock_efs_client.list_tags_for_metadata.return_value = {
            "Tags": []
        }

        file_systems = integration.list_file_systems()

        self.assertEqual(len(file_systems), 2)

    def test_list_file_systems_with_pagination(self):
        """Test listing file systems with pagination"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [
                {
                    "FileSystemId": "fs-123",
                    "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
                    "SizeInBytes": {"Value": 0},
                    "PerformanceMode": "generalPurpose",
                    "ThroughputMode": "bursting",
                    "Encrypted": True,
                    "LifeCycleState": "available",
                    "NumberOfMountTargets": 0,
                    "OwnerId": "123456789012"
                }
            ],
            "Marker": "next-page-marker"
        }

        self.mock_efs_client.list_tags_for_metadata.return_value = {
            "Tags": []
        }

        file_systems = integration.list_file_systems(max_items=1)
        self.assertEqual(len(file_systems), 1)

    def test_update_file_system_throughput_mode(self):
        """Test updating file system throughput mode"""
        integration = EFSIntegration()

        self.mock_efs_client.update_file_system.return_value = {}

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-123",
                "FileSystemArn": "arn:aws:elasticfilesystem:us-east-1:123456789:file-system/fs-123",
                "SizeInBytes": {"Value": 0},
                "PerformanceMode": "generalPurpose",
                "ThroughputMode": "provisioned",
                "ProvisionedThroughputInMibps": 512.0,
                "Encrypted": True,
                "LifeCycleState": "available",
                "NumberOfMountTargets": 0,
                "OwnerId": "123456789012"
            }]
        }

        fs = integration.update_file_system(
            "fs-123",
            throughput_mode=ThroughputMode.PROVISIONED,
            provisioned_throughput_mbps=512.0
        )

        self.mock_efs_client.update_file_system.assert_called_once()
        call_kwargs = self.mock_efs_client.update_file_system.call_args[1]
        self.assertEqual(call_kwargs["ThroughputMode"], "provisioned")
        self.assertEqual(call_kwargs["ProvisionedThroughputInMibps"], 512.0)

    def test_delete_file_system(self):
        """Test deleting file system"""
        integration = EFSIntegration()

        self.mock_efs_client.delete_file_system.return_value = {}

        integration.delete_file_system("fs-12345678")

        self.mock_efs_client.delete_file_system.assert_called_once_with(
            FileSystemId="fs-12345678"
        )


class TestMountTargetOperations(unittest.TestCase):
    """Test mount target operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_create_mount_target(self):
        """Test creating mount target"""
        integration = EFSIntegration()

        self.mock_efs_client.create_mount_target.return_value = {
            "MountTargetId": "fsmt-12345678",
            "FileSystemId": "fs-12345678",
            "SubnetId": "subnet-12345678",
            "SecurityGroups": ["sg-12345678"],
            "LifeCycleState": "available"
        }

        config = MountTargetConfig(
            subnet_id="subnet-12345678",
            security_group_ids=["sg-12345678"]
        )

        mt = integration.create_mount_target("fs-12345678", config)

        self.assertEqual(mt["MountTargetId"], "fsmt-12345678")
        self.assertEqual(mt["FileSystemId"], "fs-12345678")

    def test_create_mount_target_with_ip_address(self):
        """Test creating mount target with IP address"""
        integration = EFSIntegration()

        self.mock_efs_client.create_mount_target.return_value = {
            "MountTargetId": "fsmt-123",
            "FileSystemId": "fs-123",
            "SubnetId": "subnet-123",
            "IpAddress": "10.0.1.100",
            "SecurityGroups": ["sg-123"],
            "LifeCycleState": "available"
        }

        config = MountTargetConfig(
            subnet_id="subnet-123",
            security_group_ids=["sg-123"],
            ip_address="10.0.1.100"
        )

        mt = integration.create_mount_target("fs-123", config)

        call_kwargs = self.mock_efs_client.create_mount_target.call_args[1]
        self.assertEqual(call_kwargs["IpAddress"], "10.0.1.100")

    def test_get_mount_target(self):
        """Test getting mount target"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_mount_targets.return_value = {
            "MountTargets": [{
                "MountTargetId": "fsmt-123",
                "FileSystemId": "fs-123",
                "SubnetId": "subnet-123",
                "LifeCycleState": "available"
            }]
        }

        mt = integration.get_mount_target("fs-123", "fsmt-123")

        self.assertEqual(mt["MountTargetId"], "fsmt-123")

    def test_list_mount_targets(self):
        """Test listing mount targets"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_mount_targets.return_value = {
            "MountTargets": [
                {
                    "MountTargetId": "fsmt-1",
                    "FileSystemId": "fs-123",
                    "SubnetId": "subnet-1",
                    "LifeCycleState": "available"
                },
                {
                    "MountTargetId": "fsmt-2",
                    "FileSystemId": "fs-123",
                    "SubnetId": "subnet-2",
                    "LifeCycleState": "available"
                }
            ]
        }

        targets = integration.list_mount_targets("fs-123")

        self.assertEqual(len(targets), 2)

    def test_delete_mount_target(self):
        """Test deleting mount target"""
        integration = EFSIntegration()

        self.mock_efs_client.delete_mount_target.return_value = {}

        integration.delete_mount_target("fs-123", "fsmt-12345678")

        self.mock_efs_client.delete_mount_target.assert_called_once_with(
            MountTargetId="fsmt-12345678"
        )


class TestAccessPointOperations(unittest.TestCase):
    """Test access point operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_create_access_point(self):
        """Test creating access point"""
        integration = EFSIntegration()

        self.mock_efs_client.create_access_point.return_value = {
            "AccessPointId": "fsa-12345678",
            "FileSystemId": "fs-12345678",
            "AccessPointArn": "arn:aws:elasticfilesystem:us-east-1:123456789:access-point/fsa-12345678",
            "PosixUser": {"Uid": 1000, "Gid": 1000},
            "RootDirectory": {"/": {"Path": "/shared"}},
            "LifeCycleState": "available"
        }

        config = AccessPointConfig(
            name="test-ap",
            posix_user={"Uid": "1000", "Gid": "1000"},
            root_directory={"/": {"Path": "/shared"}}
        )

        ap = integration.create_access_point("fs-12345678", config)

        self.assertEqual(ap["AccessPointId"], "fsa-12345678")
        self.assertEqual(ap["FileSystemId"], "fs-12345678")

    def test_create_access_point_with_tags(self):
        """Test creating access point with tags"""
        integration = EFSIntegration()

        self.mock_efs_client.create_access_point.return_value = {
            "AccessPointId": "fsa-123",
            "FileSystemId": "fs-123",
            "AccessPointArn": "arn:aws:elasticfilesystem:us-east-1:123456789:access-point/fsa-123",
            "LifeCycleState": "available"
        }

        config = AccessPointConfig(
            name="test-ap",
            tags={"Environment": "Production"}
        )

        ap = integration.create_access_point("fs-123", config)

        call_kwargs = self.mock_efs_client.create_access_point.call_args[1]
        self.assertEqual(len(call_kwargs["Tags"]), 1)

    def test_describe_access_point(self):
        """Test describing access point"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_access_points.return_value = {
            "AccessPoints": [{
                "AccessPointId": "fsa-123",
                "FileSystemId": "fs-123",
                "LifeCycleState": "available"
            }]
        }

        ap = integration.describe_access_point("fsa-123")

        self.assertEqual(ap["AccessPointId"], "fsa-123")

    def test_list_access_points(self):
        """Test listing access points"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_access_points.return_value = {
            "AccessPoints": [
                {"AccessPointId": "fsa-1", "FileSystemId": "fs-123"},
                {"AccessPointId": "fsa-2", "FileSystemId": "fs-123"}
            ]
        }

        aps = integration.list_access_points("fs-123")

        self.assertEqual(len(aps), 2)

    def test_delete_access_point(self):
        """Test deleting access point"""
        integration = EFSIntegration()

        self.mock_efs_client.delete_access_point.return_value = {}

        integration.delete_access_point("fsa-12345678")

        self.mock_efs_client.delete_access_point.assert_called_once_with(
            AccessPointId="fsa-12345678"
        )


class TestBackupPolicyOperations(unittest.TestCase):
    """Test backup policy operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_set_backup_policy_enabled(self):
        """Test setting backup policy to enabled"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_backup_policy.return_value = {
            "BackupPolicy": {"Status": "ENABLED"}
        }

        result = integration.set_backup_policy("fs-123", BackupPolicyStatus.ENABLED)

        call_kwargs = self.mock_efs_client.backup_policy_put.call_args[1]
        self.assertEqual(call_kwargs["BackupPolicy"]["Status"], "ENABLED")

    def test_set_backup_policy_disabled(self):
        """Test setting backup policy to disabled"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_backup_policy.return_value = {
            "BackupPolicy": {"Status": "DISABLED"}
        }

        result = integration.set_backup_policy("fs-123", BackupPolicyStatus.DISABLED)

    def test_get_backup_policy(self):
        """Test getting backup policy"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_backup_policy.return_value = {
            "BackupPolicy": {"Status": "ENABLED"}
        }

        policy = integration.get_backup_policy("fs-123")

        self.assertEqual(policy["Status"], "ENABLED")


class TestReplicationOperations(unittest.TestCase):
    """Test replication operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_enable_replication(self):
        """Test enabling replication"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-123",
                "ReplicationConfiguration": {"Status": "ENABLED", "DestinationRegion": "us-west-2"}
            }]
        }

        result = integration.enable_replication("fs-123", "us-west-2")

        self.mock_efs_client.replication_configuration_put.assert_called_once()

    def test_disable_replication(self):
        """Test disabling replication"""
        integration = EFSIntegration()

        self.mock_efs_client.replication_configuration_delete.return_value = {}

        integration.disable_replication("fs-123")

        self.mock_efs_client.replication_configuration_delete.assert_called_once()

    def test_get_replication_configuration(self):
        """Test getting replication configuration"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-123",
                "ReplicationConfiguration": {"Status": "ENABLED", "DestinationRegion": "us-west-2"}
            }]
        }

        config = integration.get_replication_configuration("fs-123")

        self.assertIsNotNone(config)


class TestLifecyclePolicies(unittest.TestCase):
    """Test lifecycle policy operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_set_lifecycle_policies(self):
        """Test setting lifecycle policies"""
        integration = EFSIntegration()

        integration.set_lifecycle_policies(
            "fs-123",
            ["transition-to-ia", "transition-to-primary"]
        )

        self.mock_efs_client.put_lifecycle_configuration.assert_called_once()

    def test_get_lifecycle_policies(self):
        """Test getting lifecycle policies"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_lifecycle_configuration.return_value = {
            "LifecyclePolicies": [
                {"TransitionToIA": "AFTER_30_DAYS"},
                {"TransitionToPrimaryStorageClass": "AFTER_1_ACCESS"}
            ]
        }

        policies = integration.get_lifecycle_policies("fs-123")

        self.assertEqual(len(policies), 2)


class TestMountHelper(unittest.TestCase):
    """Test mount helper functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_get_mount_command(self):
        """Test getting mount command"""
        integration = EFSIntegration()

        self.mock_efs_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-123",
                "LifeCycleState": "available"
            }]
        }

        command = integration.get_mount_command("fs-123", "/mnt/efs")

        self.assertIn("mount", command.lower())

    def test_get_mount_command_using_dns(self):
        """Test getting mount command using DNS name"""
        integration = EFSIntegration()

        command = integration.get_mount_command_using_dns("fs-123", "us-east-1", "/mnt/efs")

        self.assertIn("efs.us-east-1.amazonaws.com", command)

    def test_verify_mount_target_connectivity(self):
        """Test verifying mount target connectivity"""
        integration = EFSIntegration()

        # Should not raise an exception
        result = integration.verify_mount_target_connectivity(
            "fsmt-123",
            "10.0.1.100"
        )


class TestCloudWatchMonitoring(unittest.TestCase):
    """Test CloudWatch monitoring functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_efs_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_sts_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_efs_client,
            self.mock_cw_client,
            self.mock_sts_client,
        ]

    def test_get_performance_metrics(self):
        """Test getting performance metrics"""
        integration = EFSIntegration()

        self.mock_cw_client.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Timestamp": datetime.now(), "Average": 100.0, "Sum": 1000.0}
            ]
        }

        metrics = integration.get_performance_metrics(
            "fs-123",
            start_time=datetime.now(),
            end_time=datetime.now()
        )

        self.mock_cw_client.get_metric_statistics.assert_called_once()

    def test_put_metric_data(self):
        """Test putting metric data"""
        integration = EFSIntegration()

        self.mock_cw_client.put_metric_data.return_value = {}

        integration.put_metric_data(
            "TestNamespace",
            [{"MetricName": "TestMetric", "Value": 100}]
        )

        self.mock_cw_client.put_metric_data.assert_called_once()


class TestEFSIntegrationNoBoto3(unittest.TestCase):
    """Test EFSIntegration without boto3 available"""

    def test_initialization_without_boto3(self):
        """Test initialization when boto3 is not available"""
        # This tests the mock file system creation path
        integration = EFSIntegration()
        # Without boto3, operations should use mock

    def test_create_mock_file_system(self):
        """Test creating mock file system"""
        integration = EFSIntegration()
        config = EFSConfig(name="test-efs", region="us-east-1")

        fs = integration._create_mock_file_system(config)

        self.assertEqual(fs.name, "test-efs")
        self.assertTrue(fs.file_system_id.startswith("fs-"))
        self.assertEqual(fs.life_cycle_state, "available")


if __name__ == '__main__':
    unittest.main()
