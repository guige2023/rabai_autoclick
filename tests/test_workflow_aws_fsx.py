"""
Tests for workflow_aws_fsx module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import types

# Create mock boto3 module before importing workflow_aws_fsx
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

sys.modules['boto3'] = mock_boto3

import src.workflow_aws_fsx as _fsx_module

if _fsx_module is not None:
    FSxIntegration = _fsx_module.FSxIntegration
    FSxFileSystemType = _fsx_module.FSxFileSystemType
    FSxWindowsDeploymentType = _fsx_module.FSxWindowsDeploymentType
    FSxLustreDeploymentType = _fsx_module.FSxLustreDeploymentType
    FSxOpenZFSDeploymentType = _fsx_module.FSxOpenZFSDeploymentType
    FSxNetAppONTAPDeploymentType = _fsx_module.FSxNetAppONTAPDeploymentType
    DataRepositoryAssociationState = _fsx_module.DataRepositoryAssociationState
    FSxWindowsConfig = _fsx_module.FSxWindowsConfig
    FSxLustreConfig = _fsx_module.FSxLustreConfig
    FSxOpenZFSConfig = _fsx_module.FSxOpenZFSConfig
    FSxNetAppONTAPConfig = _fsx_module.FSxNetAppONTAPConfig
    DataRepositoryAssociationConfig = _fsx_module.DataRepositoryAssociationConfig
    BackupConfig = _fsx_module.BackupConfig
    ActiveDirectoryConfig = _fsx_module.ActiveDirectoryConfig
    StorageVirtualMachineConfig = _fsx_module.StorageVirtualMachineConfig
    VolumeConfig = _fsx_module.VolumeConfig
    CloudWatchConfig = _fsx_module.CloudWatchConfig


class TestFSxFileSystemType(unittest.TestCase):
    """Test FSxFileSystemType enum"""

    def test_values(self):
        self.assertEqual(FSxFileSystemType.WINDOWS.value, "WINDOWS")
        self.assertEqual(FSxFileSystemType.LUSTRE.value, "LUSTRE")
        self.assertEqual(FSxFileSystemType.OPEN_ZFS.value, "OPEN_ZFS")
        self.assertEqual(FSxFileSystemType.NETAPP_ONTAP.value, "NETAPP_ONTAP")


class TestFSxWindowsDeploymentType(unittest.TestCase):
    """Test FSxWindowsDeploymentType enum"""

    def test_values(self):
        self.assertEqual(FSxWindowsDeploymentType.SINGLE_AZ.value, "SINGLE_AZ_1")
        self.assertEqual(FSxWindowsDeploymentType.SINGLE_AZ_2.value, "SINGLE_AZ_2")
        self.assertEqual(FSxWindowsDeploymentType.MULTI_AZ_1.value, "MULTI_AZ_1")
        self.assertEqual(FSxWindowsDeploymentType.MULTI_AZ_2.value, "MULTI_AZ_2")


class TestFSxLustreDeploymentType(unittest.TestCase):
    """Test FSxLustreDeploymentType enum"""

    def test_values(self):
        self.assertEqual(FSxLustreDeploymentType.PERSISTENT_1.value, "PERSISTENT_1")
        self.assertEqual(FSxLustreDeploymentType.PERSISTENT_2.value, "PERSISTENT_2")
        self.assertEqual(FSxLustreDeploymentType.SCRATCH_1.value, "SCRATCH_1")
        self.assertEqual(FSxLustreDeploymentType.SCRATCH_2.value, "SCRATCH_2")


class TestFSxOpenZFSDeploymentType(unittest.TestCase):
    """Test FSxOpenZFSDeploymentType enum"""

    def test_values(self):
        self.assertEqual(FSxOpenZFSDeploymentType.SINGLE_AZ.value, "SINGLE_AZ_1")
        self.assertEqual(FSxOpenZFSDeploymentType.MULTI_AZ.value, "MULTI_AZ_1")


class TestFSxNetAppONTAPDeploymentType(unittest.TestCase):
    """Test FSxNetAppONTAPDeploymentType enum"""

    def test_values(self):
        self.assertEqual(FSxNetAppONTAPDeploymentType.SINGLE_AZ.value, "SINGLE_AZ_1")
        self.assertEqual(FSxNetAppONTAPDeploymentType.MULTI_AZ.value, "MULTI_AZ_1")


class TestDataRepositoryAssociationState(unittest.TestCase):
    """Test DataRepositoryAssociationState enum"""

    def test_values(self):
        self.assertEqual(DataRepositoryAssociationState.CREATING.value, "CREATING")
        self.assertEqual(DataRepositoryAssociationState.AVAILABLE.value, "AVAILABLE")
        self.assertEqual(DataRepositoryAssociationState.MISCONFIGURED.value, "MISCONFIGURED")
        self.assertEqual(DataRepositoryAssociationState.DELETING.value, "DELETING")
        self.assertEqual(DataRepositoryAssociationState.FAILED.value, "FAILED")


class TestFSxWindowsConfig(unittest.TestCase):
    """Test FSxWindowsConfig dataclass"""

    def test_default_values(self):
        config = FSxWindowsConfig()
        self.assertIsNone(config.file_system_id)
        self.assertEqual(config.deployment_type, FSxWindowsDeploymentType.SINGLE_AZ)
        self.assertEqual(config.storage_capacity_gb, 32)
        self.assertEqual(config.storage_type, "SSD")
        self.assertEqual(config.throughput_capacity_mbps, 8)
        self.assertIsNone(config.preferred_subnet_id)
        self.assertEqual(config.security_group_ids, [])
        self.assertEqual(config.subnet_ids, [])
        self.assertEqual(config.skip_final_backup, False)
        self.assertIsNone(config.final_backup_tags)
        self.assertIsNone(config.active_directory_id)
        self.assertIsNone(config.self_managed_active_directory)
        self.assertEqual(config.copy_tags_to_backups, False)
        self.assertIsNone(config.backup_id)
        self.assertIsNone(config.kms_key_id)
        self.assertEqual(config.automatic_backup_retention_days, 0)
        self.assertIsNone(config.daily_automatic_backup_start_time)
        self.assertIsNone(config.weekly_maintenance_start_time)
        self.assertIsNone(config.tags)

    def test_custom_values(self):
        config = FSxWindowsConfig(
            deployment_type=FSxWindowsDeploymentType.MULTI_AZ_1,
            storage_capacity_gb=100,
            throughput_capacity_mbps=64,
            subnet_ids=["subnet-1", "subnet-2"],
            tags={"env": "prod"}
        )
        self.assertEqual(config.deployment_type, FSxWindowsDeploymentType.MULTI_AZ_1)
        self.assertEqual(config.storage_capacity_gb, 100)
        self.assertEqual(config.throughput_capacity_mbps, 64)
        self.assertEqual(len(config.subnet_ids), 2)
        self.assertEqual(config.tags, {"env": "prod"})


class TestFSxLustreConfig(unittest.TestCase):
    """Test FSxLustreConfig dataclass"""

    def test_default_values(self):
        config = FSxLustreConfig()
        self.assertIsNone(config.file_system_id)
        self.assertEqual(config.deployment_type, FSxLustreDeploymentType.SCRATCH_2)
        self.assertEqual(config.storage_capacity_gb, 1200)
        self.assertEqual(config.storage_type, "SSD")
        self.assertEqual(config.per_unit_storage_throughput, 50)
        self.assertEqual(config.subnet_ids, [])
        self.assertEqual(config.security_group_ids, [])
        self.assertEqual(config.data_repository_associations, [])
        self.assertIsNone(config.auto_import_policy)
        self.assertIsNone(config.export_path)
        self.assertEqual(config.imported_file_chunk_size_mib, 1024)
        self.assertIsNone(config.kms_key_id)
        self.assertIsNone(config.weekly_maintenance_start_time)
        self.assertIsNone(config.drive_cache_type)
        self.assertIsNone(config.tags)

    def test_custom_values(self):
        config = FSxLustreConfig(
            deployment_type=FSxLustreDeploymentType.PERSISTENT_1,
            storage_capacity_gb=2400,
            per_unit_storage_throughput=100,
            subnet_ids=["subnet-1"],
            auto_import_policy="NEW",
            export_path="s3://my-bucket/export",
            tags={"project": "analytics"}
        )
        self.assertEqual(config.deployment_type, FSxLustreDeploymentType.PERSISTENT_1)
        self.assertEqual(config.storage_capacity_gb, 2400)
        self.assertEqual(config.per_unit_storage_throughput, 100)
        self.assertEqual(config.auto_import_policy, "NEW")
        self.assertEqual(config.export_path, "s3://my-bucket/export")


class TestFSxOpenZFSConfig(unittest.TestCase):
    """Test FSxOpenZFSConfig dataclass"""

    def test_default_values(self):
        config = FSxOpenZFSConfig()
        self.assertIsNone(config.file_system_id)
        self.assertEqual(config.deployment_type, FSxOpenZFSDeploymentType.SINGLE_AZ)
        self.assertEqual(config.storage_capacity_gb, 64)
        self.assertEqual(config.storage_type, "SSD")
        self.assertEqual(config.throughput_capacity_mbps, 8)
        self.assertEqual(config.subnet_ids, [])
        self.assertIsNone(config.preferred_subnet_id)
        self.assertEqual(config.security_group_ids, [])
        self.assertIsNone(config.root_volume_configuration)
        self.assertIsNone(config.zfs_config)
        self.assertEqual(config.copy_tags_to_backups, False)
        self.assertIsNone(config.backup_id)
        self.assertIsNone(config.kms_key_id)
        self.assertEqual(config.automatic_backup_retention_days, 0)

    def test_custom_values(self):
        config = FSxOpenZFSConfig(
            deployment_type=FSxOpenZFSDeploymentType.MULTI_AZ,
            storage_capacity_gb=128,
            throughput_capacity_mbps=16,
            subnet_ids=["subnet-1"],
            zfs_config={"DataCompressionType": "ZSTD"},
            automatic_backup_retention_days=7
        )
        self.assertEqual(config.deployment_type, FSxOpenZFSDeploymentType.MULTI_AZ)
        self.assertEqual(config.storage_capacity_gb, 128)
        self.assertEqual(config.throughput_capacity_mbps, 16)
        self.assertEqual(config.zfs_config["DataCompressionType"], "ZSTD")
        self.assertEqual(config.automatic_backup_retention_days, 7)


class TestFSxNetAppONTAPConfig(unittest.TestCase):
    """Test FSxNetAppONTAPConfig dataclass"""

    def test_default_values(self):
        config = FSxNetAppONTAPConfig()
        self.assertIsNone(config.file_system_id)
        self.assertEqual(config.deployment_type, FSxNetAppONTAPDeploymentType.SINGLE_AZ)
        self.assertEqual(config.storage_capacity_gb, 1024)
        self.assertEqual(config.storage_type, "SSD")
        self.assertEqual(config.throughput_capacity_mbps, 128)
        self.assertEqual(config.subnet_ids, [])
        self.assertIsNone(config.preferred_subnet_id)
        self.assertEqual(config.security_group_ids, [])
        self.assertIsNone(config.fsx_admin_password)
        self.assertIsNone(config.active_directory)
        self.assertIsNone(config.backup_id)
        self.assertIsNone(config.kms_key_id)
        self.assertEqual(config.automatic_backup_retention_days, 0)
        self.assertEqual(config.route_table_ids, [])
        self.assertEqual(config.copy_tags_to_backups, False)

    def test_custom_values(self):
        config = FSxNetAppONTAPConfig(
            deployment_type=FSxNetAppONTAPDeploymentType.MULTI_AZ,
            storage_capacity_gb=2048,
            throughput_capacity_mbps=256,
            subnet_ids=["subnet-1", "subnet-2"],
            route_table_ids=["rtb-1", "rtb-2"],
            endpoint_ip_address_range="10.0.0.0/24"
        )
        self.assertEqual(config.deployment_type, FSxNetAppONTAPDeploymentType.MULTI_AZ)
        self.assertEqual(config.storage_capacity_gb, 2048)
        self.assertEqual(config.throughput_capacity_mbps, 256)
        self.assertEqual(len(config.subnet_ids), 2)
        self.assertEqual(config.endpoint_ip_address_range, "10.0.0.0/24")


class TestDataRepositoryAssociationConfig(unittest.TestCase):
    """Test DataRepositoryAssociationConfig dataclass"""

    def test_default_values(self):
        config = DataRepositoryAssociationConfig()
        self.assertIsNone(config.association_id)
        self.assertEqual(config.file_system_id, "")
        self.assertEqual(config.file_system_path, "")
        self.assertEqual(config.data_repository_path, "")
        self.assertIsNone(config.batch_import_meta_data)
        self.assertEqual(config.delete_data_in_filesystem, False)
        self.assertEqual(config.import_metadata_on_creation, True)
        self.assertEqual(config.new_capacity, 0)

    def test_custom_values(self):
        config = DataRepositoryAssociationConfig(
            file_system_id="fs-12345678",
            file_system_path="/data",
            data_repository_path="s3://my-bucket/data",
            delete_data_in_filesystem=True,
            new_capacity=100
        )
        self.assertEqual(config.file_system_id, "fs-12345678")
        self.assertEqual(config.file_system_path, "/data")
        self.assertEqual(config.data_repository_path, "s3://my-bucket/data")
        self.assertEqual(config.delete_data_in_filesystem, True)
        self.assertEqual(config.new_capacity, 100)


class TestBackupConfig(unittest.TestCase):
    """Test BackupConfig dataclass"""

    def test_default_values(self):
        config = BackupConfig()
        self.assertIsNone(config.backup_id)
        self.assertIsNone(config.volume_id)
        self.assertIsNone(config.file_system_id)
        self.assertEqual(config.backup_type, "AUTO")
        self.assertIsNone(config.tags)
        self.assertIsNone(config.kms_key_id)
        self.assertEqual(config.retention_period_days, 30)

    def test_custom_values(self):
        config = BackupConfig(
            file_system_id="fs-12345678",
            backup_type="USER",
            tags={"env": "prod"},
            retention_period_days=14
        )
        self.assertEqual(config.file_system_id, "fs-12345678")
        self.assertEqual(config.backup_type, "USER")
        self.assertEqual(config.tags, {"env": "prod"})
        self.assertEqual(config.retention_period_days, 14)


class TestActiveDirectoryConfig(unittest.TestCase):
    """Test ActiveDirectoryConfig dataclass"""

    def test_default_values(self):
        config = ActiveDirectoryConfig()
        self.assertIsNone(config.directory_id)
        self.assertEqual(config.domain_name, "")
        self.assertEqual(config.net_bios_name, "")
        self.assertIsNone(config.file_system_administrators_group)
        self.assertIsNone(config.organizational_unit_distinguished_name)
        self.assertEqual(config.dns_ips, [])
        self.assertIsNone(config.backup_directory_id)
        self.assertIsNone(config.replication_secret_arn)
        self.assertEqual(config.time_offset_in_seconds, 0)

    def test_custom_values(self):
        config = ActiveDirectoryConfig(
            domain_name="example.com",
            net_bios_name="FSX",
            dns_ips=["10.0.0.1", "10.0.0.2"],
            organizational_unit_distinguished_name="OU=Computers,DC=example,DC=com"
        )
        self.assertEqual(config.domain_name, "example.com")
        self.assertEqual(config.net_bios_name, "FSX")
        self.assertEqual(len(config.dns_ips), 2)
        self.assertEqual(config.organizational_unit_distinguished_name, "OU=Computers,DC=example,DC=com")


class TestStorageVirtualMachineConfig(unittest.TestCase):
    """Test StorageVirtualMachineConfig dataclass"""

    def test_default_values(self):
        config = StorageVirtualMachineConfig()
        self.assertIsNone(config.svm_id)
        self.assertEqual(config.file_system_id, "")
        self.assertEqual(config.name, "")
        self.assertIsNone(config.ad_domain_membership)
        self.assertIsNone(config.f_policy)
        self.assertEqual(config.svm_root_volume_security_style, "UNIX")
        self.assertIsNone(config.backup_config)
        self.assertIsNone(config.uuid)
        self.assertIsNone(config.endpoints)
        self.assertIsNone(config.created_at)
        self.assertIsNone(config.resource_arn)

    def test_custom_values(self):
        config = StorageVirtualMachineConfig(
            file_system_id="fs-12345678",
            name="svm1",
            svm_root_volume_security_style="NTFS",
            endpoints={"nfs": {"dnsName": "svm1.example.com"}}
        )
        self.assertEqual(config.file_system_id, "fs-12345678")
        self.assertEqual(config.name, "svm1")
        self.assertEqual(config.svm_root_volume_security_style, "NTFS")
        self.assertEqual(config.endpoints["nfs"]["dnsName"], "svm1.example.com")


class TestVolumeConfig(unittest.TestCase):
    """Test VolumeConfig dataclass"""

    def test_default_values(self):
        config = VolumeConfig()
        self.assertIsNone(config.volume_id)
        self.assertEqual(config.name, "")
        self.assertEqual(config.file_system_id, "")
        self.assertEqual(config.svm_id, "")
        self.assertEqual(config.size_in_megabytes, 1048576)
        self.assertEqual(config.security_style, "UNIX")
        self.assertEqual(config.storage_efficiency_enabled, True)
        self.assertIsNone(config.junction_path)
        self.assertIsNone(config.aggregate_name)
        self.assertIsNone(config.storage_virtual_machine_id)
        self.assertEqual(config.copy_tags_to_backups, False)
        self.assertEqual(config.volume_style, "FLEXVOL")
        self.assertIsNone(config.tiering_policy)
        self.assertIsNone(config.qos_policy_group_id)
        self.assertIsNone(config.snapshot_policy)
        self.assertEqual(config.root_volume, False)

    def test_custom_values(self):
        config = VolumeConfig(
            name="volume1",
            file_system_id="fs-12345678",
            svm_id="svm-123",
            size_in_megabytes=2097152,
            security_style="NTFS",
            junction_path="/volume1"
        )
        self.assertEqual(config.name, "volume1")
        self.assertEqual(config.file_system_id, "fs-12345678")
        self.assertEqual(config.svm_id, "svm-123")
        self.assertEqual(config.size_in_megabytes, 2097152)
        self.assertEqual(config.security_style, "NTFS")
        self.assertEqual(config.junction_path, "/volume1")


class TestCloudWatchConfig(unittest.TestCase):
    """Test CloudWatchConfig dataclass"""

    def test_default_values(self):
        config = CloudWatchConfig()
        self.assertEqual(config.enable_performance_metrics, True)
        self.assertEqual(config.enable阿里云_logging, False)
        self.assertIsNone(config.log_group_name)
        self.assertEqual(config.metrics_interval_minutes, 5)
        self.assertEqual(config.resource_ids, [])

    def test_custom_values(self):
        config = CloudWatchConfig(
            enable_performance_metrics=True,
            log_group_name="/aws/fsx/mylustre",
            metrics_interval_minutes=1,
            resource_ids=["fs-12345678"]
        )
        self.assertEqual(config.enable_performance_metrics, True)
        self.assertEqual(config.log_group_name, "/aws/fsx/mylustre")
        self.assertEqual(config.metrics_interval_minutes, 1)
        self.assertEqual(len(config.resource_ids), 1)


class TestFSxIntegrationInit(unittest.TestCase):
    """Test FSxIntegration initialization"""

    def test_default_init(self):
        integration = FSxIntegration()
        self.assertEqual(integration.region, "us-east-1")
        self.assertIsNone(integration.profile)
        self.assertIsNone(integration._session)
        self.assertIsNone(integration._client)
        self.assertIsNone(integration._resource)
        self.assertEqual(integration._windows_config, {})
        self.assertEqual(integration._lustre_config, {})
        self.assertEqual(integration._ontap_config, {})

    def test_custom_region(self):
        integration = FSxIntegration(region="us-west-2")
        self.assertEqual(integration.region, "us-west-2")

    def test_custom_profile(self):
        integration = FSxIntegration(profile="my-profile")
        self.assertEqual(integration.profile, "my-profile")

    def test_boto_session(self):
        mock_session = MagicMock()
        integration = FSxIntegration(boto_session=mock_session)
        self.assertEqual(integration._session, mock_session)


class TestFSxIntegrationWindows(unittest.TestCase):
    """Test FSxIntegration Windows file system methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_windows_file_system(self):
        config = FSxWindowsConfig(
            storage_capacity_gb=100,
            subnet_ids=["subnet-1"],
            throughput_capacity_mbps=64
        )
        self.mock_client.create_file_system.return_value = {
            "FileSystem": {
                "FileSystemId": "fs-12345678",
                "FileSystemType": "WINDOWS",
                "StorageCapacity": 100
            }
        }

        result = self.integration.create_windows_file_system(config)

        self.assertEqual(result["FileSystemId"], "fs-12345678")
        self.assertEqual(result["FileSystemType"], "WINDOWS")
        self.assertIn("fs-12345678", self.integration._windows_config)

    def test_describe_windows_file_system(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-12345678",
                "FileSystemType": "WINDOWS"
            }]
        }

        result = self.integration.describe_windows_file_system("fs-12345678")

        self.assertEqual(result["FileSystems"][0]["FileSystemId"], "fs-12345678")

    def test_update_windows_file_system(self):
        self.mock_client.update_file_system.return_value = {
            "FileSystem": {
                "FileSystemId": "fs-12345678",
                "ThroughputCapacity": 128
            }
        }

        result = self.integration.update_windows_file_system(
            "fs-12345678",
            ThroughputCapacity=128
        )

        self.assertEqual(result["FileSystem"]["ThroughputCapacity"], 128)

    def test_delete_windows_file_system(self):
        self.mock_client.delete_file_system.return_value = {}

        result = self.integration.delete_windows_file_system("fs-12345678")

        self.assertEqual(result, {})


class TestFSxIntegrationLustre(unittest.TestCase):
    """Test FSxIntegration Lustre file system methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_lustre_file_system(self):
        config = FSxLustreConfig(
            storage_capacity_gb=1200,
            subnet_ids=["subnet-1"],
            per_unit_storage_throughput=50
        )
        self.mock_client.create_file_system.return_value = {
            "FileSystem": {
                "FileSystemId": "fs-87654321",
                "FileSystemType": "LUSTRE"
            }
        }

        result = self.integration.create_lustre_file_system(config)

        self.assertEqual(result["FileSystemId"], "fs-87654321")
        self.assertEqual(result["FileSystemType"], "LUSTRE")
        self.assertIn("fs-87654321", self.integration._lustre_config)

    def test_describe_lustre_file_system(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-87654321",
                "FileSystemType": "LUSTRE"
            }]
        }

        result = self.integration.describe_lustre_file_system("fs-87654321")

        self.assertEqual(result["FileSystems"][0]["FileSystemId"], "fs-87654321")

    def test_update_lustre_file_system(self):
        self.mock_client.update_file_system.return_value = {
            "FileSystem": {"FileSystemId": "fs-87654321"}
        }

        result = self.integration.update_lustre_file_system(
            "fs-87654321",
            StorageCapacity=2400
        )

        self.assertEqual(result["FileSystem"]["FileSystemId"], "fs-87654321")

    def test_delete_lustre_file_system(self):
        self.mock_client.delete_file_system.return_value = {}

        result = self.integration.delete_lustre_file_system("fs-87654321")

        self.assertEqual(result, {})


class TestFSxIntegrationOpenZFS(unittest.TestCase):
    """Test FSxIntegration OpenZFS file system methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_openzfs_file_system(self):
        config = FSxOpenZFSConfig(
            storage_capacity_gb=64,
            subnet_ids=["subnet-1"]
        )
        self.mock_client.create_file_system.return_value = {
            "FileSystem": {
                "FileSystemId": "fs-openzfs-1",
                "FileSystemType": "OPEN_ZFS"
            }
        }

        result = self.integration.create_openzfs_file_system(config)

        self.assertEqual(result["FileSystemId"], "fs-openzfs-1")
        self.assertEqual(result["FileSystemType"], "OPEN_ZFS")

    def test_describe_openzfs_file_system(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-openzfs-1",
                "FileSystemType": "OPEN_ZFS"
            }]
        }

        result = self.integration.describe_openzfs_file_system("fs-openzfs-1")

        self.assertEqual(result["FileSystems"][0]["FileSystemId"], "fs-openzfs-1")

    def test_delete_openzfs_file_system(self):
        self.mock_client.delete_file_system.return_value = {}

        result = self.integration.delete_openzfs_file_system("fs-openzfs-1")

        self.assertEqual(result, {})


class TestFSxIntegrationNetAppONTAP(unittest.TestCase):
    """Test FSxIntegration NetApp ONTAP file system methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_netapp_ontap_file_system(self):
        config = FSxNetAppONTAPConfig(
            storage_capacity_gb=1024,
            subnet_ids=["subnet-1"]
        )
        self.mock_client.create_file_system.return_value = {
            "FileSystem": {
                "FileSystemId": "fs-ontap-1",
                "FileSystemType": "NETAPP_ONTAP"
            }
        }

        result = self.integration.create_netapp_ontap_file_system(config)

        self.assertEqual(result["FileSystemId"], "fs-ontap-1")
        self.assertEqual(result["FileSystemType"], "NETAPP_ONTAP")

    def test_describe_netapp_ontap_file_system(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-ontap-1",
                "FileSystemType": "NETAPP_ONTAP"
            }]
        }

        result = self.integration.describe_netapp_ontap_file_system("fs-ontap-1")

        self.assertEqual(result["FileSystems"][0]["FileSystemId"], "fs-ontap-1")

    def test_delete_netapp_ontap_file_system(self):
        self.mock_client.delete_file_system.return_value = {}

        result = self.integration.delete_netapp_ontap_file_system("fs-ontap-1")

        self.assertEqual(result, {})


class TestFSxIntegrationDataRepositoryAssociations(unittest.TestCase):
    """Test FSxIntegration data repository association methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_data_repository_association(self):
        config = DataRepositoryAssociationConfig(
            file_system_id="fs-12345678",
            file_system_path="/data",
            data_repository_path="s3://my-bucket/data"
        )
        self.mock_client.create_data_repository_association.return_value = {
            "DataRepositoryAssociation": {
                "AssociationId": "dra-12345678"
            }
        }

        result = self.integration.create_data_repository_association(config)

        self.assertEqual(result["AssociationId"], "dra-12345678")

    def test_list_data_repository_associations(self):
        self.mock_client.describe_data_repository_associations.return_value = {
            "Associations": [{"AssociationId": "dra-1"}]
        }

        result = self.integration.list_data_repository_associations()

        self.assertEqual(len(result.get("Associations", [])), 1)

    def test_delete_data_repository_association(self):
        self.mock_client.delete_data_repository_association.return_value = {}

        result = self.integration.delete_data_repository_association("dra-12345678")

        self.assertEqual(result, {})


class TestFSxIntegrationBackups(unittest.TestCase):
    """Test FSxIntegration backup methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_backup(self):
        config = BackupConfig(file_system_id="fs-12345678")
        self.mock_client.create_backup.return_value = {
            "Backup": {
                "BackupId": "backup-12345678"
            }
        }

        result = self.integration.create_backup(config)

        self.assertEqual(result["BackupId"], "backup-12345678")

    def test_describe_backup(self):
        self.mock_client.describe_backups.return_value = {
            "Backups": [{"BackupId": "backup-12345678"}]
        }

        result = self.integration.describe_backup("backup-12345678")

        self.assertEqual(result["Backups"][0]["BackupId"], "backup-12345678")

    def test_list_backups(self):
        self.mock_client.describe_backups.return_value = {
            "Backups": [{"BackupId": "backup-1"}, {"BackupId": "backup-2"}]
        }

        result = self.integration.list_backups()

        self.assertEqual(len(result.get("Backups", [])), 2)

    def test_delete_backup(self):
        self.mock_client.delete_backup.return_value = {}

        result = self.integration.delete_backup("backup-12345678")

        self.assertEqual(result, {})


class TestFSxIntegrationActiveDirectory(unittest.TestCase):
    """Test FSxIntegration Active Directory methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_join_active_directory(self):
        self.mock_client.update_file_system.return_value = {
            "FileSystem": {"FileSystemId": "fs-12345678"}
        }

        result = self.integration.join_active_directory(
            "fs-12345678",
            "d-123456789",
            "example.com"
        )

        self.assertEqual(result["FileSystem"]["FileSystemId"], "fs-12345678")

    def test_disjoin_active_directory(self):
        self.mock_client.update_file_system.return_value = {
            "FileSystem": {"FileSystemId": "fs-12345678"}
        }

        result = self.integration.disjoin_active_directory("fs-12345678")

        self.assertEqual(result["FileSystem"]["FileSystemId"], "fs-12345678")

    def test_describe_active_directory(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-12345678",
                "WindowsConfiguration": {
                    "ActiveDirectoryId": "d-123456789"
                }
            }]
        }

        result = self.integration.describe_active_directory("fs-12345678")

        self.assertEqual(result["FileSystems"][0]["FileSystemId"], "fs-12345678")


class TestFSxIntegrationStorageVirtualMachines(unittest.TestCase):
    """Test FSxIntegration storage virtual machine methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_storage_virtual_machine(self):
        config = StorageVirtualMachineConfig(
            file_system_id="fs-12345678",
            name="svm1"
        )
        self.mock_client.create_storage_virtual_machine.return_value = {
            "StorageVirtualMachine": {
                "StorageVirtualMachineId": "svm-12345678"
            }
        }

        result = self.integration.create_storage_virtual_machine(config)

        self.assertEqual(result["StorageVirtualMachineId"], "svm-12345678")

    def test_describe_storage_virtual_machine(self):
        self.mock_client.describe_storage_virtual_machines.return_value = {
            "StorageVirtualMachines": [{"StorageVirtualMachineId": "svm-12345678"}]
        }

        result = self.integration.describe_storage_virtual_machine("svm-12345678")

        self.assertEqual(result["StorageVirtualMachines"][0]["StorageVirtualMachineId"], "svm-12345678")

    def test_list_storage_virtual_machines(self):
        self.mock_client.describe_storage_virtual_machines.return_value = {
            "StorageVirtualMachines": [{"StorageVirtualMachineId": "svm-1"}]
        }

        result = self.integration.list_storage_virtual_machines()

        self.assertEqual(len(result.get("StorageVirtualMachines", [])), 1)

    def test_delete_storage_virtual_machine(self):
        self.mock_client.delete_storage_virtual_machine.return_value = {}

        result = self.integration.delete_storage_virtual_machine("svm-12345678")

        self.assertEqual(result, {})


class TestFSxIntegrationVolumes(unittest.TestCase):
    """Test FSxIntegration volume methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_create_volume(self):
        config = VolumeConfig(
            name="volume1",
            file_system_id="fs-12345678",
            svm_id="svm-12345678"
        )
        self.mock_client.create_volume.return_value = {
            "Volume": {
                "VolumeId": "vol-12345678"
            }
        }

        result = self.integration.create_volume(config)

        self.assertEqual(result["VolumeId"], "vol-12345678")

    def test_describe_volume(self):
        self.mock_client.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-12345678"}]
        }

        result = self.integration.describe_volume("vol-12345678")

        self.assertEqual(result["Volumes"][0]["VolumeId"], "vol-12345678")

    def test_list_volumes(self):
        self.mock_client.describe_volumes.return_value = {
            "Volumes": [{"VolumeId": "vol-1"}]
        }

        result = self.integration.list_volumes()

        self.assertEqual(len(result.get("Volumes", [])), 1)

    def test_delete_volume(self):
        self.mock_client.delete_volume.return_value = {}

        result = self.integration.delete_volume("vol-12345678")

        self.assertEqual(result, {})


class TestFSxIntegrationCloudWatch(unittest.TestCase):
    """Test FSxIntegration CloudWatch monitoring methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_session.client.side_effect = lambda service, **kwargs: {
            "fsx": self.mock_client,
            "cloudwatch": self.mock_cw_client
        }.get(service, MagicMock())
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_configure_cloudwatch_monitoring(self):
        config = CloudWatchConfig(
            enable_performance_metrics=True,
            metrics_interval_minutes=5
        )

        result = self.integration.configure_cloudwatch_monitoring(config)

        self.assertTrue(result.get("performance_metrics_enabled"))
        self.assertEqual(result.get("metrics_interval_minutes"), 5)

    def test_get_cloudwatch_metrics(self):
        self.mock_cw_client.get_metric_statistics.return_value = {
            "Datapoints": [{"Average": 45.5}]
        }

        result = self.integration.get_cloudwatch_metrics("fs-12345678")

        self.assertIn("StorageCapacity", result)


class TestFSxIntegrationUtility(unittest.TestCase):
    """Test FSxIntegration utility methods"""

    def setUp(self):
        self.mock_session = MagicMock()
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client
        self.integration = FSxIntegration(boto_session=self.mock_session)

    def test_list_file_systems(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{"FileSystemId": "fs-1"}]
        }

        result = self.integration.list_file_systems()

        self.assertEqual(len(result.get("FileSystems", [])), 1)

    def test_get_file_system_health_status(self):
        self.mock_client.describe_file_systems.return_value = {
            "FileSystems": [{
                "FileSystemId": "fs-12345678",
                "Lifecycle": "AVAILABLE",
                "HealthCheck": {"Status": "PASS"},
                "FailureDetails": {}
            }]
        }

        result = self.integration.get_file_system_health_status("fs-12345678")

        self.assertEqual(result["file_system_id"], "fs-12345678")
        self.assertEqual(result["lifecycle"], "AVAILABLE")

    def test_tag_resource(self):
        self.mock_client.tag_resource.return_value = {}

        result = self.integration.tag_resource(
            "arn:aws:fsx:us-east-1:123456789012:volume/fs-12345678/vol-12345678",
            {"env": "prod"}
        )

        self.assertEqual(result, {})

    def test_untag_resource(self):
        self.mock_client.untag_resource.return_value = {}

        result = self.integration.untag_resource(
            "arn:aws:fsx:us-east-1:123456789012:volume/fs-12345678/vol-12345678",
            ["env"]
        )

        self.assertEqual(result, {})

    def test_get_config_summary(self):
        result = self.integration.get_config_summary()

        self.assertIn("region", result)
        self.assertIn("windows_file_systems", result)
        self.assertIn("lustre_file_systems", result)


if __name__ == "__main__":
    unittest.main()
