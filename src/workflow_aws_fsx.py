"""AWS FSx file system integration workflow.

This module provides an FSxIntegration class for managing AWS FSx file systems,
including Windows, Lustre, OpenZFS, NetApp ONTAP, data repository associations,
backups, Active Directory integration, storage virtual machines, volumes, and
CloudWatch monitoring.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class FSxFileSystemType(str, Enum):
    """Supported FSx file system types."""

    WINDOWS = "WINDOWS"
    LUSTRE = "LUSTRE"
    OPEN_ZFS = "OPEN_ZFS"
    NETAPP_ONTAP = "NETAPP_ONTAP"


class FSxWindowsDeploymentType(str, Enum):
    """FSx for Windows deployment types."""

    SINGLE_AZ = "SINGLE_AZ_1"
    SINGLE_AZ_2 = "SINGLE_AZ_2"
    MULTI_AZ_1 = "MULTI_AZ_1"
    MULTI_AZ_2 = "MULTI_AZ_2"


class FSxLustreDeploymentType(str, Enum):
    """FSx for Lustre deployment types."""

    PERSISTENT_1 = "PERSISTENT_1"
    PERSISTENT_2 = "PERSISTENT_2"
    SCRATCH_1 = "SCRATCH_1"
    SCRATCH_2 = "SCRATCH_2"


class FSxOpenZFSDeploymentType(str, Enum):
    """FSx for OpenZFS deployment types."""

    SINGLE_AZ = "SINGLE_AZ_1"
    MULTI_AZ = "MULTI_AZ_1"


class FSxNetAppONTAPDeploymentType(str, Enum):
    """FSx for NetApp ONTAP deployment types."""

    SINGLE_AZ = "SINGLE_AZ_1"
    MULTI_AZ = "MULTI_AZ_1"


class DataRepositoryAssociationState(str, Enum):
    """Data repository association lifecycle states."""

    CREATING = "CREATING"
    AVAILABLE = "AVAILABLE"
    MISCONFIGURED = "MISCONFIGURED"
    DELETING = "DELETING"
    FAILED = "FAILED"


@dataclass
class FSxWindowsConfig:
    """Configuration for FSx for Windows file server."""

    file_system_id: Optional[str] = None
    deployment_type: FSxWindowsDeploymentType = FSxWindowsDeploymentType.SINGLE_AZ
    storage_capacity_gb: int = 32
    storage_type: str = "SSD"
    throughput_capacity_mbps: int = 8
    preferred_subnet_id: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    subnet_ids: List[str] = field(default_factory=list)
    skip_final_backup: bool = False
    final_backup_tags: Optional[Dict[str, str]] = None
    active_directory_id: Optional[str] = None
    self_managed_active_directory: Optional[Dict[str, Any]] = None
    copy_tags_to_backups: bool = False
    backup_id: Optional[str] = None
    kms_key_id: Optional[str] = None
    automatic_backup_retention_days: int = 0
    daily_automatic_backup_start_time: Optional[str] = None
    weekly_maintenance_start_time: Optional[str] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class FSxLustreConfig:
    """Configuration for FSx for Lustre file system."""

    file_system_id: Optional[str] = None
    deployment_type: FSxLustreDeploymentType = FSxLustreDeploymentType.SCRATCH_2
    storage_capacity_gb: int = 1200
    storage_type: str = "SSD"
    per_unit_storage_throughput: int = 50
    subnet_ids: List[str] = field(default_factory=list)
    security_group_ids: List[str] = field(default_factory=list)
    data_repository_associations: List[str] = field(default_factory=list)
    auto_import_policy: Optional[str] = None
    export_path: Optional[str] = None
    imported_file_chunk_size_mib: int = 1024
    kms_key_id: Optional[str] = None
    weekly_maintenance_start_time: Optional[str] = None
    drive_cache_type: Optional[str] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class FSxOpenZFSConfig:
    """Configuration for FSx for OpenZFS file system."""

    file_system_id: Optional[str] = None
    deployment_type: FSxOpenZFSDeploymentType = FSxOpenZFSDeploymentType.SINGLE_AZ
    storage_capacity_gb: int = 64
    storage_type: str = "SSD"
    throughput_capacity_mbps: int = 8
    subnet_ids: List[str] = field(default_factory=list)
    preferred_subnet_id: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    root_volume_configuration: Optional[Dict[str, Any]] = None
    zfs_config: Optional[Dict[str, Any]] = None
    copy_tags_to_backups: bool = False
    backup_id: Optional[str] = None
    kms_key_id: Optional[str] = None
    automatic_backup_retention_days: int = 0
    daily_automatic_backup_start_time: Optional[str] = None
    weekly_maintenance_start_time: Optional[str] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class FSxNetAppONTAPConfig:
    """Configuration for FSx for NetApp ONTAP file system."""

    file_system_id: Optional[str] = None
    deployment_type: FSxNetAppONTAPDeploymentType = FSxNetAppONTAPDeploymentType.SINGLE_AZ
    storage_capacity_gb: int = 1024
    storage_type: str = "SSD"
    throughput_capacity_mbps: int = 128
    subnet_ids: List[str] = field(default_factory=list)
    preferred_subnet_id: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    fsx_admin_password: Optional[str] = None
    active_directory: Optional[Dict[str, Any]] = None
    backup_id: Optional[str] = None
    kms_key_id: Optional[str] = None
    automatic_backup_retention_days: int = 0
    daily_automatic_backup_start_time: Optional[str] = None
    weekly_maintenance_start_time: Optional[str] = None
    endpoint_ip_address_range: Optional[str] = None
    route_table_ids: List[str] = field(default_factory=list)
    copy_tags_to_backups: bool = False
    tags: Optional[Dict[str, str]] = None


@dataclass
class DataRepositoryAssociationConfig:
    """Configuration for a data repository association."""

    association_id: Optional[str] = None
    file_system_id: str = ""
    file_system_path: str = ""
    data_repository_path: str = ""
    batch_import_meta_data: Optional[str] = None
    delete_data_in_filesystem: bool = False
    import_metadata_on_creation: bool = True
    new_capacity: int = 0


@dataclass
class BackupConfig:
    """Configuration for FSx backups."""

    backup_id: Optional[str] = None
    volume_id: Optional[str] = None
    file_system_id: Optional[str] = None
    backup_type: str = "AUTO"
    tags: Optional[Dict[str, str]] = None
    kms_key_id: Optional[str] = None
    retention_period_days: int = 30


@dataclass
class ActiveDirectoryConfig:
    """Configuration for Active Directory integration."""

    directory_id: Optional[str] = None
    domain_name: str = ""
    net_bios_name: str = ""
    file_system_administrators_group: Optional[str] = None
    organizational_unit_distinguished_name: Optional[str] = None
    dns_ips: List[str] = field(default_factory=list)
    backup_directory_id: Optional[str] = None
    replication_secret_arn: Optional[str] = None
    time_offset_in_seconds: int = 0


@dataclass
class StorageVirtualMachineConfig:
    """Configuration for ONTAP storage virtual machines."""

    svm_id: Optional[str] = None
    file_system_id: str = ""
    name: str = ""
    ad_domain_membership: Optional[Dict[str, Any]] = None
    f_policy: Optional[Dict[str, Any]] = None
    svm_root_volume_security_style: str = "UNIX"
    backup_config: Optional[Dict[str, Any]] = None
    uuid: Optional[str] = None
    endpoints: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    resource_arn: Optional[str] = None


@dataclass
class VolumeConfig:
    """Configuration for ONTAP volumes."""

    volume_id: Optional[str] = None
    name: str = ""
    file_system_id: str = ""
    svm_id: str = ""
    size_in_megabytes: int = 1048576
    security_style: str = "UNIX"
    storage_efficiency_enabled: bool = True
    junction_path: Optional[str] = None
    aggregate_name: Optional[str] = None
    storage_virtual_machine_id: Optional[str] = None
    copy_tags_to_backups: bool = False
    volume_style: str = "FLEXVOL"
    tiering_policy: Optional[Dict[str, Any]] = None
    qos_policy_group_id: Optional[str] = None
    snapshot_policy: Optional[str] = None
    root_volume: bool = False


@dataclass
class CloudWatchConfig:
    """Configuration for CloudWatch monitoring."""

    enable_performance_metrics: bool = True
    enable阿里云_logging: bool = False
    log_group_name: Optional[str] = None
    metrics_interval_minutes: int = 5
    resource_ids: List[str] = field(default_factory=list)


class FSxIntegration:
    """AWS FSx integration class for managing file systems and related resources.

    Supports FSx for Windows, FSx for Lustre, FSx for OpenZFS, and FSx for NetApp ONTAP.
    Provides methods for managing file systems, data repository associations, backups,
    Active Directory integration, storage virtual machines, volumes, and CloudWatch monitoring.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        profile: Optional[str] = None,
        boto_session: Optional[Any] = None,
    ):
        """Initialize the FSx integration.

        Args:
            region: AWS region for FSx operations.
            profile: AWS profile name for boto3 session.
            boto_session: Pre-configured boto3 session (overrides profile).
        """
        self.region = region
        self.profile = profile
        self._session = boto_session
        self._client = None
        self._resource = None
        self._windows_config: Dict[str, FSxWindowsConfig] = {}
        self._lustre_config: Dict[str, FSxLustreConfig] = {}
        self._openzfs_config: Dict[str, FSxOpenZFSConfig] = {}
        self._ontap_config: Dict[str, FSxNetAppONTAPConfig] = {}
        self._dra_configs: Dict[str, DataRepositoryAssociationConfig] = {}
        self._backup_configs: Dict[str, BackupConfig] = {}
        self._ad_configs: Dict[str, ActiveDirectoryConfig] = {}
        self._svm_configs: Dict[str, StorageVirtualMachineConfig] = {}
        self._volume_configs: Dict[str, VolumeConfig] = {}
        self._cloudwatch_config: Optional[CloudWatchConfig] = None

    @property
    def client(self) -> Any:
        """Get or create the FSx client lazily."""
        if self._client is None:
            import boto3

            if self._session:
                self._client = self._session.client("fsx", region_name=self.region)
            else:
                self._client = boto3.Session(
                    profile_name=self.profile, region_name=self.region
                ).client("fsx")
        return self._client

    @property
    def resource(self) -> Any:
        """Get or create the FSx resource lazily."""
        if self._resource is None:
            import boto3

            if self._session:
                self._resource = self._session.resource("fsx", region_name=self.region)
            else:
                self._resource = boto3.Session(
                    profile_name=self.profile, region_name=self.region
                ).resource("fsx")
        return self._resource

    # ========================================================================
    # FSx for Windows
    # ========================================================================

    def create_windows_file_system(
        self, config: FSxWindowsConfig
    ) -> Dict[str, Any]:
        """Create an FSx for Windows file server.

        Args:
            config: Windows file system configuration.

        Returns:
            Created file system details.
        """
        logger.info("Creating FSx for Windows file system in region %s", self.region)

        kwargs: Dict[str, Any] = {
            "FileSystemType": FSxFileSystemType.WINDOWS.value,
            "StorageCapacity": config.storage_capacity_gb,
            "StorageType": config.storage_type,
            "SubnetIds": config.subnet_ids,
            "WindowsConfiguration": {
                "DeploymentType": config.deployment_type.value,
                "ThroughputCapacity": config.throughput_capacity_mbps,
            },
        }

        if config.security_group_ids:
            kwargs["SecurityGroupIds"] = config.security_group_ids
        if config.preferred_subnet_id:
            kwargs["WindowsConfiguration"]["PreferredSubnetId"] = (
                config.preferred_subnet_id
            )
        if config.skip_final_backup:
            kwargs["WindowsConfiguration"]["SkipFinalBackup"] = True
        if config.copy_tags_to_backups:
            kwargs["WindowsConfiguration"]["CopyTagsToBackups"] = True
        if config.active_directory_id:
            kwargs["WindowsConfiguration"]["ActiveDirectoryId"] = (
                config.active_directory_id
            )
        if config.self_managed_active_directory:
            kwargs["WindowsConfiguration"][
                "SelfManagedActiveDirectoryConfiguration"
            ] = config.self_managed_active_directory
        if config.kms_key_id:
            kwargs["KmsKeyId"] = config.kms_key_id
        if config.automatic_backup_retention_days > 0:
            kwargs["WindowsConfiguration"][
                "AutomaticBackupRetentionDays"
            ] = config.automatic_backup_retention_days
        if config.daily_automatic_backup_start_time:
            kwargs["WindowsConfiguration"][
                "DailyAutomaticBackupStartTime"
            ] = config.daily_automatic_backup_start_time
        if config.weekly_maintenance_start_time:
            kwargs["WindowsConfiguration"][
                "WeeklyMaintenanceStartTime"
            ] = config.weekly_maintenance_start_time
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]

        response = self.client.create_file_system(**kwargs)
        fs = response.get("FileSystem", {})
        fs_id = fs.get("FileSystemId", "")
        self._windows_config[fs_id] = config
        self._windows_config[fs_id].file_system_id = fs_id
        logger.info("Created FSx for Windows file system: %s", fs_id)
        return fs

    def describe_windows_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Describe an FSx for Windows file system.

        Args:
            file_system_id: ID of the file system to describe.

        Returns:
            File system details.
        """
        logger.info("Describing FSx for Windows file system: %s", file_system_id)
        return self.client.describe_file_systems(FileSystemIds=[file_system_id])

    def update_windows_file_system(
        self, file_system_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update an FSx for Windows file system.

        Args:
            file_system_id: ID of the file system to update.
            **kwargs: Update parameters (ThroughputCapacity, etc.).

        Returns:
            Updated file system details.
        """
        logger.info("Updating FSx for Windows file system: %s", file_system_id)
        kwargs["FileSystemId"] = file_system_id
        return self.client.update_file_system(**kwargs)

    def delete_windows_file_system(
        self, file_system_id: str, skip_final_backup: bool = False
    ) -> Dict[str, Any]:
        """Delete an FSx for Windows file system.

        Args:
            file_system_id: ID of the file system to delete.
            skip_final_backup: Whether to skip final backup.

        Returns:
            Deletion result.
        """
        logger.info("Deleting FSx for Windows file system: %s", file_system_id)
        kwargs: Dict[str, Any] = {"FileSystemId": file_system_id}
        if skip_final_backup:
            kwargs["SkipFinalBackup"] = True
        return self.client.delete_file_system(**kwargs)

    # ========================================================================
    # FSx for Lustre
    # ========================================================================

    def create_lustre_file_system(self, config: FSxLustreConfig) -> Dict[str, Any]:
        """Create an FSx for Lustre file system.

        Args:
            config: Lustre file system configuration.

        Returns:
            Created file system details.
        """
        logger.info("Creating FSx for Lustre file system in region %s", self.region)

        kwargs: Dict[str, Any] = {
            "FileSystemType": FSxFileSystemType.LUSTRE.value,
            "StorageCapacity": config.storage_capacity_gb,
            "StorageType": config.storage_type,
            "SubnetIds": config.subnet_ids,
            "LustreConfiguration": {
                "DeploymentType": config.deployment_type.value,
                "PerUnitStorageThroughput": config.per_unit_storage_throughput,
            },
        }

        if config.security_group_ids:
            kwargs["SecurityGroupIds"] = config.security_group_ids
        if config.data_repository_associations:
            kwargs["LustreConfiguration"][
                "DataRepositoryAssociations"
            ] = config.data_repository_associations
        if config.auto_import_policy:
            kwargs["LustreConfiguration"]["AutoImportPolicy"] = (
                config.auto_import_policy
            )
        if config.export_path:
            kwargs["LustreConfiguration"]["ExportPath"] = config.export_path
        if config.imported_file_chunk_size_mib:
            kwargs["LustreConfiguration"][
                "ImportedFileChunkSize"
            ] = config.imported_file_chunk_size_mib
        if config.kms_key_id:
            kwargs["KmsKeyId"] = config.kms_key_id
        if config.weekly_maintenance_start_time:
            kwargs["LustreConfiguration"][
                "WeeklyMaintenanceStartTime"
            ] = config.weekly_maintenance_start_time
        if config.drive_cache_type:
            kwargs["LustreConfiguration"]["DriveCacheType"] = config.drive_cache_type
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]

        response = self.client.create_file_system(**kwargs)
        fs = response.get("FileSystem", {})
        fs_id = fs.get("FileSystemId", "")
        self._lustre_config[fs_id] = config
        self._lustre_config[fs_id].file_system_id = fs_id
        logger.info("Created FSx for Lustre file system: %s", fs_id)
        return fs

    def describe_lustre_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Describe an FSx for Lustre file system.

        Args:
            file_system_id: ID of the file system to describe.

        Returns:
            File system details.
        """
        logger.info("Describing FSx for Lustre file system: %s", file_system_id)
        return self.client.describe_file_systems(FileSystemIds=[file_system_id])

    def update_lustre_file_system(
        self, file_system_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update an FSx for Lustre file system.

        Args:
            file_system_id: ID of the file system to update.
            **kwargs: Update parameters.

        Returns:
            Updated file system details.
        """
        logger.info("Updating FSx for Lustre file system: %s", file_system_id)
        kwargs["FileSystemId"] = file_system_id
        return self.client.update_file_system(**kwargs)

    def delete_lustre_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Delete an FSx for Lustre file system.

        Args:
            file_system_id: ID of the file system to delete.

        Returns:
            Deletion result.
        """
        logger.info("Deleting FSx for Lustre file system: %s", file_system_id)
        return self.client.delete_file_system(FileSystemId=file_system_id)

    # ========================================================================
    # FSx for OpenZFS
    # ========================================================================

    def create_openzfs_file_system(self, config: FSxOpenZFSConfig) -> Dict[str, Any]:
        """Create an FSx for OpenZFS file system.

        Args:
            config: OpenZFS file system configuration.

        Returns:
            Created file system details.
        """
        logger.info("Creating FSx for OpenZFS file system in region %s", self.region)

        kwargs: Dict[str, Any] = {
            "FileSystemType": FSxFileSystemType.OPEN_ZFS.value,
            "StorageCapacity": config.storage_capacity_gb,
            "StorageType": config.storage_type,
            "SubnetIds": config.subnet_ids,
            "OpenZFSConfiguration": {
                "DeploymentType": config.deployment_type.value,
                "ThroughputCapacity": config.throughput_capacity_mbps,
            },
        }

        if config.security_group_ids:
            kwargs["SecurityGroupIds"] = config.security_group_ids
        if config.preferred_subnet_id:
            kwargs["OpenZFSConfiguration"]["PreferredSubnetId"] = (
                config.preferred_subnet_id
            )
        if config.root_volume_configuration:
            kwargs["OpenZFSConfiguration"]["RootVolumeConfiguration"] = config.root_volume_configuration
        if config.copy_tags_to_backups:
            kwargs["OpenZFSConfiguration"]["CopyTagsToBackups"] = True
        if config.backup_id:
            kwargs["OpenZFSConfiguration"]["BackupId"] = config.backup_id
        if config.kms_key_id:
            kwargs["KmsKeyId"] = config.kms_key_id
        if config.automatic_backup_retention_days > 0:
            kwargs["OpenZFSConfiguration"][
                "AutomaticBackupRetentionDays"
            ] = config.automatic_backup_retention_days
        if config.daily_automatic_backup_start_time:
            kwargs["OpenZFSConfiguration"][
                "DailyAutomaticBackupStartTime"
            ] = config.daily_automatic_backup_start_time
        if config.weekly_maintenance_start_time:
            kwargs["OpenZFSConfiguration"][
                "WeeklyMaintenanceStartTime"
            ] = config.weekly_maintenance_start_time
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]

        response = self.client.create_file_system(**kwargs)
        fs = response.get("FileSystem", {})
        fs_id = fs.get("FileSystemId", "")
        self._openzfs_config[fs_id] = config
        self._openzfs_config[fs_id].file_system_id = fs_id
        logger.info("Created FSx for OpenZFS file system: %s", fs_id)
        return fs

    def describe_openzfs_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Describe an FSx for OpenZFS file system.

        Args:
            file_system_id: ID of the file system to describe.

        Returns:
            File system details.
        """
        logger.info("Describing FSx for OpenZFS file system: %s", file_system_id)
        return self.client.describe_file_systems(FileSystemIds=[file_system_id])

    def update_openzfs_file_system(
        self, file_system_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update an FSx for OpenZFS file system.

        Args:
            file_system_id: ID of the file system to update.
            **kwargs: Update parameters.

        Returns:
            Updated file system details.
        """
        logger.info("Updating FSx for OpenZFS file system: %s", file_system_id)
        kwargs["FileSystemId"] = file_system_id
        return self.client.update_file_system(**kwargs)

    def delete_openzfs_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Delete an FSx for OpenZFS file system.

        Args:
            file_system_id: ID of the file system to delete.

        Returns:
            Deletion result.
        """
        logger.info("Deleting FSx for OpenZFS file system: %s", file_system_id)
        return self.client.delete_file_system(FileSystemId=file_system_id)

    # ========================================================================
    # FSx for NetApp ONTAP
    # ========================================================================

    def create_netapp_ontap_file_system(
        self, config: FSxNetAppONTAPConfig
    ) -> Dict[str, Any]:
        """Create an FSx for NetApp ONTAP file system.

        Args:
            config: ONTAP file system configuration.

        Returns:
            Created file system details.
        """
        logger.info(
            "Creating FSx for NetApp ONTAP file system in region %s", self.region
        )

        kwargs: Dict[str, Any] = {
            "FileSystemType": FSxFileSystemType.NETAPP_ONTAP.value,
            "StorageCapacity": config.storage_capacity_gb,
            "StorageType": config.storage_type,
            "SubnetIds": config.subnet_ids,
            "OntapConfiguration": {
                "DeploymentType": config.deployment_type.value,
                "ThroughputCapacity": config.throughput_capacity_mbps,
                "RouteTableIds": config.route_table_ids,
            },
        }

        if config.security_group_ids:
            kwargs["SecurityGroupIds"] = config.security_group_ids
        if config.preferred_subnet_id:
            kwargs["OntapConfiguration"]["PreferredSubnetId"] = (
                config.preferred_subnet_id
            )
        if config.fsx_admin_password:
            kwargs["OntapConfiguration"]["FsxAdminPassword"] = (
                config.fsx_admin_password
            )
        if config.active_directory:
            kwargs["OntapConfiguration"]["ActiveDirectoryConfiguration"] = (
                config.active_directory
            )
        if config.backup_id:
            kwargs["OntapConfiguration"]["BackupId"] = config.backup_id
        if config.kms_key_id:
            kwargs["KmsKeyId"] = config.kms_key_id
        if config.automatic_backup_retention_days > 0:
            kwargs["OntapConfiguration"][
                "AutomaticBackupRetentionDays"
            ] = config.automatic_backup_retention_days
        if config.daily_automatic_backup_start_time:
            kwargs["OntapConfiguration"][
                "DailyAutomaticBackupStartTime"
            ] = config.daily_automatic_backup_start_time
        if config.weekly_maintenance_start_time:
            kwargs["OntapConfiguration"][
                "WeeklyMaintenanceStartTime"
            ] = config.weekly_maintenance_start_time
        if config.endpoint_ip_address_range:
            kwargs["OntapConfiguration"]["EndpointIpAddressRange"] = (
                config.endpoint_ip_address_range
            )
        if config.copy_tags_to_backups:
            kwargs["OntapConfiguration"]["CopyTagsToBackups"] = True
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]

        response = self.client.create_file_system(**kwargs)
        fs = response.get("FileSystem", {})
        fs_id = fs.get("FileSystemId", "")
        self._ontap_config[fs_id] = config
        self._ontap_config[fs_id].file_system_id = fs_id
        logger.info("Created FSx for NetApp ONTAP file system: %s", fs_id)
        return fs

    def describe_netapp_ontap_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Describe an FSx for NetApp ONTAP file system.

        Args:
            file_system_id: ID of the file system to describe.

        Returns:
            File system details.
        """
        logger.info(
            "Describing FSx for NetApp ONTAP file system: %s", file_system_id
        )
        return self.client.describe_file_systems(FileSystemIds=[file_system_id])

    def update_netapp_ontap_file_system(
        self, file_system_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update an FSx for NetApp ONTAP file system.

        Args:
            file_system_id: ID of the file system to update.
            **kwargs: Update parameters.

        Returns:
            Updated file system details.
        """
        logger.info(
            "Updating FSx for NetApp ONTAP file system: %s", file_system_id
        )
        kwargs["FileSystemId"] = file_system_id
        return self.client.update_file_system(**kwargs)

    def delete_netapp_ontap_file_system(self, file_system_id: str) -> Dict[str, Any]:
        """Delete an FSx for NetApp ONTAP file system.

        Args:
            file_system_id: ID of the file system to delete.

        Returns:
            Deletion result.
        """
        logger.info(
            "Deleting FSx for NetApp ONTAP file system: %s", file_system_id
        )
        return self.client.delete_file_system(FileSystemId=file_system_id)

    # ========================================================================
    # Data Repository Associations
    # ========================================================================

    def create_data_repository_association(
        self, config: DataRepositoryAssociationConfig
    ) -> Dict[str, Any]:
        """Create a data repository association.

        Args:
            config: DRA configuration.

        Returns:
            Created DRA details.
        """
        logger.info(
            "Creating data repository association for file system: %s",
            config.file_system_id,
        )

        kwargs: Dict[str, Any] = {
            "FileSystemId": config.file_system_id,
            "FileSystemPath": config.file_system_path,
            "DataRepositoryPath": config.data_repository_path,
        }

        if config.batch_import_meta_data:
            kwargs["BatchImportMetaDataOnCreate"] = config.batch_import_meta_data
        if config.delete_data_in_filesystem:
            kwargs["DeleteDataInFilesystem"] = True
        if config.import_metadata_on_creation:
            kwargs["ImportMetadataOnCreation"] = True
        if config.new_capacity > 0:
            kwargs["NewCapacity"] = config.new_capacity

        response = self.client.create_data_repository_association(**kwargs)
        dra = response.get("DataRepositoryAssociation", {})
        dra_id = dra.get("AssociationId", "")
        config.association_id = dra_id
        self._dra_configs[dra_id] = config
        logger.info("Created data repository association: %s", dra_id)
        return dra

    def describe_data_repository_association(
        self, association_id: str
    ) -> Dict[str, Any]:
        """Describe a data repository association.

        Args:
            association_id: ID of the DRA to describe.

        Returns:
            DRA details.
        """
        logger.info(
            "Describing data repository association: %s", association_id
        )
        return self.client.describe_data_repository_associations(
            AssociationIds=[association_id]
        )

    def list_data_repository_associations(
        self, file_system_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """List data repository associations.

        Args:
            file_system_id: Filter by file system ID.

        Returns:
            List of DRAs.
        """
        kwargs: Dict[str, Any] = {}
        if file_system_id:
            kwargs["FileSystemId"] = file_system_id
        return self.client.describe_data_repository_associations(**kwargs)

    def update_data_repository_association(
        self, association_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update a data repository association.

        Args:
            association_id: ID of the DRA to update.
            **kwargs: Update parameters.

        Returns:
            Updated DRA details.
        """
        logger.info(
            "Updating data repository association: %s", association_id
        )
        kwargs["AssociationId"] = association_id
        return self.client.update_data_repository_association(**kwargs)

    def delete_data_repository_association(
        self, association_id: str, delete_data_in_filesystem: bool = False
    ) -> Dict[str, Any]:
        """Delete a data repository association.

        Args:
            association_id: ID of the DRA to delete.
            delete_data_in_filesystem: Whether to delete data in the file system.

        Returns:
            Deletion result.
        """
        logger.info(
            "Deleting data repository association: %s", association_id
        )
        kwargs: Dict[str, Any] = {"AssociationId": association_id}
        if delete_data_in_filesystem:
            kwargs["DeleteDataInFilesystem"] = True
        return self.client.delete_data_repository_association(**kwargs)

    def associate_file_system_to_data_repository(
        self,
        file_system_id: str,
        data_repository_path: str,
        file_system_path: str = "/",
        **kwargs
    ) -> Dict[str, Any]:
        """Associate an FSx file system to an external data repository.

        Args:
            file_system_id: FSx file system ID.
            data_repository_path: Path to the external data repository (S3).
            file_system_path: Mount path on the FSx file system.
            **kwargs: Additional association parameters.

        Returns:
            Association details.
        """
        logger.info(
            "Associating file system %s to data repository %s",
            file_system_id,
            data_repository_path,
        )

        config = DataRepositoryAssociationConfig(
            file_system_id=file_system_id,
            file_system_path=file_system_path,
            data_repository_path=data_repository_path,
            **kwargs
        )
        return self.create_data_repository_association(config)

    # ========================================================================
    # Backups
    # ========================================================================

    def create_backup(
        self, config: BackupConfig
    ) -> Dict[str, Any]:
        """Create an FSx backup.

        Args:
            config: Backup configuration.

        Returns:
            Created backup details.
        """
        logger.info("Creating FSx backup for file system: %s", config.file_system_id)

        kwargs: Dict[str, Any] = {}

        if config.file_system_id:
            kwargs["FileSystemId"] = config.file_system_id
        if config.volume_id:
            kwargs["VolumeId"] = config.volume_id
        if config.backup_type:
            kwargs["BackupType"] = config.backup_type
        if config.tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in config.tags.items()]
        if config.kms_key_id:
            kwargs["KmsKeyId"] = config.kms_key_id
        if config.retention_period_days:
            kwargs["RetentionPeriod"] = {"Days": config.retention_period_days}

        response = self.client.create_backup(**kwargs)
        backup = response.get("Backup", {})
        backup_id = backup.get("BackupId", "")
        config.backup_id = backup_id
        self._backup_configs[backup_id] = config
        logger.info("Created FSx backup: %s", backup_id)
        return backup

    def describe_backup(self, backup_id: str) -> Dict[str, Any]:
        """Describe an FSx backup.

        Args:
            backup_id: ID of the backup to describe.

        Returns:
            Backup details.
        """
        logger.info("Describing FSx backup: %s", backup_id)
        return self.client.describe_backups(BackupIds=[backup_id])

    def list_backups(
        self,
        file_system_id: Optional[str] = None,
        backup_type: Optional[str] = None,
        filter_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """List FSx backups.

        Args:
            file_system_id: Filter by file system ID.
            backup_type: Filter by backup type (AUTO, USER_INITIATED).
            filter_args: Additional filter arguments.

        Returns:
            List of backups.
        """
        kwargs: Dict[str, Any] = {}
        if file_system_id:
            kwargs["FileSystemId"] = file_system_id
        if backup_type:
            kwargs["BackupType"] = backup_type
        if filter_args:
            kwargs["Filters"] = filter_args
        return self.client.describe_backups(**kwargs)

    def update_backup(self, backup_id: str, **kwargs) -> Dict[str, Any]:
        """Update an FSx backup (e.g., update tags).

        Args:
            backup_id: ID of the backup to update.
            **kwargs: Update parameters.

        Returns:
            Updated backup details.
        """
        logger.info("Updating FSx backup: %s", backup_id)
        kwargs["BackupId"] = backup_id
        return self.client.update_backup(**kwargs)

    def delete_backup(self, backup_id: str) -> Dict[str, Any]:
        """Delete an FSx backup.

        Args:
            backup_id: ID of the backup to delete.

        Returns:
            Deletion result.
        """
        logger.info("Deleting FSx backup: %s", backup_id)
        return self.client.delete_backup(BackupId=backup_id)

    def copy_backup(
        self,
        source_backup_id: str,
        source_region: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Copy an FSx backup.

        Args:
            source_backup_id: ID of the source backup to copy.
            source_region: Source region (defaults to current region).
            kms_key_id: KMS key ID for encryption.
            tags: Tags to apply to the copied backup.

        Returns:
            Copied backup details.
        """
        logger.info("Copying FSx backup: %s", source_backup_id)
        kwargs: Dict[str, Any] = {"SourceBackupId": source_backup_id}
        if source_region:
            kwargs["SourceRegion"] = source_region
        if kms_key_id:
            kwargs["KmsKeyId"] = kms_key_id
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        response = self.client.copy_backup(**kwargs)
        backup = response.get("Backup", {})
        logger.info("Copied FSx backup: %s", backup.get("BackupId", ""))
        return backup

    # ========================================================================
    # Active Directory Integration
    # ========================================================================

    def configure_active_directory(
        self, config: ActiveDirectoryConfig, file_system_id: str
    ) -> Dict[str, Any]:
        """Configure Active Directory integration for FSx for Windows.

        Args:
            config: Active Directory configuration.
            file_system_id: File system ID to associate with AD.

        Returns:
            AD configuration result.
        """
        logger.info(
            "Configuring Active Directory for file system: %s", file_system_id
        )

        kwargs: Dict[str, Any] = {
            "FileSystemId": file_system_id,
            "SelfManagedActiveDirectoryConfiguration": {
                "DomainName": config.domain_name,
                "NetBiosName": config.net_bios_name,
            },
        }

        if config.file_system_administrators_group:
            kwargs["SelfManagedActiveDirectoryConfiguration"][
                "FileSystemAdministratorsGroup"
            ] = config.file_system_administrators_group
        if config.organizational_unit_distinguished_name:
            kwargs["SelfManagedActiveDirectoryConfiguration"][
                "OrganizationalUnitDistinguishedName"
            ] = config.organizational_unit_distinguished_name
        if config.dns_ips:
            kwargs["SelfManagedActiveDirectoryConfiguration"]["DnsIps"] = config.dns_ips
        if config.backup_directory_id:
            kwargs["SelfManagedActiveDirectoryConfiguration"][
                "BackupDirectoryId"
            ] = config.backup_directory_id
        if config.replication_secret_arn:
            kwargs["SelfManagedActiveDirectoryConfiguration"][
                "ReplicationSecretArn"
            ] = config.replication_secret_arn
        if config.time_offset_in_seconds:
            kwargs["SelfManagedActiveDirectoryConfiguration"][
                "TimeOffsetInSeconds"
            ] = config.time_offset_in_seconds

        result = self.client.update_file_system(**kwargs)
        self._ad_configs[file_system_id] = config
        logger.info("Configured Active Directory for file system: %s", file_system_id)
        return result

    def join_active_directory(
        self,
        file_system_id: str,
        directory_id: str,
        domain_name: str,
        organizational_unit: Optional[str] = None,
        dns_ips: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Join an FSx for Windows file system to an Active Directory.

        Args:
            file_system_id: File system ID.
            directory_id: AWS Managed Microsoft AD directory ID.
            domain_name: Active Directory domain name.
            organizational_unit: Organizational unit DN.
            dns_ips: DNS server IP addresses.

        Returns:
            Join result.
        """
        logger.info(
            "Joining file system %s to Active Directory: %s",
            file_system_id,
            domain_name,
        )

        windows_config: Dict[str, Any] = {"ActiveDirectoryId": directory_id}

        if organizational_unit:
            windows_config["SelfManagedActiveDirectoryConfiguration"] = {
                "OrganizationalUnitDistinguishedName": organizational_unit,
                "DomainName": domain_name,
            }
            if dns_ips:
                windows_config["SelfManagedActiveDirectoryConfiguration"]["DnsIps"] = (
                    dns_ips
                )

        return self.client.update_file_system(
            FileSystemId=file_system_id,
            WindowsConfiguration=windows_config,
        )

    def disjoin_active_directory(self, file_system_id: str) -> Dict[str, Any]:
        """Disjoin an FSx for Windows file system from Active Directory.

        Args:
            file_system_id: File system ID.

        Returns:
            Disjoin result.
        """
        logger.info(
            "Disjoining file system %s from Active Directory", file_system_id
        )
        return self.client.update_file_system(
            FileSystemId=file_system_id,
            WindowsConfiguration={"SelfManagedActiveDirectoryConfiguration": {}},
        )

    def describe_active_directory(
        self, file_system_id: str
    ) -> Dict[str, Any]:
        """Describe Active Directory configuration for a file system.

        Args:
            file_system_id: File system ID.

        Returns:
            AD configuration details.
        """
        logger.info(
            "Describing Active Directory for file system: %s", file_system_id
        )
        fs_response = self.client.describe_file_systems(FileSystemIds=[file_system_id])
        return fs_response

    # ========================================================================
    # Storage Virtual Machines (SVMs) for ONTAP
    # ========================================================================

    def create_storage_virtual_machine(
        self, config: StorageVirtualMachineConfig
    ) -> Dict[str, Any]:
        """Create a storage virtual machine (SVM) for ONTAP.

        Args:
            config: SVM configuration.

        Returns:
            Created SVM details.
        """
        logger.info(
            "Creating storage virtual machine for file system: %s",
            config.file_system_id,
        )

        kwargs: Dict[str, Any] = {
            "FileSystemId": config.file_system_id,
            "Name": config.name,
            "SvmRootVolumeSecurityStyle": config.svm_root_volume_security_style,
        }

        if config.ad_domain_membership:
            kwargs["AdDomainMembership"] = config.ad_domain_membership
        if config.f_policy:
            kwargs["FPolicy"] = config.f_policy
        if config.backup_config:
            kwargs["BackupConfiguration"] = config.backup_config

        response = self.client.create_storage_virtual_machine(**kwargs)
        svm = response.get("StorageVirtualMachine", {})
        svm_id = svm.get("StorageVirtualMachineId", "")
        config.svm_id = svm_id
        self._svm_configs[svm_id] = config
        logger.info("Created storage virtual machine: %s", svm_id)
        return svm

    def describe_storage_virtual_machine(self, svm_id: str) -> Dict[str, Any]:
        """Describe a storage virtual machine.

        Args:
            svm_id: SVM ID.

        Returns:
            SVM details.
        """
        logger.info("Describing storage virtual machine: %s", svm_id)
        return self.client.describe_storage_virtual_machines(
            StorageVirtualMachineIds=[svm_id]
        )

    def list_storage_virtual_machines(
        self, file_system_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """List storage virtual machines.

        Args:
            file_system_id: Filter by file system ID.

        Returns:
            List of SVMs.
        """
        kwargs: Dict[str, Any] = {}
        if file_system_id:
            kwargs["FileSystemId"] = file_system_id
        return self.client.describe_storage_virtual_machines(**kwargs)

    def update_storage_virtual_machine(
        self, svm_id: str, **kwargs
    ) -> Dict[str, Any]:
        """Update a storage virtual machine.

        Args:
            svm_id: SVM ID.
            **kwargs: Update parameters.

        Returns:
            Updated SVM details.
        """
        logger.info("Updating storage virtual machine: %s", svm_id)
        kwargs["StorageVirtualMachineId"] = svm_id
        return self.client.update_storage_virtual_machine(**kwargs)

    def delete_storage_virtual_machine(
        self, svm_id: str, copy_tags_to_destinations: bool = False
    ) -> Dict[str, Any]:
        """Delete a storage virtual machine.

        Args:
            svm_id: SVM ID.
            copy_tags_to_destinations: Whether to copy tags to destination volumes.

        Returns:
            Deletion result.
        """
        logger.info("Deleting storage virtual machine: %s", svm_id)
        kwargs: Dict[str, Any] = {"StorageVirtualMachineId": svm_id}
        if copy_tags_to_destinations:
            kwargs["CopyTagsToBackups"] = True
        return self.client.delete_storage_virtual_machine(**kwargs)

    # ========================================================================
    # Volumes for ONTAP
    # ========================================================================

    def create_volume(self, config: VolumeConfig) -> Dict[str, Any]:
        """Create a volume for ONTAP.

        Args:
            config: Volume configuration.

        Returns:
            Created volume details.
        """
        logger.info("Creating volume %s for SVM: %s", config.name, config.svm_id)

        kwargs: Dict[str, Any] = {
            "Name": config.name,
            "StorageVirtualMachineId": config.storage_virtual_machine_id or config.svm_id,
            "SizeInMegabytes": config.size_in_megabytes,
            "SecurityStyle": config.security_style,
            "StorageEfficiencyEnabled": config.storage_efficiency_enabled,
        }

        if config.junction_path:
            kwargs["JunctionPath"] = config.junction_path
        if config.aggregate_name:
            kwargs["AggregateName"] = config.aggregate_name
        if config.copy_tags_to_backups:
            kwargs["CopyTagsToBackups"] = True
        if config.tiering_policy:
            kwargs["TieringPolicy"] = config.tiering_policy
        if config.qos_policy_group_id:
            kwargs["QosPolicyGroupId"] = config.qos_policy_group_id
        if config.snapshot_policy:
            kwargs["SnapshotPolicy"] = config.snapshot_policy

        response = self.client.create_volume(**kwargs)
        volume = response.get("Volume", {})
        volume_id = volume.get("VolumeId", "")
        config.volume_id = volume_id
        self._volume_configs[volume_id] = config
        logger.info("Created volume: %s", volume_id)
        return volume

    def describe_volume(self, volume_id: str) -> Dict[str, Any]:
        """Describe a volume.

        Args:
            volume_id: Volume ID.

        Returns:
            Volume details.
        """
        logger.info("Describing volume: %s", volume_id)
        return self.client.describe_volumes(VolumeIds=[volume_id])

    def list_volumes(
        self,
        file_system_id: Optional[str] = None,
        svm_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List volumes.

        Args:
            file_system_id: Filter by file system ID.
            svm_id: Filter by SVM ID.

        Returns:
            List of volumes.
        """
        kwargs: Dict[str, Any] = {}
        if file_system_id:
            kwargs["FileSystemId"] = file_system_id
        if svm_id:
            kwargs["Filters"] = [{"Name": "storage-virtual-machine-id", "Values": [svm_id]}]
        return self.client.describe_volumes(**kwargs)

    def update_volume(self, volume_id: str, **kwargs) -> Dict[str, Any]:
        """Update a volume.

        Args:
            volume_id: Volume ID.
            **kwargs: Update parameters.

        Returns:
            Updated volume details.
        """
        logger.info("Updating volume: %s", volume_id)
        kwargs["VolumeId"] = volume_id
        return self.client.update_volume(**kwargs)

    def delete_volume(self, volume_id: str) -> Dict[str, Any]:
        """Delete a volume.

        Args:
            volume_id: Volume ID.

        Returns:
            Deletion result.
        """
        logger.info("Deleting volume: %s", volume_id)
        return self.client.delete_volume(VolumeId=volume_id)

    # ========================================================================
    # CloudWatch Integration
    # ========================================================================

    def configure_cloudwatch_monitoring(
        self, config: CloudWatchConfig
    ) -> Dict[str, Any]:
        """Configure CloudWatch monitoring for FSx.

        Args:
            config: CloudWatch configuration.

        Returns:
            Configuration result.
        """
        logger.info("Configuring CloudWatch monitoring in region: %s", self.region)
        self._cloudwatch_config = config

        if config.enable_performance_metrics:
            logger.info("Performance metrics enabled")
        if config.enable阿里云_logging:
            logger.info("File system access logging enabled")

        return {
            "region": self.region,
            "performance_metrics_enabled": config.enable_performance_metrics,
            "file_access_logging_enabled": config.enable阿里云_logging,
            "log_group_name": config.log_group_name,
            "metrics_interval_minutes": config.metrics_interval_minutes,
        }

    def get_cloudwatch_metrics(
        self,
        file_system_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        metric_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get CloudWatch metrics for an FSx file system.

        Args:
            file_system_id: FSx file system ID.
            start_time: Start of the time range.
            end_time: End of the time range.
            period: Metric period in seconds.
            metric_names: List of metric names to retrieve.

        Returns:
            CloudWatch metrics data.
        """
        import boto3

        if self._session:
            cw = self._session.client("cloudwatch", region_name=self.region)
        else:
            cw = boto3.Session(region_name=self.region).client("cloudwatch")

        logger.info("Fetching CloudWatch metrics for file system: %s", file_system_id)

        if metric_names is None:
            metric_names = [
                "StorageCapacity",
                "StorageCapacityConsumed",
                "Throughput",
                "ReadThroughput",
                "WriteThroughput",
                "IOPS",
                "ReadIOPS",
                "WriteIOPS",
            ]

        results = {}
        for metric_name in metric_names:
            try:
                response = cw.get_metric_statistics(
                    Namespace="AWS/FSx",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "FileSystemId", "Value": file_system_id}
                    ],
                    StartTime=start_time or datetime.utcnow().replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ),
                    EndTime=end_time or datetime.utcnow(),
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum", "Sum"],
                )
                results[metric_name] = response.get("Datapoints", [])
            except Exception as e:
                logger.warning(
                    "Failed to fetch metric %s for file system %s: %s",
                    metric_name,
                    file_system_id,
                    e,
                )
                results[metric_name] = []

        return results

    def enable_cloudwatch_logging(
        self,
        file_system_id: str,
        log_group_name: str,
    ) -> Dict[str, Any]:
        """Enable CloudWatch file system access logging for FSx.

        Args:
            file_system_id: FSx file system ID.
            log_group_name: CloudWatch log group name.

        Returns:
            Operation result.
        """
        logger.info(
            "Enabling CloudWatch logging for file system: %s, log group: %s",
            file_system_id,
            log_group_name,
        )
        return self.client.associate_file_system_with_log_group(
            FileSystemId=file_system_id,
            LogGroupName=log_group_name,
        )

    def disable_cloudwatch_logging(
        self, file_system_id: str
    ) -> Dict[str, Any]:
        """Disable CloudWatch file system access logging for FSx.

        Args:
            file_system_id: FSx file system ID.

        Returns:
            Operation result.
        """
        logger.info(
            "Disabling CloudWatch logging for file system: %s", file_system_id
        )
        return self.client.disassociate_file_system_from_log_group(
            FileSystemId=file_system_id
        )

    def create_cloudwatch_alarm(
        self,
        file_system_id: str,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 2,
        period: int = 300,
        statistic: str = "Average",
    ) -> str:
        """Create a CloudWatch alarm for FSx metrics.

        Args:
            file_system_id: FSx file system ID.
            alarm_name: Name of the alarm.
            metric_name: Metric to monitor.
            threshold: Threshold value.
            comparison_operator: Comparison operator.
            evaluation_periods: Number of evaluation periods.
            period: Period in seconds.
            statistic: Statistic type.

        Returns:
            Alarm ARN.
        """
        import boto3

        if self._session:
            cw = boto3.Session(region_name=self.region).client("cloudwatch")
        else:
            cw = boto3.Session(profile_name=self.profile, region_name=self.region).client(
                "cloudwatch"
            )

        logger.info(
            "Creating CloudWatch alarm %s for file system %s", alarm_name, file_system_id
        )

        alarm_args = {
            "AlarmName": alarm_name,
            "Namespace": "AWS/FSx",
            "MetricName": metric_name,
            "Dimensions": [{"Name": "FileSystemId", "Value": file_system_id}],
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "Statistic": statistic,
            "ActionsEnabled": True,
        }

        response = cw.put_metric_alarm(**alarm_args)
        logger.info("Created CloudWatch alarm: %s", alarm_name)
        return alarm_name

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def list_file_systems(
        self,
        file_system_ids: Optional[List[str]] = None,
        file_system_type: Optional[FSxFileSystemType] = None,
    ) -> Dict[str, Any]:
        """List FSx file systems.

        Args:
            file_system_ids: List of specific file system IDs.
            file_system_type: Filter by file system type.

        Returns:
            List of file systems.
        """
        kwargs: Dict[str, Any] = {}
        if file_system_ids:
            kwargs["FileSystemIds"] = file_system_ids
        if file_system_type:
            kwargs["FileSystemType"] = file_system_type.value
        return self.client.describe_file_systems(**kwargs)

    def get_file_system_health_status(
        self, file_system_id: str
    ) -> Dict[str, Any]:
        """Get health status of an FSx file system.

        Args:
            file_system_id: File system ID.

        Returns:
            Health status information.
        """
        logger.info("Getting health status for file system: %s", file_system_id)
        response = self.client.describe_file_systems(FileSystemIds=[file_system_id])
        fs_list = response.get("FileSystems", [])
        if fs_list:
            fs = fs_list[0]
            return {
                "file_system_id": file_system_id,
                "lifecycle": fs.get("Lifecycle", "UNKNOWN"),
                "health_check": fs.get("HealthCheck", {}),
                "failure_details": fs.get("FailureDetails", {}),
            }
        return {"file_system_id": file_system_id, "status": "NOT_FOUND"}

    def tag_resource(
        self, resource_arn: str, tags: Dict[str, str]
    ) -> Dict[str, Any]:
        """Add or update tags on an FSx resource.

        Args:
            resource_arn: ARN of the resource.
            tags: Tags to apply.

        Returns:
            Tag operation result.
        """
        logger.info("Tagging resource: %s", resource_arn)
        tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
        return self.client.tag_resource(ResourceARN=resource_arn, Tags=tag_list)

    def untag_resource(self, resource_arn: str, tag_keys: List[str]) -> Dict[str, Any]:
        """Remove tags from an FSx resource.

        Args:
            resource_arn: ARN of the resource.
            tag_keys: List of tag keys to remove.

        Returns:
            Untag operation result.
        """
        logger.info("Untagging resource: %s", resource_arn)
        return self.client.untag_resource(ResourceARN=resource_arn, TagKeys=tag_keys)

    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of all configured resources.

        Returns:
            Configuration summary.
        """
        return {
            "region": self.region,
            "windows_file_systems": list(self._windows_config.keys()),
            "lustre_file_systems": list(self._lustre_config.keys()),
            "openzfs_file_systems": list(self._openzfs_config.keys()),
            "ontap_file_systems": list(self._ontap_config.keys()),
            "data_repository_associations": list(self._dra_configs.keys()),
            "backups": list(self._backup_configs.keys()),
            "active_directory_configs": list(self._ad_configs.keys()),
            "storage_virtual_machines": list(self._svm_configs.keys()),
            "volumes": list(self._volume_configs.keys()),
            "cloudwatch_configured": self._cloudwatch_config is not None,
        }
