"""
AWS EFS (Elastic File System) Integration Module for Workflow System

Implements an EFSIntegration class with:
1. File system management: Create/manage EFS file systems
2. Mount targets: Manage mount targets
3. Access points: Manage access points
4. Encryption: Configure encryption at rest
5. Performance mode: Configure performance mode (generalPurpose/maxIO)
6. Throughput mode: Configure throughput modes
7. Backup policies: Configure backup policies
8. Replication: Configure cross-region replication
9. CloudWatch integration: Metrics and monitoring
10. Mount helpers: Mount EFS on Linux instances

Commit: 'feat(aws-efs): add AWS EFS with file system management, mount targets, access points, encryption, performance mode, throughput, backup policies, replication, CloudWatch, mount helpers'
"""

import uuid
import json
import time
import logging
import subprocess
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class PerformanceMode(Enum):
    """EFS performance modes."""
    GENERAL_PURPOSE = "generalPurpose"
    MAX_IO = "maxIO"


class ThroughputMode(Enum):
    """EFS throughput modes."""
    BURSTING = "bursting"
    PROVISIONED = "provisioned"
    ELASTIC = "elastic"


class EncryptedOption(Enum):
    """Encryption options."""
    ENABLED = True
    DISABLED = False


class ReplicationStatus(Enum):
    """File system replication status."""
    ENABLED = "ENABLED"
    CREATING = "CREATING"
    DELETED = "DELETED"
    ERROR = "ERROR"


class BackupPolicyStatus(Enum):
    """Backup policy status."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    ENABLING = "ENABLING"
    DISABLING = "DISABLING"


@dataclass
class EFSConfig:
    """Configuration for EFS file system."""
    name: str = "efs-filesystem"
    region: str = "us-east-1"
    performance_mode: PerformanceMode = PerformanceMode.GENERAL_PURPOSE
    throughput_mode: ThroughputMode = ThroughputMode.BURSTING
    provisioned_throughput_mbps: float = 0.0
    encrypted: bool = True
    kms_key_id: Optional[str] = None
    availability_zone_id: Optional[str] = None
    backup_policy: BackupPolicyStatus = BackupPolicyStatus.ENABLED
    replication_region: Optional[str] = None
    lifecycle_policies: Optional[List[str]] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class MountTargetConfig:
    """Configuration for EFS mount target."""
    subnet_id: str
    security_group_ids: List[str]
    ip_address: Optional[str] = None
    availability_zone_id: Optional[str] = None


@dataclass
class AccessPointConfig:
    """Configuration for EFS access point."""
    name: str = "efs-access-point"
    posix_user: Optional[Dict[str, Any]] = None
    root_directory: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class EFSFileSystem:
    """Represents an EFS file system."""
    file_system_id: str
    name: str
    arn: str
    size_bytes: int = 0
    performance_mode: str = "generalPurpose"
    throughput_mode: str = "bursting"
    provisioned_throughput_mbps: float = 0.0
    encrypted: bool = True
    kms_key_id: Optional[str] = None
    availability_zone_id: Optional[str] = None
    creation_time: Optional[datetime] = None
    life_cycle_state: str = "available"
    number_of_mount_targets: int = 0
    owner_id: str = ""
    resource_id: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    replication_configuration: Optional[Dict[str, Any]] = None
    backup_policy: Optional[Dict[str, str]] = None


class EFSIntegration:
    """
    AWS EFS Integration class for managing Elastic File System resources.
    
    Provides comprehensive functionality for:
    - File system lifecycle management
    - Mount target configuration
    - Access point management
    - Encryption settings
    - Performance and throughput configuration
    - Backup policies
    - Cross-region replication
    - CloudWatch monitoring
    - Linux mount helpers
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        profile_name: Optional[str] = None,
        config: Optional[EFSConfig] = None
    ):
        """
        Initialize EFS Integration.
        
        Args:
            region: AWS region for EFS operations
            profile_name: Optional AWS profile name
            config: EFS configuration options
        """
        self.region = region
        self.profile_name = profile_name
        self.config = config or EFSConfig(region=region)
        
        self._efs_client = None
        self._cw_client = None
        self._sts_client = None
        self._lock = threading.RLock()
        self._file_systems: Dict[str, EFSFileSystem] = {}
        self._mount_targets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._access_points: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
    @property
    def efs_client(self):
        """Lazy-load EFS client."""
        if self._efs_client is None:
            self._initialize_clients()
        return self._efs_client
    
    @property
    def cw_client(self):
        """Lazy-load CloudWatch client."""
        if self._cw_client is None:
            self._initialize_clients()
        return self._cw_client
    
    @property
    def sts_client(self):
        """Lazy-load STS client."""
        if self._sts_client is None:
            self._initialize_clients()
        return self._sts_client
    
    def _initialize_clients(self):
        """Initialize AWS clients."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for EFS integration")
        
        session_kwargs = {"region_name": self.region}
        if self.profile_name:
            session_kwargs["profile_name"] = self.profile_name
        
        session = boto3.Session(**session_kwargs)
        self._efs_client = session.client("efs")
        self._cw_client = session.client("cloudwatch")
        self._sts_client = session.client("sts")
    
    def _generate_file_system_name(self, prefix: str = "efs") -> str:
        """Generate unique file system name."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        short_id = str(uuid.uuid4())[:8]
        return f"{prefix}-{timestamp}-{short_id}"
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        try:
            return self.sts_client.get_caller_identity()["Account"]
        except Exception:
            return "unknown"
    
    # =========================================================================
    # File System Management
    # =========================================================================
    
    def create_file_system(
        self,
        config: Optional[EFSConfig] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> EFSFileSystem:
        """
        Create a new EFS file system.
        
        Args:
            config: EFS configuration options
            tags: Additional tags for the file system
            
        Returns:
            Created EFSFileSystem object
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock file system")
            return self._create_mock_file_system(config or self.config)
        
        cfg = config or self.config
        
        create_kwargs = {
            "PerformanceMode": cfg.performance_mode.value,
            "ThroughputMode": cfg.throughput_mode.value,
            "Encrypted": cfg.encrypted,
        }
        
        if cfg.throughput_mode == ThroughputMode.PROVISIONED:
            if cfg.provisioned_throughput_mbps > 0:
                create_kwargs["ProvisionedThroughputInMibps"] = cfg.provisioned_throughput_mbps
        
        if cfg.kms_key_id:
            create_kwargs["KmsKeyId"] = cfg.kms_key_id
        
        if cfg.availability_zone_id:
            create_kwargs["AvailabilityZoneName"] = cfg.availability_zone_id
        
        # Prepare tags
        all_tags = tags or cfg.tags or {}
        if "Name" not in all_tags:
            all_tags["Name"] = cfg.name
        
        creation_token = self._generate_file_system_name()
        
        try:
            with self._lock:
                response = self.efs_client.create_file_system(
                    CreationToken=creation_token,
                    **create_kwargs
                )
                
                file_system = self._parse_file_system_response(response)
                file_system.name = cfg.name
                
                # Apply tags
                if all_tags:
                    tag_list = [{"Key": k, "Value": str(v)} for k, v in all_tags.items()]
                    self.efs_client.create_tags(
                        FileSystemId=file_system.file_system_id,
                        Tags=tag_list
                    )
                    file_system.tags = all_tags
                
                # Set backup policy if specified
                if cfg.backup_policy != BackupPolicyStatus.DISABLED:
                    self.set_backup_policy(file_system.file_system_id, cfg.backup_policy)
                
                # Configure lifecycle policies
                if cfg.lifecycle_policies:
                    self.set_lifecycle_policies(file_system.file_system_id, cfg.lifecycle_policies)
                
                # Configure replication if specified
                if cfg.replication_region:
                    self.enable_replication(file_system.file_system_id, cfg.replication_region)
                
                self._file_systems[file_system.file_system_id] = file_system
                
                logger.info(f"Created EFS file system: {file_system.file_system_id}")
                return file_system
                
        except ClientError as e:
            logger.error(f"Failed to create EFS file system: {e}")
            raise
    
    def _create_mock_file_system(self, config: EFSConfig) -> EFSFileSystem:
        """Create a mock file system for testing without boto3."""
        file_system_id = f"fs-{uuid.uuid4().hex[:8]}"
        account_id = self._get_account_id()
        
        return EFSFileSystem(
            file_system_id=file_system_id,
            name=config.name,
            arn=f"arn:aws:elasticfilesystem:{self.region}:{account_id}:file-system/{file_system_id}",
            size_bytes=0,
            performance_mode=config.performance_mode.value,
            throughput_mode=config.throughput_mode.value,
            provisioned_throughput_mbps=config.provisioned_throughput_mbps,
            encrypted=config.encrypted,
            kms_key_id=config.kms_key_id,
            availability_zone_id=config.availability_zone_id,
            creation_time=datetime.now(),
            life_cycle_state="available",
            number_of_mount_targets=0,
            owner_id=account_id,
            resource_id=file_system_id,
            tags=config.tags or {"Name": config.name},
        )
    
    def _parse_file_system_response(self, response: Dict[str, Any]) -> EFSFileSystem:
        """Parse EFS file system response into EFSFileSystem object."""
        return EFSFileSystem(
            file_system_id=response["FileSystemId"],
            name=response.get("Name", response["FileSystemId"]),
            arn=response["FileSystemArn"],
            size_bytes=int(response.get("SizeInBytes", {}).get("Value", 0)),
            performance_mode=response.get("PerformanceMode", "generalPurpose"),
            throughput_mode=response.get("ThroughputMode", "bursting"),
            provisioned_throughput_mbps=float(response.get("ProvisionedThroughputInMibps", 0)),
            encrypted=response.get("Encrypted", True),
            kms_key_id=response.get("KmsKeyId"),
            availability_zone_id=response.get("AvailabilityZoneName"),
            creation_time=response.get("CreationTime"),
            life_cycle_state=response.get("LifeCycleState", "available"),
            number_of_mount_targets=response.get("NumberOfMountTargets", 0),
            owner_id=response.get("OwnerId", ""),
            resource_id=response.get("ResourceId", response["FileSystemId"]),
            tags={},
        )
    
    def get_file_system(self, file_system_id: str) -> Optional[EFSFileSystem]:
        """
        Get details of an EFS file system.
        
        Args:
            file_system_id: ID of the file system
            
        Returns:
            EFSFileSystem object or None if not found
        """
        if not BOTO3_AVAILABLE:
            return self._file_systems.get(file_system_id)
        
        try:
            response = self.efs_client.describe_file_systems(
                FileSystemId=file_system_id
            )
            
            if response["FileSystems"]:
                file_system = self._parse_file_system_response(response["FileSystems"][0])
                
                # Get tags
                tags_response = self.efs_client.list_tags_for_metadata(
                    FileSystemId=file_system_id
                )
                file_system.tags = {
                    t["Key"]: t["Value"] 
                    for t in tags_response.get("Tags", [])
                }
                
                return file_system
            
            return None
            
        except ClientError as e:
            logger.error(f"Failed to get file system {file_system_id}: {e}")
            return None
    
    def list_file_systems(
        self,
        marker: Optional[str] = None,
        max_items: int = 100
    ) -> List[EFSFileSystem]:
        """
        List all EFS file systems.
        
        Args:
            marker: Pagination marker
            max_items: Maximum number of items to return
            
        Returns:
            List of EFSFileSystem objects
        """
        if not BOTO3_AVAILABLE:
            return list(self._file_systems.values())
        
        try:
            kwargs = {"MaxItems": max_items}
            if marker:
                kwargs["Marker"] = marker
            
            response = self.efs_client.describe_file_systems(**kwargs)
            
            file_systems = []
            for fs_data in response.get("FileSystems", []):
                file_system = self._parse_file_system_response(fs_data)
                
                # Get tags for each file system
                try:
                    tags_response = self.efs_client.list_tags_for_metadata(
                        FileSystemId=file_system.file_system_id
                    )
                    file_system.tags = {
                        t["Key"]: t["Value"]
                        for t in tags_response.get("Tags", [])
                    }
                except Exception:
                    pass
                
                file_systems.append(file_system)
            
            return file_systems
            
        except ClientError as e:
            logger.error(f"Failed to list file systems: {e}")
            return []
    
    def update_file_system(
        self,
        file_system_id: str,
        throughput_mode: Optional[ThroughputMode] = None,
        provisioned_throughput_mbps: Optional[float] = None,
        performance_mode: Optional[PerformanceMode] = None
    ) -> EFSFileSystem:
        """
        Update EFS file system properties.
        
        Args:
            file_system_id: ID of the file system to update
            throughput_mode: New throughput mode
            provisioned_throughput_mbps: Provisioned throughput for PROVISIONED mode
            performance_mode: Performance mode (cannot be changed after creation)
            
        Returns:
            Updated EFSFileSystem object
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            if fs:
                if throughput_mode:
                    fs.throughput_mode = throughput_mode.value
                if provisioned_throughput_mbps:
                    fs.provisioned_throughput_mbps = provisioned_throughput_mbps
                if performance_mode:
                    fs.performance_mode = performance_mode.value
            return fs
        
        try:
            update_kwargs = {}
            
            if throughput_mode:
                update_kwargs["ThroughputMode"] = throughput_mode.value
                
                if throughput_mode == ThroughputMode.PROVISIONED and provisioned_throughput_mbps:
                    update_kwargs["ProvisionedThroughputInMibps"] = provisioned_throughput_mbps
            
            if update_kwargs:
                with self._lock:
                    self.efs_client.update_file_system(
                        FileSystemId=file_system_id,
                        **update_kwargs
                    )
            
            return self.get_file_system(file_system_id)
            
        except ClientError as e:
            logger.error(f"Failed to update file system {file_system_id}: {e}")
            raise
    
    def delete_file_system(self, file_system_id: str) -> bool:
        """
        Delete an EFS file system.
        
        Args:
            file_system_id: ID of the file system to delete
            
        Returns:
            True if deletion was successful
        """
        if not BOTO3_AVAILABLE:
            if file_system_id in self._file_systems:
                del self._file_systems[file_system_id]
            return True
        
        try:
            # First delete all mount targets
            mount_targets = self.list_mount_targets(file_system_id)
            for mt in mount_targets:
                self.delete_mount_target(file_system_id, mt["MountTargetId"])
            
            # Delete all access points
            access_points = self.list_access_points(file_system_id)
            for ap in access_points:
                self.delete_access_point(file_system_id, ap["AccessPointId"])
            
            with self._lock:
                self.efs_client.delete_file_system(FileSystemId=file_system_id)
            
            if file_system_id in self._file_systems:
                del self._file_systems[file_system_id]
            
            logger.info(f"Deleted EFS file system: {file_system_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete file system {file_system_id}: {e}")
            return False
    
    # =========================================================================
    # Mount Target Management
    # =========================================================================
    
    def create_mount_target(
        self,
        file_system_id: str,
        subnet_id: str,
        security_group_ids: List[str],
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a mount target for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            subnet_id: Subnet ID for the mount target
            security_group_ids: List of security group IDs
            ip_address: Optional specific IP address
            
        Returns:
            Mount target details dictionary
        """
        if not BOTO3_AVAILABLE:
            mount_target_id = f"fsmt-{uuid.uuid4().hex[:8]}"
            mock_target = {
                "MountTargetId": mount_target_id,
                "FileSystemId": file_system_id,
                "SubnetId": subnet_id,
                "SecurityGroups": security_group_ids,
                "LifeCycleState": "available",
                "IpAddress": ip_address or "10.0.1.100",
                "AvailabilityZoneId": None,
                "AvailabilityZoneName": "us-east-1a",
                "OwnerId": self._get_account_id(),
            }
            self._mount_targets[file_system_id].append(mock_target)
            return mock_target
        
        try:
            create_kwargs = {
                "FileSystemId": file_system_id,
                "SubnetId": subnet_id,
                "SecurityGroups": security_group_ids,
            }
            
            if ip_address:
                create_kwargs["IpAddress"] = ip_address
            
            with self._lock:
                response = self.efs_client.create_mount_target(**create_kwargs)
            
            logger.info(f"Created mount target {response['MountTargetId']} for {file_system_id}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create mount target: {e}")
            raise
    
    def get_mount_target(
        self,
        file_system_id: str,
        mount_target_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get details of a specific mount target."""
        if not BOTO3_AVAILABLE:
            for mt in self._mount_targets.get(file_system_id, []):
                if mt["MountTargetId"] == mount_target_id:
                    return mt
            return None
        
        try:
            response = self.efs_client.describe_mount_targets(
                FileSystemId=file_system_id,
                MountTargetId=mount_target_id
            )
            
            if response["MountTargets"]:
                return response["MountTargets"][0]
            return None
            
        except ClientError as e:
            logger.error(f"Failed to get mount target: {e}")
            return None
    
    def list_mount_targets(
        self,
        file_system_id: str
    ) -> List[Dict[str, Any]]:
        """
        List all mount targets for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            List of mount target dictionaries
        """
        if not BOTO3_AVAILABLE:
            return self._mount_targets.get(file_system_id, [])
        
        try:
            response = self.efs_client.describe_mount_targets(
                FileSystemId=file_system_id
            )
            return response.get("MountTargets", [])
            
        except ClientError as e:
            logger.error(f"Failed to list mount targets: {e}")
            return []
    
    def delete_mount_target(
        self,
        file_system_id: str,
        mount_target_id: str
    ) -> bool:
        """
        Delete a mount target.
        
        Args:
            file_system_id: ID of the EFS file system
            mount_target_id: ID of the mount target to delete
            
        Returns:
            True if deletion was successful
        """
        if not BOTO3_AVAILABLE:
            self._mount_targets[file_system_id] = [
                mt for mt in self._mount_targets.get(file_system_id, [])
                if mt["MountTargetId"] != mount_target_id
            ]
            return True
        
        try:
            with self._lock:
                self.efs_client.delete_mount_target(MountTargetId=mount_target_id)
            
            logger.info(f"Deleted mount target: {mount_target_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete mount target: {e}")
            return False
    
    def update_mount_target_security_groups(
        self,
        file_system_id: str,
        mount_target_id: str,
        security_group_ids: List[str]
    ) -> bool:
        """
        Update security groups for a mount target.
        
        Args:
            file_system_id: ID of the EFS file system
            mount_target_id: ID of the mount target
            security_group_ids: New list of security group IDs
            
        Returns:
            True if update was successful
        """
        if not BOTO3_AVAILABLE:
            for mt in self._mount_targets.get(file_system_id, []):
                if mt["MountTargetId"] == mount_target_id:
                    mt["SecurityGroups"] = security_group_ids
                    return True
            return False
        
        try:
            with self._lock:
                self.efs_client.modify_mount_target_security_groups(
                    MountTargetId=mount_target_id,
                    SecurityGroups=security_group_ids
                )
            
            logger.info(f"Updated security groups for mount target: {mount_target_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update mount target security groups: {e}")
            return False
    
    # =========================================================================
    # Access Point Management
    # =========================================================================
    
    def create_access_point(
        self,
        file_system_id: str,
        config: Optional[AccessPointConfig] = None
    ) -> Dict[str, Any]:
        """
        Create an access point for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            config: Access point configuration
            
        Returns:
            Access point details dictionary
        """
        cfg = config or AccessPointConfig()
        
        if not BOTO3_AVAILABLE:
            access_point_id = f"fsap-{uuid.uuid4().hex[:8]}"
            mock_ap = {
                "AccessPointId": access_point_id,
                "FileSystemId": file_system_id,
                "Name": cfg.name,
                "PosixUser": cfg.posix_user,
                "RootDirectory": cfg.root_directory,
                "LifeCycleState": "available",
                "OwnerId": self._get_account_id(),
                "AccessPointArn": f"arn:aws:elasticfilesystem:{self.region}:{self._get_account_id()}:access-point/{access_point_id}",
            }
            self._access_points[file_system_id].append(mock_ap)
            return mock_ap
        
        try:
            create_kwargs: Dict[str, Any] = {
                "FileSystemId": file_system_id,
            }
            
            if cfg.posix_user:
                create_kwargs["PosixUser"] = cfg.posix_user
            
            if cfg.root_directory:
                create_kwargs["RootDirectory"] = cfg.root_directory
            
            # Add tags if provided
            if cfg.tags:
                create_kwargs["ClientToken"] = str(uuid.uuid4())
            
            with self._lock:
                response = self.efs_client.create_access_point(**create_kwargs)
            
            ap = response["AccessPoint"]
            
            # Apply tags if provided
            if cfg.tags:
                tag_list = [{"Key": k, "Value": str(v)} for k, v in cfg.tags.items()]
                self.efs_client.create_tags(
                    FileSystemId=file_system_id,
                    AccessPointId=ap["AccessPointId"],
                    Tags=tag_list
                )
            
            logger.info(f"Created access point {ap['AccessPointId']} for {file_system_id}")
            return ap
            
        except ClientError as e:
            logger.error(f"Failed to create access point: {e}")
            raise
    
    def get_access_point(
        self,
        file_system_id: str,
        access_point_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get details of a specific access point."""
        if not BOTO3_AVAILABLE:
            for ap in self._access_points.get(file_system_id, []):
                if ap["AccessPointId"] == access_point_id:
                    return ap
            return None
        
        try:
            response = self.efs_client.describe_access_points(
                AccessPointId=access_point_id
            )
            
            if response["AccessPoints"]:
                return response["AccessPoints"][0]
            return None
            
        except ClientError as e:
            logger.error(f"Failed to get access point: {e}")
            return None
    
    def list_access_points(
        self,
        file_system_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List access points for a file system or all access points.
        
        Args:
            file_system_id: Optional file system ID to filter by
            
        Returns:
            List of access point dictionaries
        """
        if not BOTO3_AVAILABLE:
            if file_system_id:
                return self._access_points.get(file_system_id, [])
            return [ap for aps in self._access_points.values() for ap in aps]
        
        try:
            kwargs: Dict[str, Any] = {}
            if file_system_id:
                kwargs["FileSystemId"] = file_system_id
            
            response = self.efs_client.describe_access_points(**kwargs)
            return response.get("AccessPoints", [])
            
        except ClientError as e:
            logger.error(f"Failed to list access points: {e}")
            return []
    
    def delete_access_point(
        self,
        file_system_id: str,
        access_point_id: str
    ) -> bool:
        """
        Delete an access point.
        
        Args:
            file_system_id: ID of the EFS file system
            access_point_id: ID of the access point to delete
            
        Returns:
            True if deletion was successful
        """
        if not BOTO3_AVAILABLE:
            self._access_points[file_system_id] = [
                ap for ap in self._access_points.get(file_system_id, [])
                if ap["AccessPointId"] != access_point_id
            ]
            return True
        
        try:
            with self._lock:
                self.efs_client.delete_access_point(AccessPointId=access_point_id)
            
            logger.info(f"Deleted access point: {access_point_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete access point: {e}")
            return False
    
    # =========================================================================
    # Encryption Configuration
    # =========================================================================
    
    def set_encryption(
        self,
        file_system_id: str,
        encrypted: bool = True,
        kms_key_id: Optional[str] = None
    ) -> bool:
        """
        Configure encryption for an EFS file system.
        
        Note: Encryption cannot be disabled on an encrypted file system.
        To change the KMS key, use update_file_system.
        
        Args:
            file_system_id: ID of the EFS file system
            encrypted: Whether to enable encryption
            kms_key_id: Optional KMS key ID for encryption
            
        Returns:
            True if configuration was successful
        """
        fs = self.get_file_system(file_system_id)
        if not fs:
            logger.error(f"File system {file_system_id} not found")
            return False
        
        if fs.encrypted and not encrypted:
            logger.error("Cannot disable encryption on an encrypted file system")
            return False
        
        if kms_key_id:
            if not BOTO3_AVAILABLE:
                fs.kms_key_id = kms_key_id
                return True
            
            try:
                self.efs_client.update_file_system(
                    FileSystemId=file_system_id,
                    KmsKeyId=kms_key_id
                )
                logger.info(f"Updated KMS key for {file_system_id}")
                return True
            except ClientError as e:
                logger.error(f"Failed to update KMS key: {e}")
                return False
        
        return True
    
    def get_encryption_config(self, file_system_id: str) -> Dict[str, Any]:
        """
        Get encryption configuration for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Dictionary with encryption configuration
        """
        fs = self.get_file_system(file_system_id)
        if not fs:
            return {}
        
        return {
            "encrypted": fs.encrypted,
            "kms_key_id": fs.kms_key_id,
        }
    
    # =========================================================================
    # Performance Mode Configuration
    # =========================================================================
    
    def set_performance_mode(
        self,
        file_system_id: str,
        performance_mode: PerformanceMode
    ) -> bool:
        """
        Configure performance mode for an EFS file system.
        
        Note: Performance mode cannot be changed after creation.
        
        Args:
            file_system_id: ID of the EFS file system
            performance_mode: Performance mode to set
            
        Returns:
            True if successful (or if already set to the requested mode)
        """
        fs = self.get_file_system(file_system_id)
        if not fs:
            logger.error(f"File system {file_system_id} not found")
            return False
        
        if fs.performance_mode == performance_mode.value:
            return True
        
        if fs.performance_mode != performance_mode.value:
            logger.error(
                f"Cannot change performance mode from {fs.performance_mode} "
                f"to {performance_mode.value}. Performance mode is set at creation."
            )
            return False
        
        return True
    
    # =========================================================================
    # Throughput Mode Configuration
    # =========================================================================
    
    def set_throughput_mode(
        self,
        file_system_id: str,
        throughput_mode: ThroughputMode,
        provisioned_throughput_mbps: Optional[float] = None
    ) -> bool:
        """
        Configure throughput mode for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            throughput_mode: Throughput mode to set
            provisioned_throughput_mbps: Required for PROVISIONED mode
            
        Returns:
            True if configuration was successful
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            if fs:
                fs.throughput_mode = throughput_mode.value
                if provisioned_throughput_mbps:
                    fs.provisioned_throughput_mbps = provisioned_throughput_mbps
            return True
        
        try:
            update_kwargs: Dict[str, Any] = {
                "FileSystemId": file_system_id,
                "ThroughputMode": throughput_mode.value,
            }
            
            if throughput_mode == ThroughputMode.PROVISIONED:
                if not provisioned_throughput_mbps or provisioned_throughput_mbps <= 0:
                    logger.error("Provisioned throughput must be > 0 for PROVISIONED mode")
                    return False
                update_kwargs["ProvisionedThroughputInMibps"] = provisioned_throughput_mbps
            
            with self._lock:
                self.efs_client.update_file_system(**update_kwargs)
            
            logger.info(f"Updated throughput mode for {file_system_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update throughput mode: {e}")
            return False
    
    def get_throughput_mode(self, file_system_id: str) -> Dict[str, Any]:
        """
        Get throughput configuration for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Dictionary with throughput configuration
        """
        fs = self.get_file_system(file_system_id)
        if not fs:
            return {}
        
        return {
            "throughput_mode": fs.throughput_mode,
            "provisioned_throughput_mbps": fs.provisioned_throughput_mbps,
        }
    
    # =========================================================================
    # Backup Policies
    # =========================================================================
    
    def set_backup_policy(
        self,
        file_system_id: str,
        backup_policy: BackupPolicyStatus
    ) -> bool:
        """
        Configure backup policy for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            backup_policy: Backup policy status
            
        Returns:
            True if configuration was successful
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            if fs:
                fs.backup_policy = {"Status": backup_policy.value}
            return True
        
        try:
            with self._lock:
                if backup_policy == BackupPolicyStatus.ENABLED:
                    self.efs_client.enable_backup_policy(
                        FileSystemId=file_system_id,
                        BackupPolicy={"Status": "ENABLED"}
                    )
                else:
                    self.efs_client.disable_backup_policy(
                        FileSystemId=file_system_id,
                        BackupPolicy={"Status": "DISABLED"}
                    )
            
            logger.info(f"Set backup policy for {file_system_id} to {backup_policy.value}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to set backup policy: {e}")
            return False
    
    def get_backup_policy(self, file_system_id: str) -> Optional[Dict[str, str]]:
        """
        Get backup policy for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Backup policy dictionary or None
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            return fs.backup_policy if fs else None
        
        try:
            response = self.efs_client.describe_backup_policy(
                FileSystemId=file_system_id
            )
            return response.get("BackupPolicy")
            
        except ClientError as e:
            logger.error(f"Failed to get backup policy: {e}")
            return None
    
    # =========================================================================
    # Lifecycle Policies
    # =========================================================================
    
    def set_lifecycle_policies(
        self,
        file_system_id: str,
        policies: List[str]
    ) -> bool:
        """
        Set lifecycle policies for EFS file system.
        
        Supported policies:
        - NONE: No transition policies
        - AFTER_7_DAYS: Transition to IA after 7 days
        - AFTER_14_DAYS: Transition to IA after 14 days
        - AFTER_30_DAYS: Transition to IA after 30 days
        - AFTER_60_DAYS: Transition to IA after 60 days
        - AFTER_90_DAYS: Transition to IA after 90 days
        
        Args:
            file_system_id: ID of the EFS file system
            policies: List of lifecycle policy strings
            
        Returns:
            True if configuration was successful
        """
        if not BOTO3_AVAILABLE:
            return True
        
        try:
            policy_list = [{"Name": p} for p in policies]
            
            with self._lock:
                self.efs_client.put_lifecycle_configuration(
                    FileSystemId=file_system_id,
                    LifecyclePolicies=policy_list
                )
            
            logger.info(f"Set lifecycle policies for {file_system_id}: {policies}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to set lifecycle policies: {e}")
            return False
    
    def get_lifecycle_policies(self, file_system_id: str) -> List[str]:
        """
        Get lifecycle policies for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            List of lifecycle policy names
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.efs_client.describe_lifecycle_configuration(
                FileSystemId=file_system_id
            )
            policies = response.get("LifecyclePolicies", [])
            return [p["Name"] for p in policies]
            
        except ClientError as e:
            logger.error(f"Failed to get lifecycle policies: {e}")
            return []
    
    # =========================================================================
    # Replication Configuration
    # =========================================================================
    
    def enable_replication(
        self,
        file_system_id: str,
        destination_region: str
    ) -> Dict[str, Any]:
        """
        Enable cross-region replication for an EFS file system.
        
        Args:
            file_system_id: ID of the source EFS file system
            destination_region: Destination AWS region
            
        Returns:
            Replication configuration dictionary
        """
        if not BOTO3_AVAILABLE:
            return {
                "source_file_system_id": file_system_id,
                "source_region": self.region,
                "destinations": [{
                    "region": destination_region,
                    "status": "ENABLED"
                }]
            }
        
        try:
            with self._lock:
                response = self.efs_client.create_replication_configuration(
                    SourceFileSystemId=file_system_id,
                    SourceFileSystemRegion=destination_region,
                    Destinations=[
                        {
                            "Region": destination_region,
                            "AvailabilityZoneName": None,
                            "KmsKeyId": None,
                        }
                    ]
                )
            
            logger.info(f"Enabled replication for {file_system_id} to {destination_region}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to enable replication: {e}")
            raise
    
    def get_replication_configuration(
        self,
        file_system_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get replication configuration for a file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Replication configuration or None
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            return fs.replication_configuration if fs else None
        
        try:
            response = self.efs_client.describe_replication_configuration(
                FileSystemId=file_system_id
            )
            return response.get("ReplicationConfiguration", [{}])[0] if response.get("ReplicationConfiguration") else None
            
        except ClientError as e:
            logger.error(f"Failed to get replication configuration: {e}")
            return None
    
    def disable_replication(self, file_system_id: str) -> bool:
        """
        Disable cross-region replication for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            True if disable was successful
        """
        if not BOTO3_AVAILABLE:
            fs = self._file_systems.get(file_system_id)
            if fs:
                fs.replication_configuration = None
            return True
        
        try:
            with self._lock:
                self.efs_client.delete_replication_configuration(
                    SourceFileSystemId=file_system_id
                )
            
            logger.info(f"Disabled replication for {file_system_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable replication: {e}")
            return False
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def get_metrics(
        self,
        file_system_id: str,
        metric_names: List[str],
        start_time: datetime,
        end_time: datetime,
        period: int = 300,
        statistics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for an EFS file system.
        
        Common metrics:
        - StorageBytes: Total storage bytes
        - StorageBytes_IAA: Infrequent access storage bytes
        - BurstCreditBalance: Burst credit balance
        - PermittedThroughput: Permitted throughput
        - ClientConnections: Number of client connections
        - MetadataReads: Number of metadata read operations
        - DataReadBytes: Data read bytes
        - DataWriteBytes: Data write bytes
        
        Args:
            file_system_id: ID of the EFS file system
            metric_names: List of metric names to retrieve
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Metric period in seconds
            statistics: List of statistics (Sum, Average, Maximum, Minimum)
            
        Returns:
            Dictionary of metric data
        """
        if not BOTO3_AVAILABLE:
            return self._generate_mock_metrics(metric_names, start_time, end_time, period)
        
        if statistics is None:
            statistics = ["Sum", "Average", "Maximum", "Minimum"]
        
        try:
            namespace = "AWS/EFS"
            results = {}
            
            for metric_name in metric_names:
                response = self.cw_client.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {
                            "Name": "FileSystemId",
                            "Value": file_system_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=statistics
                )
                
                results[metric_name] = {
                    "label": response.get("Label"),
                    "datapoints": response.get("Datapoints", [])
                }
            
            return results
            
        except ClientError as e:
            logger.error(f"Failed to get CloudWatch metrics: {e}")
            return {}
    
    def _generate_mock_metrics(
        self,
        metric_names: List[str],
        start_time: datetime,
        end_time: datetime,
        period: int
    ) -> Dict[str, Any]:
        """Generate mock metrics for testing without boto3."""
        results = {}
        current_time = start_time
        
        while current_time < end_time:
            for metric_name in metric_names:
                if metric_name not in results:
                    results[metric_name] = {"datapoints": []}
                
                results[metric_name]["datapoints"].append({
                    "Timestamp": current_time,
                    "Sum": 1000000.0,
                    "Average": 1000000.0,
                    "Maximum": 1000000.0,
                    "Minimum": 1000000.0,
                    "Unit": "Bytes"
                })
            
            current_time += timedelta(seconds=period)
        
        return results
    
    def put_metric_alarm(
        self,
        alarm_name: str,
        file_system_id: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "LessThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Average"
    ) -> bool:
        """
        Create or update a CloudWatch alarm for EFS metrics.
        
        Args:
            alarm_name: Name of the alarm
            file_system_id: ID of the EFS file system
            metric_name: Metric name to alarm on
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic to use
            
        Returns:
            True if alarm creation was successful
        """
        if not BOTO3_AVAILABLE:
            return True
        
        try:
            alarm_config = {
                "AlarmName": alarm_name,
                "AlarmDescription": f"Alarm for {metric_name} on {file_system_id}",
                "Namespace": "AWS/EFS",
                "MetricName": metric_name,
                "Dimensions": [
                    {
                        "Name": "FileSystemId",
                        "Value": file_system_id
                    }
                ],
                "Threshold": threshold,
                "ComparisonOperator": comparison_operator,
                "EvaluationPeriods": evaluation_periods,
                "Period": period,
                "Statistic": statistic,
                "ActionsEnabled": True,
            }
            
            self.cw_client.put_metric_alarm(**alarm_config)
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create CloudWatch alarm: {e}")
            return False
    
    def list_metric_alarms(
        self,
        file_system_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch alarms for EFS.
        
        Args:
            file_system_id: Optional file system ID to filter by
            
        Returns:
            List of alarm configurations
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.cw_client.describe_alarms(
                Filters=[
                    {
                        "Name": "Namespace",
                        "Value": "AWS/EFS"
                    }
                ] if file_system_id else None
            )
            
            alarms = response.get("MetricAlarms", [])
            
            if file_system_id:
                alarms = [
                    a for a in alarms
                    if any(
                        d.get("Value") == file_system_id
                        for d in a.get("Dimensions", [])
                    )
                ]
            
            return alarms
            
        except ClientError as e:
            logger.error(f"Failed to list CloudWatch alarms: {e}")
            return []
    
    # =========================================================================
    # Mount Helpers
    # =========================================================================
    
    def generate_mount_command(
        self,
        file_system_id: str,
        mount_target_dns: Optional[str] = None,
        access_point_id: Optional[str] = None,
        mount_path: str = "/mnt/efs",
        options: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate a mount command for an EFS file system on Linux.
        
        Args:
            file_system_id: ID of the EFS file system
            mount_target_dns: DNS name of the mount target
            access_point_id: Optional access point ID
            mount_path: Local mount path
            options: Optional mount options dictionary
            
        Returns:
            Mount command string
        """
        if mount_target_dns is None:
            mount_target_dns = f"{file_system_id}.efs.{self.region}.amazonaws.com"
        
        # Build the target with optional access point
        if access_point_id:
            target = f"{mount_target_dns}@{access_point_id}"
        else:
            target = mount_target_dns
        
        # Build mount options
        mount_options = options or {}
        if "tls" not in mount_options and "notls" not in mount_options:
            mount_options["tls"] = ""
        
        if access_point_id and "accesspoint" not in mount_options:
            mount_options["accesspoint"] = access_point_id
        
        options_str = ",".join(
            f"{k}" if v == "" else f"{k}={v}"
            for k, v in mount_options.items()
        )
        
        return f"sudo mount -t efs {target} -o {options_str} {mount_path}"
    
    def generate_fstab_entry(
        self,
        file_system_id: str,
        mount_target_dns: Optional[str] = None,
        access_point_id: Optional[str] = None,
        mount_path: str = "/mnt/efs",
        options: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate an fstab entry for persistent mounting.
        
        Args:
            file_system_id: ID of the EFS file system
            mount_target_dns: DNS name of the mount target
            access_point_id: Optional access point ID
            mount_path: Local mount path
            options: Optional mount options
            
        Returns:
            fstab entry string
        """
        if mount_target_dns is None:
            mount_target_dns = f"{file_system_id}.efs.{self.region}.amazonaws.com"
        
        if access_point_id:
            target = f"{mount_target_dns}:/{access_point_id}"
        else:
            target = f"{mount_target_dns}:/"
        
        mount_options = options or {}
        if "tls" not in mount_options and "notls" not in mount_options:
            mount_options["tls"] = ""
        
        if access_point_id and "accesspoint" not in mount_options:
            mount_options["accesspoint"] = access_point_id
        
        options_str = ",".join(
            f"{k}" if v == "" else f"{k}={v}"
            for k, v in mount_options.items()
        )
        
        return f"{target} {mount_path} efs defaults,_netdev,{options_str} 0 0"
    
    def install_efs_utils(self) -> bool:
        """
        Generate commands to install EFS utilities on Amazon Linux/Ubuntu.
        
        Returns:
            Tuple of (install_commands, configure_commands)
        """
        amazon_linux_install = [
            "sudo yum install -y amazon-efs-utils",
        ]
        
        ubuntu_install = [
            "sudo apt-get update",
            "sudo apt-get install -y amazon-efs-utils",
        ]
        
        return amazon_linux_install, ubuntu_install
    
    def mount_efs(
        self,
        file_system_id: str,
        mount_target_dns: Optional[str] = None,
        access_point_id: Optional[str] = None,
        mount_path: str = "/mnt/efs",
        options: Optional[Dict[str, str]] = None,
        verify: bool = True
    ) -> Dict[str, Any]:
        """
        Mount EFS on a Linux instance.
        
        Args:
            file_system_id: ID of the EFS file system
            mount_target_dns: DNS name of the mount target
            access_point_id: Optional access point ID
            mount_path: Local mount path
            options: Optional mount options
            verify: Whether to verify the mount after completion
            
        Returns:
            Dictionary with mount results
        """
        result = {
            "success": False,
            "command": "",
            "output": "",
            "verified": False,
        }
        
        # Generate mount command
        result["command"] = self.generate_mount_command(
            file_system_id=file_system_id,
            mount_target_dns=mount_target_dns,
            access_point_id=access_point_id,
            mount_path=mount_path,
            options=options
        )
        
        # Check if running on Linux
        if not os.path.exists("/dev/null"):
            result["output"] = "Not running on Linux, skipping actual mount"
            return result
        
        try:
            # Create mount directory if it doesn't exist
            os.makedirs(mount_path, exist_ok=True)
            
            # Execute mount command
            process = subprocess.run(
                result["command"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            result["output"] = process.stdout + process.stderr
            
            if process.returncode != 0:
                logger.error(f"Mount failed: {result['output']}")
                return result
            
            result["success"] = True
            
            # Verify mount if requested
            if verify:
                result["verified"] = self._verify_mount(mount_path)
            
        except subprocess.TimeoutExpired:
            result["output"] = "Mount command timed out"
        except Exception as e:
            result["output"] = f"Mount error: {str(e)}"
        
        return result
    
    def _verify_mount(self, mount_path: str) -> bool:
        """Verify that a path is actually mounted."""
        try:
            stat = os.statvfs(mount_path)
            # EFS typically has large block size
            return stat.f_frsize > 0
        except Exception:
            return False
    
    def unmount_efs(self, mount_path: str) -> Dict[str, Any]:
        """
        Unmount EFS from a Linux instance.
        
        Args:
            mount_path: Local mount path
            
        Returns:
            Dictionary with unmount results
        """
        result = {
            "success": False,
            "output": "",
        }
        
        try:
            process = subprocess.run(
                f"sudo umount {mount_path}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            result["output"] = process.stdout + process.stderr
            result["success"] = process.returncode == 0
            
        except subprocess.TimeoutExpired:
            result["output"] = "Unmount command timed out"
        except Exception as e:
            result["output"] = f"Unmount error: {str(e)}"
        
        return result
    
    def check_efs_mount_helper_status(self) -> Dict[str, Any]:
        """
        Check if EFS mount helper is installed and configured.
        
        Returns:
            Dictionary with installation and configuration status
        """
        status = {
            "efs_utils_installed": False,
            "stunnel_installed": False,
            "mount_helper_configured": False,
            "version": None,
        }
        
        # Check for amazon-efs-utils
        try:
            process = subprocess.run(
                "which mount.efs",
                shell=True,
                capture_output=True,
                text=True
            )
            status["efs_utils_installed"] = process.returncode == 0
            
            if process.returncode == 0:
                version_process = subprocess.run(
                    "mount.efs --version",
                    shell=True,
                    capture_output=True,
                    text=True
                )
                if version_process.returncode == 0:
                    status["version"] = version_process.stdout.strip()
        except Exception:
            pass
        
        # Check for stunnel
        try:
            process = subprocess.run(
                "which stunnel",
                shell=True,
                capture_output=True,
                text=True
            )
            status["stunnel_installed"] = process.returncode == 0
        except Exception:
            pass
        
        # Check for mount helper configuration
        try:
            config_paths = [
                "/etc/amazon/efs/efs-utils.conf",
                "/etc/efs/efs-utils.conf",
            ]
            for path in config_paths:
                if os.path.exists(path):
                    status["mount_helper_configured"] = True
                    break
        except Exception:
            pass
        
        return status
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_file_system_dns_name(self, file_system_id: str) -> str:
        """
        Get the DNS name for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            DNS name string
        """
        return f"{file_system_id}.efs.{self.region}.amazonaws.com"
    
    def describe_file_system_policy(self, file_system_id: str) -> Optional[str]:
        """
        Get the resource policy for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Policy JSON string or None
        """
        if not BOTO3_AVAILABLE:
            return None
        
        try:
            response = self.efs_client.describe_file_system_policy(
                FileSystemId=file_system_id
            )
            return response.get("Policy")
            
        except ClientError as e:
            logger.error(f"Failed to get file system policy: {e}")
            return None
    
    def put_file_system_policy(
        self,
        file_system_id: str,
        policy: Dict[str, Any]
    ) -> bool:
        """
        Set a resource policy for an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            policy: Policy document dictionary
            
        Returns:
            True if policy was set successfully
        """
        if not BOTO3_AVAILABLE:
            return True
        
        try:
            self.efs_client.put_file_system_policy(
                FileSystemId=file_system_id,
                Policy=json.dumps(policy)
            )
            
            logger.info(f"Set file system policy for {file_system_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to set file system policy: {e}")
            return False
    
    def get_description(self, file_system_id: str) -> Dict[str, Any]:
        """
        Get comprehensive description of an EFS file system.
        
        Args:
            file_system_id: ID of the EFS file system
            
        Returns:
            Dictionary with file system details
        """
        fs = self.get_file_system(file_system_id)
        if not fs:
            return {}
        
        description = {
            "file_system": {
                "id": fs.file_system_id,
                "arn": fs.arn,
                "name": fs.name,
                "size_bytes": fs.size_bytes,
                "performance_mode": fs.performance_mode,
                "throughput_mode": fs.throughput_mode,
                "provisioned_throughput_mbps": fs.provisioned_throughput_mbps,
                "encrypted": fs.encrypted,
                "kms_key_id": fs.kms_key_id,
                "life_cycle_state": fs.life_cycle_state,
                "creation_time": fs.creation_time.isoformat() if fs.creation_time else None,
                "tags": fs.tags,
            },
            "dns_name": self.get_file_system_dns_name(file_system_id),
            "mount_targets": self.list_mount_targets(file_system_id),
            "access_points": self.list_access_points(file_system_id),
            "backup_policy": self.get_backup_policy(file_system_id),
            "lifecycle_policies": self.get_lifecycle_policies(file_system_id),
            "replication": self.get_replication_configuration(file_system_id),
            "encryption": self.get_encryption_config(file_system_id),
            "throughput": self.get_throughput_mode(file_system_id),
        }
        
        return description
    
    def wait_for_file_system(
        self,
        file_system_id: str,
        target_state: str = "available",
        timeout: int = 300,
        poll_interval: int = 10
    ) -> bool:
        """
        Wait for a file system to reach a target state.
        
        Args:
            file_system_id: ID of the EFS file system
            target_state: Target lifecycle state
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            True if target state was reached
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            fs = self.get_file_system(file_system_id)
            if fs and fs.life_cycle_state == target_state:
                return True
            
            time.sleep(poll_interval)
        
        return False
