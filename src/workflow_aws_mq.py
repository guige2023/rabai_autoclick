"""
AWS Amazon MQ Integration Module for Workflow System

Implements an MQIntegration class with:
1. Broker management: Create/manage message brokers
2. Configuration: Manage broker configurations
3. Users: Manage broker users
4. Deployments: Single-instance and clustered deployments
5. Tags: Manage resource tags
6. Encryption: Broker encryption
7. CloudWatch integration: Broker metrics and monitoring
8. Logs: Broker logging configuration
9. Security: Security groups for Amazon MQ
10. Reboot: Broker reboot operations

Commit: 'feat(aws-mq): add Amazon MQ with broker management, configurations, users, deployments, encryption, CloudWatch, logs, security groups, reboot'
"""

import uuid
import json
import time
import logging
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


class BrokerEngine(Enum):
    """Supported Amazon MQ broker engines."""
    ACTIVEMQ = "ActiveMQ"
    RABBITMQ = "RabbitMQ"


class BrokerInstanceType(Enum):
    """Amazon MQ broker instance types."""
    T2_MICRO = "mq.t2.micro"
    T2_SMALL = "mq.t2.small"
    T2_MEDIUM = "mq.t2.medium"
    T3_MICRO = "mq.t3.micro"
    T3_SMALL = "mq.t3.small"
    T3_MEDIUM = "mq.t3.medium"
    M4_LARGE = "mq.m4.large"
    M4_XLARGE = "mq.m4.xlarge"
    M4_2XLARGE = "mq.m4.2xlarge"
    M5_LARGE = "mq.m5.large"
    M5_XLARGE = "mq.m5.xlarge"
    M5_2XLARGE = "mq.m5.2xlarge"
    M5_4XLARGE = "mq.m5.4xlarge"


class DeploymentMode(Enum):
    """Amazon MQ deployment modes."""
    SINGLE_INSTANCE = "SINGLE_INSTANCE"
    ACTIVE_STANDBY_MULTI_AZ = "ACTIVE_STANDBY_MULTI_AZ"
    CLUSTER = "CLUSTER"
    CLUSTER_MULTI_AZ = "CLUSTER_MULTI_AZ"


class BrokerState(Enum):
    """Amazon MQ broker states."""
    CREATION_IN_PROGRESS = "CREATION_IN_PROGRESS"
    CREATION_FAILED = "CREATION_FAILED"
    DELETION_IN_PROGRESS = "DELETION_IN_PROGRESS"
    RUNNING = "RUNNING"
    REBOOT_IN_PROGRESS = "REBOOT_IN_PROGRESS"


class LogType(Enum):
    """Types of logs for Amazon MQ."""
    GENERAL = "GENERAL"
    AUDIT = "AUDIT"


class EncryptionAlgorithm(Enum):
    """Encryption algorithms for Amazon MQ."""
    AES_128 = "AES_128"
    AES_256 = "AES_256"
    RSAES_OAEP_SHA_256 = "RSAES_OAEP_SHA_256"
    RSAES_OAEP_SHA_512 = "RSAES_OAEP_SHA_512"


@dataclass
class MQConfig:
    """Configuration for Amazon MQ connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class BrokerConfig:
    """Configuration for creating an Amazon MQ broker."""
    broker_name: str
    engine_type: BrokerEngine = BrokerEngine.ACTIVEMQ
    engine_version: str = "5.17.6"
    instance_type: BrokerInstanceType = BrokerInstanceType.T2_MICRO
    deployment_mode: DeploymentMode = DeploymentMode.SINGLE_INSTANCE
    master_username: str = "admin"
    master_password: str = ""
    vpc_id: Optional[str] = None
    subnet_ids: List[str] = field(default_factory=list)
    security_groups: List[str] = field(default_factory=list)
    publicly_accessible: bool = False
    auto_minor_version_upgrade: bool = True
    configuration_id: Optional[str] = None
    configuration_revision: Optional[int] = None
    kms_key_id: Optional[str] = None
    encryption_enabled: bool = True
    encryption_algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_256
    log_general: bool = True
    log_audit: bool = False
    host_instance_type: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class UserConfig:
    """Configuration for creating an Amazon MQ user."""
    broker_name: str
    username: str
    password: str = ""
    console_access: bool = False
    groups: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConfigurationConfig:
    """Configuration for creating a broker configuration."""
    name: str
    engine_type: BrokerEngine = BrokerEngine.ACTIVEMQ
    engine_version: Optional[str] = None
    data: Optional[str] = None
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TagsConfig:
    """Configuration for managing resource tags."""
    resource_type: str  # "broker", "configuration", "user"
    resource_id: str
    tags: Dict[str, str] = field(default_factory=dict)


class MQIntegration:
    """
    AWS Amazon MQ Integration.
    
    Provides comprehensive Amazon MQ broker management including:
    - Broker lifecycle management (create, delete, reboot)
    - Configuration management for brokers
    - User management
    - Single-instance and clustered deployments
    - Resource tagging
    - Encryption at rest
    - CloudWatch monitoring integration
    - Log configuration
    - Security group management
    """
    
    def __init__(self, config: Optional[MQConfig] = None):
        """
        Initialize the MQ integration.
        
        Args:
            config: MQ configuration. If None, uses default config with
                   credentials from environment or IAM role.
        """
        self.config = config or MQConfig()
        self._mq_client = None
        self._cw_client = None
        self._ec2_client = None
        self._lock = threading.RLock()
        self._cache = {}
        self._cache_ttl = 60
    
    @property
    def mq_client(self):
        """Get or create MQ client with lazy initialization."""
        if self._mq_client is None:
            with self._lock:
                if self._mq_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    
                    if BOTO3_AVAILABLE:
                        self._mq_client = boto3.client("mq", **kwargs)
                    else:
                        raise RuntimeError("boto3 is not available. Install with: pip install boto3")
        return self._mq_client
    
    @property
    def cw_client(self):
        """Get or create CloudWatch client for monitoring."""
        if self._cw_client is None:
            with self._lock:
                if self._cw_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    
                    if BOTO3_AVAILABLE:
                        self._cw_client = boto3.client("cloudwatch", **kwargs)
                    else:
                        raise RuntimeError("boto3 is not available")
        return self._cw_client
    
    @property
    def ec2_client(self):
        """Get or create EC2 client for security group operations."""
        if self._ec2_client is None:
            with self._lock:
                if self._ec2_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name
                    
                    if BOTO3_AVAILABLE:
                        self._ec2_client = boto3.client("ec2", **kwargs)
                    else:
                        raise RuntimeError("boto3 is not available")
        return self._ec2_client
    
    def _get_cache_key(self, operation: str, identifier: str) -> str:
        """Generate a cache key for an operation."""
        return f"{operation}:{identifier}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self._cache:
            return False
        entry = self._cache[cache_key]
        return (datetime.now() - entry["timestamp"]).total_seconds() < self._cache_ttl
    
    def _set_cache(self, cache_key: str, value: Any) -> None:
        """Set a cache entry with current timestamp."""
        self._cache[cache_key] = {
            "value": value,
            "timestamp": datetime.now()
        }
    
    def _invalidate_cache(self, pattern: Optional[str] = None) -> None:
        """Invalidate cache entries matching pattern."""
        if pattern is None:
            self._cache.clear()
        else:
            keys_to_remove = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_remove:
                del self._cache[key]
    
    # ========================================================================
    # Broker Management
    # ========================================================================
    
    def create_broker(self, config: BrokerConfig) -> Dict[str, Any]:
        """
        Create a new Amazon MQ broker.
        
        Args:
            config: Broker configuration
            
        Returns:
            Dict containing created broker information
        """
        try:
            params = {
                "BrokerName": config.broker_name,
                "EngineType": config.engine_type.value,
                "EngineVersion": config.engine_version,
                "HostInstanceType": config.instance_type.value,
                "DeploymentMode": config.deployment_mode.value,
                "MasterUsername": config.master_username,
                "MasterUserPassword": config.master_password,
                "AutoMinorVersionUpgrade": config.auto_minor_version_upgrade,
                "PubliclyAccessible": config.publicly_accessible,
                "Tags": [
                    {"Key": k, "Value": v} for k, v in config.tags.items()
                ] if config.tags else [],
            }
            
            if config.vpc_id:
                params["VpcId"] = config.vpc_id
            
            if config.subnet_ids:
                params["SubnetIds"] = config.subnet_ids
            
            if config.security_groups:
                params["SecurityGroups"] = config.security_groups
            
            if config.configuration_id:
                params["Configuration"] = {
                    "Id": config.configuration_id,
                }
                if config.configuration_revision:
                    params["Configuration"]["Revision"] = config.configuration_revision
            
            if config.kms_key_id:
                params["KmsKeyId"] = config.kms_key_id
            
            params["EncryptionOptions"] = {
                "Enabled": config.encryption_enabled,
                "EncryptionAlgorithm": config.encryption_algorithm.value,
            }
            
            if not config.encryption_enabled and config.kms_key_id:
                params["EncryptionOptions"]["KmsKeyId"] = config.kms_key_id
            
            logs_params = {}
            if config.log_general:
                logs_params["General"] = True
            if config.log_audit:
                logs_params["Audit"] = True
            if logs_params:
                params["Logs"] = logs_params
            
            response = self.mq_client.create_broker(**params)
            self._invalidate_cache("brokers")
            
            logger.info(f"Created broker: {config.broker_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Error creating broker: {e}")
            raise
    
    def describe_broker(self, broker_name: str, refresh: bool = False) -> Dict[str, Any]:
        """
        Get detailed information about a broker.
        
        Args:
            broker_name: Name of the broker
            refresh: Force refresh from AWS
            
        Returns:
            Dict containing broker details
        """
        cache_key = self._get_cache_key("broker", broker_name)
        
        if not refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]["value"]
        
        try:
            response = self.mq_client.describe_broker(BrokerName=broker_name)
            self._set_cache(cache_key, response)
            return response
        except ClientError as e:
            logger.error(f"Error describing broker: {e}")
            raise
    
    def list_brokers(self, refresh: bool = False) -> List[Dict[str, Any]]:
        """
        List all Amazon MQ brokers.
        
        Args:
            refresh: Force refresh from AWS
            
        Returns:
            List of broker information dictionaries
        """
        cache_key = "brokers:list"
        
        if not refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]["value"]
        
        try:
            brokers = []
            paginator = self.mq_client.get_paginator("list_brokers")
            
            for page in paginator.paginate():
                brokers.extend(page.get("BrokerSummaries", []))
            
            self._set_cache(cache_key, brokers)
            return brokers
        except ClientError as e:
            logger.error(f"Error listing brokers: {e}")
            raise
    
    def delete_broker(self, broker_name: str) -> Dict[str, Any]:
        """
        Delete an Amazon MQ broker.
        
        Args:
            broker_name: Name of the broker to delete
            
        Returns:
            Dict containing operation result
        """
        try:
            response = self.mq_client.delete_broker(BrokerName=broker_name)
            self._invalidate_cache("brokers")
            self._invalidate_cache(f"broker:{broker_name}")
            
            logger.info(f"Deleted broker: {broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting broker: {e}")
            raise
    
    def get_broker_state(self, broker_name: str) -> BrokerState:
        """
        Get the current state of a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            BrokerState enum value
        """
        try:
            info = self.describe_broker(broker_name)
            state_str = info.get("BrokerState", "")
            return BrokerState(state_str)
        except ClientError:
            return BrokerState.CREATION_FAILED
    
    def wait_for_broker_state(
        self, 
        broker_name: str, 
        target_states: List[BrokerState], 
        timeout: int = 600,
        poll_interval: int = 30
    ) -> bool:
        """
        Wait for broker to reach a target state.
        
        Args:
            broker_name: Name of the broker
            target_states: List of acceptable target states
            timeout: Maximum seconds to wait
            poll_interval: Seconds between status checks
            
        Returns:
            True if target state reached, False if timeout
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            current_state = self.get_broker_state(broker_name)
            
            if current_state in target_states:
                return True
            
            logger.info(f"Broker {broker_name} state: {current_state.value}, waiting...")
            time.sleep(poll_interval)
        
        return False
    
    # ========================================================================
    # Reboot Operations
    # ========================================================================
    
    def reboot_broker(self, broker_name: str, reboot_ standby: bool = True) -> Dict[str, Any]:
        """
        Reboot a broker.
        
        Args:
            broker_name: Name of the broker to reboot
            reboot_standby: Whether to reboot standby brokers in multi-AZ
            
        Returns:
            Dict containing operation result
        """
        try:
            response = self.mq_client.reboot_broker(
                BrokerName=broker_name,
                RebootRebootStandbyBrokers=reboot_standby
            )
            
            logger.info(f"Rebooting broker: {broker_name}")
            self._invalidate_cache(f"broker:{broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error rebooting broker: {e}")
            raise
    
    # ========================================================================
    # Configuration Management
    # ========================================================================
    
    def create_configuration(
        self, 
        name: str, 
        engine_type: BrokerEngine = BrokerEngine.ACTIVEMQ,
        engine_version: Optional[str] = None,
        description: str = ""
    ) -> Dict[str, Any]:
        """
        Create a new broker configuration.
        
        Args:
            name: Name of the configuration
            engine_type: Type of broker engine
            engine_version: Version of the broker engine
            description: Description of the configuration
            
        Returns:
            Dict containing created configuration information
        """
        try:
            params = {
                "EngineType": engine_type.value,
                "EngineVersion": engine_version or "5.17.6",
                "Name": name,
            }
            
            if description:
                params["Description"] = description
            
            response = self.mq_client.create_configuration(**params)
            
            logger.info(f"Created configuration: {name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating configuration: {e}")
            raise
    
    def describe_configuration(
        self, 
        configuration_id: str, 
        refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get details of a configuration.
        
        Args:
            configuration_id: ID of the configuration
            refresh: Force refresh from AWS
            
        Returns:
            Dict containing configuration details
        """
        cache_key = self._get_cache_key("config", configuration_id)
        
        if not refresh and self._is_cache_valid(cache_key):
            return self._cache[cache_key]["value"]
        
        try:
            response = self.mq_client.describe_configuration(ConfigurationId=configuration_id)
            self._set_cache(cache_key, response)
            return response
        except ClientError as e:
            logger.error(f"Error describing configuration: {e}")
            raise
    
    def describe_configuration_revision(
        self, 
        configuration_id: str, 
        revision: int
    ) -> Dict[str, Any]:
        """
        Get a specific revision of a configuration.
        
        Args:
            configuration_id: ID of the configuration
            revision: Revision number
            
        Returns:
            Dict containing configuration revision details
        """
        try:
            response = self.mq_client.describe_configuration_revision(
                ConfigurationId=configuration_id,
                ConfigurationRevision=revision
            )
            return response
        except ClientError as e:
            logger.error(f"Error describing configuration revision: {e}")
            raise
    
    def list_configurations(self) -> List[Dict[str, Any]]:
        """
        List all configurations.
        
        Returns:
            List of configuration information dictionaries
        """
        try:
            response = self.mq_client.list_configurations()
            return response.get("Configurations", [])
        except ClientError as e:
            logger.error(f"Error listing configurations: {e}")
            raise
    
    def update_configuration(
        self, 
        configuration_id: str, 
        data: str
    ) -> Dict[str, Any]:
        """
        Update a configuration with new data.
        
        Args:
            configuration_id: ID of the configuration
            data: New configuration data (XML for ActiveMQ)
            
        Returns:
            Dict containing update result
        """
        try:
            response = self.mq_client.update_configuration(
                ConfigurationId=configuration_id,
                Data=data
            )
            
            logger.info(f"Updated configuration: {configuration_id}")
            self._invalidate_cache(f"config:{configuration_id}")
            return response
        except ClientError as e:
            logger.error(f"Error updating configuration: {e}")
            raise
    
    def delete_configuration(self, configuration_id: str) -> Dict[str, Any]:
        """
        Delete a configuration.
        
        Args:
            configuration_id: ID of the configuration to delete
            
        Returns:
            Dict containing operation result
        """
        try:
            response = self.mq_client.delete_configuration(ConfigurationId=configuration_id)
            
            logger.info(f"Deleted configuration: {configuration_id}")
            self._invalidate_cache(f"config:{configuration_id}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting configuration: {e}")
            raise
    
    # ========================================================================
    # User Management
    # ========================================================================
    
    def create_user(
        self, 
        broker_name: str, 
        username: str, 
        password: str = "",
        console_access: bool = False,
        groups: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new user for a broker.
        
        Args:
            broker_name: Name of the broker
            username: Username for the new user
            password: Password for the new user
            console_access: Whether user can access ActiveMQ console
            groups: List of groups the user belongs to
            
        Returns:
            Dict containing operation result
        """
        try:
            params = {
                "BrokerName": broker_name,
                "Username": username,
                "Password": password,
                "ConsoleAccess": console_access,
            }
            
            if groups:
                params["Groups"] = groups
            
            response = self.mq_client.create_user(**params)
            
            logger.info(f"Created user {username} for broker {broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating user: {e}")
            raise
    
    def describe_user(self, broker_name: str, username: str) -> Dict[str, Any]:
        """
        Get details of a broker user.
        
        Args:
            broker_name: Name of the broker
            username: Username to describe
            
        Returns:
            Dict containing user details
        """
        try:
            response = self.mq_client.describe_user(BrokerName=broker_name, Username=username)
            return response
        except ClientError as e:
            logger.error(f"Error describing user: {e}")
            raise
    
    def list_users(self, broker_name: str) -> List[Dict[str, Any]]:
        """
        List all users for a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            List of user information dictionaries
        """
        try:
            response = self.mq_client.list_users(BrokerName=broker_name)
            return response.get("Users", [])
        except ClientError as e:
            logger.error(f"Error listing users: {e}")
            raise
    
    def update_user(
        self, 
        broker_name: str, 
        username: str, 
        password: Optional[str] = None,
        console_access: Optional[bool] = None,
        groups: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Update a broker user.
        
        Args:
            broker_name: Name of the broker
            username: Username to update
            password: New password (if changing)
            console_access: New console access setting
            groups: New groups list
            
        Returns:
            Dict containing operation result
        """
        try:
            params = {
                "BrokerName": broker_name,
                "Username": username,
            }
            
            if password:
                params["Password"] = password
            
            if console_access is not None:
                params["ConsoleAccess"] = console_access
            
            if groups is not None:
                params["Groups"] = groups
            
            response = self.mq_client.update_user(**params)
            
            logger.info(f"Updated user {username} for broker {broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error updating user: {e}")
            raise
    
    def delete_user(self, broker_name: str, username: str) -> Dict[str, Any]:
        """
        Delete a broker user.
        
        Args:
            broker_name: Name of the broker
            username: Username to delete
            
        Returns:
            Dict containing operation result
        """
        try:
            response = self.mq_client.delete_user(BrokerName=broker_name, Username=username)
            
            logger.info(f"Deleted user {username} from broker {broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting user: {e}")
            raise
    
    # ========================================================================
    # Tags Management
    # ========================================================================
    
    def list_tags(self, resource_type: str, resource_name: str) -> Dict[str, str]:
        """
        List tags for a resource.
        
        Args:
            resource_type: Type of resource ("broker" or "configuration")
            resource_name: Name of the resource
            
        Returns:
            Dict of tag key-value pairs
        """
        try:
            if resource_type == "broker":
                response = self.mq_client.list_tags(ResourceArn=self._get_broker_arn(resource_name))
            elif resource_type == "configuration":
                response = self.mq_client.list_tags(ResourceArn=self._get_configuration_arn(resource_name))
            else:
                raise ValueError(f"Unsupported resource type: {resource_type}")
            
            tags = {}
            for tag in response.get("Tags", []):
                tags[tag["Key"]] = tag["Value"]
            return tags
        except ClientError as e:
            logger.error(f"Error listing tags: {e}")
            raise
    
    def add_tags(
        self, 
        resource_type: str, 
        resource_name: str, 
        tags: Dict[str, str]
    ) -> None:
        """
        Add tags to a resource.
        
        Args:
            resource_type: Type of resource ("broker" or "configuration")
            resource_name: Name of the resource
            tags: Dict of tags to add
        """
        try:
            tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            if resource_type == "broker":
                self.mq_client.tag_resource(
                    ResourceArn=self._get_broker_arn(resource_name),
                    Tags=tag_list
                )
            elif resource_type == "configuration":
                self.mq_client.tag_resource(
                    ResourceArn=self._get_configuration_arn(resource_name),
                    Tags=tag_list
                )
            else:
                raise ValueError(f"Unsupported resource type: {resource_type}")
            
            logger.info(f"Added tags to {resource_type} {resource_name}")
        except ClientError as e:
            logger.error(f"Error adding tags: {e}")
            raise
    
    def remove_tags(
        self, 
        resource_type: str, 
        resource_name: str, 
        tag_keys: List[str]
    ) -> None:
        """
        Remove tags from a resource.
        
        Args:
            resource_type: Type of resource ("broker" or "configuration")
            resource_name: Name of the resource
            tag_keys: List of tag keys to remove
        """
        try:
            if resource_type == "broker":
                self.mq_client.untag_resource(
                    ResourceArn=self._get_broker_arn(resource_name),
                    TagKeys=tag_keys
                )
            elif resource_type == "configuration":
                self.mq_client.untag_resource(
                    ResourceArn=self._get_configuration_arn(resource_name),
                    TagKeys=tag_keys
                )
            else:
                raise ValueError(f"Unsupported resource type: {resource_type}")
            
            logger.info(f"Removed tags from {resource_type} {resource_name}")
        except ClientError as e:
            logger.error(f"Error removing tags: {e}")
            raise
    
    def _get_broker_arn(self, broker_name: str) -> str:
        """Get ARN for a broker."""
        return f"arn:aws:mq:{self.config.region_name}:123456789012:broker:{broker_name}"
    
    def _get_configuration_arn(self, configuration_id: str) -> str:
        """Get ARN for a configuration."""
        return f"arn:aws:mq:{self.config.region_name}:123456789012:configuration:{configuration_id}"
    
    # ========================================================================
    # Security Groups Management
    # ========================================================================
    
    def describe_security_groups(self, vpc_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Describe security groups in a VPC.
        
        Args:
            vpc_id: Optional VPC ID to filter security groups
            
        Returns:
            List of security group information dictionaries
        """
        try:
            params = {}
            if vpc_id:
                params["Filters"] = [{"Name": "vpc-id", "Values": [vpc_id]}]
            
            response = self.ec2_client.describe_security_groups(**params)
            return response.get("SecurityGroups", [])
        except ClientError as e:
            logger.error(f"Error describing security groups: {e}")
            raise
    
    def create_security_group(self, group_name: str, description: str, vpc_id: str) -> Dict[str, Any]:
        """
        Create a security group for Amazon MQ.
        
        Args:
            group_name: Name of the security group
            description: Description of the security group
            vpc_id: VPC ID to create the security group in
            
        Returns:
            Dict containing created security group information
        """
        try:
            response = self.ec2_client.create_security_group(
                GroupName=group_name,
                Description=description,
                VpcId=vpc_id
            )
            
            security_group_id = response["GroupId"]
            
            self.ec2_client.authorize_security_group_ingress(
                GroupId=security_group_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 5672,
                        "ToPort": 5672,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 8883,
                        "ToPort": 8883,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 61617,
                        "ToPort": 61617,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 443,
                        "ToPort": 443,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 15672,
                        "ToPort": 15672,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
                    }
                ]
            )
            
            logger.info(f"Created security group: {group_name} ({security_group_id})")
            return response
        except ClientError as e:
            logger.error(f"Error creating security group: {e}")
            raise
    
    def delete_security_group(self, group_id: str) -> Dict[str, Any]:
        """
        Delete a security group.
        
        Args:
            group_id: ID of the security group to delete
            
        Returns:
            Dict containing operation result
        """
        try:
            response = self.ec2_client.delete_security_group(GroupId=group_id)
            
            logger.info(f"Deleted security group: {group_id}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting security group: {e}")
            raise
    
    # ========================================================================
    # CloudWatch Integration
    # ========================================================================
    
    def get_broker_metrics(
        self, 
        broker_name: str, 
        metric_names: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for a broker.
        
        Args:
            broker_name: Name of the broker
            metric_names: List of metric names to retrieve
            start_time: Start of time range (default: 1 hour ago)
            end_time: End of time range (default: now)
            period: Period in seconds for the metrics
            
        Returns:
            List of metric data points
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        
        broker_arn = self._get_broker_arn(broker_name)
        
        try:
            metrics_data = []
            
            for metric_name in metric_names:
                response = self.cw_client.get_metric_statistics(
                    Namespace="AWS/AmazonMQ",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "Broker", "Value": broker_name}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Average", "Maximum", "Minimum"]
                )
                
                metrics_data.append({
                    "MetricName": metric_name,
                    "Datapoints": response.get("Datapoints", [])
                })
            
            return metrics_data
        except ClientError as e:
            logger.error(f"Error getting broker metrics: {e}")
            raise
    
    def list_available_metrics(self) -> List[str]:
        """
        List available CloudWatch metrics for Amazon MQ.
        
        Returns:
            List of available metric names
        """
        return [
            "ActiveConnections",
            "ConnectionsOpened",
            "ConnectionsClosed",
            "MessagesReceived",
            "MessagesSent",
            "MessagesPendingCount",
            "QueueSize",
            "DiskUsage",
            "CpuUtilization",
            "MemoryUtilization",
            "Threads",
            "HeapUsage",
            "NetworkIn",
            "NetworkOut",
        ]
    
    def put_cloudwatch_alarm(
        self,
        broker_name: str,
        metric_name: str,
        alarm_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 2,
        period: int = 300
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for a broker metric.
        
        Args:
            broker_name: Name of the broker
            metric_name: Name of the metric
            alarm_name: Name for the alarm
            threshold: Threshold value for the alarm
            comparison_operator: Comparison operator
            evaluation_periods: Number of periods to evaluate
            period: Period in seconds
            
        Returns:
            Dict containing created alarm information
        """
        try:
            broker_arn = self._get_broker_arn(broker_name)
            
            response = self.cw_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alarm for {broker_name} {metric_name}",
                Namespace="AWS/AmazonMQ",
                MetricName=metric_name,
                Dimensions=[
                    {"Name": "Broker", "Value": broker_name}
                ],
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=period,
                Statistic="Average"
            )
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating CloudWatch alarm: {e}")
            raise
    
    # ========================================================================
    # Logs Configuration
    # ========================================================================
    
    def update_broker_logging(
        self, 
        broker_name: str, 
        log_general: bool = True, 
        log_audit: bool = False
    ) -> Dict[str, Any]:
        """
        Update broker logging configuration.
        
        Args:
            broker_name: Name of the broker
            log_general: Enable general logs
            log_audit: Enable audit logs
            
        Returns:
            Dict containing operation result
        """
        try:
            logs_config = {}
            if log_general:
                logs_config["General"] = True
            if log_audit:
                logs_config["Audit"] = True
            
            response = self.mq_client.update_broker(
                BrokerName=broker_name,
                Logs=logs_config
            )
            
            logger.info(f"Updated logging for broker: {broker_name}")
            self._invalidate_cache(f"broker:{broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error updating broker logging: {e}")
            raise
    
    def get_broker_logging_status(self, broker_name: str) -> Dict[str, bool]:
        """
        Get current logging configuration for a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            Dict containing logging status
        """
        try:
            info = self.describe_broker(broker_name)
            logs = info.get("Logs", {})
            
            return {
                "general": logs.get("General", False),
                "audit": logs.get("Audit", False)
            }
        except ClientError as e:
            logger.error(f"Error getting broker logging status: {e}")
            raise
    
    # ========================================================================
    # Encryption Management
    # ========================================================================
    
    def update_broker_encryption(
        self, 
        broker_name: str, 
        encryption_enabled: bool = True,
        kms_key_id: Optional[str] = None,
        encryption_algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_256
    ) -> Dict[str, Any]:
        """
        Update broker encryption settings.
        
        Args:
            broker_name: Name of the broker
            encryption_enabled: Enable or disable encryption
            kms_key_id: KMS key ID for encryption (required if encryption_enabled)
            encryption_algorithm: Encryption algorithm to use
            
        Returns:
            Dict containing operation result
        """
        try:
            encryption_options = {
                "Enabled": encryption_enabled,
                "EncryptionAlgorithm": encryption_algorithm.value,
            }
            
            if kms_key_id:
                encryption_options["KmsKeyId"] = kms_key_id
            
            response = self.mq_client.update_broker(
                BrokerName=broker_name,
                EncryptionOptions=encryption_options
            )
            
            logger.info(f"Updated encryption for broker: {broker_name}")
            self._invalidate_cache(f"broker:{broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error updating broker encryption: {e}")
            raise
    
    def get_broker_encryption(self, broker_name: str) -> Dict[str, Any]:
        """
        Get current encryption settings for a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            Dict containing encryption settings
        """
        try:
            info = self.describe_broker(broker_name)
            encryption = info.get("EncryptionOptions", {})
            
            return {
                "enabled": encryption.get("Enabled", False),
                "kms_key_id": encryption.get("KmsKeyId"),
                "encryption_algorithm": encryption.get("EncryptionAlgorithm")
            }
        except ClientError as e:
            logger.error(f"Error getting broker encryption: {e}")
            raise
    
    # ========================================================================
    # Deployment Mode Management
    # ========================================================================
    
    def update_broker_deployment_mode(
        self, 
        broker_name: str, 
        deployment_mode: DeploymentMode,
        vpc_id: Optional[str] = None,
        subnet_ids: Optional[List[str]] = None,
        security_groups: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Update broker deployment mode.
        
        Args:
            broker_name: Name of the broker
            deployment_mode: New deployment mode
            vpc_id: VPC ID (required for clustered modes)
            subnet_ids: List of subnet IDs (required for clustered modes)
            security_groups: List of security group IDs
            
        Returns:
            Dict containing operation result
        """
        try:
            params = {
                "BrokerName": broker_name,
                "DeploymentMode": deployment_mode.value,
            }
            
            if subnet_ids:
                params["SubnetIds"] = subnet_ids
            
            if security_groups:
                params["SecurityGroups"] = security_groups
            
            response = self.mq_client.update_broker(**params)
            
            logger.info(f"Updated deployment mode for broker: {broker_name} to {deployment_mode.value}")
            self._invalidate_cache(f"broker:{broker_name}")
            return response
        except ClientError as e:
            logger.error(f"Error updating broker deployment mode: {e}")
            raise
    
    def get_deployment_info(self, broker_name: str) -> Dict[str, Any]:
        """
        Get deployment information for a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            Dict containing deployment information
        """
        try:
            info = self.describe_broker(broker_name)
            
            return {
                "deployment_mode": info.get("DeploymentMode"),
                "broker_instances": info.get("BrokerInstances", []),
                "vpc_id": info.get("VpcId"),
                "subnet_ids": info.get("SubnetIds", []),
                "security_groups": info.get("SecurityGroups", []),
                "publicly_accessible": info.get("PubliclyAccessible", False)
            }
        except ClientError as e:
            logger.error(f"Error getting deployment info: {e}")
            raise
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def get_connection_endpoint(self, broker_name: str) -> Dict[str, str]:
        """
        Get connection endpoints for a broker.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            Dict containing connection URLs
        """
        try:
            info = self.describe_broker(broker_name)
            instances = info.get("BrokerInstances", [])
            
            endpoints = {}
            for instance in instances:
                if "Amqp" in instance.get("Endpoints", []):
                    endpoints["amqp"] = instance["Endpoints"].get("Amqp", [""])[0]
                if "MQTT" in instance.get("Endpoints", []):
                    endpoints["mqtt"] = instance["Endpoints"].get("MQTT", [""])[0]
                if "OpenWire" in instance.get("Endpoints", []):
                    endpoints["openwire"] = instance["Endpoints"].get("OpenWire", [""])[0]
                if "WSS" in instance.get("Endpoints", []):
                    endpoints["wss"] = instance["Endpoints"].get("WSS", [""])[0]
                if "Console" in instance.get("Endpoints", []):
                    endpoints["console"] = instance["Endpoints"].get("Console", [""])[0]
            
            return endpoints
        except ClientError as e:
            logger.error(f"Error getting connection endpoint: {e}")
            raise
    
    def get_broker_info_summary(self, broker_name: str) -> Dict[str, Any]:
        """
        Get a comprehensive summary of broker information.
        
        Args:
            broker_name: Name of the broker
            
        Returns:
            Dict containing broker summary information
        """
        try:
            info = self.describe_broker(broker_name)
            
            return {
                "name": broker_name,
                "state": info.get("BrokerState"),
                "engine": info.get("EngineType"),
                "engine_version": info.get("EngineVersion"),
                "instance_type": info.get("HostInstanceType"),
                "deployment_mode": info.get("DeploymentMode"),
                "endpoints": self.get_connection_endpoint(broker_name),
                "encryption": self.get_broker_encryption(broker_name),
                "logging": self.get_broker_logging_status(broker_name),
                "deployment": self.get_deployment_info(broker_name),
                "users_count": len(self.list_users(broker_name)),
                "tags": self.list_tags("broker", broker_name)
            }
        except ClientError as e:
            logger.error(f"Error getting broker summary: {e}")
            raise
