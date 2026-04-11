"""
AWS IoT Integration Module for Workflow System

Implements an IoTIntegration class with:
1. Thing management: Create/manage IoT things
2. Thing types: Manage thing types
3. Thing groups: Manage thing groups
4. Certificates: Manage X.509 certificates
5. Policies: Manage IoT policies
6. Rules: Create/manage IoT rules
7. Jobs: Manage IoT jobs
8. Secure tunneling: Manage secure tunnels
9. Fleet indexing: Fleet indexing configuration
10. CloudWatch integration: IoT metrics and monitoring

Commit: 'feat(aws-iot): add AWS IoT with thing management, thing types, thing groups, certificates, policies, rules, jobs, secure tunneling, fleet indexing, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os

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


class ThingAttributeType(Enum):
    """IoT Thing attribute types."""
    STRING = "string"
    STRING_LIST = "string-list"
    STRING_MAP = "string-map"


class ThingTypeStatus(Enum):
    """IoT Thing type status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    DEPRECATED = "DEPRECATED"


class CertificateStatus(Enum):
    """IoT Certificate status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    REVOKED = "REVOKED"
    PENDING_TRANSFER = "PENDING_TRANSFER"
    REGISTRATION_IN_PROGRESS = "REGISTRATION_IN_PROGRESS"
    PENDING_ACTIVATION = "PENDING_ACTIVATION"


class PolicyType(Enum):
    """IoT Policy types."""
    AWS_IOT_POLICY = "AWS_IOT_POLICY"
    MANAGED_POLICY = "MANAGED_POLICY"


class JobStatus(Enum):
    """IoT Job status."""
    IN_PROGRESS = "IN_PROGRESS"
    QUEUED = "QUEUED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    TIMED_OUT = "TIMED_OUT"


class JobExecutionStatus(Enum):
    """IoT Job execution status."""
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"


class TunnelStatus(Enum):
    """IoT Secure Tunnel status."""
    OPEN = "Open"
    CLOSED = "Closed"


@dataclass
class IoTConfig:
    """Configuration for IoT connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class ThingConfig:
    """Configuration for creating an IoT thing."""
    thing_name: str
    thing_type_name: Optional[str] = None
    attribute_payload: Optional[Dict[str, Any]] = None
    thing_groups: List[str] = field(default_factory=list)
    billing_group_name: Optional[str] = None


@dataclass
class ThingInfo:
    """Information about an IoT thing."""
    thing_name: str
    thing_arn: str
    thing_type_name: Optional[str] = None
    thing_id: Optional[str] = None
    version: Optional[int] = None
    attributes: Dict[str, str] = field(default_factory=dict)
    dynamic: bool = False


@dataclass
class ThingTypeConfig:
    """Configuration for creating a thing type."""
    thing_type_name: str
    thing_type_properties: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ThingTypeInfo:
    """Information about a thing type."""
    thing_type_name: str
    thing_type_arn: str
    thing_type_id: str
    thing_type_properties: Dict[str, Any] = field(default_factory=dict)
    thing_type_metadata: Dict[str, Any] = field(default_factory=dict)
    status: ThingTypeStatus = ThingTypeStatus.ACTIVE


@dataclass
class ThingGroupConfig:
    """Configuration for creating a thing group."""
    group_name: str
    parent_group_name: Optional[str] = None
    thing_group_properties: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ThingGroupInfo:
    """Information about a thing group."""
    group_name: str
    group_arn: str
    group_id: Optional[str] = None
    version: Optional[int] = None
    thing_group_properties: Dict[str, Any] = field(default_factory=dict)
    parent_group_name: Optional[str] = None


@dataclass
class CertificateConfig:
    """Configuration for certificate creation."""
    certificate_pem: Optional[str] = None
    public_key: Optional[str] = None
    private_key: Optional[str] = None
    certificate_id: Optional[str] = None
    certificate_arn: Optional[str] = None


@dataclass
class CertificateInfo:
    """Information about a certificate."""
    certificate_id: str
    certificate_arn: str
    status: CertificateStatus
    creation_date: Optional[str] = None
    modified_date: Optional[str] = None


@dataclass
class PolicyConfig:
    """Configuration for creating a policy."""
    policy_name: str
    policy_document: Dict[str, Any]
    policy_description: str = ""


@dataclass
class PolicyInfo:
    """Information about a policy."""
    policy_name: str
    policy_arn: str
    policy_version_id: str
    default_version_id: str


@dataclass
class RuleConfig:
    """Configuration for creating an IoT rule."""
    rule_name: str
    sql: str
    actions: List[Dict[str, Any]]
    description: str = ""
    aws_iot_sql_version: str = "2016-03-23"
    rule_disabled: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class RuleInfo:
    """Information about an IoT rule."""
    rule_name: str
    rule_arn: str
    sql: str
    description: str
    rule_disabled: bool
    actions: List[Dict[str, Any]]
    created_at: Optional[str] = None
    last_modified_at: Optional[str] = None


@dataclass
class JobConfig:
    """Configuration for creating a job."""
    job_id: str
    targets: List[str]
    document: Union[str, Dict[str, Any]]
    description: str = ""
    job_template_id: Optional[str] = None
    presigned_url_config: Optional[Dict[str, Any]] = None
    target_selection: str = "CONTINUOUS"
    job_execution_timeout_minutes: int = 60
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class JobInfo:
    """Information about a job."""
    job_id: str
    job_arn: str
    status: JobStatus
    targets: List[str] = field(default_factory=list)
    description: Optional[str] = None
    created_at: Optional[str] = None
    last_modified_at: Optional[str] = None


@dataclass
class TunnelConfig:
    """Configuration for creating a tunnel."""
    thing_name: Optional[str] = None
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    timeout: int = 3600


@dataclass
class TunnelInfo:
    """Information about a tunnel."""
    tunnel_id: str
    tunnel_arn: str
    thing_name: Optional[str] = None
    description: Optional[str] = None
    status: TunnelStatus = TunnelStatus.OPEN
    created_at: Optional[str] = None


@dataclass
class FleetIndexingConfig:
    """Configuration for fleet indexing."""
    thing_indexing_mode: str = "REGISTRY_AND_SHADOW"
    thing_connectivity_indexing_mode: str = "STATUS"
    field_indexing_mode: str = "REGISTRY_AND_SHADOW"
    managed_fields: List[str] = field(default_factory=list)
    custom_fields: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class FleetIndexingInfo:
    """Fleet indexing configuration status."""
    thing_indexing_mode: str
    thing_connectivity_indexing_mode: str
    field_indexing_mode: Optional[str] = None
    managed_fields: List[str] = field(default_factory=list)
    custom_fields: List[Dict[str, str]] = field(default_factory=list)


class IoTIntegration:
    """
    AWS IoT integration class for IoT operations.
    
    Supports:
    - Thing creation, update, delete, and management
    - Thing type creation and management
    - Thing group creation and management
    - X.509 certificate management
    - IoT policy management
    - Rule creation and management
    - Job creation and management
    - Secure tunnel management
    - Fleet indexing configuration
    - CloudWatch metrics and monitoring
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        iot_client: Optional[Any] = None,
        iot_data_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None,
        iam_client: Optional[Any] = None,
        s3_client: Optional[Any] = None
    ):
        """
        Initialize IoT integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: IoT endpoint URL (for testing with LocalStack, etc.)
            iot_client: Pre-configured IoT client (overrides boto3 creation)
            iot_data_client: Pre-configured IoT Data client for shadow operations
            cloudwatch_client: Pre-configured CloudWatch client for metrics
            iam_client: Pre-configured IAM client for policy management
            s3_client: Pre-configured S3 client for job document storage
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for IoT integration. Install with: pip install boto3")
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        
        session_kwargs = {"region_name": region_name}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        if iot_client:
            self.iot_client = iot_client
        else:
            self.iot_client = boto3.client("iot", endpoint_url=endpoint_url, **session_kwargs)
        
        if iot_data_client:
            self.iot_data_client = iot_data_client
        else:
            self.iot_data_client = boto3.client("iot-data", region_name=region_name)
        
        if cloudwatch_client:
            self.cloudwatch_client = cloudwatch_client
        else:
            self.cloudwatch_client = boto3.client("cloudwatch", region_name=region_name)
        
        if iam_client:
            self.iam_client = iam_client
        else:
            self.iam_client = boto3.client("iam", region_name=region_name)
        
        if s3_client:
            self.s3_client = s3_client
        else:
            self.s3_client = boto3.client("s3", region_name=region_name)
        
        self._thing_cache: Dict[str, ThingInfo] = {}
        self._thing_type_cache: Dict[str, ThingTypeInfo] = {}
        self._thing_group_cache: Dict[str, ThingGroupInfo] = {}
        self._lock = threading.Lock()
    
    # =========================================================================
    # Thing Management
    # =========================================================================
    
    def create_thing(self, config: ThingConfig) -> ThingInfo:
        """
        Create a new IoT thing.
        
        Args:
            config: Thing configuration
            
        Returns:
            ThingInfo object with created thing details
        """
        kwargs = {"thingName": config.thing_name}
        
        if config.thing_type_name:
            kwargs["thingTypeName"] = config.thing_type_name
        
        if config.attribute_payload:
            kwargs["attributePayload"] = config.attribute_payload
        
        try:
            response = self.iot_client.create_thing(**kwargs)
            
            thing_info = ThingInfo(
                thing_name=response["thingName"],
                thing_arn=response["thingArn"],
                thing_type_name=config.thing_type_name,
                thing_id=response.get("thingId"),
                attributes=config.attribute_payload.get("attributes", {}) if config.attribute_payload else {}
            )
            
            if config.thing_groups:
                for group_name in config.thing_groups:
                    self.add_thing_to_group(config.thing_name, group_name)
            
            with self._lock:
                self._thing_cache[config.thing_name] = thing_info
            
            logger.info(f"Created IoT thing: {config.thing_name}")
            return thing_info
            
        except ClientError as e:
            logger.error(f"Error creating thing: {e}")
            raise
    
    def get_thing(self, thing_name: str) -> ThingInfo:
        """
        Get information about an IoT thing.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            ThingInfo object with thing details
        """
        with self._lock:
            if thing_name in self._thing_cache:
                return self._thing_cache[thing_name]
        
        try:
            response = self.iot_client.describe_thing(thingName=thing_name)
            
            thing_info = ThingInfo(
                thing_name=response["thingName"],
                thing_arn=response["thingArn"],
                thing_type_name=response.get("thingTypeName"),
                thing_id=response.get("thingId"),
                version=response.get("version"),
                attributes=response.get("attributes", {})
            )
            
            with self._lock:
                self._thing_cache[thing_name] = thing_info
            
            return thing_info
            
        except ClientError as e:
            logger.error(f"Error getting thing: {e}")
            raise
    
    def update_thing(
        self,
        thing_name: str,
        thing_type_name: Optional[str] = None,
        attribute_payload: Optional[Dict[str, Any]] = None,
        expected_version: Optional[int] = None
    ) -> bool:
        """
        Update an IoT thing.
        
        Args:
            thing_name: Name of the thing to update
            thing_type_name: New thing type name (or None to keep current)
            attribute_payload: New attributes
            expected_version: Expected thing version for optimistic locking
            
        Returns:
            True if successful
        """
        kwargs = {"thingName": thing_name}
        
        if thing_type_name is not None:
            kwargs["thingTypeName"] = thing_type_name
        
        if attribute_payload is not None:
            kwargs["attributePayload"] = attribute_payload
        
        if expected_version is not None:
            kwargs["expectedVersion"] = expected_version
        
        try:
            self.iot_client.update_thing(**kwargs)
            
            with self._lock:
                if thing_name in self._thing_cache:
                    del self._thing_cache[thing_name]
            
            logger.info(f"Updated IoT thing: {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating thing: {e}")
            raise
    
    def delete_thing(self, thing_name: str) -> bool:
        """
        Delete an IoT thing.
        
        Args:
            thing_name: Name of the thing to delete
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_thing(thingName=thing_name)
            
            with self._lock:
                if thing_name in self._thing_cache:
                    del self._thing_cache[thing_name]
            
            logger.info(f"Deleted IoT thing: {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting thing: {e}")
            raise
    
    def list_things(
        self,
        thing_type_name: Optional[str] = None,
        attribute_name: Optional[str] = None,
        attribute_value: Optional[str] = None,
        max_results: int = 100
    ) -> List[ThingInfo]:
        """
        List IoT things.
        
        Args:
            thing_type_name: Filter by thing type
            attribute_name: Filter by attribute name
            attribute_value: Filter by attribute value
            max_results: Maximum number of results
            
        Returns:
            List of ThingInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if thing_type_name:
            kwargs["thingTypeName"] = thing_type_name
        
        if attribute_name:
            kwargs["attributeName"] = attribute_name
            if attribute_value:
                kwargs["attributeValue"] = attribute_value
        
        things = []
        
        try:
            paginator = self.iot_client.get_paginator("list_things")
            
            for page in paginator.paginate(**kwargs):
                for thing in page.get("things", []):
                    things.append(ThingInfo(
                        thing_name=thing["thingName"],
                        thing_arn=thing["thingArn"],
                        thing_type_name=thing.get("thingTypeName"),
                        thing_id=thing.get("thingId"),
                        attributes=thing.get("attributes", {})
                    ))
            
            return things
            
        except ClientError as e:
            logger.error(f"Error listing things: {e}")
            raise
    
    def list_thing_principals(self, thing_name: str) -> List[str]:
        """
        List principals attached to a thing.
        
        Args:
            thing_name: Name of the thing
            
        Returns:
            List of principal ARNs
        """
        try:
            response = self.iot_client.list_thing_principals(thingName=thing_name)
            return response.get("principals", [])
            
        except ClientError as e:
            logger.error(f"Error listing thing principals: {e}")
            raise
    
    def attach_thing_principal(self, thing_name: str, principal: str) -> bool:
        """
        Attach a principal to a thing.
        
        Args:
            thing_name: Name of the thing
            principal: Principal ARN (certificate ARN)
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.attach_thing_principal(thingName=thing_name, principal=principal)
            logger.info(f"Attached principal to thing: {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error attaching thing principal: {e}")
            raise
    
    def detach_thing_principal(self, thing_name: str, principal: str) -> bool:
        """
        Detach a principal from a thing.
        
        Args:
            thing_name: Name of the thing
            principal: Principal ARN (certificate ARN)
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.detach_thing_principal(thingName=thing_name, principal=principal)
            logger.info(f"Detached principal from thing: {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error detaching thing principal: {e}")
            raise
    
    # =========================================================================
    # Thing Type Management
    # =========================================================================
    
    def create_thing_type(self, config: ThingTypeConfig) -> ThingTypeInfo:
        """
        Create a new thing type.
        
        Args:
            config: Thing type configuration
            
        Returns:
            ThingTypeInfo object with created thing type details
        """
        kwargs = {"thingTypeName": config.thing_type_name}
        
        if config.thing_type_properties:
            kwargs["thingTypeProperties"] = config.thing_type_properties
        
        if config.tags:
            kwargs["tags"] = self._prepare_tags(config.tags)
        
        try:
            response = self.iot_client.create_thing_type(**kwargs)
            
            thing_type_info = ThingTypeInfo(
                thing_type_name=response["thingTypeName"],
                thing_type_arn=response["thingTypeArn"],
                thing_type_id=response.get("thingTypeId"),
                thing_type_properties=response.get("thingTypeProperties", {}),
                thing_type_metadata=response.get("thingTypeMetadata", {}),
                status=ThingTypeStatus(response.get("thingTypeMetadata", {}).get("status", "ACTIVE"))
            )
            
            with self._lock:
                self._thing_type_cache[config.thing_type_name] = thing_type_info
            
            logger.info(f"Created thing type: {config.thing_type_name}")
            return thing_type_info
            
        except ClientError as e:
            logger.error(f"Error creating thing type: {e}")
            raise
    
    def get_thing_type(self, thing_type_name: str) -> ThingTypeInfo:
        """
        Get information about a thing type.
        
        Args:
            thing_type_name: Name of the thing type
            
        Returns:
            ThingTypeInfo object with thing type details
        """
        with self._lock:
            if thing_type_name in self._thing_type_cache:
                return self._thing_type_cache[thing_type_name]
        
        try:
            response = self.iot_client.describe_thing_type(thingTypeName=thing_type_name)
            
            thing_type_info = ThingTypeInfo(
                thing_type_name=response["thingTypeName"],
                thing_type_arn=response["thingTypeArn"],
                thing_type_id=response.get("thingTypeId"),
                thing_type_properties=response.get("thingTypeProperties", {}),
                thing_type_metadata=response.get("thingTypeMetadata", {}),
                status=ThingTypeStatus(response.get("thingTypeMetadata", {}).get("status", "ACTIVE"))
            )
            
            with self._lock:
                self._thing_type_cache[thing_type_name] = thing_type_info
            
            return thing_type_info
            
        except ClientError as e:
            logger.error(f"Error getting thing type: {e}")
            raise
    
    def deprecate_thing_type(self, thing_type_name: str) -> bool:
        """
        Deprecate a thing type.
        
        Args:
            thing_type_name: Name of the thing type to deprecate
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.deprecate_thing_type(thingTypeName=thing_type_name)
            
            with self._lock:
                if thing_type_name in self._thing_type_cache:
                    del self._thing_type_cache[thing_type_name]
            
            logger.info(f"Deprecated thing type: {thing_type_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deprecating thing type: {e}")
            raise
    
    def delete_thing_type(self, thing_type_name: str) -> bool:
        """
        Delete a thing type.
        
        Args:
            thing_type_name: Name of the thing type to delete
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_thing_type(thingTypeName=thing_type_name)
            
            with self._lock:
                if thing_type_name in self._thing_type_cache:
                    del self._thing_type_cache[thing_type_name]
            
            logger.info(f"Deleted thing type: {thing_type_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting thing type: {e}")
            raise
    
    def list_thing_types(
        self,
        thing_type_name_prefix: Optional[str] = None,
        status: Optional[ThingTypeStatus] = None,
        max_results: int = 100
    ) -> List[ThingTypeInfo]:
        """
        List thing types.
        
        Args:
            thing_type_name_prefix: Filter by prefix
            status: Filter by status
            max_results: Maximum number of results
            
        Returns:
            List of ThingTypeInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if thing_type_name_prefix:
            kwargs["thingTypeNamePrefix"] = thing_type_name_prefix
        
        if status:
            kwargs["status"] = status.value
        
        thing_types = []
        
        try:
            paginator = self.iot_client.get_paginator("list_thing_types")
            
            for page in paginator.paginate(**kwargs):
                for thing_type in page.get("thingTypes", []):
                    thing_types.append(ThingTypeInfo(
                        thing_type_name=thing_type["thingTypeName"],
                        thing_type_arn=thing_type["thingTypeArn"],
                        thing_type_id=thing_type.get("thingTypeId"),
                        thing_type_properties=thing_type.get("thingTypeProperties", {}),
                        thing_type_metadata=thing_type.get("thingTypeMetadata", {}),
                        status=ThingTypeStatus(thing_type.get("thingTypeMetadata", {}).get("status", "ACTIVE"))
                    ))
            
            return thing_types
            
        except ClientError as e:
            logger.error(f"Error listing thing types: {e}")
            raise
    
    # =========================================================================
    # Thing Group Management
    # =========================================================================
    
    def create_thing_group(self, config: ThingGroupConfig) -> ThingGroupInfo:
        """
        Create a new thing group.
        
        Args:
            config: Thing group configuration
            
        Returns:
            ThingGroupInfo object with created thing group details
        """
        kwargs = {"groupName": config.group_name}
        
        if config.parent_group_name:
            kwargs["parentGroupName"] = config.parent_group_name
        
        if config.thing_group_properties:
            kwargs["thingGroupProperties"] = config.thing_group_properties
        
        if config.tags:
            kwargs["tags"] = self._prepare_tags(config.tags)
        
        try:
            response = self.iot_client.create_thing_group(**kwargs)
            
            thing_group_info = ThingGroupInfo(
                group_name=response["groupName"],
                group_arn=response["groupArn"],
                group_id=response.get("groupId"),
                version=response.get("version"),
                thing_group_properties=response.get("thingGroupProperties", {}),
                parent_group_name=config.parent_group_name
            )
            
            with self._lock:
                self._thing_group_cache[config.group_name] = thing_group_info
            
            logger.info(f"Created thing group: {config.group_name}")
            return thing_group_info
            
        except ClientError as e:
            logger.error(f"Error creating thing group: {e}")
            raise
    
    def get_thing_group(self, group_name: str) -> ThingGroupInfo:
        """
        Get information about a thing group.
        
        Args:
            group_name: Name of the thing group
            
        Returns:
            ThingGroupInfo object with thing group details
        """
        with self._lock:
            if group_name in self._thing_group_cache:
                return self._thing_group_cache[group_name]
        
        try:
            response = self.iot_client.describe_thing_group(groupName=group_name)
            
            thing_group_info = ThingGroupInfo(
                group_name=response["groupName"],
                group_arn=response["groupArn"],
                group_id=response.get("groupId"),
                version=response.get("version"),
                thing_group_properties=response.get("thingGroupProperties", {}),
                parent_group_name=response.get("parentGroupName")
            )
            
            with self._lock:
                self._thing_group_cache[group_name] = thing_group_info
            
            return thing_group_info
            
        except ClientError as e:
            logger.error(f"Error getting thing group: {e}")
            raise
    
    def update_thing_group(
        self,
        group_name: str,
        thing_group_properties: Optional[Dict[str, Any]] = None,
        expected_version: Optional[int] = None
    ) -> bool:
        """
        Update a thing group.
        
        Args:
            group_name: Name of the thing group to update
            thing_group_properties: New properties
            expected_version: Expected version for optimistic locking
            
        Returns:
            True if successful
        """
        kwargs = {"groupName": group_name}
        
        if thing_group_properties:
            kwargs["thingGroupProperties"] = thing_group_properties
        
        if expected_version is not None:
            kwargs["expectedVersion"] = expected_version
        
        try:
            self.iot_client.update_thing_group(**kwargs)
            
            with self._lock:
                if group_name in self._thing_group_cache:
                    del self._thing_group_cache[group_name]
            
            logger.info(f"Updated thing group: {group_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating thing group: {e}")
            raise
    
    def delete_thing_group(self, group_name: str) -> bool:
        """
        Delete a thing group.
        
        Args:
            group_name: Name of the thing group to delete
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_thing_group(groupName=group_name)
            
            with self._lock:
                if group_name in self._thing_group_cache:
                    del self._thing_group_cache[group_name]
            
            logger.info(f"Deleted thing group: {group_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting thing group: {e}")
            raise
    
    def add_thing_to_group(self, thing_group_name: str, thing_name: str, override_dynamic: bool = False) -> bool:
        """
        Add a thing to a group.
        
        Args:
            thing_group_name: Name of the thing group
            thing_name: Name of the thing
            override_dynamic: Override dynamic group behavior
            
        Returns:
            True if successful
        """
        kwargs = {"thingGroupName": thing_group_name, "thingName": thing_name}
        
        if override_dynamic:
            kwargs["overrideDynamicGroups"] = True
        
        try:
            self.iot_client.add_thing_to_thing_group(**kwargs)
            logger.info(f"Added thing {thing_name} to group {thing_group_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error adding thing to group: {e}")
            raise
    
    def remove_thing_from_group(self, thing_group_name: str, thing_name: str) -> bool:
        """
        Remove a thing from a group.
        
        Args:
            thing_group_name: Name of the thing group
            thing_name: Name of the thing
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.remove_thing_from_thing_group(
                thingGroupName=thing_group_name,
                thingName=thing_name
            )
            logger.info(f"Removed thing {thing_name} from group {thing_group_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error removing thing from group: {e}")
            raise
    
    def list_thing_groups(
        self,
        parent_group: Optional[str] = None,
        name_prefix_filter: Optional[str] = None,
        max_results: int = 100
    ) -> List[ThingGroupInfo]:
        """
        List thing groups.
        
        Args:
            parent_group: Filter by parent group
            name_prefix_filter: Filter by name prefix
            max_results: Maximum number of results
            
        Returns:
            List of ThingGroupInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if parent_group:
            kwargs["parentGroup"] = parent_group
        
        if name_prefix_filter:
            kwargs["namePrefixFilter"] = name_prefix_filter
        
        groups = []
        
        try:
            paginator = self.iot_client.get_paginator("list_thing_groups")
            
            for page in paginator.paginate(**kwargs):
                for group in page.get("thingGroups", []):
                    groups.append(ThingGroupInfo(
                        group_name=group["groupName"],
                        group_arn=group["groupArn"],
                        group_id=group.get("groupId")
                    ))
            
            return groups
            
        except ClientError as e:
            logger.error(f"Error listing thing groups: {e}")
            raise
    
    def list_things_in_group(self, thing_group_name: str, max_results: int = 100) -> List[str]:
        """
        List things in a group.
        
        Args:
            thing_group_name: Name of the thing group
            max_results: Maximum number of results
            
        Returns:
            List of thing names
        """
        things = []
        
        try:
            paginator = self.iot_client.get_paginator("list_things_in_thing_group")
            
            for page in paginator.paginate(thingGroupName=thing_group_name, maxResults=max_results):
                things.extend(page.get("things", []))
            
            return things
            
        except ClientError as e:
            logger.error(f"Error listing things in group: {e}")
            raise
    
    # =========================================================================
    # Certificate Management
    # =========================================================================
    
    def create_certificate(self) -> CertificateInfo:
        """
        Create a new X.509 certificate.
        
        Returns:
            CertificateInfo object with certificate details
        """
        try:
            response = self.iot_client.create_keys_and_certificate(setAsActive=True)
            
            cert_info = CertificateInfo(
                certificate_id=response["certificateId"],
                certificate_arn=response["certificateArn"],
                status=CertificateStatus.ACTIVE,
                creation_date=response.get("certificateCreationDate"),
                modified_date=response.get("certificateModifiedDate")
            )
            
            logger.info(f"Created certificate: {cert_info.certificate_id}")
            return cert_info
            
        except ClientError as e:
            logger.error(f"Error creating certificate: {e}")
            raise
    
    def register_certificate(
        self,
        certificate_pem: str,
        ca_certificate_pem: Optional[str] = None,
        status: CertificateStatus = CertificateStatus.ACTIVE
    ) -> CertificateInfo:
        """
        Register a certificate.
        
        Args:
            certificate_pem: Certificate PEM
            ca_certificate_pem: CA certificate PEM
            status: Certificate status
            
        Returns:
            CertificateInfo object
        """
        kwargs = {"certificatePem": certificate_pem, "status": status.value}
        
        if ca_certificate_pem:
            kwargs["caCertificatePem"] = ca_certificate_pem
        
        try:
            response = self.iot_client.register_certificate(**kwargs)
            
            cert_info = CertificateInfo(
                certificate_id=response["certificateId"],
                certificate_arn=response.get("certificateArn", ""),
                status=status,
                creation_date=response.get("certificateCreationDate"),
                modified_date=response.get("certificateModifiedDate")
            )
            
            logger.info(f"Registered certificate: {cert_info.certificate_id}")
            return cert_info
            
        except ClientError as e:
            logger.error(f"Error registering certificate: {e}")
            raise
    
    def describe_certificate(self, certificate_id: str) -> CertificateInfo:
        """
        Get information about a certificate.
        
        Args:
            certificate_id: Certificate ID
            
        Returns:
            CertificateInfo object
        """
        try:
            response = self.iot_client.describe_certificate(certificateId=certificate_id)
            cert_info = CertificateInfo(
                certificate_id=response["certificateDescription"]["certificateId"],
                certificate_arn=response["certificateDescription"]["certificateArn"],
                status=CertificateStatus(response["certificateDescription"]["status"]),
                creation_date=response["certificateDescription"].get("creationDate"),
                modified_date=response["certificateDescription"].get("lastModifiedDate")
            )
            
            return cert_info
            
        except ClientError as e:
            logger.error(f"Error describing certificate: {e}")
            raise
    
    def update_certificate(self, certificate_id: str, new_status: CertificateStatus) -> bool:
        """
        Update certificate status.
        
        Args:
            certificate_id: Certificate ID
            new_status: New status
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.update_certificate(
                certificateId=certificate_id,
                newStatus=new_status.value
            )
            logger.info(f"Updated certificate status: {certificate_id} -> {new_status.value}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating certificate: {e}")
            raise
    
    def delete_certificate(self, certificate_id: str) -> bool:
        """
        Delete a certificate.
        
        Args:
            certificate_id: Certificate ID
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_certificate(certificateId=certificate_id)
            logger.info(f"Deleted certificate: {certificate_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting certificate: {e}")
            raise
    
    def list_certificates(self, max_results: int = 100) -> List[CertificateInfo]:
        """
        List certificates.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of CertificateInfo objects
        """
        certs = []
        
        try:
            paginator = self.iot_client.get_paginator("list_certificates")
            
            for page in paginator.paginate(maxResults=max_results):
                for cert in page.get("certificates", []):
                    certs.append(CertificateInfo(
                        certificate_id=cert["certificateId"],
                        certificate_arn=cert["certificateArn"],
                        status=CertificateStatus(cert["status"]),
                        creation_date=cert.get("creationDate"),
                        modified_date=cert.get("lastModifiedDate")
                    ))
            
            return certs
            
        except ClientError as e:
            logger.error(f"Error listing certificates: {e}")
            raise
    
    def attach_certificate_to_thing(self, certificate_id: str, thing_name: str) -> bool:
        """
        Attach a certificate to a thing.
        
        Args:
            certificate_id: Certificate ID
            thing_name: Thing name
            
        Returns:
            True if successful
        """
        try:
            principal = f"arn:aws:iot:{self.region_name}:*:cert/{certificate_id}"
            self.attach_thing_principal(thing_name, principal)
            logger.info(f"Attached certificate {certificate_id} to thing {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error attaching certificate to thing: {e}")
            raise
    
    def detach_certificate_from_thing(self, certificate_id: str, thing_name: str) -> bool:
        """
        Detach a certificate from a thing.
        
        Args:
            certificate_id: Certificate ID
            thing_name: Thing name
            
        Returns:
            True if successful
        """
        try:
            principal = f"arn:aws:iot:{self.region_name}:*:cert/{certificate_id}"
            self.detach_thing_principal(thing_name, principal)
            logger.info(f"Detached certificate {certificate_id} from thing {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error detaching certificate from thing: {e}")
            raise
    
    # =========================================================================
    # Policy Management
    # =========================================================================
    
    def create_policy(self, config: PolicyConfig) -> PolicyInfo:
        """
        Create a new IoT policy.
        
        Args:
            config: Policy configuration
            
        Returns:
            PolicyInfo object with policy details
        """
        kwargs = {
            "policyName": config.policy_name,
            "policyDocument": config.policy_document
        }
        
        try:
            response = self.iot_client.create_policy(**kwargs)
            
            policy_info = PolicyInfo(
                policy_name=response["policyName"],
                policy_arn=response["policyArn"],
                policy_version_id=response["policyVersionId"],
                default_version_id=response["policyVersionId"]
            )
            
            logger.info(f"Created policy: {config.policy_name}")
            return policy_info
            
        except ClientError as e:
            logger.error(f"Error creating policy: {e}")
            raise
    
    def get_policy(self, policy_name: str) -> PolicyInfo:
        """
        Get information about a policy.
        
        Args:
            policy_name: Policy name
            
        Returns:
            PolicyInfo object
        """
        try:
            response = self.iot_client.get_policy(policyName=policy_name)
            
            policy_info = PolicyInfo(
                policy_name=response["policyName"],
                policy_arn=response["policyArn"],
                policy_version_id=response["defaultVersionId"],
                default_version_id=response["defaultVersionId"]
            )
            
            return policy_info
            
        except ClientError as e:
            logger.error(f"Error getting policy: {e}")
            raise
    
    def update_policy(
        self,
        policy_name: str,
        policy_document: Dict[str, Any],
        replace: bool = True
    ) -> bool:
        """
        Update a policy.
        
        Args:
            policy_name: Policy name
            policy_document: New policy document
            replace: Replace the default version (True) or create new version (False)
            
        Returns:
            True if successful
        """
        kwargs = {
            "policyName": policy_name,
            "policyDocument": policy_document
        }
        
        if replace:
            kwargs["setAsDefault"] = True
        
        try:
            self.iot_client.create_policy_version(**kwargs)
            logger.info(f"Updated policy: {policy_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating policy: {e}")
            raise
    
    def delete_policy(self, policy_name: str) -> bool:
        """
        Delete a policy.
        
        Args:
            policy_name: Policy name
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_policy(policyName=policy_name)
            logger.info(f"Deleted policy: {policy_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting policy: {e}")
            raise
    
    def list_policies(self, max_results: int = 100) -> List[PolicyInfo]:
        """
        List policies.
        
        Args:
            max_results: Maximum number of results
            
        Returns:
            List of PolicyInfo objects
        """
        policies = []
        
        try:
            paginator = self.iot_client.get_paginator("list_policies")
            
            for page in paginator.paginate(maxResults=max_results):
                for policy in page.get("policies", []):
                    policies.append(PolicyInfo(
                        policy_name=policy["policyName"],
                        policy_arn=policy["policyArn"],
                        policy_version_id=policy.get("policyVersionId", ""),
                        default_version_id=policy.get("defaultVersionId", "")
                    ))
            
            return policies
            
        except ClientError as e:
            logger.error(f"Error listing policies: {e}")
            raise
    
    def attach_policy(self, policy_name: str, target: str) -> bool:
        """
        Attach a policy to a target.
        
        Args:
            policy_name: Policy name
            target: Target (certificate ARN, thing group, etc.)
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.attach_policy(policyName=policy_name, target=target)
            logger.info(f"Attached policy {policy_name} to {target}")
            return True
            
        except ClientError as e:
            logger.error(f"Error attaching policy: {e}")
            raise
    
    def detach_policy(self, policy_name: str, target: str) -> bool:
        """
        Detach a policy from a target.
        
        Args:
            policy_name: Policy name
            target: Target
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.detach_policy(policyName=policy_name, target=target)
            logger.info(f"Detached policy {policy_name} from {target}")
            return True
            
        except ClientError as e:
            logger.error(f"Error detaching policy: {e}")
            raise
    
    def attach_policy_to_thing(self, policy_name: str, thing_name: str) -> bool:
        """
        Attach a policy to a thing (via its certificates).
        
        Args:
            policy_name: Policy name
            thing_name: Thing name
            
        Returns:
            True if successful
        """
        principals = self.list_thing_principals(thing_name)
        
        if not principals:
            raise ValueError(f"No principals attached to thing: {thing_name}")
        
        for principal in principals:
            self.attach_policy(policy_name, principal)
        
        logger.info(f"Attached policy {policy_name} to thing {thing_name}")
        return True
    
    # =========================================================================
    # Rule Management
    # =========================================================================
    
    def create_rule(self, config: RuleConfig) -> RuleInfo:
        """
        Create a new IoT rule.
        
        Args:
            config: Rule configuration
            
        Returns:
            RuleInfo object with rule details
        """
        kwargs = {
            "ruleName": config.rule_name,
            "sql": config.sql,
            "actions": config.actions,
            "awsIotSqlVersion": config.aws_iot_sql_version
        }
        
        if config.description:
            kwargs["description"] = config.description
        
        if config.rule_disabled:
            kwargs["ruleDisabled"] = config.rule_disabled
        
        if config.tags:
            kwargs["tags"] = self._prepare_tags(config.tags)
        
        try:
            response = self.iot_client.create_topic_rule(**kwargs)
            
            rule_info = RuleInfo(
                rule_name=config.rule_name,
                rule_arn=f"arn:aws:iot:{self.region_name}:*:rule/{config.rule_name}",
                sql=config.sql,
                description=config.description,
                rule_disabled=config.rule_disabled,
                actions=config.actions
            )
            
            logger.info(f"Created rule: {config.rule_name}")
            return rule_info
            
        except ClientError as e:
            logger.error(f"Error creating rule: {e}")
            raise
    
    def get_rule(self, rule_name: str) -> RuleInfo:
        """
        Get information about a rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            RuleInfo object
        """
        try:
            response = self.iot_client.get_topic_rule(ruleName=rule_name)
            rule = response["rule"]
            
            rule_info = RuleInfo(
                rule_name=rule["ruleName"],
                rule_arn=f"arn:aws:iot:{self.region_name}:*:rule/{rule['ruleName']}",
                sql=rule["sql"],
                description=rule.get("description", ""),
                rule_disabled=rule.get("ruleDisabled", False),
                actions=rule.get("actions", []),
                created_at=rule.get("createdAt"),
                last_modified_at=rule.get("lastModifiedAt")
            )
            
            return rule_info
            
        except ClientError as e:
            logger.error(f"Error getting rule: {e}")
            raise
    
    def update_rule(self, config: RuleConfig) -> bool:
        """
        Update a rule.
        
        Args:
            config: Rule configuration
            
        Returns:
            True if successful
        """
        kwargs = {
            "ruleName": config.rule_name,
            "sql": config.sql,
            "actions": config.actions,
            "awsIotSqlVersion": config.aws_iot_sql_version
        }
        
        if config.description:
            kwargs["description"] = config.description
        
        kwargs["ruleDisabled"] = config.rule_disabled
        
        try:
            self.iot_client.replace_topic_rule(**kwargs)
            logger.info(f"Updated rule: {config.rule_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating rule: {e}")
            raise
    
    def delete_rule(self, rule_name: str) -> bool:
        """
        Delete a rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_topic_rule(ruleName=rule_name)
            logger.info(f"Deleted rule: {rule_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting rule: {e}")
            raise
    
    def list_rules(
        self,
        topic: Optional[str] = None,
        max_results: int = 100
    ) -> List[RuleInfo]:
        """
        List rules.
        
        Args:
            topic: Filter by topic
            max_results: Maximum number of results
            
        Returns:
            List of RuleInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if topic:
            kwargs["topic"] = topic
        
        rules = []
        
        try:
            paginator = self.iot_client.get_paginator("list_topic_rules")
            
            for page in paginator.paginate(**kwargs):
                for rule in page.get("rules", []):
                    rules.append(RuleInfo(
                        rule_name=rule["ruleName"],
                        rule_arn=f"arn:aws:iot:{self.region_name}:*:rule/{rule['ruleName']}",
                        sql=rule["sql"],
                        description=rule.get("description", ""),
                        rule_disabled=rule.get("ruleDisabled", False),
                        actions=rule.get("actions", []),
                        created_at=rule.get("createdAt"),
                        last_modified_at=rule.get("lastModifiedAt")
                    ))
            
            return rules
            
        except ClientError as e:
            logger.error(f"Error listing rules: {e}")
            raise
    
    def enable_rule(self, rule_name: str) -> bool:
        """
        Enable a rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            True if successful
        """
        try:
            response = self.iot_client.get_topic_rule(ruleName=rule_name)
            rule = response["rule"]
            rule["ruleDisabled"] = False
            
            self.iot_client.replace_topic_rule(ruleName=rule_name, topicRulePayload=rule)
            logger.info(f"Enabled rule: {rule_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error enabling rule: {e}")
            raise
    
    def disable_rule(self, rule_name: str) -> bool:
        """
        Disable a rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            True if successful
        """
        try:
            response = self.iot_client.get_topic_rule(ruleName=rule_name)
            rule = response["rule"]
            rule["ruleDisabled"] = True
            
            self.iot_client.replace_topic_rule(ruleName=rule_name, topicRulePayload=rule)
            logger.info(f"Disabled rule: {rule_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error disabling rule: {e}")
            raise
    
    # =========================================================================
    # Job Management
    # =========================================================================
    
    def create_job(self, config: JobConfig) -> JobInfo:
        """
        Create a new job.
        
        Args:
            config: Job configuration
            
        Returns:
            JobInfo object with job details
        """
        kwargs = {
            "jobId": config.job_id,
            "targets": config.targets,
            "document": config.document,
            "targetSelection": config.target_selection
        }
        
        if config.description:
            kwargs["description"] = config.description
        
        if config.job_template_id:
            kwargs["jobTemplateId"] = config.job_template_id
        
        if config.presigned_url_config:
            kwargs["presignedUrlConfig"] = config.presigned_url_config
        
        if config.job_execution_timeout_minutes:
            kwargs["jobExecutionTimeoutMinutes"] = config.job_execution_timeout_minutes
        
        if config.tags:
            kwargs["tags"] = self._prepare_tags(config.tags)
        
        try:
            response = self.iot_client.create_job(**kwargs)
            
            job_info = JobInfo(
                job_id=response["jobId"],
                job_arn=response["jobArn"],
                status=JobStatus(response.get("status", "IN_PROGRESS")),
                targets=config.targets,
                description=config.description,
                created_at=response.get("createdAt"),
                last_modified_at=response.get("lastModifiedAt")
            )
            
            logger.info(f"Created job: {config.job_id}")
            return job_info
            
        except ClientError as e:
            logger.error(f"Error creating job: {e}")
            raise
    
    def describe_job(self, job_id: str) -> JobInfo:
        """
        Get information about a job.
        
        Args:
            job_id: Job ID
            
        Returns:
            JobInfo object
        """
        try:
            response = self.iot_client.describe_job(jobId=job_id)
            job = response["job"]
            
            job_info = JobInfo(
                job_id=job["jobId"],
                job_arn=job["jobArn"],
                status=JobStatus(job["status"]),
                targets=job.get("targets", []),
                description=job.get("description"),
                created_at=job.get("createdAt"),
                last_modified_at=job.get("lastModifiedAt")
            )
            
            return job_info
            
        except ClientError as e:
            logger.error(f"Error describing job: {e}")
            raise
    
    def cancel_job(self, job_id: str, reason: str = "") -> bool:
        """
        Cancel a job.
        
        Args:
            job_id: Job ID
            reason: Cancellation reason
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.cancel_job(jobId=job_id, reason=reason)
            logger.info(f"Canceled job: {job_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error canceling job: {e}")
            raise
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job.
        
        Args:
            job_id: Job ID
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_job(jobId=job_id)
            logger.info(f"Deleted job: {job_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting job: {e}")
            raise
    
    def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        target_thing_group_name: Optional[str] = None,
        max_results: int = 100
    ) -> List[JobInfo]:
        """
        List jobs.
        
        Args:
            status: Filter by status
            target_thing_group_name: Filter by target thing group
            max_results: Maximum number of results
            
        Returns:
            List of JobInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if status:
            kwargs["status"] = status.value
        
        if target_thing_group_name:
            kwargs["targetThingGroup"] = target_thing_group_name
        
        jobs = []
        
        try:
            if status:
                paginator = self.iot_client.get_paginator("list_jobs")
                for page in paginator.paginate(**kwargs):
                    for job in page.get("jobs", []):
                        jobs.append(JobInfo(
                            job_id=job["jobId"],
                            job_arn=job["jobArn"],
                            status=JobStatus(job["status"]),
                            targets=job.get("targets", []),
                            description=job.get("description"),
                            created_at=job.get("createdAt"),
                            last_modified_at=job.get("lastModifiedAt")
                        ))
            else:
                for status_filter in [JobStatus.IN_PROGRESS, JobStatus.QUEUED, JobStatus.SUCCEEDED, JobStatus.FAILED]:
                    kwargs["status"] = status_filter.value
                    paginator = self.iot_client.get_paginator("list_jobs")
                    for page in paginator.paginate(**kwargs):
                        for job in page.get("jobs", []):
                            jobs.append(JobInfo(
                                job_id=job["jobId"],
                                job_arn=job["jobArn"],
                                status=JobStatus(job["status"]),
                                targets=job.get("targets", []),
                                description=job.get("description"),
                                created_at=job.get("createdAt"),
                                last_modified_at=job.get("lastModifiedAt")
                            ))
            
            return jobs
            
        except ClientError as e:
            logger.error(f"Error listing jobs: {e}")
            raise
    
    def describe_job_execution(self, job_id: str, thing_name: str) -> Dict[str, Any]:
        """
        Get job execution details for a thing.
        
        Args:
            job_id: Job ID
            thing_name: Thing name
            
        Returns:
            Job execution details
        """
        try:
            response = self.iot_client.describe_job_execution(jobId=job_id, thingName=thing_name)
            return response.get("execution", {})
            
        except ClientError as e:
            logger.error(f"Error describing job execution: {e}")
            raise
    
    # =========================================================================
    # Secure Tunneling
    # =========================================================================
    
    def create_tunnel(self, config: TunnelConfig) -> TunnelInfo:
        """
        Create a new secure tunnel.
        
        Args:
            config: Tunnel configuration
            
        Returns:
            TunnelInfo object with tunnel details
        """
        kwargs = {}
        
        if config.thing_name:
            kwargs["things"] = [config.thing_name]
        
        if config.description:
            kwargs["description"] = config.description
        
        if config.tags:
            kwargs["tags"] = self._prepare_tags(config.tags)
        
        try:
            response = self.iot_client.create_tunnel(**kwargs)
            
            tunnel_info = TunnelInfo(
                tunnel_id=response["tunnelId"],
                tunnel_arn=response["tunnelArn"],
                thing_name=config.thing_name,
                description=config.description,
                status=TunnelStatus.OPEN,
                created_at=response.get("createdAt")
            )
            
            logger.info(f"Created tunnel: {tunnel_info.tunnel_id}")
            return tunnel_info
            
        except ClientError as e:
            logger.error(f"Error creating tunnel: {e}")
            raise
    
    def describe_tunnel(self, tunnel_id: str) -> TunnelInfo:
        """
        Get information about a tunnel.
        
        Args:
            tunnel_id: Tunnel ID
            
        Returns:
            TunnelInfo object
        """
        try:
            response = self.iot_client.describe_tunnel(tunnelId=tunnel_id)
            tunnel = response["tunnel"]
            
            tunnel_info = TunnelInfo(
                tunnel_id=tunnel["tunnelId"],
                tunnel_arn=tunnel["tunnelArn"],
                thing_name=tunnel.get("thing"),
                description=tunnel.get("description"),
                status=TunnelStatus(tunnel.get("status", "Open")),
                created_at=tunnel.get("createdAt")
            )
            
            return tunnel_info
            
        except ClientError as e:
            logger.error(f"Error describing tunnel: {e}")
            raise
    
    def close_tunnel(self, tunnel_id: str) -> bool:
        """
        Close a tunnel.
        
        Args:
            tunnel_id: Tunnel ID
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.close_tunnel(tunnelId=tunnel_id)
            logger.info(f"Closed tunnel: {tunnel_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error closing tunnel: {e}")
            raise
    
    def delete_tunnel(self, tunnel_id: str) -> bool:
        """
        Delete a tunnel.
        
        Args:
            tunnel_id: Tunnel ID
            
        Returns:
            True if successful
        """
        try:
            self.iot_client.delete_tunnel(tunnelId=tunnel_id)
            logger.info(f"Deleted tunnel: {tunnel_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting tunnel: {e}")
            raise
    
    def list_tunnels(
        self,
        thing_name: Optional[str] = None,
        max_results: int = 100
    ) -> List[TunnelInfo]:
        """
        List tunnels.
        
        Args:
            thing_name: Filter by thing name
            max_results: Maximum number of results
            
        Returns:
            List of TunnelInfo objects
        """
        kwargs = {"maxResults": max_results}
        
        if thing_name:
            kwargs["thingName"] = thing_name
        
        tunnels = []
        
        try:
            paginator = self.iot_client.get_paginator("list_tunnels")
            
            for page in paginator.paginate(**kwargs):
                for tunnel in page.get("tunnelSummaries", []):
                    tunnels.append(TunnelInfo(
                        tunnel_id=tunnel["tunnelId"],
                        tunnel_arn=tunnel.get("tunnelArn", ""),
                        thing_name=tunnel.get("thing"),
                        description=tunnel.get("description"),
                        status=TunnelStatus(tunnel.get("status", "Open")),
                        created_at=tunnel.get("createdAt")
                    ))
            
            return tunnels
            
        except ClientError as e:
            logger.error(f"Error listing tunnels: {e}")
            raise
    
    def open_tunnel(self, tunnel_id: str, timeout: int = 3600) -> Dict[str, Any]:
        """
        Open a tunnel and get connection details.
        
        Args:
            tunnel_id: Tunnel ID
            timeout: Connection timeout in seconds
            
        Returns:
            Connection details with access tokens
        """
        try:
            response = self.iot_client.open_tunnel(tunnelId=tunnel_id, timeout=timeout)
            return {
                "tunnel_id": tunnel_id,
                "access_token": response["sourceAccessToken"],
                "destination_token": response["destinationAccessToken"]
            }
            
        except ClientError as e:
            logger.error(f"Error opening tunnel: {e}")
            raise
    
    # =========================================================================
    # Fleet Indexing
    # =========================================================================
    
    def get_fleet_indexing_configuration(self) -> FleetIndexingInfo:
        """
        Get fleet indexing configuration.
        
        Returns:
            FleetIndexingInfo object
        """
        try:
            response = self.iot_client.get_indexing_configuration()
            indexing_config = response.get("thingIndexingConfiguration", {})
            
            fleet_indexing_info = FleetIndexingInfo(
                thing_indexing_mode=indexing_config.get("thingIndexingMode", "REGISTRY_AND_SHADOW"),
                thing_connectivity_indexing_mode=indexing_config.get("thingConnectivityIndexingMode", "STATUS"),
                field_indexing_mode=indexing_config.get("fieldIndexingMode"),
                managed_fields=indexing_config.get("managedFields", []),
                custom_fields=indexing_config.get("customFields", [])
            )
            
            return fleet_indexing_info
            
        except ClientError as e:
            logger.error(f"Error getting fleet indexing configuration: {e}")
            raise
    
    def update_fleet_indexing(
        self,
        config: FleetIndexingConfig
    ) -> bool:
        """
        Update fleet indexing configuration.
        
        Args:
            config: Fleet indexing configuration
            
        Returns:
            True if successful
        """
        kwargs = {
            "thingIndexingMode": config.thing_indexing_mode,
            "thingConnectivityIndexingMode": config.thing_connectivity_indexing_mode
        }
        
        if config.custom_fields:
            kwargs["customFields"] = config.custom_fields
        
        try:
            self.iot_client.update_indexing_configuration(**kwargs)
            logger.info("Updated fleet indexing configuration")
            return True
            
        except ClientError as e:
            logger.error(f"Error updating fleet indexing: {e}")
            raise
    
    def search_things(
        self,
        query_string: str,
        index_name: str = "AWS_Things",
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search for things using fleet indexing.
        
        Args:
            query_string: Query string
            index_name: Index name
            max_results: Maximum number of results
            
        Returns:
            List of thing search results
        """
        kwargs = {
            "queryString": query_string,
            "maxResults": max_results
        }
        
        try:
            response = self.iot_client.search_index(queryString=query_string, maxResults=max_results)
            return response.get("things", [])
            
        except ClientError as e:
            logger.error(f"Error searching things: {e}")
            raise
    
    def get_thing_shadow(self, thing_name: str) -> Dict[str, Any]:
        """
        Get thing shadow.
        
        Args:
            thing_name: Thing name
            
        Returns:
            Shadow document
        """
        try:
            response = self.iot_data_client.get_thing_shadow(thingName=thing_name)
            
            payload = json.loads(response["payload"].read())
            return payload
            
        except ClientError as e:
            logger.error(f"Error getting thing shadow: {e}")
            raise
    
    def update_thing_shadow(
        self,
        thing_name: str,
        state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update thing shadow.
        
        Args:
            thing_name: Thing name
            state: State to update
            
        Returns:
            Updated shadow document
        """
        try:
            payload = json.dumps(state).encode()
            
            response = self.iot_data_client.update_thing_shadow(
                thingName=thing_name,
                payload=payload
            )
            
            result = json.loads(response["payload"].read())
            return result
            
        except ClientError as e:
            logger.error(f"Error updating thing shadow: {e}")
            raise
    
    def delete_thing_shadow(self, thing_name: str) -> bool:
        """
        Delete thing shadow.
        
        Args:
            thing_name: Thing name
            
        Returns:
            True if successful
        """
        try:
            self.iot_data_client.delete_thing_shadow(thingName=thing_name)
            logger.info(f"Deleted shadow for thing: {thing_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Error deleting thing shadow: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def get_iot_metrics(
        self,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300,
        statistics: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get IoT metrics from CloudWatch.
        
        Args:
            metric_name: Metric name
            start_time: Start time
            end_time: End time
            period: Period in seconds
            statistics: Statistics to retrieve
            
        Returns:
            List of metric data points
        """
        if statistics is None:
            statistics = ["Average", "Sum", "Maximum", "Minimum"]
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/IoT",
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics
            )
            
            return response.get("Datapoints", [])
            
        except ClientError as e:
            logger.error(f"Error getting IoT metrics: {e}")
            raise
    
    def list_iot_metrics(
        self,
        prefix: str = "AWS/IoT"
    ) -> List[str]:
        """
        List available IoT metrics.
        
        Args:
            prefix: Namespace prefix
            
        Returns:
            List of metric names
        """
        try:
            response = self.cloudwatch_client.list_metrics(
                Namespace=prefix
            )
            
            metrics = []
            for metric in response.get("Metrics", []):
                metrics.append(metric["MetricName"])
            
            return list(set(metrics))
            
        except ClientError as e:
            logger.error(f"Error listing IoT metrics: {e}")
            raise
    
    def get_thing_connectivity(self, thing_name: str) -> Dict[str, Any]:
        """
        Get thing connectivity status.
        
        Args:
            thing_name: Thing name
            
        Returns:
            Connectivity status
        """
        try:
            response = self.iot_client.describe_thing_connectivity(thingName=thing_name)
            return {
                "connected": response.get("connected"),
                "disconnect_time": response.get("disconnectTime"),
                "last_active_time": response.get("lastActivityTime")
            }
            
        except ClientError as e:
            logger.error(f"Error getting thing connectivity: {e}")
            raise
    
    def put_metric_data(self, metric_name: str, value: float, unit: str = "Count") -> bool:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            metric_name: Metric name
            value: Metric value
            unit: Unit
            
        Returns:
            True if successful
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="Custom/IoT",
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit
                    }
                ]
            )
            logger.info(f"Put metric data: {metric_name}={value}")
            return True
            
        except ClientError as e:
            logger.error(f"Error putting metric data: {e}")
            raise
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _prepare_tags(self, tags: Dict[str, str]) -> List[Dict[str, str]]:
        """Prepare tags for AWS API."""
        return [{"Key": k, "Value": v} for k, v in tags.items()]
    
    def _get_thing_type_name(self, thing_name: str) -> Optional[str]:
        """Get thing type name for a thing."""
        try:
            info = self.get_thing(thing_name)
            return info.thing_type_name
        except Exception:
            return None
