"""
AWS Amazon Connect Integration Module for Workflow System

Implements a ConnectIntegration class with:
1. Instance management: Create/manage Connect instances
2. Contact flow management: Create/manage contact flows
3. Quick connects: Manage quick connects
4. Users: Create/manage agents/users
5. Routing profiles: Manage routing profiles
6. Queues: Manage queues
7. Hours of operation: Manage hours
8. Prompts: Manage contact center prompts
9. Contact search: Search contacts
10. CloudWatch integration: Contact center metrics

Commit: 'feat(aws-connect): add Amazon Connect with instance management, contact flows, quick connects, users, routing profiles, queues, hours of operation, prompts, contact search, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
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


class InstanceAttributeType(Enum):
    """Connect instance attribute types."""
    INBOUND_CALL = "INBOUND_CALL"
    OUTBOUND_CALL = "OUTBOUND_CALL"
    CONTACT_CAMERA_PUSH = "CONTACT_CAMERA_PUSH"
    CONTACT_CAMERA_PULL = "CONTACT_CAMERA_PULL"
    CONTACT_LENS = "CONTACT_LENS"
    EKW_TEAM = "EKW_TEAM"
    EKW_DEDICATED = "EKW_DEDICATED"
    USE_CUSTOM_TTS_VOICES = "USE_CUSTOM_TTS_VOICES"
    REDIRECT_CONTACT_CARD = "REDIRECT_CONTACT_CARD"
    OUTBOUND_VOICE_ID = "OUTBOUND_VOICE_ID"
    TAW_CONFIRONMENT_VARIABLES = "TAW_CONFIRONMENT_VARIABLES"
    TAW_ENDPOINT_URL = "TAW_ENDPOINT_URL"
    ASSISTED_TRANSFER = "ASSISTED_TRANSFER"
    TRANSFER_DESTINATION = "TRANSFER_DESTINATION"
    CALLBACK_NUMBERS = "CALLBACK_NUMBERS"
    ENHANCED_CHAT_CONTACT_ROUTING = "ENHANCED_CHAT_CONTACT_ROUTING"
    ENHANCED_MONITORING = "ENHANCED_MONITORING"
    USER_ATTRIBUTE_TYPES = "USER_ATTRIBUTE_TYPES"
    CALLER_IDENTIFIER = "CALLER_IDENTIFIER"


class ContactStateType(Enum):
    """Contact state types."""
    INCOMING = "INCOMING"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    CONNECTED_ON_QUEUE = "CONNECTED_ON_QUEUE"
    TRANSFERRING = "TRANSFERRING"
    TRANSFERRED = "TRANSFERRED"
    HOLD = "HOLD"
    DISCONNECTING = "DISCONNECTING"
    ENDED = "ENDED"


class ContactInitiationMethod(Enum):
    """Contact initiation methods."""
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"
    TRANSFER = "TRANSFER"
    CALLBACK = "CALLBACK"
    QUEUE_TRANSFER = "QUEUE_TRANSFER"


class QueueType(Enum):
    """Queue types."""
    STANDARD = "STANDARD"
    AGENT = "AGENT"


class RoutingProfileEventType(Enum):
    """Routing profile event types."""
    ANY = "ANY"
    VOICE = "VOICE"
    CHAT = "CHAT"


class QuickConnectType(Enum):
    """Quick connect types."""
    PHONE_NUMBER = "PHONE_NUMBER"
    QUEUE = "QUEUE"
    USER = "USER"
    TRANSFER = "TRANSFER"


class HoursOfOperationType(Enum):
    """Hours of operation types."""
    CUSTOM = "CUSTOM"


class HoursOfOperationTimeShiftType(Enum):
    """Hours of operation time shift types."""
    SHIFT_FORWARD = "SHIFT_FORWARD"
    SHIFT_BACKWARD = "SHIFT_BACKWARD"


class AgentStatusType(Enum):
    """Agent status types."""
    INIT = "INIT"
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    AWAY = "AWAY"
    BUSY = "BUSY"
    ROUTABLE = "ROUTABLE"


class ContactFlowType(Enum):
    """Contact flow types."""
    CONTACT_FLOW = "CONTACT_FLOW"
    CUSTOMER_QUEUE = "CUSTOMER_QUEUE"
    CUSTOMER_HOLD = "CUSTOMER_HOLD"
    CUSTOMER_WHISTLEBLOWER = "CUSTOMER_WHISTLEBLOWER"
    AGENT_HOLD = "AGENT_HOLD"
    AGENT_WHISTLEBLOWER = "AGENT_WHISTLEBLOWER"
    OUTBOUND_WHISPER = "OUTBOUND_WHISPER"
    WHISPER_TRANSFER = "WHISPER_TRANSFER"
    MONITORING = "MONITORING"


class SecurityProfileType(Enum):
    """Security profile types."""
    AGENT = "AGENT"
    ADMIN = "ADMIN"
    QUALITY_ASSURANCE = "QUALITY_ASSURANCE"


@dataclass
class ConnectConfig:
    """Configuration for Connect connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    config: Optional[Any] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class InstanceConfig:
    """Configuration for creating a Connect instance."""
    identity_management_type: str
    instance_alias: Optional[str] = None
    inbound_calls_enabled: bool = True
    outbound_calls_enabled: bool = True
    contact_attributes: Optional[Dict[str, Any]] = None
    attributes: Optional[Dict[str, str]] = None


@dataclass
class InstanceInfo:
    """Information about a Connect instance."""
    instance_id: str
    instance_arn: str
    identity_management_type: str
    instance_alias: Optional[str] = None
    created_time: Optional[str] = None
    service_role: Optional[str] = None
    status: Optional[str] = None


@dataclass
class ContactFlowConfig:
    """Configuration for creating a contact flow."""
    name: str
    description: str = ""
    contact_flow_state: str = "ACTIVE"
    content: Optional[Dict[str, Any]] = None
    type: str = "CONTACT_FLOW"


@dataclass
class ContactFlowInfo:
    """Information about a contact flow."""
    contact_flow_id: str
    contact_flow_arn: str
    name: str
    description: str
    type: str
    state: str
    status: Optional[str] = None


@dataclass
class QuickConnectConfig:
    """Configuration for creating a quick connect."""
    name: str
    description: str = ""
    quick_connect_type: Union[QuickConnectType, str]
    destination: Optional[Dict[str, Any]] = None


@dataclass
class QuickConnectInfo:
    """Information about a quick connect."""
    quick_connect_id: str
    quick_connect_arn: str
    name: str
    description: str
    type: str
    destination: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UserConfig:
    """Configuration for creating a user/agent."""
    username: str
    password: str
    identity_info: Dict[str, Any]
    phone_config: Dict[str, Any]
    routing_profile_id: str
    security_profile_ids: List[str]
    hierarchy_group_id: Optional[str] = None


@dataclass
class UserInfo:
    """Information about a user/agent."""
    id: str
    arn: str
    username: str
    identity_info: Dict[str, Any]
    phone_config: Dict[str, Any]
    routing_profile_id: str
    routing_profile_arn: str
    security_profile_ids: List[str]
    hierarchy_group_id: Optional[str] = None
    status: Optional[Dict[str, str]] = None


@dataclass
class RoutingProfileConfig:
    """Configuration for creating a routing profile."""
    name: str
    description: str = ""
    instance_id: str
    default_outbound_queue_id: str
    media_concurrencies: List[Dict[str, Any]]
    routing_profile_queue_configs: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RoutingProfileInfo:
    """Information about a routing profile."""
    routing_profile_id: str
    routing_profile_arn: str
    name: str
    description: str
    instance_id: str
    default_outbound_queue_id: str
    media_concurrencies: List[Dict[str, Any]]
    queue_configs: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class QueueConfig:
    """Configuration for creating a queue."""
    name: str
    description: str = ""
    queue_type: Union[QueueType, str] = QueueType.STANDARD
    hours_of_operation_id: Optional[str] = None
    max_contacts: Optional[int] = None
    quick_connect_ids: List[str] = field(default_factory=list)


@dataclass
class QueueInfo:
    """Information about a queue."""
    queue_id: str
    queue_arn: str
    name: str
    description: str
    queue_type: str
    status: Optional[str] = None
    hours_of_operation_id: Optional[str] = None
    max_contacts: Optional[int] = None


@dataclass
class HoursOfOperationConfig:
    """Configuration for hours of operation."""
    name: str
    description: str = ""
    time_zone: str
    hours_of_operation_config: List[Dict[str, Any]]


@dataclass
class HoursOfOperationInfo:
    """Information about hours of operation."""
    hours_of_operation_id: str
    hours_of_operation_arn: str
    name: str
    description: str
    time_zone: str
    config: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PromptConfig:
    """Configuration for creating a prompt."""
    name: str
    description: str = ""
    prompt_file: Optional[bytes] = None
    s3_uri: Optional[str] = None


@dataclass
class PromptInfo:
    """Information about a prompt."""
    prompt_id: str
    prompt_arn: str
    name: str
    description: str
    status: Optional[str] = None


@dataclass
class ContactSearchResult:
    """Contact search result."""
    contact_id: str
    contact_arn: str
    initial_contact_id: Optional[str] = None
    previous_contact_id: Optional[str] = None
    initiation_method: Optional[str] = None
    state: Optional[str] = None
    state_start_timestamp: Optional[str] = None
    queue_info: Optional[Dict[str, Any]] = None
    connected_to_agent_timestamp: Optional[str] = None
    customer_endpoint: Optional[Dict[str, Any]] = None
    agent_endpoint: Optional[Dict[str, Any]] = None
    total_contact_duration: Optional[int] = None
    queue_duration: Optional[int] = None
    wait_time: Optional[int] = None


@dataclass
class ContactMetrics:
    """Contact center metrics."""
    metrics: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[str] = None
    period: Optional[int] = None


class ConnectIntegration:
    """
    AWS Amazon Connect integration class for contact center operations.
    
    Supports:
    - Instance creation and management
    - Contact flow creation and management
    - Quick connects for easy transfers
    - User/agent management
    - Routing profiles for call distribution
    - Queue management
    - Hours of operation configuration
    - Contact center prompts
    - Contact search and history
    - CloudWatch metrics integration
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        connect_client: Optional[Any] = None,
        cloudwatch_client: Optional[Any] = None
    ):
        """
        Initialize Connect integration.
        
        Args:
            aws_access_key_id: AWS access key ID (uses boto3 credentials if None)
            aws_secret_access_key: AWS secret access key (uses boto3 credentials if None)
            region_name: AWS region name
            endpoint_url: Connect endpoint URL (for testing with LocalStack, etc.)
            connect_client: Pre-configured Connect client (overrides boto3 creation)
            cloudwatch_client: Pre-configured CloudWatch client for metrics
        """
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for ConnectIntegration. "
                "Install it with: pip install boto3"
            )
        
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._clients = {}
        self._lock = threading.Lock()
        
        if connect_client:
            self._clients['connect'] = connect_client
        if cloudwatch_client:
            self._clients['cloudwatch'] = cloudwatch_client
        
        self._session = None
        self._config = None
        
        if aws_access_key_id and aws_secret_access_key:
            self._session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token if 'aws_session_token' in dir() else None,
                region_name=region_name
            )
        else:
            self._session = boto3.Session(region_name=region_name)
    
    def _get_client(self, service_name: str):
        """Get or create a boto3 client with caching."""
        with self._lock:
            if service_name not in self._clients:
                self._clients[service_name] = self._session.client(
                    service_name,
                    endpoint_url=self.endpoint_url
                )
            return self._clients[service_name]
    
    @property
    def connect(self):
        """Get Connect client."""
        return self._get_client('connect')
    
    @property
    def cloudwatch(self):
        """Get CloudWatch client."""
        return self._get_client('cloudwatch')
    
    # =========================================================================
    # Instance Management
    # =========================================================================
    
    def create_instance(self, config: InstanceConfig) -> InstanceInfo:
        """
        Create a new Amazon Connect instance.
        
        Args:
            config: Instance configuration
            
        Returns:
            InstanceInfo object with instance details
        """
        try:
            params = {
                'IdentityManagementType': config.identity_management_type,
                'InboundCallsEnabled': config.inbound_calls_enabled,
                'OutboundCallsEnabled': config.outbound_calls_enabled
            }
            
            if config.instance_alias:
                params['InstanceAlias'] = config.instance_alias
            
            if config.attributes:
                params['Attributes'] = config.attributes
            
            response = self.connect.create_instance(**params)
            
            return InstanceInfo(
                instance_id=response['InstanceId'],
                instance_arn=response['InstanceArn'],
                identity_management_type=config.identity_management_type,
                instance_alias=config.instance_alias,
                created_time=response.get('CreatedTime'),
                service_role=response.get('ServiceRole'),
                status=response.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to create instance: {e}")
            raise
    
    def describe_instance(self, instance_id: str) -> InstanceInfo:
        """
        Describe a Connect instance.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            InstanceInfo object with instance details
        """
        try:
            response = self.connect.describe_instance(
                InstanceId=instance_id
            )
            
            info = response['Instance']
            return InstanceInfo(
                instance_id=info['InstanceId'],
                instance_arn=info['InstanceArn'],
                identity_management_type=info['IdentityManagementType'],
                instance_alias=info.get('InstanceAlias'),
                created_time=info.get('CreatedTime'),
                service_role=info.get('ServiceRole'),
                status=info.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to describe instance: {e}")
            raise
    
    def list_instances(self) -> List[InstanceInfo]:
        """
        List all Connect instances.
        
        Returns:
            List of InstanceInfo objects
        """
        try:
            response = self.connect.list_instances()
            instances = []
            
            for info in response.get('InstanceSummaryList', []):
                instances.append(InstanceInfo(
                    instance_id=info['InstanceId'],
                    instance_arn=info['InstanceArn'],
                    identity_management_type=info['IdentityManagementType'],
                    instance_alias=info.get('InstanceAlias'),
                    created_time=info.get('CreatedTime'),
                    status=info.get('Status')
                ))
            
            return instances
        except ClientError as e:
            logger.error(f"Failed to list instances: {e}")
            raise
    
    def delete_instance(self, instance_id: str) -> bool:
        """
        Delete a Connect instance.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_instance(InstanceId=instance_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete instance: {e}")
            raise
    
    def associate_instance_attribute(
        self,
        instance_id: str,
        attribute_type: Union[InstanceAttributeType, str],
        value: str
    ) -> bool:
        """
        Associate an attribute with an instance.
        
        Args:
            instance_id: Instance ID
            attribute_type: Attribute type
            value: Attribute value
            
        Returns:
            True if successful
        """
        try:
            attr_type = attribute_type.value if isinstance(attribute_type, InstanceAttributeType) else attribute_type
            
            self.connect.associate_instance_attribute(
                InstanceId=instance_id,
                AttributeType=attr_type,
                Value=value
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to associate attribute: {e}")
            raise
    
    # =========================================================================
    # Contact Flow Management
    # =========================================================================
    
    def create_contact_flow(
        self,
        instance_id: str,
        config: ContactFlowConfig
    ) -> ContactFlowInfo:
        """
        Create a contact flow.
        
        Args:
            instance_id: Instance ID
            config: Contact flow configuration
            
        Returns:
            ContactFlowInfo object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Name': config.name,
                'Type': config.type,
                'State': config.contact_flow_state
            }
            
            if config.description:
                params['Description'] = config.description
            
            if config.content:
                params['Content'] = json.dumps(config.content) if isinstance(config.content, dict) else config.content
            
            response = self.connect.create_contact_flow(**params)
            
            return ContactFlowInfo(
                contact_flow_id=response['ContactFlowId'],
                contact_flow_arn=response['ContactFlowArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                type=response['Type'],
                state=response['State'],
                status=response.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to create contact flow: {e}")
            raise
    
    def describe_contact_flow(
        self,
        instance_id: str,
        contact_flow_id: str
    ) -> ContactFlowInfo:
        """
        Describe a contact flow.
        
        Args:
            instance_id: Instance ID
            contact_flow_id: Contact flow ID
            
        Returns:
            ContactFlowInfo object
        """
        try:
            response = self.connect.describe_contact_flow(
                InstanceId=instance_id,
                ContactFlowId=contact_flow_id
            )
            
            info = response['ContactFlow']
            return ContactFlowInfo(
                contact_flow_id=info['ContactFlowId'],
                contact_flow_arn=info['ContactFlowArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                type=info['Type'],
                state=info['State'],
                status=info.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to describe contact flow: {e}")
            raise
    
    def list_contact_flows(
        self,
        instance_id: str,
        contact_flow_type: Optional[str] = None
    ) -> List[ContactFlowInfo]:
        """
        List contact flows.
        
        Args:
            instance_id: Instance ID
            contact_flow_type: Optional filter by type
            
        Returns:
            List of ContactFlowInfo objects
        """
        try:
            params = {'InstanceId': instance_id}
            if contact_flow_type:
                params['ContactFlowType'] = contact_flow_type
            
            response = self.connect.list_contact_flows(**params)
            flows = []
            
            for info in response.get('ContactFlowSummaryList', []):
                flows.append(ContactFlowInfo(
                    contact_flow_id=info['Id'],
                    contact_flow_arn=info['Arn'],
                    name=info['Name'],
                    description=info.get('Description', ''),
                    type=info['Type'],
                    state=info.get('State', 'ACTIVE')
                ))
            
            return flows
        except ClientError as e:
            logger.error(f"Failed to list contact flows: {e}")
            raise
    
    def update_contact_flow_content(
        self,
        instance_id: str,
        contact_flow_id: str,
        content: Dict[str, Any]
    ) -> bool:
        """
        Update contact flow content.
        
        Args:
            instance_id: Instance ID
            contact_flow_id: Contact flow ID
            content: Contact flow content (JSON)
            
        Returns:
            True if successful
        """
        try:
            self.connect.update_contact_flow_content(
                InstanceId=instance_id,
                ContactFlowId=contact_flow_id,
                Content=json.dumps(content) if isinstance(content, dict) else content
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update contact flow content: {e}")
            raise
    
    def delete_contact_flow(
        self,
        instance_id: str,
        contact_flow_id: str
    ) -> bool:
        """
        Delete a contact flow.
        
        Args:
            instance_id: Instance ID
            contact_flow_id: Contact flow ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_contact_flow(
                InstanceId=instance_id,
                ContactFlowId=contact_flow_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete contact flow: {e}")
            raise
    
    def start_contact_flow(
        self,
        instance_id: str,
        contact_flow_id: str,
        source_id: str,
        channel: str = "VOICE"
    ) -> Dict[str, Any]:
        """
        Start a contact flow.
        
        Args:
            instance_id: Instance ID
            contact_flow_id: Contact flow ID
            source_id: Source identifier (phone number, queue, etc.)
            channel: Channel type (VOICE, CHAT)
            
        Returns:
            Start contact response
        """
        try:
            response = self.connect.start_contact_flow(
                InstanceId=instance_id,
                ContactFlowId=contact_flow_id,
                Source=source_id,
                Channel=channel
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to start contact flow: {e}")
            raise
    
    # =========================================================================
    # Quick Connects
    # =========================================================================
    
    def create_quick_connect(
        self,
        instance_id: str,
        config: QuickConnectConfig
    ) -> QuickConnectInfo:
        """
        Create a quick connect.
        
        Args:
            instance_id: Instance ID
            config: Quick connect configuration
            
        Returns:
            QuickConnectInfo object
        """
        try:
            qc_type = config.quick_connect_type.value if isinstance(config.quick_connect_type, QuickConnectType) else config.quick_connect_type
            
            params = {
                'InstanceId': instance_id,
                'Name': config.name,
                'Type': qc_type
            }
            
            if config.description:
                params['Description'] = config.description
            
            if config.destination:
                params['QuickConnectConfig'] = {
                    'QuickConnectType': qc_type,
                    'Destination': config.destination
                }
            
            response = self.connect.create_quick_connect(**params)
            
            return QuickConnectInfo(
                quick_connect_id=response['QuickConnectId'],
                quick_connect_arn=response['QuickConnectArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                type=response['Type'],
                destination=response.get('QuickConnectConfig', {}).get('Destination', {})
            )
        except ClientError as e:
            logger.error(f"Failed to create quick connect: {e}")
            raise
    
    def describe_quick_connect(
        self,
        instance_id: str,
        quick_connect_id: str
    ) -> QuickConnectInfo:
        """
        Describe a quick connect.
        
        Args:
            instance_id: Instance ID
            quick_connect_id: Quick connect ID
            
        Returns:
            QuickConnectInfo object
        """
        try:
            response = self.connect.describe_quick_connect(
                InstanceId=instance_id,
                QuickConnectId=quick_connect_id
            )
            
            info = response['QuickConnect']
            return QuickConnectInfo(
                quick_connect_id=info['QuickConnectId'],
                quick_connect_arn=info['QuickConnectArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                type=info['Type'],
                destination=info.get('QuickConnectConfig', {}).get('Destination', {})
            )
        except ClientError as e:
            logger.error(f"Failed to describe quick connect: {e}")
            raise
    
    def list_quick_connects(
        self,
        instance_id: str
    ) -> List[QuickConnectInfo]:
        """
        List quick connects.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            List of QuickConnectInfo objects
        """
        try:
            response = self.connect.list_quick_connects(InstanceId=instance_id)
            connects = []
            
            for info in response.get('QuickConnectSummaryList', []):
                connects.append(QuickConnectInfo(
                    quick_connect_id=info['Id'],
                    quick_connect_arn=info['Arn'],
                    name=info['Name'],
                    description=info.get('Description', ''),
                    type=info['Type'],
                    destination={}
                ))
            
            return connects
        except ClientError as e:
            logger.error(f"Failed to list quick connects: {e}")
            raise
    
    def delete_quick_connect(
        self,
        instance_id: str,
        quick_connect_id: str
    ) -> bool:
        """
        Delete a quick connect.
        
        Args:
            instance_id: Instance ID
            quick_connect_id: Quick connect ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_quick_connect(
                InstanceId=instance_id,
                QuickConnectId=quick_connect_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete quick connect: {e}")
            raise
    
    # =========================================================================
    # Users (Agents)
    # =========================================================================
    
    def create_user(self, instance_id: str, config: UserConfig) -> UserInfo:
        """
        Create a user/agent.
        
        Args:
            instance_id: Instance ID
            config: User configuration
            
        Returns:
            UserInfo object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Username': config.username,
                'Password': config.password,
                'IdentityInfo': config.identity_info,
                'PhoneConfig': config.phone_config,
                'RoutingProfileId': config.routing_profile_id,
                'SecurityProfileIds': config.security_profile_ids
            }
            
            if config.hierarchy_group_id:
                params['HierarchyGroupId'] = config.hierarchy_group_id
            
            response = self.connect.create_user(**params)
            
            return UserInfo(
                id=response['UserId'],
                arn=response['UserArn'],
                username=response['Username'],
                identity_info=response.get('IdentityInfo', {}),
                phone_config=response.get('PhoneConfig', {}),
                routing_profile_id=response.get('RoutingProfileId', ''),
                routing_profile_arn=response.get('RoutingProfileArn', ''),
                security_profile_ids=response.get('SecurityProfileIds', []),
                hierarchy_group_id=response.get('HierarchyGroupId'),
                status=response.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to create user: {e}")
            raise
    
    def describe_user(
        self,
        instance_id: str,
        user_id: str
    ) -> UserInfo:
        """
        Describe a user.
        
        Args:
            instance_id: Instance ID
            user_id: User ID
            
        Returns:
            UserInfo object
        """
        try:
            response = self.connect.describe_user(
                InstanceId=instance_id,
                UserId=user_id
            )
            
            info = response['User']
            return UserInfo(
                id=info['Id'],
                arn=info['Arn'],
                username=info['Username'],
                identity_info=info.get('IdentityInfo', {}),
                phone_config=info.get('PhoneConfig', {}),
                routing_profile_id=info.get('RoutingProfileId', ''),
                routing_profile_arn=info.get('RoutingProfileArn', ''),
                security_profile_ids=info.get('SecurityProfileIds', []),
                hierarchy_group_id=info.get('HierarchyGroupId'),
                status=info.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to describe user: {e}")
            raise
    
    def list_users(self, instance_id: str) -> List[UserInfo]:
        """
        List users.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            List of UserInfo objects
        """
        try:
            response = self.connect.list_users(InstanceId=instance_id)
            users = []
            
            for info in response.get('UserSummaryList', []):
                users.append(UserInfo(
                    id=info['Id'],
                    arn=info['Arn'],
                    username=info['Username'],
                    identity_info={},
                    phone_config={},
                    routing_profile_id=info.get('RoutingProfileId', ''),
                    routing_profile_arn=info.get('RoutingProfileArn', ''),
                    security_profile_ids=info.get('SecurityProfileIds', []),
                    hierarchy_group_id=info.get('HierarchyGroupId')
                ))
            
            return users
        except ClientError as e:
            logger.error(f"Failed to list users: {e}")
            raise
    
    def update_user_identity_info(
        self,
        instance_id: str,
        user_id: str,
        identity_info: Dict[str, Any]
    ) -> bool:
        """
        Update user identity info.
        
        Args:
            instance_id: Instance ID
            user_id: User ID
            identity_info: Identity information
            
        Returns:
            True if successful
        """
        try:
            self.connect.update_user_identity_info(
                InstanceId=instance_id,
                UserId=user_id,
                IdentityInfo=identity_info
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update user identity info: {e}")
            raise
    
    def update_user_routing_profile(
        self,
        instance_id: str,
        user_id: str,
        routing_profile_id: str
    ) -> bool:
        """
        Update user routing profile.
        
        Args:
            instance_id: Instance ID
            user_id: User ID
            routing_profile_id: Routing profile ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.update_user_routing_profile(
                InstanceId=instance_id,
                UserId=user_id,
                RoutingProfileId=routing_profile_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update user routing profile: {e}")
            raise
    
    def delete_user(self, instance_id: str, user_id: str) -> bool:
        """
        Delete a user.
        
        Args:
            instance_id: Instance ID
            user_id: User ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_user(
                InstanceId=instance_id,
                UserId=user_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete user: {e}")
            raise
    
    # =========================================================================
    # Routing Profiles
    # =========================================================================
    
    def create_routing_profile(
        self,
        instance_id: str,
        config: RoutingProfileConfig
    ) -> RoutingProfileInfo:
        """
        Create a routing profile.
        
        Args:
            instance_id: Instance ID
            config: Routing profile configuration
            
        Returns:
            RoutingProfileInfo object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Name': config.name,
                'Description': config.description,
                'InstanceId': config.instance_id,
                'DefaultOutboundQueueId': config.default_outbound_queue_id,
                'MediaConcurrencies': config.media_concurrencies
            }
            
            if config.routing_profile_queue_configs:
                params['RoutingProfileQueueConfigs'] = config.routing_profile_queue_configs
            
            response = self.connect.create_routing_profile(**params)
            
            return RoutingProfileInfo(
                routing_profile_id=response['RoutingProfileId'],
                routing_profile_arn=response['RoutingProfileArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                instance_id=response['InstanceId'],
                default_outbound_queue_id=response['DefaultOutboundQueueId'],
                media_concurrencies=response.get('MediaConcurrencies', []),
                queue_configs=response.get('RoutingProfileQueueConfigs', [])
            )
        except ClientError as e:
            logger.error(f"Failed to create routing profile: {e}")
            raise
    
    def describe_routing_profile(
        self,
        instance_id: str,
        routing_profile_id: str
    ) -> RoutingProfileInfo:
        """
        Describe a routing profile.
        
        Args:
            instance_id: Instance ID
            routing_profile_id: Routing profile ID
            
        Returns:
            RoutingProfileInfo object
        """
        try:
            response = self.connect.describe_routing_profile(
                InstanceId=instance_id,
                RoutingProfileId=routing_profile_id
            )
            
            info = response['RoutingProfile']
            return RoutingProfileInfo(
                routing_profile_id=info['RoutingProfileId'],
                routing_profile_arn=info['RoutingProfileArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                instance_id=info['InstanceId'],
                default_outbound_queue_id=info['DefaultOutboundQueueId'],
                media_concurrencies=info.get('MediaConcurrencies', []),
                queue_configs=info.get('RoutingProfileQueueConfigs', [])
            )
        except ClientError as e:
            logger.error(f"Failed to describe routing profile: {e}")
            raise
    
    def list_routing_profiles(self, instance_id: str) -> List[RoutingProfileInfo]:
        """
        List routing profiles.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            List of RoutingProfileInfo objects
        """
        try:
            response = self.connect.list_routing_profiles(InstanceId=instance_id)
            profiles = []
            
            for info in response.get('RoutingProfileSummaryList', []):
                profiles.append(RoutingProfileInfo(
                    routing_profile_id=info['Id'],
                    routing_profile_arn=info['Arn'],
                    name=info['Name'],
                    description=info.get('Description', ''),
                    instance_id=instance_id,
                    default_outbound_queue_id='',
                    media_concurrencies=[]
                ))
            
            return profiles
        except ClientError as e:
            logger.error(f"Failed to list routing profiles: {e}")
            raise
    
    def update_routing_profile(
        self,
        instance_id: str,
        routing_profile_id: str,
        config: Dict[str, Any]
    ) -> bool:
        """
        Update a routing profile.
        
        Args:
            instance_id: Instance ID
            routing_profile_id: Routing profile ID
            config: Update configuration
            
        Returns:
            True if successful
        """
        try:
            params = {
                'InstanceId': instance_id,
                'RoutingProfileId': routing_profile_id
            }
            params.update(config)
            
            self.connect.update_routing_profile(**params)
            return True
        except ClientError as e:
            logger.error(f"Failed to update routing profile: {e}")
            raise
    
    def delete_routing_profile(
        self,
        instance_id: str,
        routing_profile_id: str
    ) -> bool:
        """
        Delete a routing profile.
        
        Args:
            instance_id: Instance ID
            routing_profile_id: Routing profile ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_routing_profile(
                InstanceId=instance_id,
                RoutingProfileId=routing_profile_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete routing profile: {e}")
            raise
    
    # =========================================================================
    # Queues
    # =========================================================================
    
    def create_queue(
        self,
        instance_id: str,
        config: QueueConfig
    ) -> QueueInfo:
        """
        Create a queue.
        
        Args:
            instance_id: Instance ID
            config: Queue configuration
            
        Returns:
            QueueInfo object
        """
        try:
            q_type = config.queue_type.value if isinstance(config.queue_type, QueueType) else config.queue_type
            
            params = {
                'InstanceId': instance_id,
                'Name': config.name,
                'QueueType': q_type
            }
            
            if config.description:
                params['Description'] = config.description
            
            if config.hours_of_operation_id:
                params['HoursOfOperationId'] = config.hours_of_operation_id
            
            if config.max_contacts is not None:
                params['MaxContacts'] = config.max_contacts
            
            if config.quick_connect_ids:
                params['QuickConnectIds'] = config.quick_connect_ids
            
            response = self.connect.create_queue(**params)
            
            return QueueInfo(
                queue_id=response['QueueId'],
                queue_arn=response['QueueArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                queue_type=response['QueueType'],
                status=response.get('Status'),
                hours_of_operation_id=response.get('HoursOfOperationId'),
                max_contacts=response.get('MaxContacts')
            )
        except ClientError as e:
            logger.error(f"Failed to create queue: {e}")
            raise
    
    def describe_queue(
        self,
        instance_id: str,
        queue_id: str
    ) -> QueueInfo:
        """
        Describe a queue.
        
        Args:
            instance_id: Instance ID
            queue_id: Queue ID
            
        Returns:
            QueueInfo object
        """
        try:
            response = self.connect.describe_queue(
                InstanceId=instance_id,
                QueueId=queue_id
            )
            
            info = response['Queue']
            return QueueInfo(
                queue_id=info['QueueId'],
                queue_arn=info['QueueArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                queue_type=info['QueueType'],
                status=info.get('Status'),
                hours_of_operation_id=info.get('HoursOfOperationId'),
                max_contacts=info.get('MaxContacts')
            )
        except ClientError as e:
            logger.error(f"Failed to describe queue: {e}")
            raise
    
    def list_queues(
        self,
        instance_id: str,
        queue_type: Optional[str] = None
    ) -> List[QueueInfo]:
        """
        List queues.
        
        Args:
            instance_id: Instance ID
            queue_type: Optional filter by queue type
            
        Returns:
            List of QueueInfo objects
        """
        try:
            params = {'InstanceId': instance_id}
            if queue_type:
                params['QueueType'] = queue_type
            
            response = self.connect.list_queues(**params)
            queues = []
            
            for info in response.get('QueueSummaryList', []):
                queues.append(QueueInfo(
                    queue_id=info['Id'],
                    queue_arn=info['Arn'],
                    name=info['Name'],
                    description='',
                    queue_type=info.get('QueueType', 'STANDARD'),
                    status=info.get('Status')
                ))
            
            return queues
        except ClientError as e:
            logger.error(f"Failed to list queues: {e}")
            raise
    
    def update_queue_hours_of_operation(
        self,
        instance_id: str,
        queue_id: str,
        hours_of_operation_id: str
    ) -> bool:
        """
        Update queue hours of operation.
        
        Args:
            instance_id: Instance ID
            queue_id: Queue ID
            hours_of_operation_id: Hours of operation ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.update_queue_hours_of_operation(
                InstanceId=instance_id,
                QueueId=queue_id,
                HoursOfOperationId=hours_of_operation_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update queue hours: {e}")
            raise
    
    def delete_queue(self, instance_id: str, queue_id: str) -> bool:
        """
        Delete a queue.
        
        Args:
            instance_id: Instance ID
            queue_id: Queue ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_queue(
                InstanceId=instance_id,
                QueueId=queue_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete queue: {e}")
            raise
    
    # =========================================================================
    # Hours of Operation
    # =========================================================================
    
    def create_hours_of_operation(
        self,
        instance_id: str,
        config: HoursOfOperationConfig
    ) -> HoursOfOperationInfo:
        """
        Create hours of operation.
        
        Args:
            instance_id: Instance ID
            config: Hours of operation configuration
            
        Returns:
            HoursOfOperationInfo object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Name': config.name,
                'TimeZone': config.time_zone,
                'Config': config.hours_of_operation_config
            }
            
            if config.description:
                params['Description'] = config.description
            
            response = self.connect.create_hours_of_operation(**params)
            
            return HoursOfOperationInfo(
                hours_of_operation_id=response['HoursOfOperationId'],
                hours_of_operation_arn=response['HoursOfOperationArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                time_zone=response['TimeZone'],
                config=response.get('Config', [])
            )
        except ClientError as e:
            logger.error(f"Failed to create hours of operation: {e}")
            raise
    
    def describe_hours_of_operation(
        self,
        instance_id: str,
        hours_of_operation_id: str
    ) -> HoursOfOperationInfo:
        """
        Describe hours of operation.
        
        Args:
            instance_id: Instance ID
            hours_of_operation_id: Hours of operation ID
            
        Returns:
            HoursOfOperationInfo object
        """
        try:
            response = self.connect.describe_hours_of_operation(
                InstanceId=instance_id,
                HoursOfOperationId=hours_of_operation_id
            )
            
            info = response['HoursOfOperation']
            return HoursOfOperationInfo(
                hours_of_operation_id=info['HoursOfOperationId'],
                hours_of_operation_arn=info['HoursOfOperationArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                time_zone=info['TimeZone'],
                config=info.get('Config', [])
            )
        except ClientError as e:
            logger.error(f"Failed to describe hours of operation: {e}")
            raise
    
    def list_hours_of_operations(self, instance_id: str) -> List[HoursOfOperationInfo]:
        """
        List hours of operations.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            List of HoursOfOperationInfo objects
        """
        try:
            response = self.connect.list_hours_of_operations(InstanceId=instance_id)
            hours_list = []
            
            for info in response.get('HoursOfOperationSummaryList', []):
                hours_list.append(HoursOfOperationInfo(
                    hours_of_operation_id=info['Id'],
                    hours_of_operation_arn=info['Arn'],
                    name=info['Name'],
                    description='',
                    time_zone=info.get('TimeZone', 'UTC')
                ))
            
            return hours_list
        except ClientError as e:
            logger.error(f"Failed to list hours of operation: {e}")
            raise
    
    def update_hours_of_operation(
        self,
        instance_id: str,
        hours_of_operation_id: str,
        config: HoursOfOperationConfig
    ) -> bool:
        """
        Update hours of operation.
        
        Args:
            instance_id: Instance ID
            hours_of_operation_id: Hours of operation ID
            config: Updated configuration
            
        Returns:
            True if successful
        """
        try:
            params = {
                'InstanceId': instance_id,
                'HoursOfOperationId': hours_of_operation_id,
                'Name': config.name,
                'TimeZone': config.time_zone,
                'Config': config.hours_of_operation_config
            }
            
            if config.description:
                params['Description'] = config.description
            
            self.connect.update_hours_of_operation(**params)
            return True
        except ClientError as e:
            logger.error(f"Failed to update hours of operation: {e}")
            raise
    
    def delete_hours_of_operation(
        self,
        instance_id: str,
        hours_of_operation_id: str
    ) -> bool:
        """
        Delete hours of operation.
        
        Args:
            instance_id: Instance ID
            hours_of_operation_id: Hours of operation ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_hours_of_operation(
                InstanceId=instance_id,
                HoursOfOperationId=hours_of_operation_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete hours of operation: {e}")
            raise
    
    # =========================================================================
    # Prompts
    # =========================================================================
    
    def create_prompt(
        self,
        instance_id: str,
        config: PromptConfig
    ) -> PromptInfo:
        """
        Create a prompt.
        
        Args:
            instance_id: Instance ID
            config: Prompt configuration
            
        Returns:
            PromptInfo object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Name': config.name
            }
            
            if config.description:
                params['Description'] = config.description
            
            if config.s3_uri:
                params['S3Uri'] = config.s3_uri
            
            response = self.connect.create_prompt(**params)
            
            return PromptInfo(
                prompt_id=response['PromptId'],
                prompt_arn=response['PromptArn'],
                name=response['Name'],
                description=response.get('Description', ''),
                status=response.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to create prompt: {e}")
            raise
    
    def describe_prompt(
        self,
        instance_id: str,
        prompt_id: str
    ) -> PromptInfo:
        """
        Describe a prompt.
        
        Args:
            instance_id: Instance ID
            prompt_id: Prompt ID
            
        Returns:
            PromptInfo object
        """
        try:
            response = self.connect.describe_prompt(
                InstanceId=instance_id,
                PromptId=prompt_id
            )
            
            info = response['Prompt']
            return PromptInfo(
                prompt_id=info['PromptId'],
                prompt_arn=info['PromptArn'],
                name=info['Name'],
                description=info.get('Description', ''),
                status=info.get('Status')
            )
        except ClientError as e:
            logger.error(f"Failed to describe prompt: {e}")
            raise
    
    def list_prompts(self, instance_id: str) -> List[PromptInfo]:
        """
        List prompts.
        
        Args:
            instance_id: Instance ID
            
        Returns:
            List of PromptInfo objects
        """
        try:
            response = self.connect.list_prompts(InstanceId=instance_id)
            prompts = []
            
            for info in response.get('PromptSummaryList', []):
                prompts.append(PromptInfo(
                    prompt_id=info['Id'],
                    prompt_arn=info['Arn'],
                    name=info['Name'],
                    description=info.get('Description', ''),
                    status=info.get('Status')
                ))
            
            return prompts
        except ClientError as e:
            logger.error(f"Failed to list prompts: {e}")
            raise
    
    def delete_prompt(self, instance_id: str, prompt_id: str) -> bool:
        """
        Delete a prompt.
        
        Args:
            instance_id: Instance ID
            prompt_id: Prompt ID
            
        Returns:
            True if successful
        """
        try:
            self.connect.delete_prompt(
                InstanceId=instance_id,
                PromptId=prompt_id
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete prompt: {e}")
            raise
    
    # =========================================================================
    # Contact Search
    # =========================================================================
    
    def search_contacts(
        self,
        instance_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        max_results: int = 100
    ) -> List[ContactSearchResult]:
        """
        Search contacts.
        
        Args:
            instance_id: Instance ID
            start_time: Start time (ISO 8601 format)
            end_time: End time (ISO 8601 format)
            filters: Search filters
            max_results: Maximum number of results
            
        Returns:
            List of ContactSearchResult objects
        """
        try:
            params = {
                'InstanceId': instance_id,
                'MaxResults': max_results
            }
            
            if start_time or end_time:
                time_filter = {}
                if start_time:
                    time_filter['StartTime'] = start_time
                if end_time:
                    time_filter['EndTime'] = end_time
                params['Filters'] = {'ContactState': [{'Equals': 'CONNECTED'}]}
                if start_time:
                    params['Filters']['Channel'] = [{'Equals': 'VOICE'}]
            
            if filters:
                if 'Filters' in params:
                    params['Filters'].update(filters)
                else:
                    params['Filters'] = filters
            
            response = self.connect.search_contacts(**params)
            contacts = []
            
            for info in response.get('Contacts', []):
                contacts.append(ContactSearchResult(
                    contact_id=info['ContactId'],
                    contact_arn=info['ContactArn'],
                    initial_contact_id=info.get('InitialContactId'),
                    previous_contact_id=info.get('PreviousContactId'),
                    initiation_method=info.get('InitiationMethod'),
                    state=info.get('State'),
                    state_start_timestamp=info.get('StateStartTimestamp'),
                    queue_info=info.get('QueueInfo'),
                    connected_to_agent_timestamp=info.get('ConnectedToAgentTimestamp'),
                    customer_endpoint=info.get('CustomerEndpoint'),
                    agent_endpoint=info.get('AgentEndpoint'),
                    total_contact_duration=info.get('TotalContactDuration'),
                    queue_duration=info.get('QueueDuration'),
                    wait_time=info.get('WaitDuration')
                ))
            
            return contacts
        except ClientError as e:
            logger.error(f"Failed to search contacts: {e}")
            raise
    
    def describe_contact(
        self,
        instance_id: str,
        contact_id: str
    ) -> ContactSearchResult:
        """
        Describe a contact.
        
        Args:
            instance_id: Instance ID
            contact_id: Contact ID
            
        Returns:
            ContactSearchResult object
        """
        try:
            response = self.connect.describe_contact(
                InstanceId=instance_id,
                ContactId=contact_id
            )
            
            info = response['Contact']
            return ContactSearchResult(
                contact_id=info['ContactId'],
                contact_arn=info['ContactArn'],
                initial_contact_id=info.get('InitialContactId'),
                previous_contact_id=info.get('PreviousContactId'),
                initiation_method=info.get('InitiationMethod'),
                state=info.get('State'),
                state_start_timestamp=info.get('StateStartTimestamp'),
                queue_info=info.get('QueueInfo'),
                connected_to_agent_timestamp=info.get('ConnectedToAgentTimestamp'),
                customer_endpoint=info.get('CustomerEndpoint'),
                agent_endpoint=info.get('AgentEndpoint'),
                total_contact_duration=info.get('TotalContactDuration'),
                queue_duration=info.get('QueueDuration'),
                wait_time=info.get('WaitDuration')
            )
        except ClientError as e:
            logger.error(f"Failed to describe contact: {e}")
            raise
    
    def get_contact_attributes(
        self,
        instance_id: str,
        contact_id: str
    ) -> Dict[str, Any]:
        """
        Get contact attributes.
        
        Args:
            instance_id: Instance ID
            contact_id: Contact ID
            
        Returns:
            Dictionary of contact attributes
        """
        try:
            response = self.connect.get_contact_attributes(
                InstanceId=instance_id,
                ContactId=contact_id
            )
            return response.get('Attributes', {})
        except ClientError as e:
            logger.error(f"Failed to get contact attributes: {e}")
            raise
    
    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    def get_current_metric_data(
        self,
        instance_id: str,
        queues: Optional[List[str]] = None,
        routing_profiles: Optional[List[str]] = None
    ) -> ContactMetrics:
        """
        Get current metric data.
        
        Args:
            instance_id: Instance ID
            queues: Optional list of queue IDs
            routing_profiles: Optional list of routing profile IDs
            
        Returns:
            ContactMetrics object
        """
        try:
            params = {
                'InstanceId': instance_id,
                'Filters': {}
            }
            
            if queues:
                params['Filters']['Queues'] = queues
            if routing_profiles:
                params['Filters']['RoutingProfiles'] = routing_profiles
            
            response = self.connect.get_current_metric_data(**params)
            
            return ContactMetrics(
                metrics=response.get('MetricResults', [{}])[0] if response.get('MetricResults') else {},
                timestamp=response.get('DataSnapshotTime'),
                period=response.get('MetricDetails', {}).get('SamplingPeriodSeconds')
            )
        except ClientError as e:
            logger.error(f"Failed to get current metric data: {e}")
            raise
    
    def get_metric_data(
        self,
        instance_id: str,
        start_time: str,
        end_time: str,
        metrics: List[Dict[str, Any]],
        interval: str = "PT5M",
        queues: Optional[List[str]] = None,
        routing_profiles: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get historical metric data.
        
        Args:
            instance_id: Instance ID
            start_time: Start time (ISO 8601)
            end_time: End time (ISO 8601)
            metrics: List of metric configurations
            interval: Aggregation interval (PT5M, PT15M, PT30M, PT1H)
            queues: Optional list of queue IDs
            routing_profiles: Optional list of routing profile IDs
            
        Returns:
            List of metric results
        """
        try:
            params = {
                'InstanceId': instance_id,
                'StartTime': start_time,
                'EndTime': end_time,
                'Interval': interval,
                'Filters': {}
            }
            
            if queues:
                params['Filters']['Queues'] = queues
            if routing_profiles:
                params['Filters']['RoutingProfiles'] = routing_profiles
            
            params['MetricFilters'] = metrics
            
            response = self.connect.get_metric_data(**params)
            return response.get('MetricResults', [])
        except ClientError as e:
            logger.error(f"Failed to get metric data: {e}")
            raise
    
    def get_queue_metrics(
        self,
        instance_id: str,
        queue_id: str,
        start_time: str,
        end_time: str,
        interval: str = "PT5M"
    ) -> List[Dict[str, Any]]:
        """
        Get queue-specific metrics.
        
        Args:
            instance_id: Instance ID
            queue_id: Queue ID
            start_time: Start time (ISO 8601)
            end_time: End time (ISO 8601)
            interval: Aggregation interval
            
        Returns:
            List of queue metric results
        """
        try:
            metrics = [
                {'Name': 'QueueSize', 'Unit': 'Count'},
                {'Name': 'LongestWaitTime', 'Unit': 'Seconds'},
                {'Name': 'ContactsInQueue', 'Unit': 'Count'},
                {'Name': 'QueueAvailableCapacity', 'Unit': 'Count'}
            ]
            
            return self.get_metric_data(
                instance_id=instance_id,
                start_time=start_time,
                end_time=end_time,
                metrics=metrics,
                interval=interval,
                queues=[queue_id]
            )
        except ClientError as e:
            logger.error(f"Failed to get queue metrics: {e}")
            raise
    
    def get_agent_metrics(
        self,
        instance_id: str,
        start_time: str,
        end_time: str,
        agent_ids: Optional[List[str]] = None,
        interval: str = "PT5M"
    ) -> List[Dict[str, Any]]:
        """
        Get agent-specific metrics.
        
        Args:
            instance_id: Instance ID
            start_time: Start time (ISO 8601)
            end_time: End time (ISO 8601)
            agent_ids: Optional list of agent IDs
            interval: Aggregation interval
            
        Returns:
            List of agent metric results
        """
        try:
            params = {
                'InstanceId': instance_id,
                'StartTime': start_time,
                'EndTime': end_time,
                'Interval': interval,
                'Filters': {}
            }
            
            if agent_ids:
                params['Filters']['Agents'] = agent_ids
            
            response = self.connect.get_current_metric_data(**params)
            return response.get('MetricResults', [])
        except ClientError as e:
            logger.error(f"Failed to get agent metrics: {e}")
            raise
    
    def put_metric_data(
        self,
        namespace: str,
        metric_data: List[Dict[str, Any]]
    ) -> bool:
        """
        Put custom metric data to CloudWatch.
        
        Args:
            namespace: CloudWatch namespace
            metric_data: List of metric data points
            
        Returns:
            True if successful
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise
    
    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300,
        statistics: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get metric statistics from CloudWatch.
        
        Args:
            namespace: CloudWatch namespace
            metric_name: Name of the metric
            start_time: Start time
            end_time: End time
            period: Period in seconds
            statistics: List of statistics (Sum, Average, Maximum, Minimum)
            
        Returns:
            List of metric data points
        """
        try:
            if statistics is None:
                statistics = ['Average', 'Sum', 'Maximum', 'Minimum']
            
            response = self.cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics
            )
            
            return response.get('Datapoints', [])
        except ClientError as e:
            logger.error(f"Failed to get metric statistics: {e}")
            raise
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_instance_id_from_arn(self, instance_arn: str) -> str:
        """
        Extract instance ID from ARN.
        
        Args:
            instance_arn: Instance ARN
            
        Returns:
            Instance ID
        """
        return instance_arn.split('/')[-1]
    
    def generate_contact_flow_template(
        self,
        name: str,
        description: str = ""
    ) -> Dict[str, Any]:
        """
        Generate a basic contact flow template.
        
        Args:
            name: Contact flow name
            description: Contact flow description
            
        Returns:
            Contact flow content template
        """
        return {
            "Version": "2019-10-30",
            "StartAction": "12345678-1234-1234-1234-123456789001",
            "Metadata": {
                "entryModePosition": {"x": 0, "y": 0},
                "name": name,
                "description": description
            },
            "Actions": {
                "12345678-1234-1234-1234-123456789001": {
                    "Type": "Message",
                    "Parameters": {
                        "Text": "Welcome to our contact center."
                    },
                    "Transitions": {
                        "NextAction": "12345678-1234-1234-1234-123456789002",
                        "Errors": [],
                        "Conditions": []
                    }
                },
                "12345678-1234-1234-1234-123456789002": {
                    "Type": "Disconnect",
                    "Parameters": {},
                    "Transitions": {
                        "NextAction": None,
                        "Errors": [],
                        "Conditions": []
                    }
                }
            },
            "ActionsMetadata": {},
            "Variables": []
        }
    
    def validate_phone_number(self, phone_number: str) -> bool:
        """
        Validate phone number format.
        
        Args:
            phone_number: Phone number to validate
            
        Returns:
            True if valid
        """
        import re
        pattern = r'^\+?[1-9]\d{1,14}$'
        return bool(re.match(pattern, phone_number.replace(' ', '').replace('-', '')))
    
    def calculate_wait_time_bucket(self, wait_time_seconds: int) -> str:
        """
        Categorize wait time into buckets.
        
        Args:
            wait_time_seconds: Wait time in seconds
            
        Returns:
            Wait time bucket category
        """
        if wait_time_seconds <= 30:
            return "less_than_30s"
        elif wait_time_seconds <= 60:
            return "30s_to_1m"
        elif wait_time_seconds <= 180:
            return "1m_to_3m"
        elif wait_time_seconds <= 300:
            return "3m_to_5m"
        elif wait_time_seconds <= 600:
            return "5m_to_10m"
        else:
            return "greater_than_10m"
