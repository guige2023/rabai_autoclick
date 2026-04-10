"""
AWS IAM Identity Management Integration Module for Workflow System

Implements an IAMIntegration class with:
1. User management: Create/manage IAM users
2. Group management: Create/manage groups
3. Role management: Create/manage IAM roles
4. Policy management: Create/manage policies
5. Access keys: Manage access keys
6. MFA: Multi-factor authentication
7. Federation: SAML/OIDC federation
8. SSO integration: AWS SSO integration
9. Password policy: Configure password policies
10. CloudWatch integration: CloudTrail integration

Commit: 'feat(aws-iam): add AWS IAM with user/group/role management, policies, access keys, MFA, federation, SSO, password policy, CloudTrail'
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


class IAMUserState(Enum):
    """IAM user states."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    CREATING = "creating"
    DELETING = "deleting"


class IAMGroupState(Enum):
    """IAM group states."""
    ACTIVE = "active"
    DELETING = "deleting"


class IAMRoleState(Enum):
    """IAM role states."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    CREATING = "creating"
    DELETING = "deleting"


class AccessKeyStatus(Enum):
    """Access key status."""
    ACTIVE = "Active"
    INACTIVE = "Inactive"


class MFADeviceType(Enum):
    """MFA device types."""
    TOTP = "TOTP"
    FIDO = "FIDO"
    U2F = "U2F"
    SMS = "SMS"


class MFADeviceState(Enum):
    """MFA device state."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    PENDING = "pending"


class FederationType(Enum):
    """Federation types."""
    SAML = "SAML"
    OIDC = "OIDC"


class PasswordPolicyMode(Enum):
    """Password policy modes."""
    CUSTOM = "custom"
    CIS = "cis"
    FTC = "ftc"
    NIST = "nist"


@dataclass
class IAMConfig:
    """Configuration for IAM connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class IAMUserConfig:
    """Configuration for creating an IAM user."""
    user_name: str
    path: Optional[str] = None
    permissions_boundary: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    create_access_key: bool = True


@dataclass
class IAMGroupConfig:
    """Configuration for creating an IAM group."""
    group_name: str
    path: Optional[str] = None
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class IAMRoleConfig:
    """Configuration for creating an IAM role."""
    role_name: str
    assume_role_policy_document: Dict[str, Any]
    description: Optional[str] = None
    max_session_duration: int = 3600
    permissions_boundary: Optional[str] = None
    path: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class IAMPolicyConfig:
    """Configuration for creating an IAM policy."""
    policy_name: str
    policy_document: Dict[str, Any]
    description: Optional[str] = None
    path: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AccessKeyConfig:
    """Configuration for access key management."""
    user_name: str
    key_id: Optional[str] = None
    status: AccessKeyStatus = AccessKeyStatus.ACTIVE


@dataclass
class MFAConfig:
    """Configuration for MFA device."""
    user_name: str
    device_type: MFADeviceType = MFADeviceType.TOTP
    serial_number: Optional[str] = None
    authenticator_code: Optional[str] = None


@dataclass
class FederationConfig:
    """Configuration for identity federation."""
    name: str
    federation_type: FederationType
    metadata_document: Optional[str] = None
    url: Optional[str] = None
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class SSOConfig:
    """Configuration for AWS SSO."""
    instance_arn: Optional[str] = None
    identity_store_id: Optional[str] = None
    permission_set_arn: Optional[str] = None


@dataclass
class PasswordPolicyConfig:
    """Configuration for password policy."""
    minimum_password_length: int = 14
    require_symbols: bool = True
    require_numbers: bool = True
    require_uppercase: bool = True
    require_lowercase: bool = True
    allow_users_to_change_password: bool = True
    max_password_age: int = 90
    password_reuse_prevention: int = 24
    hard_expiry: bool = False
    mode: PasswordPolicyMode = PasswordPolicyMode.CUSTOM


@dataclass
class CloudTrailConfig:
    """Configuration for CloudTrail."""
    name: str
    s3_bucket_name: str
    s3_key_prefix: Optional[str] = None
    include_global_service_events: bool = True
    is_multi_region_trail: bool = False
    enable_log_file_validation: bool = True
    cloud_watch_logs_log_group: Optional[str] = None
    cloud_watch_logs_role_arn: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class UserInfo:
    """Information about an IAM user."""
    user_name: str
    user_id: str
    arn: str
    path: str
    create_date: datetime
    password_last_used: Optional[datetime] = None
    permissions_boundary: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    status: IAMUserState = IAMUserState.ACTIVE
    access_keys: List[str] = field(default_factory=list)
    groups: List[str] = field(default_factory=list)
    mfa_devices: List[str] = field(default_factory=list)


@dataclass
class GroupInfo:
    """Information about an IAM group."""
    group_name: str
    group_id: str
    arn: str
    path: str
    create_date: datetime
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    users: List[str] = field(default_factory=list)
    status: IAMGroupState = IAMGroupState.ACTIVE


@dataclass
class RoleInfo:
    """Information about an IAM role."""
    role_name: str
    role_id: str
    arn: str
    path: str
    create_date: datetime
    description: Optional[str] = None
    max_session_duration: int = 3600
    assume_role_policy_document: Dict[str, Any] = field(default_factory=dict)
    permissions_boundary: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    status: IAMRoleState = IAMRoleState.ACTIVE
    instance_profiles: List[str] = field(default_factory=list)


@dataclass
class PolicyInfo:
    """Information about an IAM policy."""
    policy_name: str
    policy_id: str
    arn: str
    create_date: datetime
    update_date: datetime
    path: Optional[str] = None
    default_version_id: str = "v1"
    attachment_count: int = 0
    permissions_boundary_usage_count: int = 0
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AccessKeyInfo:
    """Information about an access key."""
    access_key_id: str
    user_name: str
    status: AccessKeyStatus
    create_date: datetime
    last_used_date: Optional[datetime] = None
    last_used_service: Optional[str] = None


@dataclass
class MFADeviceInfo:
    """Information about an MFA device."""
    serial_number: str
    user_name: str
    device_type: MFADeviceType
    enable_date: datetime
    status: MFADeviceState = MFADeviceState.ENABLED


@dataclass
class FederationInfo:
    """Information about identity federation."""
    name: str
    federation_type: FederationType
    arn: str
    create_date: datetime
    url: Optional[str] = None


@dataclass
class SSOInfo:
    """Information about AWS SSO configuration."""
    instance_arn: str
    identity_store_id: str
    sso_region: str


@dataclass
class PasswordPolicyInfo:
    """Information about password policy."""
    minimum_password_length: int
    require_symbols: bool
    require_numbers: bool
    require_uppercase: bool
    require_lowercase: bool
    allow_users_to_change_password: bool
    max_password_age: int
    password_reuse_prevention: int
    hard_expiry: bool
    mode: PasswordPolicyMode = PasswordPolicyMode.CUSTOM


@dataclass
class CloudTrailInfo:
    """Information about CloudTrail configuration."""
    name: str
    trail_arn: str
    is_multi_region_trail: bool
    include_global_service_events: bool
    s3_bucket_name: str
    s3_key_prefix: Optional[str] = None
    cloud_watch_logs_log_group: Optional[str] = None
    is_logging: bool = False
    start_logging_time: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


class IAMIntegration:
    """
    AWS IAM Identity Management Integration.
    
    Provides comprehensive IAM management including:
    - User lifecycle management (create, modify, delete, enable/disable)
    - Group management for organizing users
    - Role management for AWS service and external entity access
    - Policy management (managed and inline policies)
    - Access key lifecycle management
    - MFA device management (TOTP, FIDO, U2F, SMS)
    - SAML/OIDC identity federation
    - AWS SSO integration
    - Password policy configuration
    - CloudTrail integration for audit logging
    """
    
    def __init__(self, config: Optional[IAMConfig] = None):
        """
        Initialize the IAM integration.
        
        Args:
            config: IAM configuration options
        """
        self.config = config or IAMConfig()
        self._client = None
        self._resource_groups_client = None
        self._sso_client = None
        self._cloudtrail_client = None
        self._sts_client = None
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS clients with proper configuration."""
        with self._lock:
            try:
                session_kwargs = {}
                if self.config.profile_name:
                    session_kwargs['profile_name'] = self.config.profile_name
                
                session = boto3.Session(**session_kwargs)
                
                client_kwargs = {
                    'region_name': self.config.region_name
                }
                
                if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                    client_kwargs['aws_access_key_id'] = self.config.aws_access_key_id
                    client_kwargs['aws_secret_access_key'] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        client_kwargs['aws_session_token'] = self.config.aws_session_token
                
                self._client = session.client('iam', **client_kwargs)
                self._sts_client = session.client('sts', **client_kwargs)
                
            except Exception as e:
                logger.warning(f"Failed to initialize IAM clients: {e}")
    
    @property
    def client(self):
        """Get the IAM client."""
        return self._client
    
    @property
    def is_available(self) -> bool:
        """Check if boto3 is available and clients are initialized."""
        return BOTO3_AVAILABLE and self._client is not None
    
    # =========================================================================
    # USER MANAGEMENT
    # =========================================================================
    
    def create_user(self, config: IAMUserConfig) -> UserInfo:
        """
        Create a new IAM user.
        
        Args:
            config: User configuration
            
        Returns:
            UserInfo object with user details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'UserName': config.user_name
            }
            if config.path:
                kwargs['Path'] = config.path
            if config.permissions_boundary:
                kwargs['PermissionsBoundary'] = config.permissions_boundary
            if config.tags:
                kwargs['Tags'] = [{'Key': k, 'Value': v} for k, v in config.tags.items()]
            
            response = self._client.create_user(**kwargs)
            user = response['User']
            
            user_info = UserInfo(
                user_name=user['UserName'],
                user_id=user['UserId'],
                arn=user['Arn'],
                path=user.get('Path', '/'),
                create_date=user['CreateDate'],
                password_last_used=user.get('PasswordLastUsed'),
                permissions_boundary=user.get('PermissionsBoundary'),
                tags={t['Key']: t['Value'] for t in user.get('Tags', [])},
                status=IAMUserState.ACTIVE
            )
            
            if config.create_access_key:
                access_key = self.create_access_key(config.user_name)
                user_info.access_keys.append(access_key.access_key_id)
            
            logger.info(f"Created IAM user: {config.user_name}")
            return user_info
            
        except ClientError as e:
            logger.error(f"Failed to create user {config.user_name}: {e}")
            raise
    
    def get_user(self, user_name: str) -> UserInfo:
        """
        Get information about an IAM user.
        
        Args:
            user_name: Name of the user
            
        Returns:
            UserInfo object with user details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_user(UserName=user_name)
            user = response['User']
            
            groups = self.get_user_groups(user_name)
            access_keys = self.list_access_keys(user_name)
            mfa_devices = self.list_mfa_devices(user_name)
            
            return UserInfo(
                user_name=user['UserName'],
                user_id=user['UserId'],
                arn=user['Arn'],
                path=user.get('Path', '/'),
                create_date=user['CreateDate'],
                password_last_used=user.get('PasswordLastUsed'),
                permissions_boundary=user.get('PermissionsBoundary'),
                tags={t['Key']: t['Value'] for t in user.get('Tags', [])},
                status=IAMUserState.ACTIVE,
                access_keys=[ak.access_key_id for ak in access_keys],
                groups=[g.group_name for g in groups],
                mfa_devices=[m.serial_number for m in mfa_devices]
            )
            
        except ClientError as e:
            logger.error(f"Failed to get user {user_name}: {e}")
            raise
    
    def update_user(self, user_name: str, new_user_name: Optional[str] = None, 
                    path: Optional[str] = None) -> bool:
        """
        Update an IAM user.
        
        Args:
            user_name: Current user name
            new_user_name: New user name (optional)
            path: New path (optional)
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            if new_user_name:
                self._client.update_user(UserName=user_name, NewUserName=new_user_name)
                user_name = new_user_name
            
            if path:
                self._client.update_user(UserName=user_name, NewPath=path)
            
            logger.info(f"Updated IAM user: {user_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update user {user_name}: {e}")
            raise
    
    def delete_user(self, user_name: str, remove_user_certs: bool = True) -> bool:
        """
        Delete an IAM user and associated resources.
        
        Args:
            user_name: Name of the user to delete
            remove_user_certs: Remove associated certificates
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            access_keys = self.list_access_keys(user_name)
            for ak in access_keys:
                self.delete_access_key(user_name, ak.access_key_id)
            
            mfa_devices = self.list_mfa_devices(user_name)
            for mfa in mfa_devices:
                self.deactivate_mfa_device(user_name, mfa.serial_number)
            
            groups = self.get_user_groups(user_name)
            for group in groups:
                self.remove_user_from_group(group.group_name, user_name)
            
            self._client.delete_user(UserName=user_name)
            
            logger.info(f"Deleted IAM user: {user_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete user {user_name}: {e}")
            raise
    
    def list_users(self, path_prefix: Optional[str] = None, 
                   max_items: int = 100) -> List[UserInfo]:
        """
        List IAM users.
        
        Args:
            path_prefix: Filter by path prefix
            max_items: Maximum number of users to return
            
        Returns:
            List of UserInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {}
            if path_prefix:
                kwargs['PathPrefix'] = path_prefix
            if max_items:
                kwargs['MaxItems'] = max_items
            
            users = []
            paginator = self._client.get_paginator('list_users')
            
            for page in paginator.paginate(**kwargs):
                for user in page['Users']:
                    users.append(UserInfo(
                        user_name=user['UserName'],
                        user_id=user['UserId'],
                        arn=user['Arn'],
                        path=user.get('Path', '/'),
                        create_date=user['CreateDate'],
                        password_last_used=user.get('PasswordLastUsed'),
                        permissions_boundary=user.get('PermissionsBoundary'),
                        tags={t['Key']: t['Value'] for t in user.get('Tags', [])},
                        status=IAMUserState.ACTIVE
                    ))
            
            return users
            
        except ClientError as e:
            logger.error(f"Failed to list users: {e}")
            raise
    
    def enable_user(self, user_name: str) -> bool:
        """Enable an IAM user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.enable_user(UserName=user_name)
            logger.info(f"Enabled IAM user: {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to enable user {user_name}: {e}")
            raise
    
    def disable_user(self, user_name: str) -> bool:
        """Disable an IAM user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.disable_user(UserName=user_name)
            logger.info(f"Disabled IAM user: {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to disable user {user_name}: {e}")
            raise
    
    # =========================================================================
    # GROUP MANAGEMENT
    # =========================================================================
    
    def create_group(self, config: IAMGroupConfig) -> GroupInfo:
        """
        Create a new IAM group.
        
        Args:
            config: Group configuration
            
        Returns:
            GroupInfo object with group details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'GroupName': config.group_name
            }
            if config.path:
                kwargs['Path'] = config.path
            if config.description:
                kwargs['Description'] = config.description
            if config.tags:
                kwargs['Tags'] = [{'Key': k, 'Value': v} for k, v in config.tags.items()]
            
            response = self._client.create_group(**kwargs)
            group = response['Group']
            
            return GroupInfo(
                group_name=group['GroupName'],
                group_id=group['GroupId'],
                arn=group['Arn'],
                path=group.get('Path', '/'),
                create_date=group['CreateDate'],
                description=config.description,
                tags={t['Key']: t['Value'] for t in group.get('Tags', [])},
                status=IAMGroupState.ACTIVE
            )
            
        except ClientError as e:
            logger.error(f"Failed to create group {config.group_name}: {e}")
            raise
    
    def get_group(self, group_name: str) -> GroupInfo:
        """
        Get information about an IAM group.
        
        Args:
            group_name: Name of the group
            
        Returns:
            GroupInfo object with group details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_group(GroupName=group_name)
            group = response['Group']
            
            users = self.list_group_users(group_name)
            
            return GroupInfo(
                group_name=group['GroupName'],
                group_id=group['GroupId'],
                arn=group['Arn'],
                path=group.get('Path', '/'),
                create_date=group['CreateDate'],
                description=group.get('Description'),
                tags={t['Key']: t['Value'] for t in group.get('Tags', [])},
                users=[u.user_name for u in users],
                status=IAMGroupState.ACTIVE
            )
            
        except ClientError as e:
            logger.error(f"Failed to get group {group_name}: {e}")
            raise
    
    def delete_group(self, group_name: str) -> bool:
        """
        Delete an IAM group.
        
        Args:
            group_name: Name of the group to delete
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_group(GroupName=group_name)
            logger.info(f"Deleted IAM group: {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete group {group_name}: {e}")
            raise
    
    def list_groups(self, path_prefix: Optional[str] = None, 
                    max_items: int = 100) -> List[GroupInfo]:
        """
        List IAM groups.
        
        Args:
            path_prefix: Filter by path prefix
            max_items: Maximum number of groups to return
            
        Returns:
            List of GroupInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {}
            if path_prefix:
                kwargs['PathPrefix'] = path_prefix
            if max_items:
                kwargs['MaxItems'] = max_items
            
            groups = []
            paginator = self._client.get_paginator('list_groups')
            
            for page in paginator.paginate(**kwargs):
                for group in page['Groups']:
                    groups.append(GroupInfo(
                        group_name=group['GroupName'],
                        group_id=group['GroupId'],
                        arn=group['Arn'],
                        path=group.get('Path', '/'),
                        create_date=group['CreateDate'],
                        description=group.get('Description'),
                        tags={t['Key']: t['Value'] for t in group.get('Tags', [])},
                        status=IAMGroupState.ACTIVE
                    ))
            
            return groups
            
        except ClientError as e:
            logger.error(f"Failed to list groups: {e}")
            raise
    
    def add_user_to_group(self, group_name: str, user_name: str) -> bool:
        """
        Add a user to a group.
        
        Args:
            group_name: Name of the group
            user_name: Name of the user
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.add_user_to_group(GroupName=group_name, UserName=user_name)
            logger.info(f"Added user {user_name} to group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to add user {user_name} to group {group_name}: {e}")
            raise
    
    def remove_user_from_group(self, group_name: str, user_name: str) -> bool:
        """
        Remove a user from a group.
        
        Args:
            group_name: Name of the group
            user_name: Name of the user
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.remove_user_from_group(GroupName=group_name, UserName=user_name)
            logger.info(f"Removed user {user_name} from group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to remove user {user_name} from group {group_name}: {e}")
            raise
    
    def get_user_groups(self, user_name: str) -> List[GroupInfo]:
        """
        Get groups for a user.
        
        Args:
            user_name: Name of the user
            
        Returns:
            List of GroupInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            groups = []
            paginator = self._client.get_paginator('list_groups_for_user')
            
            for page in paginator.paginate(UserName=user_name):
                for group in page['Groups']:
                    groups.append(GroupInfo(
                        group_name=group['GroupName'],
                        group_id=group['GroupId'],
                        arn=group['Arn'],
                        path=group.get('Path', '/'),
                        create_date=group['CreateDate'],
                        description=group.get('Description'),
                        tags={t['Key']: t['Value'] for t in group.get('Tags', [])},
                        status=IAMGroupState.ACTIVE
                    ))
            
            return groups
            
        except ClientError as e:
            logger.error(f"Failed to get groups for user {user_name}: {e}")
            raise
    
    def list_group_users(self, group_name: str) -> List[UserInfo]:
        """
        Get users in a group.
        
        Args:
            group_name: Name of the group
            
        Returns:
            List of UserInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            users = []
            paginator = self._client.get_paginator('list_users')
            
            for page in paginator.paginate():
                for user in page['Users']:
                    user_groups = self.get_user_groups(user['UserName'])
                    if any(g.group_name == group_name for g in user_groups):
                        users.append(UserInfo(
                            user_name=user['UserName'],
                            user_id=user['UserId'],
                            arn=user['Arn'],
                            path=user.get('Path', '/'),
                            create_date=user['CreateDate'],
                            password_last_used=user.get('PasswordLastUsed'),
                            permissions_boundary=user.get('PermissionsBoundary'),
                            tags={t['Key']: t['Value'] for t in user.get('Tags', [])},
                            status=IAMUserState.ACTIVE
                        ))
            
            return users
            
        except ClientError as e:
            logger.error(f"Failed to list users in group {group_name}: {e}")
            raise
    
    # =========================================================================
    # ROLE MANAGEMENT
    # =========================================================================
    
    def create_role(self, config: IAMRoleConfig) -> RoleInfo:
        """
        Create a new IAM role.
        
        Args:
            config: Role configuration
            
        Returns:
            RoleInfo object with role details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'RoleName': config.role_name,
                'AssumeRolePolicyDocument': config.assume_role_policy_document
            }
            if config.description:
                kwargs['Description'] = config.description
            if config.max_session_duration:
                kwargs['MaxSessionDuration'] = config.max_session_duration
            if config.permissions_boundary:
                kwargs['PermissionsBoundary'] = config.permissions_boundary
            if config.path:
                kwargs['Path'] = config.path
            if config.tags:
                kwargs['Tags'] = [{'Key': k, 'Value': v} for k, v in config.tags.items()]
            
            response = self._client.create_role(**kwargs)
            role = response['Role']
            
            return RoleInfo(
                role_name=role['RoleName'],
                role_id=role['RoleId'],
                arn=role['Arn'],
                path=role.get('Path', '/'),
                create_date=role['CreateDate'],
                description=config.description,
                max_session_duration=config.max_session_duration,
                assume_role_policy_document=config.assume_role_policy_document,
                permissions_boundary=config.permissions_boundary,
                tags={t['Key']: t['Value'] for t in role.get('Tags', [])},
                status=IAMRoleState.ACTIVE
            )
            
        except ClientError as e:
            logger.error(f"Failed to create role {config.role_name}: {e}")
            raise
    
    def get_role(self, role_name: str) -> RoleInfo:
        """
        Get information about an IAM role.
        
        Args:
            role_name: Name of the role
            
        Returns:
            RoleInfo object with role details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_role(RoleName=role_name)
            role = response['Role']
            
            instance_profiles = self.list_instance_profiles_for_role(role_name)
            
            return RoleInfo(
                role_name=role['RoleName'],
                role_id=role['RoleId'],
                arn=role['Arn'],
                path=role.get('Path', '/'),
                create_date=role['CreateDate'],
                description=role.get('Description'),
                max_session_duration=role.get('MaxSessionDuration', 3600),
                assume_role_policy_document=role.get('AssumeRolePolicyDocument', {}),
                permissions_boundary=role.get('PermissionsBoundary'),
                tags={t['Key']: t['Value'] for t in role.get('Tags', [])},
                status=IAMRoleState.ACTIVE,
                instance_profiles=[ip['InstanceProfileName'] for ip in instance_profiles]
            )
            
        except ClientError as e:
            logger.error(f"Failed to get role {role_name}: {e}")
            raise
    
    def update_role(self, role_name: str, description: Optional[str] = None,
                    max_session_duration: Optional[int] = None) -> bool:
        """
        Update an IAM role.
        
        Args:
            role_name: Name of the role
            description: New description
            max_session_duration: New max session duration
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            if description:
                self._client.update_role(RoleName=role_name, Description=description)
            
            if max_session_duration:
                self._client.update_role_max_duration(RoleName=role_name, MaxSessionDuration=max_session_duration)
            
            logger.info(f"Updated IAM role: {role_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update role {role_name}: {e}")
            raise
    
    def delete_role(self, role_name: str) -> bool:
        """
        Delete an IAM role.
        
        Args:
            role_name: Name of the role to delete
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            attached_policies = self.list_attached_role_policies(role_name)
            for policy in attached_policies:
                self.detach_role_policy(role_name, policy.arn)
            
            inline_policies = self.list_role_policies(role_name)
            for policy_name in inline_policies:
                self.delete_role_policy(role_name, policy_name)
            
            instance_profiles = self.list_instance_profiles_for_role(role_name)
            for ip in instance_profiles:
                self.remove_role_from_instance_profile(ip['InstanceProfileName'], role_name)
            
            self._client.delete_role(RoleName=role_name)
            logger.info(f"Deleted IAM role: {role_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete role {role_name}: {e}")
            raise
    
    def list_roles(self, path_prefix: Optional[str] = None, 
                   max_items: int = 100) -> List[RoleInfo]:
        """
        List IAM roles.
        
        Args:
            path_prefix: Filter by path prefix
            max_items: Maximum number of roles to return
            
        Returns:
            List of RoleInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {}
            if path_prefix:
                kwargs['PathPrefix'] = path_prefix
            if max_items:
                kwargs['MaxItems'] = max_items
            
            roles = []
            paginator = self._client.get_paginator('list_roles')
            
            for page in paginator.paginate(**kwargs):
                for role in page['Roles']:
                    roles.append(RoleInfo(
                        role_name=role['RoleName'],
                        role_id=role['RoleId'],
                        arn=role['Arn'],
                        path=role.get('Path', '/'),
                        create_date=role['CreateDate'],
                        description=role.get('Description'),
                        max_session_duration=role.get('MaxSessionDuration', 3600),
                        assume_role_policy_document=role.get('AssumeRolePolicyDocument', {}),
                        permissions_boundary=role.get('PermissionsBoundary'),
                        tags={t['Key']: t['Value'] for t in role.get('Tags', [])},
                        status=IAMRoleState.ACTIVE
                    ))
            
            return roles
            
        except ClientError as e:
            logger.error(f"Failed to list roles: {e}")
            raise
    
    def assume_role(self, role_arn: str, role_session_name: str,
                    duration_seconds: int = 3600,
                    external_id: Optional[str] = None,
                    serial_number: Optional[str] = None,
                    token_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Assume an IAM role and return temporary credentials.
        
        Args:
            role_arn: ARN of the role to assume
            role_session_name: Name for the role session
            duration_seconds: Duration of the session
            external_id: External ID for role assumption
            serial_number: MFA serial number
            token_code: MFA token code
            
        Returns:
            Dictionary with temporary credentials
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'RoleArn': role_arn,
                'RoleSessionName': role_session_name,
                'DurationSeconds': duration_seconds
            }
            
            if external_id:
                kwargs['ExternalId'] = external_id
            
            if serial_number and token_code:
                kwargs['SerialNumber'] = serial_number
                kwargs['TokenCode'] = token_code
            
            response = self._sts_client.assume_role(**kwargs)
            
            return {
                'access_key_id': response['Credentials']['AccessKeyId'],
                'secret_access_key': response['Credentials']['SecretAccessKey'],
                'session_token': response['Credentials']['SessionToken'],
                'expiration': response['Credentials']['Expiration']
            }
            
        except ClientError as e:
            logger.error(f"Failed to assume role {role_arn}: {e}")
            raise
    
    def list_instance_profiles_for_role(self, role_name: str) -> List[Dict[str, Any]]:
        """List instance profiles associated with a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_instance_profiles_for_role(RoleName=role_name)
            return response['InstanceProfiles']
        except ClientError as e:
            logger.error(f"Failed to list instance profiles for role {role_name}: {e}")
            raise
    
    def create_instance_profile(self, instance_profile_name: str, 
                                 path: Optional[str] = None) -> Dict[str, Any]:
        """Create an instance profile."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {'InstanceProfileName': instance_profile_name}
            if path:
                kwargs['Path'] = path
            
            response = self._client.create_instance_profile(**kwargs)
            logger.info(f"Created instance profile: {instance_profile_name}")
            return response['InstanceProfile']
        except ClientError as e:
            logger.error(f"Failed to create instance profile {instance_profile_name}: {e}")
            raise
    
    def add_role_to_instance_profile(self, instance_profile_name: str, 
                                       role_name: str) -> bool:
        """Add a role to an instance profile."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.add_role_to_instance_profile(
                InstanceProfileName=instance_profile_name,
                RoleName=role_name
            )
            logger.info(f"Added role {role_name} to instance profile {instance_profile_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to add role to instance profile: {e}")
            raise
    
    def remove_role_from_instance_profile(self, instance_profile_name: str,
                                            role_name: str) -> bool:
        """Remove a role from an instance profile."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.remove_role_from_instance_profile(
                InstanceProfileName=instance_profile_name,
                RoleName=role_name
            )
            logger.info(f"Removed role {role_name} from instance profile {instance_profile_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to remove role from instance profile: {e}")
            raise
    
    # =========================================================================
    # POLICY MANAGEMENT
    # =========================================================================
    
    def create_policy(self, config: IAMPolicyConfig) -> PolicyInfo:
        """
        Create a new IAM managed policy.
        
        Args:
            config: Policy configuration
            
        Returns:
            PolicyInfo object with policy details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'PolicyName': config.policy_name,
                'PolicyDocument': config.policy_document
            }
            if config.description:
                kwargs['Description'] = config.description
            if config.path:
                kwargs['Path'] = config.path
            if config.tags:
                kwargs['Tags'] = [{'Key': k, 'Value': v} for k, v in config.tags.items()]
            
            response = self._client.create_policy(**kwargs)
            policy = response['Policy']
            
            return PolicyInfo(
                policy_name=policy['PolicyName'],
                policy_id=policy['PolicyId'],
                arn=policy['Arn'],
                path=policy.get('Path'),
                default_version_id=policy.get('DefaultVersionId', 'v1'),
                attachment_count=policy.get('AttachmentCount', 0),
                permissions_boundary_usage_count=policy.get('PermissionsBoundaryUsageCount', 0),
                description=config.description,
                create_date=policy['CreateDate'],
                update_date=policy['UpdateDate'],
                tags={t['Key']: t['Value'] for t in policy.get('Tags', [])}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create policy {config.policy_name}: {e}")
            raise
    
    def get_policy(self, policy_arn: str) -> PolicyInfo:
        """Get information about an IAM policy."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_policy(PolicyArn=policy_arn)
            policy = response['Policy']
            
            return PolicyInfo(
                policy_name=policy['PolicyName'],
                policy_id=policy['PolicyId'],
                arn=policy['Arn'],
                path=policy.get('Path'),
                default_version_id=policy.get('DefaultVersionId', 'v1'),
                attachment_count=policy.get('AttachmentCount', 0),
                permissions_boundary_usage_count=policy.get('PermissionsBoundaryUsageCount', 0),
                description=policy.get('Description'),
                create_date=policy['CreateDate'],
                update_date=policy['UpdateDate'],
                tags={t['Key']: t['Value'] for t in policy.get('Tags', [])}
            )
            
        except ClientError as e:
            logger.error(f"Failed to get policy {policy_arn}: {e}")
            raise
    
    def delete_policy(self, policy_arn: str) -> bool:
        """Delete an IAM managed policy."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_policy(PolicyArn=policy_arn)
            logger.info(f"Deleted IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete policy {policy_arn}: {e}")
            raise
    
    def list_policies(self, scope: str = "Local", 
                      only_attached: bool = False,
                      path_prefix: Optional[str] = None,
                      max_items: int = 100) -> List[PolicyInfo]:
        """
        List IAM managed policies.
        
        Args:
            scope: Policy scope (Local, AWS, AWSManaged, CustomerManaged)
            only_attached: Only return attached policies
            path_prefix: Filter by path prefix
            max_items: Maximum number of policies to return
            
        Returns:
            List of PolicyInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'Scope': scope,
                'OnlyAttached': only_attached
            }
            if path_prefix:
                kwargs['PathPrefix'] = path_prefix
            if max_items:
                kwargs['MaxItems'] = max_items
            
            policies = []
            paginator = self._client.get_paginator('list_policies')
            
            for page in paginator.paginate(**kwargs):
                for policy in page['Policies']:
                    policies.append(PolicyInfo(
                        policy_name=policy['PolicyName'],
                        policy_id=policy['PolicyId'],
                        arn=policy['Arn'],
                        path=policy.get('Path'),
                        default_version_id=policy.get('DefaultVersionId', 'v1'),
                        attachment_count=policy.get('AttachmentCount', 0),
                        permissions_boundary_usage_count=policy.get('PermissionsBoundaryUsageCount', 0),
                        description=policy.get('Description'),
                        create_date=policy['CreateDate'],
                        update_date=policy['UpdateDate'],
                        tags={t['Key']: t['Value'] for t in policy.get('Tags', [])}
                    ))
            
            return policies
            
        except ClientError as e:
            logger.error(f"Failed to list policies: {e}")
            raise
    
    def attach_user_policy(self, user_name: str, policy_arn: str) -> bool:
        """Attach a managed policy to a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.attach_user_policy(UserName=user_name, PolicyArn=policy_arn)
            logger.info(f"Attached policy {policy_arn} to user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to attach policy to user: {e}")
            raise
    
    def detach_user_policy(self, user_name: str, policy_arn: str) -> bool:
        """Detach a managed policy from a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.detach_user_policy(UserName=user_name, PolicyArn=policy_arn)
            logger.info(f"Detached policy {policy_arn} from user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to detach policy from user: {e}")
            raise
    
    def attach_group_policy(self, group_name: str, policy_arn: str) -> bool:
        """Attach a managed policy to a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
            logger.info(f"Attached policy {policy_arn} to group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to attach policy to group: {e}")
            raise
    
    def detach_group_policy(self, group_name: str, policy_arn: str) -> bool:
        """Detach a managed policy from a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.detach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
            logger.info(f"Detached policy {policy_arn} from group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to detach policy from group: {e}")
            raise
    
    def attach_role_policy(self, role_name: str, policy_arn: str) -> bool:
        """Attach a managed policy to a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            logger.info(f"Attached policy {policy_arn} to role {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to attach policy to role: {e}")
            raise
    
    def detach_role_policy(self, role_name: str, policy_arn: str) -> bool:
        """Detach a managed policy from a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            logger.info(f"Detached policy {policy_arn} from role {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to detach policy from role: {e}")
            raise
    
    def list_attached_user_policies(self, user_name: str) -> List[PolicyInfo]:
        """List policies attached to a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            policies = []
            paginator = self._client.get_paginator('list_attached_user_policies')
            
            for page in paginator.paginate(UserName=user_name):
                for policy in page['AttachedPolicies']:
                    policies.append(PolicyInfo(
                        policy_name=policy['PolicyName'],
                        policy_id='',
                        arn=policy['PolicyArn'],
                        default_version_id='v1',
                        attachment_count=0,
                        permissions_boundary_usage_count=0,
                        create_date=datetime.now(),
                        update_date=datetime.now()
                    ))
            
            return policies
            
        except ClientError as e:
            logger.error(f"Failed to list attached user policies: {e}")
            raise
    
    def list_attached_group_policies(self, group_name: str) -> List[PolicyInfo]:
        """List policies attached to a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            policies = []
            paginator = self._client.get_paginator('list_attached_group_policies')
            
            for page in paginator.paginate(GroupName=group_name):
                for policy in page['AttachedPolicies']:
                    policies.append(PolicyInfo(
                        policy_name=policy['PolicyName'],
                        policy_id='',
                        arn=policy['PolicyArn'],
                        default_version_id='v1',
                        attachment_count=0,
                        permissions_boundary_usage_count=0,
                        create_date=datetime.now(),
                        update_date=datetime.now()
                    ))
            
            return policies
            
        except ClientError as e:
            logger.error(f"Failed to list attached group policies: {e}")
            raise
    
    def list_attached_role_policies(self, role_name: str) -> List[PolicyInfo]:
        """List policies attached to a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            policies = []
            paginator = self._client.get_paginator('list_attached_role_policies')
            
            for page in paginator.paginate(RoleName=role_name):
                for policy in page['AttachedPolicies']:
                    policies.append(PolicyInfo(
                        policy_name=policy['PolicyName'],
                        policy_id='',
                        arn=policy['PolicyArn'],
                        default_version_id='v1',
                        attachment_count=0,
                        permissions_boundary_usage_count=0,
                        create_date=datetime.now(),
                        update_date=datetime.now()
                    ))
            
            return policies
            
        except ClientError as e:
            logger.error(f"Failed to list attached role policies: {e}")
            raise
    
    def put_user_policy(self, user_name: str, policy_name: str,
                        policy_document: Dict[str, Any]) -> bool:
        """Create or update an inline policy for a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=policy_document
            )
            logger.info(f"Put inline policy {policy_name} for user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to put user inline policy: {e}")
            raise
    
    def put_group_policy(self, group_name: str, policy_name: str,
                         policy_document: Dict[str, Any]) -> bool:
        """Create or update an inline policy for a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.put_group_policy(
                GroupName=group_name,
                PolicyName=policy_name,
                PolicyDocument=policy_document
            )
            logger.info(f"Put inline policy {policy_name} for group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to put group inline policy: {e}")
            raise
    
    def put_role_policy(self, role_name: str, policy_name: str,
                        policy_document: Dict[str, Any]) -> bool:
        """Create or update an inline policy for a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=policy_document
            )
            logger.info(f"Put inline policy {policy_name} for role {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to put role inline policy: {e}")
            raise
    
    def delete_user_policy(self, user_name: str, policy_name: str) -> bool:
        """Delete an inline policy from a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_user_policy(UserName=user_name, PolicyName=policy_name)
            logger.info(f"Deleted inline policy {policy_name} from user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete user inline policy: {e}")
            raise
    
    def delete_group_policy(self, group_name: str, policy_name: str) -> bool:
        """Delete an inline policy from a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
            logger.info(f"Deleted inline policy {policy_name} from group {group_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete group inline policy: {e}")
            raise
    
    def delete_role_policy(self, role_name: str, policy_name: str) -> bool:
        """Delete an inline policy from a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            logger.info(f"Deleted inline policy {policy_name} from role {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete role inline policy: {e}")
            raise
    
    def list_user_policies(self, user_name: str) -> List[str]:
        """List inline policies for a user."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_user_policies(UserName=user_name)
            return response['PolicyNames']
        except ClientError as e:
            logger.error(f"Failed to list user inline policies: {e}")
            raise
    
    def list_group_policies(self, group_name: str) -> List[str]:
        """List inline policies for a group."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_group_policies(GroupName=group_name)
            return response['PolicyNames']
        except ClientError as e:
            logger.error(f"Failed to list group inline policies: {e}")
            raise
    
    def list_role_policies(self, role_name: str) -> List[str]:
        """List inline policies for a role."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_role_policies(RoleName=role_name)
            return response['PolicyNames']
        except ClientError as e:
            logger.error(f"Failed to list role inline policies: {e}")
            raise
    
    # =========================================================================
    # ACCESS KEY MANAGEMENT
    # =========================================================================
    
    def create_access_key(self, user_name: str) -> AccessKeyInfo:
        """
        Create a new access key for a user.
        
        Args:
            user_name: Name of the user
            
        Returns:
            AccessKeyInfo object with access key details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.create_access_key(UserName=user_name)
            key = response['AccessKey']
            
            return AccessKeyInfo(
                access_key_id=key['AccessKeyId'],
                user_name=key['UserName'],
                status=AccessKeyStatus.ACTIVE if key['Status'] == 'Active' else AccessKeyStatus.INACTIVE,
                create_date=key['CreateDate']
            )
            
        except ClientError as e:
            logger.error(f"Failed to create access key for {user_name}: {e}")
            raise
    
    def get_access_key(self, access_key_id: str) -> AccessKeyInfo:
        """Get information about an access key."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_access_key(AccessKeyId=access_key_id)
            key = response['AccessKey']
            
            return AccessKeyInfo(
                access_key_id=key['AccessKeyId'],
                user_name=key['UserName'],
                status=AccessKeyStatus.ACTIVE if key['Status'] == 'Active' else AccessKeyStatus.INACTIVE,
                create_date=key['CreateDate']
            )
            
        except ClientError as e:
            logger.error(f"Failed to get access key {access_key_id}: {e}")
            raise
    
    def update_access_key(self, user_name: str, access_key_id: str,
                          status: AccessKeyStatus) -> bool:
        """
        Update the status of an access key.
        
        Args:
            user_name: Name of the user
            access_key_id: Access key ID
            status: New status
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.update_access_key(
                UserName=user_name,
                AccessKeyId=access_key_id,
                Status=status.value
            )
            logger.info(f"Updated access key {access_key_id} status to {status.value}")
            return True
        except ClientError as e:
            logger.error(f"Failed to update access key: {e}")
            raise
    
    def delete_access_key(self, user_name: str, access_key_id: str) -> bool:
        """Delete an access key."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_access_key(UserName=user_name, AccessKeyId=access_key_id)
            logger.info(f"Deleted access key {access_key_id} for user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete access key: {e}")
            raise
    
    def list_access_keys(self, user_name: str) -> List[AccessKeyInfo]:
        """
        List access keys for a user.
        
        Args:
            user_name: Name of the user
            
        Returns:
            List of AccessKeyInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            keys = []
            response = self._client.list_access_keys(UserName=user_name)
            
            for key in response['AccessKeyMetadata']:
                keys.append(AccessKeyInfo(
                    access_key_id=key['AccessKeyId'],
                    user_name=key['UserName'],
                    status=AccessKeyStatus.ACTIVE if key['Status'] == 'Active' else AccessKeyStatus.INACTIVE,
                    create_date=key['CreateDate']
                ))
            
            return keys
            
        except ClientError as e:
            logger.error(f"Failed to list access keys for {user_name}: {e}")
            raise
    
    def get_access_key_last_used(self, access_key_id: str) -> Dict[str, Any]:
        """Get last used information for an access key."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_access_key_last_used(AccessKeyId=access_key_id)
            last_used = response.get('AccessKeyLastUsed', {})
            
            return {
                'last_used_date': last_used.get('LastUsedDate'),
                'last_used_service': last_used.get('ServiceName'),
                'region': last_used.get('Region')
            }
            
        except ClientError as e:
            logger.error(f"Failed to get access key last used: {e}")
            raise
    
    def rotate_access_key(self, user_name: str, access_key_id: str) -> AccessKeyInfo:
        """
        Rotate an access key by deactivating the old one and creating a new one.
        
        Args:
            user_name: Name of the user
            access_key_id: Current access key ID to rotate
            
        Returns:
            New AccessKeyInfo object
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self.update_access_key(user_name, access_key_id, AccessKeyStatus.INACTIVE)
            new_key = self.create_access_key(user_name)
            logger.info(f"Rotated access key for user {user_name}")
            return new_key
        except ClientError as e:
            logger.error(f"Failed to rotate access key: {e}")
            raise
    
    # =========================================================================
    # MFA MANAGEMENT
    # =========================================================================
    
    def enable_mfa_device(self, config: MFAConfig) -> MFADeviceInfo:
        """
        Enable an MFA device for a user.
        
        Args:
            config: MFA configuration
            
        Returns:
            MFADeviceInfo object with device details
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            if config.device_type == MFADeviceType.TOTP:
                response = self._client.enable_mfa_device(
                    UserName=config.user_name,
                    SerialNumber=config.serial_number,
                    AuthenticationCode1=config.authenticator_code
                )
                
                return MFADeviceInfo(
                    serial_number=config.serial_number,
                    user_name=config.user_name,
                    device_type=config.device_type,
                    enable_date=datetime.now(),
                    status=MFADeviceState.ENABLED
                )
            else:
                raise ValueError(f"Unsupported MFA device type: {config.device_type}")
            
        except ClientError as e:
            logger.error(f"Failed to enable MFA device: {e}")
            raise
    
    def deactivate_mfa_device(self, user_name: str, serial_number: str) -> bool:
        """
        Deactivate an MFA device.
        
        Args:
            user_name: Name of the user
            serial_number: MFA device serial number
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.deactivate_mfa_device(UserName=user_name, SerialNumber=serial_number)
            logger.info(f"Deactivated MFA device {serial_number} for user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to deactivate MFA device: {e}")
            raise
    
    def list_mfa_devices(self, user_name: str) -> List[MFADeviceInfo]:
        """
        List MFA devices for a user.
        
        Args:
            user_name: Name of the user
            
        Returns:
            List of MFADeviceInfo objects
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            devices = []
            response = self._client.list_mfa_devices(UserName=user_name)
            
            for device in response['MFADevices']:
                device_type = MFADeviceType.TOTP
                if device.get('DeviceType'):
                    device_type = MFADeviceType(device.get('DeviceType', 'TOTP'))
                
                devices.append(MFADeviceInfo(
                    serial_number=device['SerialNumber'],
                    user_name=device['UserName'],
                    device_type=device_type,
                    enable_date=device['EnableDate'],
                    status=MFADeviceState.ENABLED
                ))
            
            return devices
            
        except ClientError as e:
            logger.error(f"Failed to list MFA devices for {user_name}: {e}")
            raise
    
    def create_virtual_mfa_device(self, device_name: str, 
                                    path: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a virtual MFA device.
        
        Args:
            device_name: Name for the MFA device
            path: Path for the device
            
        Returns:
            Dictionary with device information and base32种子
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {'VirtualMFADeviceName': device_name}
            if path:
                kwargs['Path'] = path
            
            response = self._client.create_virtual_mfa_device(**kwargs)
            
            return {
                'serial_number': response['VirtualMFADevice']['SerialNumber'],
                'base32_seed': response['VirtualMFADevice']['Base32String'],
                'qr_code_png': response['VirtualMFADevice']['QRCodePNG']
            }
            
        except ClientError as e:
            logger.error(f"Failed to create virtual MFA device: {e}")
            raise
    
    def sync_mfa_device(self, user_name: str, serial_number: str,
                        code1: str, code2: str) -> bool:
        """
        Synchronize an MFA device (for authenticator apps).
        
        Args:
            user_name: Name of the user
            serial_number: MFA device serial number
            code1: First authentication code
            code2: Second authentication code
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.sync_mfa_device(
                UserName=user_name,
                SerialNumber=serial_number,
                AuthenticationCode1=code1,
                AuthenticationCode2=code2
            )
            logger.info(f"Synchronized MFA device {serial_number} for user {user_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to sync MFA device: {e}")
            raise
    
    # =========================================================================
    # FEDERATION MANAGEMENT
    # =========================================================================
    
    def create_saml_provider(self, name: str, metadata_document: str) -> str:
        """
        Create a SAML provider.
        
        Args:
            name: Name of the SAML provider
            metadata_document: SAML metadata document
            
        Returns:
            Provider ARN
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.create_saml_provider(
                Name=name,
                SAMLMetadataDocument=metadata_document
            )
            logger.info(f"Created SAML provider: {name}")
            return response['SAMLProviderArn']
        except ClientError as e:
            logger.error(f"Failed to create SAML provider: {e}")
            raise
    
    def get_saml_provider(self, saml_provider_arn: str) -> Dict[str, Any]:
        """Get information about a SAML provider."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_saml_provider(SAMLProviderArn=saml_provider_arn)
            return {
                'arn': response['SAMLProviderArn'],
                'metadata': response['SAMLMetadataDocument'],
                'create_date': response.get('CreateDate'),
                'tags': {}
            }
        except ClientError as e:
            logger.error(f"Failed to get SAML provider: {e}")
            raise
    
    def delete_saml_provider(self, saml_provider_arn: str) -> bool:
        """Delete a SAML provider."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_saml_provider(SAMLProviderArn=saml_provider_arn)
            logger.info(f"Deleted SAML provider: {saml_provider_arn}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete SAML provider: {e}")
            raise
    
    def list_saml_providers(self) -> List[Dict[str, Any]]:
        """List SAML providers."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_saml_providers()
            providers = []
            for p in response['SAMLProviderList']:
                providers.append({
                    'arn': p['Arn'],
                    'create_date': p.get('CreateDate'),
                    'tags': {}
                })
            return providers
        except ClientError as e:
            logger.error(f"Failed to list SAML providers: {e}")
            raise
    
    def create_open_id_connect_provider(self, url: str, client_id_list: List[str],
                                         thumbprint_list: List[str],
                                         tags: Optional[Dict[str, str]] = None) -> str:
        """
        Create an OIDC identity provider.
        
        Args:
            url: URL of the OIDC provider
            client_id_list: List of client IDs
            thumbprint_list: List of thumbprints
            tags: Optional tags
            
        Returns:
            Provider ARN
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'Url': url,
                'ClientIDList': client_id_list,
                'ThumbprintList': thumbprint_list
            }
            if tags:
                kwargs['Tags'] = [{'Key': k, 'Value': v} for k, v in tags.items()]
            
            response = self._client.create_open_id_connect_provider(**kwargs)
            logger.info(f"Created OIDC provider: {url}")
            return response['OpenIDConnectProviderArn']
        except ClientError as e:
            logger.error(f"Failed to create OIDC provider: {e}")
            raise
    
    def get_open_id_connect_provider(self, arn: str) -> Dict[str, Any]:
        """Get information about an OIDC provider."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            return {
                'arn': response['OpenIDConnectProviderArn'],
                'url': response.get('Url'),
                'client_ids': response.get('ClientIDList', []),
                'thumbprints': response.get('ThumbprintList', []),
                'create_date': response.get('CreateDate')
            }
        except ClientError as e:
            logger.error(f"Failed to get OIDC provider: {e}")
            raise
    
    def delete_open_id_connect_provider(self, arn: str) -> bool:
        """Delete an OIDC provider."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_open_id_connect_provider(OpenIDConnectProviderArn=arn)
            logger.info(f"Deleted OIDC provider: {arn}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete OIDC provider: {e}")
            raise
    
    def list_open_id_connect_providers(self) -> List[str]:
        """List OIDC providers."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.list_open_id_connect_providers()
            return [p['Arn'] for p in response['OpenIDConnectProviderList']]
        except ClientError as e:
            logger.error(f"Failed to list OIDC providers: {e}")
            raise
    
    # =========================================================================
    # SSO INTEGRATION
    # =========================================================================
    
    def get_sso_info(self) -> SSOInfo:
        """
        Get AWS SSO information.
        
        Returns:
            SSOInfo object with SSO configuration
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            if self._sso_client is None:
                session = boto3.Session(
                    region_name=self.config.region_name,
                    profile_name=self.config.profile_name
                )
                self._sso_client = session.client('sso-admin', region_name=self.config.region_name)
            
            response = self._sso_client.list_instances()
            
            if response['Instances']:
                instance = response['Instances'][0]
                return SSOInfo(
                    instance_arn=instance['InstanceArn'],
                    identity_store_id=instance['IdentityStoreId'],
                    sso_region=self.config.region_name
                )
            else:
                raise ValueError("No SSO instances found")
                
        except ClientError as e:
            logger.error(f"Failed to get SSO info: {e}")
            raise
    
    def list_sso_permission_sets(self, instance_arn: str) -> List[str]:
        """List SSO permission sets."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            if self._sso_client is None:
                session = boto3.Session(
                    region_name=self.config.region_name,
                    profile_name=self.config.profile_name
                )
                self._sso_client = session.client('sso-admin', region_name=self.config.region_name)
            
            response = self._sso_client.list_permission_sets(InstanceArn=instance_arn)
            return response['PermissionSets']
        except ClientError as e:
            logger.error(f"Failed to list SSO permission sets: {e}")
            raise
    
    def create_sso_permission_set(self, instance_arn: str, name: str,
                                   description: Optional[str] = None) -> str:
        """Create an SSO permission set."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            if self._sso_client is None:
                session = boto3.Session(
                    region_name=self.config.region_name,
                    profile_name=self.config.profile_name
                )
                self._sso_client = session.client('sso-admin', region_name=self.config.region_name)
            
            kwargs = {'InstanceArn': instance_arn, 'Name': name}
            if description:
                kwargs['Description'] = description
            
            response = self._sso_client.create_permission_set(**kwargs)
            logger.info(f"Created SSO permission set: {name}")
            return response['PermissionSet']['PermissionSetArn']
        except ClientError as e:
            logger.error(f"Failed to create SSO permission set: {e}")
            raise
    
    def assign_sso_permission_set(self, instance_arn: str, permission_set_arn: str,
                                    target_id: str, target_type: str = "USER") -> bool:
        """
        Assign an SSO permission set to a user or group.
        
        Args:
            instance_arn: SSO instance ARN
            permission_set_arn: Permission set ARN
            target_id: Target ID (user or group ID)
            target_type: Target type (USER or GROUP)
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            if self._sso_client is None:
                session = boto3.Session(
                    region_name=self.config.region_name,
                    profile_name=self.config.profile_name
                )
                self._sso_client = session.client('sso-admin', region_name=self.config.region_name)
            
            self._sso_client.create_account_assignment(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn,
                TargetType=target_type,
                TargetId=target_id
            )
            logger.info(f"Assigned SSO permission set to {target_type} {target_id}")
            return True
        except ClientError as e:
            logger.error(f"Failed to assign SSO permission set: {e}")
            raise
    
    # =========================================================================
    # PASSWORD POLICY
    # =========================================================================
    
    def get_password_policy(self) -> PasswordPolicyInfo:
        """
        Get the current password policy.
        
        Returns:
            PasswordPolicyInfo object
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            response = self._client.get_account_password_policy()
            policy = response['PasswordPolicy']
            
            return PasswordPolicyInfo(
                minimum_password_length=policy.get('MinimumPasswordLength', 14),
                require_symbols=policy.get('RequireSymbols', True),
                require_numbers=policy.get('RequireNumbers', True),
                require_uppercase=policy.get('RequireUppercaseCharacters', True),
                require_lowercase=policy.get('RequireLowercaseCharacters', True),
                allow_users_to_change_password=policy.get('AllowUsersToChangePassword', True),
                max_password_age=policy.get('MaxPasswordAge', 90),
                password_reuse_prevention=policy.get('PasswordReusePrevention', 24),
                hard_expiry=policy.get('HardExpiry', False)
            )
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                return PasswordPolicyInfo(
                    minimum_password_length=14,
                    require_symbols=True,
                    require_numbers=True,
                    require_uppercase=True,
                    require_lowercase=True,
                    allow_users_to_change_password=True,
                    max_password_age=90,
                    password_reuse_prevention=24,
                    hard_expiry=False
                )
            logger.error(f"Failed to get password policy: {e}")
            raise
    
    def set_password_policy(self, config: PasswordPolicyConfig) -> bool:
        """
        Set the password policy.
        
        Args:
            config: Password policy configuration
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'MinimumPasswordLength': config.minimum_password_length,
                'RequireSymbols': config.require_symbols,
                'RequireNumbers': config.require_numbers,
                'RequireUppercaseCharacters': config.require_uppercase,
                'RequireLowercaseCharacters': config.require_lowercase,
                'AllowUsersToChangePassword': config.allow_users_to_change_password,
                'MaxPasswordAge': config.max_password_age,
                'PasswordReusePrevention': config.password_reuse_prevention,
                'HardExpiry': config.hard_expiry
            }
            
            self._client.update_account_password_policy(**kwargs)
            logger.info("Updated account password policy")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to set password policy: {e}")
            raise
    
    def delete_password_policy(self) -> bool:
        """Delete the custom password policy (revert to AWS default)."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.delete_account_password_policy()
            logger.info("Deleted account password policy")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete password policy: {e}")
            raise
    
    # =========================================================================
    # CLOUDWATCH / CloudTrail INTEGRATION
    # =========================================================================
    
    def _get_cloudtrail_client(self):
        """Get or initialize CloudTrail client."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        if self._cloudtrail_client is None:
            session_kwargs = {}
            if self.config.profile_name:
                session_kwargs['profile_name'] = self.config.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            client_kwargs = {'region_name': self.config.region_name}
            if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                client_kwargs['aws_access_key_id'] = self.config.aws_access_key_id
                client_kwargs['aws_secret_access_key'] = self.config.aws_secret_access_key
                if self.config.aws_session_token:
                    client_kwargs['aws_session_token'] = self.config.aws_session_token
            
            self._cloudtrail_client = session.client('cloudtrail', **client_kwargs)
        
        return self._cloudtrail_client
    
    def create_trail(self, config: CloudTrailConfig) -> CloudTrailInfo:
        """
        Create a CloudTrail trail.
        
        Args:
            config: CloudTrail configuration
            
        Returns:
            CloudTrailInfo object with trail details
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            
            kwargs = {
                'Name': config.name,
                'S3BucketName': config.s3_bucket_name,
                'IncludeGlobalServiceEvents': config.include_global_service_events,
                'IsMultiRegionTrail': config.is_multi_region_trail,
                'EnableLogFileValidation': config.enable_log_file_validation
            }
            
            if config.s3_key_prefix:
                kwargs['S3KeyPrefix'] = config.s3_key_prefix
            
            if config.cloud_watch_logs_log_group:
                kwargs['CloudWatchLogsLogGroupArn'] = config.cloud_watch_logs_log_group
            
            if config.cloud_watch_logs_role_arn:
                kwargs['CloudWatchLogsRoleArn'] = config.cloud_watch_logs_role_arn
            
            if config.tags:
                kwargs['TagsList'] = [{'Key': k, 'Value': v} for k, v in config.tags.items()]
            
            response = client.create_trail(**kwargs)
            trail = response['Trail']
            
            return CloudTrailInfo(
                name=trail['Name'],
                trail_arn=trail['TrailARN'],
                is_multi_region_trail=trail.get('IsMultiRegionTrail', False),
                include_global_service_events=trail.get('IncludeGlobalServiceEvents', True),
                s3_bucket_name=trail['S3BucketName'],
                s3_key_prefix=trail.get('S3KeyPrefix'),
                cloud_watch_logs_log_group=trail.get('CloudWatchLogsLogGroupArn'),
                is_logging=trail.get('IsLogging', False),
                tags={t['Key']: t['Value'] for t in trail.get('TagsList', [])}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create CloudTrail: {e}")
            raise
    
    def get_trail(self, name: str) -> CloudTrailInfo:
        """Get information about a CloudTrail trail."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            response = client.describe_trails(trailNameList=[name])
            
            if not response['trailList']:
                raise ValueError(f"Trail {name} not found")
            
            trail = response['trailList'][0]
            
            return CloudTrailInfo(
                name=trail['Name'],
                trail_arn=trail['TrailARN'],
                is_multi_region_trail=trail.get('IsMultiRegionTrail', False),
                include_global_service_events=trail.get('IncludeGlobalServiceEvents', True),
                s3_bucket_name=trail['S3BucketName'],
                s3_key_prefix=trail.get('S3KeyPrefix'),
                cloud_watch_logs_log_group=trail.get('CloudWatchLogsLogGroupArn'),
                is_logging=trail.get('IsLogging', False),
                tags={t['Key']: t['Value'] for t in trail.get('TagsList', [])}
            )
            
        except ClientError as e:
            logger.error(f"Failed to get CloudTrail: {e}")
            raise
    
    def delete_trail(self, name: str) -> bool:
        """Delete a CloudTrail trail."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            client.delete_trail(Name=name)
            logger.info(f"Deleted CloudTrail: {name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete CloudTrail: {e}")
            raise
    
    def list_trails(self) -> List[CloudTrailInfo]:
        """List CloudTrail trails."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            response = client.describe_trails()
            
            trails = []
            for trail in response['trailList']:
                trails.append(CloudTrailInfo(
                    name=trail['Name'],
                    trail_arn=trail['TrailARN'],
                    is_multi_region_trail=trail.get('IsMultiRegionTrail', False),
                    include_global_service_events=trail.get('IncludeGlobalServiceEvents', True),
                    s3_bucket_name=trail['S3BucketName'],
                    s3_key_prefix=trail.get('S3KeyPrefix'),
                    cloud_watch_logs_log_group=trail.get('CloudWatchLogsLogGroupArn'),
                    is_logging=trail.get('IsLogging', False),
                    tags={t['Key']: t['Value'] for t in trail.get('TagsList', [])}
                ))
            
            return trails
            
        except ClientError as e:
            logger.error(f"Failed to list CloudTrails: {e}")
            raise
    
    def start_logging(self, name: str) -> bool:
        """Start CloudTrail logging."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            client.start_logging(Name=name)
            logger.info(f"Started CloudTrail logging: {name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to start CloudTrail logging: {e}")
            raise
    
    def stop_logging(self, name: str) -> bool:
        """Stop CloudTrail logging."""
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            client.stop_logging(Name=name)
            logger.info(f"Stopped CloudTrail logging: {name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to stop CloudTrail logging: {e}")
            raise
    
    def lookup_events(self, attributes: Optional[Dict[str, str]] = None,
                      start_time: Optional[datetime] = None,
                      end_time: Optional[datetime] = None,
                      max_results: int = 50) -> List[Dict[str, Any]]:
        """
        Look up IAM events in CloudTrail.
        
        Args:
            attributes: Event attributes to look up
            start_time: Start time for lookup
            end_time: End time for lookup
            max_results: Maximum number of results
            
        Returns:
            List of CloudTrail events
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install boto3 to enable.")
        
        try:
            client = self._get_cloudtrail_client()
            
            kwargs = {}
            if attributes:
                kwargs['LookupAttributes'] = [
                    {'AttributeKey': k, 'AttributeValue': v}
                    for k, v in attributes.items()
                ]
            if start_time:
                kwargs['StartTime'] = start_time
            if end_time:
                kwargs['EndTime'] = end_time
            if max_results:
                kwargs['MaxResults'] = max_results
            
            events = []
            paginator = client.get_paginator('lookup_events')
            
            for page in paginator.paginate(**kwargs):
                for event in page['Events']:
                    events.append({
                        'event_id': event['EventId'],
                        'event_name': event['EventName'],
                        'event_time': event['EventTime'],
                        'event_source': event['EventSource'],
                        'username': event.get('Username'),
                        'resources': event.get('Resources', []),
                        'cloudtrail_event': event.get('CloudTrailEvent')
                    })
            
            return events
            
        except ClientError as e:
            logger.error(f"Failed to lookup events: {e}")
            raise
    
    def get_iam_events(self, user_name: Optional[str] = None,
                        event_name: Optional[str] = None,
                        hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get IAM-related events from CloudTrail.
        
        Args:
            user_name: Filter by user name
            event_name: Filter by event name
            hours: Number of hours to look back
            
        Returns:
            List of IAM events
        """
        attributes = {}
        if user_name:
            attributes['Username'] = user_name
        if event_name:
            attributes['EventName'] = event_name
        
        start_time = datetime.now() - timedelta(hours=hours)
        
        return self.lookup_events(
            attributes=attributes if attributes else None,
            start_time=start_time
        )
    
    def add_tags(self, resource_arn: str, tags: Dict[str, str]) -> bool:
        """Add tags to an IAM resource."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.tag_open_id_connect_provider(
                OpenIDConnectProviderArn=resource_arn,
                Tags=[{'Key': k, 'Value': v} for k, v in tags.items()]
            )
            logger.info(f"Added tags to resource: {resource_arn}")
            return True
        except ClientError:
            try:
                self._client.tag_policy(
                    PolicyArn=resource_arn,
                    Tags=[{'Key': k, 'Value': v} for k, v in tags.items()]
                )
                logger.info(f"Added tags to resource: {resource_arn}")
                return True
            except ClientError:
                try:
                    self._client.tag_role(
                        RoleName=resource_arn.split('/')[-1],
                        Tags=[{'Key': k, 'Value': v} for k, v in tags.items()]
                    )
                    logger.info(f"Added tags to resource: {resource_arn}")
                    return True
                except ClientError as e:
                    logger.error(f"Failed to add tags: {e}")
                    raise
    
    def remove_tags(self, resource_arn: str, tag_keys: List[str]) -> bool:
        """Remove tags from an IAM resource."""
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            self._client.untag_open_id_connect_provider(
                OpenIDConnectProviderArn=resource_arn,
                TagKeys=tag_keys
            )
            logger.info(f"Removed tags from resource: {resource_arn}")
            return True
        except ClientError:
            try:
                self._client.untag_policy(
                    PolicyArn=resource_arn,
                    TagKeys=tag_keys
                )
                logger.info(f"Removed tags from resource: {resource_arn}")
                return True
            except ClientError:
                try:
                    self._client.untag_role(
                        RoleName=resource_arn.split('/')[-1],
                        TagKeys=tag_keys
                    )
                    logger.info(f"Removed tags from resource: {resource_arn}")
                    return True
                except ClientError as e:
                    logger.error(f"Failed to remove tags: {e}")
                    raise
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def generate_policy_document(self, effect: str = "Allow",
                                  actions: Optional[List[str]] = None,
                                  resources: Optional[List[str]] = None,
                                  conditions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generate a basic IAM policy document.
        
        Args:
            effect: Effect (Allow or Deny)
            actions: List of actions
            resources: List of resources
            conditions: Optional conditions
            
        Returns:
            Policy document dictionary
        """
        document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": effect,
                    "Action": actions or [],
                    "Resource": resources or ["*"]
                }
            ]
        }
        
        if conditions:
            document["Statement"][0]["Condition"] = conditions
        
        return document
    
    def generate_assume_role_policy(self, principal_type: str,
                                     principal_id: str) -> Dict[str, Any]:
        """
        Generate an assume role policy document.
        
        Args:
            principal_type: Principal type (AWS, Service, etc.)
            principal_id: Principal identifier
            
        Returns:
            Assume role policy document
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {principal_type: principal_id},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
    
    def simulate_principal_policy(self, policy_source_arn: str,
                                   action_names: List[str],
                                   resource_arns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Simulate policy execution to test permissions.
        
        Args:
            policy_source_arn: ARN of the user or role
            action_names: List of actions to simulate
            resource_arns: List of resource ARNs
            
        Returns:
            Simulation results
        """
        if not self.is_available:
            raise RuntimeError("IAM client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'PolicySourceArn': policy_source_arn,
                'ActionNames': action_names
            }
            if resource_arns:
                kwargs['ResourceArns'] = resource_arns
            
            response = self._client.simulate_principal_policy(**kwargs)
            
            return {
                'evaluation_results': [
                    {
                        'eval_action_name': r['EvalActionName'],
                        'eval_resource_name': r.get('EvalResourceName'),
                        'eval_decision': r['EvalDecision'],
                        'matched_statements': r.get('MatchedStatements', [])
                    }
                    for r in response['EvaluationResults']
                ]
            }
            
        except ClientError as e:
            logger.error(f"Failed to simulate policy: {e}")
            raise
