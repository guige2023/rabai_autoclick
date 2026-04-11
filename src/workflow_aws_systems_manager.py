"""
AWS Systems Manager Integration Module for Workflow System

Implements an SSMIntegration class with:
1. Parameter Store: Manage parameters (String, StringList, SecureString)
2. Document management: Create/manage SSM documents
3. Run Command: Execute commands on instances
4. Session Manager: Start SSH-like sessions
5. State Manager: Manage instance state
6. Maintenance Windows: Schedule maintenance
7. OpsCenter: Manage OpsItems
8. Inventory: Collect instance inventory
9. Patch Manager: Manage patches
10. CloudWatch integration: Logging and metrics

Commit: 'feat(aws-systems-manager): add AWS SSM with Parameter Store, document management, Run Command, Session Manager, State Manager, Maintenance Windows, OpsCenter, Inventory, Patch Manager, CloudWatch'
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


class ParameterType(Enum):
    """Parameter Store types."""
    STRING = "String"
    STRING_LIST = "StringList"
    SECURE_STRING = "SecureString"


class ParameterTier(Enum):
    """Parameter Store tiers."""
    STANDARD = "Standard"
    ADVANCED = "Advanced"
    INTELLIGENT_TIERING = "Intelligent-Tiering"


class DocumentType(Enum):
    """SSM Document types."""
    COMMAND = "Command"
    POLICY = "Policy"
    AUTOMATION = "Automation"
    SESSION = "Session"
    PACKAGE = "Package"
    Wizards = "Wizards"


class DocumentFormat(Enum):
    """SSM Document formats."""
    YAML = "YAML"
    JSON = "JSON"


class CommandStatus(Enum):
    """Run Command execution status."""
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    SUCCESS = "Success"
    FAILED = "Failed"
    TIMED_OUT = "TimedOut"
    CANANCELLING = "Cancelling"
    CANCELLED = "Cancelled"


class SessionStatus(Enum):
    """Session Manager session status."""
    ACTIVE = "Active"
    IDLE = "Idle"
    DISCONNECTED = "Disconnected"
    TERMINATED = "Terminated"


class AssociationStatus(Enum):
    """State Manager association status."""
    ASSOCIATED = "Associated"
    PENDING = "Pending"
    FAILED = "Failed"


class PatchComplianceLevel(Enum):
    """Patch manager compliance levels."""
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFORMATIONAL = "Informational"
    UNDEFINED = "Undefined"


class OpsItemStatus(Enum):
    """OpsCenter OpsItem status."""
    OPEN = "Open"
    IN_PROGRESS = "InProgress"
    RESOLVED = "Resolved"
    PENDING = "Pending"
    TIMED_OUT = "TimedOut"


@dataclass
class Parameter:
    """Represents a Parameter Store parameter."""
    name: str
    value: str
    param_type: ParameterType = ParameterType.STRING
    description: str = ""
    key_id: Optional[str] = None
    tier: ParameterTier = ParameterTier.STANDARD
    version: int = 1
    last_modified: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)
    arn: Optional[str] = None


@dataclass
class SSMDocument:
    """Represents an SSM document."""
    name: str
    content: Union[str, Dict]
    doc_type: DocumentType = DocumentType.COMMAND
    format: DocumentFormat = DocumentFormat.JSON
    version: Optional[str] = None
    description: str = ""
    target_type: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    arn: Optional[str] = None
    created_date: Optional[datetime] = None
    modified_date: Optional[datetime] = None


@dataclass
class CommandExecution:
    """Represents a command execution."""
    command_id: str
    status: CommandStatus
    requested_date: datetime
    completed_date: Optional[datetime] = None
    instance_ids: List[str] = field(default_factory=list)
    output: Optional[str] = None
    error: Optional[str] = None
    target_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    timeout_seconds: int = 3600


@dataclass
class SessionInfo:
    """Represents a Session Manager session."""
    session_id: str
    target: str
    status: SessionStatus
    start_date: datetime
    end_date: Optional[datetime] = None
    owner: Optional[str] = None
    output_location: Optional[str] = None


@dataclass
class Association:
    """Represents a State Manager association."""
    association_id: str
    name: str
    instance_id: str
    status: AssociationStatus
    last_execution_date: Optional[datetime] = None
    last_successful_execution: Optional[datetime] = None
    last_failed_execution: Optional[datetime] = None


@dataclass
class MaintenanceWindow:
    """Represents a maintenance window."""
    window_id: str
    name: str
    start_time: datetime
    end_time: datetime
    description: str = ""
    schedule: str = ""
    duration: int = 0
    cutoff: int = 0
    enabled: bool = True
    target_count: int = 0


@dataclass
class OpsItem:
    """Represents an OpsCenter OpsItem."""
    ops_item_id: str
    title: str
    status: OpsItemStatus
    priority: int = 3
    category: Optional[str] = None
    severity: str = "Medium"
    created_by: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    operational_data: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class InventoryEntry:
    """Represents an inventory entry."""
    instance_id: str
    capture_time: datetime
    entries: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatchState:
    """Represents patch compliance state."""
    instance_id: str
    patch_group: Optional[str] = None
    baseline_id: Optional[str] = None
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    other_count: int = 0
    last_operation_time: Optional[datetime] = None


class SSMIntegration:
    """AWS Systems Manager integration for workflow automation."""

    def __init__(
        self,
        region: str = "us-east-1",
        profile: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        ssm_client=None,
        s3_client=None,
        logs_client=None,
        cloudwatch_client=None,
    ):
        """
        Initialize SSM integration.

        Args:
            region: AWS region
            profile: AWS profile name
            endpoint_url: Custom endpoint URL
            ssm_client: Pre-configured SSM client
            s3_client: Pre-configured S3 client
            logs_client: Pre-configured CloudWatch Logs client
            cloudwatch_client: Pre-configured CloudWatch client
        """
        self.region = region
        self.profile = profile
        self.endpoint_url = endpoint_url
        self._ssm_client = ssm_client
        self._s3_client = s3_client
        self._logs_client = logs_client
        self._cloudwatch_client = cloudwatch_client
        self._lock = threading.RLock()

    @property
    def ssm(self):
        """Get or create SSM client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")
        if self._ssm_client is None:
            with self._lock:
                if self._ssm_client is None:
                    kwargs = {"region_name": self.region}
                    if self.profile:
                        kwargs["profile_name"] = self.profile
                    if self.endpoint_url:
                        kwargs["endpoint_url"] = self.endpoint_url
                    self._ssm_client = boto3.client("ssm", **kwargs)
        return self._ssm_client

    @property
    def s3(self):
        """Get or create S3 client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")
        if self._s3_client is None:
            with self._lock:
                if self._s3_client is None:
                    kwargs = {"region_name": self.region}
                    if self.profile:
                        kwargs["profile_name"] = self.profile
                    self._s3_client = boto3.client("s3", **kwargs)
        return self._s3_client

    @property
    def logs(self):
        """Get or create CloudWatch Logs client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")
        if self._logs_client is None:
            with self._lock:
                if self._logs_client is None:
                    kwargs = {"region_name": self.region}
                    if self.profile:
                        kwargs["profile_name"] = self.profile
                    self._logs_client = boto3.client("logs", **kwargs)
        return self._logs_client

    @property
    def cloudwatch(self):
        """Get or create CloudWatch client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")
        if self._cloudwatch_client is None:
            with self._lock:
                if self._cloudwatch_client is None:
                    kwargs = {"region_name": self.region}
                    if self.profile:
                        kwargs["profile_name"] = self.profile
                    self._cloudwatch_client = boto3.client("cloudwatch", **kwargs)
        return self._cloudwatch_client

    # =========================================================================
    # 1. Parameter Store
    # =========================================================================

    def put_parameter(
        self,
        name: str,
        value: str,
        param_type: ParameterType = ParameterType.STRING,
        description: str = "",
        key_id: Optional[str] = None,
        tier: ParameterTier = ParameterTier.STANDARD,
        overwrite: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> Parameter:
        """
        Create or update a parameter.

        Args:
            name: Parameter name
            value: Parameter value
            param_type: Parameter type (String, StringList, SecureString)
            description: Parameter description
            key_id: KMS key ID for SecureString
            tier: Parameter tier
            overwrite: Overwrite existing parameter
            tags: Parameter tags

        Returns:
            Created/updated Parameter object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        param_kwargs = {
            "Name": name,
            "Value": value,
            "Type": param_type.value,
            "Tier": tier.value,
            "Overwrite": overwrite,
        }

        if description:
            param_kwargs["Description"] = description

        if key_id:
            param_kwargs["KeyId"] = key_id

        if tags:
            param_kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

        try:
            response = self.ssm.put_parameter(**param_kwargs)
            return Parameter(
                name=name,
                value=value,
                param_type=param_type,
                description=description,
                key_id=key_id,
                tier=tier,
                version=response["Version"],
                tags=tags or {},
            )
        except ClientError as e:
            logger.error(f"Failed to put parameter {name}: {e}")
            raise

    def get_parameter(
        self,
        name: str,
        with_decryption: bool = True,
        version: Optional[int] = None,
    ) -> Parameter:
        """
        Get a parameter by name.

        Args:
            name: Parameter name
            with_decryption: Decrypt SecureString
            version: Specific parameter version

        Returns:
            Parameter object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"Name": name, "WithDecryption": with_decryption}
        if version:
            kwargs["Version"] = version

        try:
            response = self.ssm.get_parameter(**kwargs)
            param = response["Parameter"]
            return Parameter(
                name=param["Name"],
                value=param["Value"],
                param_type=ParameterType(param["Type"]),
                version=param["Version"],
                last_modified=datetime.fromisoformat(
                    param["LastModifiedDate"].replace("Z", "+00:00")
                ),
                arn=param.get("ARN"),
                tags={t["Key"]: t["Value"] for t in param.get("Tags", [])},
            )
        except ClientError as e:
            logger.error(f"Failed to get parameter {name}: {e}")
            raise

    def get_parameters(
        self,
        names: List[str],
        with_decryption: bool = True,
    ) -> List[Parameter]:
        """
        Get multiple parameters by names.

        Args:
            names: List of parameter names
            with_decryption: Decrypt SecureString

        Returns:
            List of Parameter objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.get_parameters(
                Names=names,
                WithDecryption=with_decryption,
            )
            return [
                Parameter(
                    name=p["Name"],
                    value=p["Value"],
                    param_type=ParameterType(p["Type"]),
                    version=p["Version"],
                    arn=p.get("ARN"),
                )
                for p in response["Parameters"]
            ]
        except ClientError as e:
            logger.error(f"Failed to get parameters: {e}")
            raise

    def delete_parameter(self, name: str) -> bool:
        """
        Delete a parameter.

        Args:
            name: Parameter name

        Returns:
            True if deleted
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.delete_parameter(Name=name)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete parameter {name}: {e}")
            raise

    def delete_parameters(self, names: List[str]) -> List[str]:
        """
        Delete multiple parameters.

        Args:
            names: List of parameter names

        Returns:
            List of deleted parameter names
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.delete_parameters(Names=names)
            return response["DeletedParameters"]
        except ClientError as e:
            logger.error(f"Failed to delete parameters: {e}")
            raise

    def list_parameters(
        self,
        path: Optional[str] = None,
        filters: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[Parameter], Optional[str]]:
        """
        List parameters with optional path prefix and filters.

        Args:
            path: Parameter path prefix
            filters: List of filter criteria
            max_results: Maximum results per page
            next_token: Pagination token

        Returns:
            Tuple of (parameters list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if path:
            kwargs["ParameterFilters"] = [{"Key": "Path", "Option": "Recursive", "Values": [path]}]
        if filters:
            kwargs["ParameterFilters"] = filters
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.describe_parameters(**kwargs)
            params = [
                Parameter(
                    name=p["Name"],
                    value="",
                    param_type=ParameterType(p["Type"]),
                    description=p.get("Description", ""),
                    version=1,
                    tags={t["Key"]: t["Value"] for t in p.get("Tags", [])},
                )
                for p in response["Parameters"]
            ]
            return params, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list parameters: {e}")
            raise

    def get_parameter_history(
        self,
        name: str,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[Parameter], Optional[str]]:
        """
        Get parameter version history.

        Args:
            name: Parameter name
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (parameters list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"Name": name, "MaxResults": max_results}
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.get_parameter_history(**kwargs)
            params = [
                Parameter(
                    name=name,
                    value=p["Value"],
                    param_type=ParameterType(p["Type"]),
                    version=p["Version"],
                    last_modified=datetime.fromisoformat(
                        p["LastModifiedDate"].replace("Z", "+00:00")
                    ),
                    key_id=p.get("KeyId"),
                    description=p.get("Description", ""),
                )
                for p in response["Parameters"]
            ]
            return params, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to get parameter history for {name}: {e}")
            raise

    # =========================================================================
    # 2. Document Management
    # =========================================================================

    def create_document(
        self,
        name: str,
        content: Union[str, Dict],
        doc_type: DocumentType = DocumentType.COMMAND,
        format: DocumentFormat = DocumentFormat.JSON,
        description: str = "",
        target_type: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> SSMDocument:
        """
        Create an SSM document.

        Args:
            name: Document name
            content: Document content (YAML/JSON string or dict)
            doc_type: Document type
            format: Document format
            description: Document description
            target_type: Target type (/AWS:EC2:Instance, etc.)
            tags: Document tags

        Returns:
            SSMDocument object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        if isinstance(content, dict):
            content = json.dumps(content) if format == DocumentFormat.JSON else content

        kwargs = {
            "Name": name,
            "Content": content,
            "DocumentType": doc_type.value,
            "DocumentFormat": format.value,
        }

        if description:
            kwargs["Description"] = description
        if target_type:
            kwargs["TargetType"] = target_type
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

        try:
            response = self.ssm.create_document(**kwargs)
            doc_info = response["DocumentDescription"]
            return SSMDocument(
                name=doc_info["Name"],
                content=content,
                doc_type=DocumentType(doc_info["DocumentType"]),
                format=DocumentFormat(doc_info["DocumentFormat"]),
                version=doc_info.get("Version"),
                description=doc_info.get("Description", ""),
                target_type=doc_info.get("TargetType"),
                arn=doc_info.get("ARN"),
                created_date=datetime.fromisoformat(
                    doc_info["CreatedDate"].replace("Z", "+00:00")
                ) if doc_info.get("CreatedDate") else None,
            )
        except ClientError as e:
            logger.error(f"Failed to create document {name}: {e}")
            raise

    def get_document(self, name: str, version: Optional[str] = None) -> SSMDocument:
        """
        Get an SSM document.

        Args:
            name: Document name
            version: Document version

        Returns:
            SSMDocument object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"Name": name}
        if version:
            kwargs["VersionName"] = version

        try:
            response = self.ssm.get_document(**kwargs)
            doc_info = response
            return SSMDocument(
                name=doc_info["Name"],
                content=doc_info["Content"],
                doc_type=DocumentType(doc_info["DocumentType"]),
                format=DocumentFormat(doc_info.get("DocumentFormat", "JSON")),
                version=doc_info.get("Version"),
                description=doc_info.get("Description", ""),
                target_type=doc_info.get("TargetType"),
                arn=doc_info.get("ARN"),
            )
        except ClientError as e:
            logger.error(f"Failed to get document {name}: {e}")
            raise

    def update_document(
        self,
        name: str,
        content: Union[str, Dict],
        format: DocumentFormat = DocumentFormat.JSON,
        version: Optional[str] = None,
    ) -> SSMDocument:
        """
        Update an SSM document.

        Args:
            name: Document name
            content: New document content
            format: Document format
            version: Document version to update

        Returns:
            Updated SSMDocument object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        if isinstance(content, dict):
            content = json.dumps(content) if format == DocumentFormat.JSON else content

        kwargs = {
            "Name": name,
            "Content": content,
            "DocumentFormat": format.value,
        }
        if version:
            kwargs["DocumentVersion"] = version

        try:
            response = self.ssm.update_document(**kwargs)
            doc_info = response["DocumentDescription"]
            return SSMDocument(
                name=doc_info["Name"],
                content=content,
                doc_type=DocumentType(doc_info["DocumentType"]),
                format=DocumentFormat(doc_info["DocumentFormat"]),
                version=doc_info.get("Version"),
                description=doc_info.get("Description", ""),
                modified_date=datetime.fromisoformat(
                    doc_info["UpdatedDate"].replace("Z", "+00:00")
                ) if doc_info.get("UpdatedDate") else None,
            )
        except ClientError as e:
            logger.error(f"Failed to update document {name}: {e}")
            raise

    def delete_document(self, name: str, force: bool = False) -> bool:
        """
        Delete an SSM document.

        Args:
            name: Document name
            force: Force delete

        Returns:
            True if deleted
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            kwargs = {"Name": name}
            if force:
                kwargs["Force"] = True
            self.ssm.delete_document(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete document {name}: {e}")
            raise

    def list_documents(
        self,
        document_filter: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[SSMDocument], Optional[str]]:
        """
        List SSM documents.

        Args:
            document_filter: Filter criteria
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (documents list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if document_filter:
            kwargs["DocumentFilterList"] = document_filter
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.list_documents(**kwargs)
            docs = [
                SSMDocument(
                    name=d["Name"],
                    content="",
                    doc_type=DocumentType(d["DocumentType"]),
                    description=d.get("Description", ""),
                    arn=d.get("Arn"),
                )
                for d in response["DocumentIdentifiers"]
            ]
            return docs, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list documents: {e}")
            raise

    # =========================================================================
    # 3. Run Command
    # =========================================================================

    def send_command(
        self,
        document_name: str,
        instance_ids: List[str],
        parameters: Optional[Dict[str, List[str]]] = None,
        comment: str = "",
        timeout_seconds: int = 3600,
        max_concurrency: Optional[str] = None,
        max_errors: Optional[str] = None,
        notification_config: Optional[Dict] = None,
        output_location: Optional[Dict] = None,
    ) -> CommandExecution:
        """
        Send a command to instances.

        Args:
            document_name: SSM document name
            instance_ids: Target instance IDs
            parameters: Command parameters
            comment: Command comment
            timeout_seconds: Command timeout
            max_concurrency: Max concurrent targets ("1" to "5" or "50%" to "100%")
            max_errors: Max errors allowed
            notification_config: Notification config
            output_location: S3 output location

        Returns:
            CommandExecution object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "DocumentName": document_name,
            "InstanceIds": instance_ids,
            "TimeoutSeconds": timeout_seconds,
        }

        if parameters:
            kwargs["Parameters"] = parameters
        if comment:
            kwargs["Comment"] = comment
        if max_concurrency:
            kwargs["MaxConcurrency"] = max_concurrency
        if max_errors:
            kwargs["MaxErrors"] = max_errors
        if notification_config:
            kwargs["NotificationConfig"] = notification_config
        if output_location:
            kwargs["OutputLocation"] = output_location

        try:
            response = self.ssm.send_command(**kwargs)
            cmd = response["Command"]
            return CommandExecution(
                command_id=cmd["CommandId"],
                status=CommandStatus(cmd["Status"]),
                requested_date=datetime.fromisoformat(
                    cmd["RequestedDateTime"].replace("Z", "+00:00")
                ),
                instance_ids=instance_ids,
                target_count=cmd.get("TargetCount", 0),
                success_count=cmd.get("SuccessCount", 0),
                failed_count=cmd.get("FailedCount", 0),
                timeout_seconds=timeout_seconds,
            )
        except ClientError as e:
            logger.error(f"Failed to send command: {e}")
            raise

    def list_commands(
        self,
        command_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        status: Optional[CommandStatus] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[CommandExecution], Optional[str]]:
        """
        List command executions.

        Args:
            command_id: Filter by command ID
            instance_id: Filter by instance ID
            status: Filter by status
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (commands list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if command_id:
            kwargs["CommandId"] = command_id
        if instance_id:
            kwargs["InstanceId"] = instance_id
        if status:
            kwargs["State"] = status.value
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.list_commands(**kwargs)
            cmds = [
                CommandExecution(
                    command_id=c["CommandId"],
                    status=CommandStatus(c["Status"]),
                    requested_date=datetime.fromisoformat(
                        c["RequestedDateTime"].replace("Z", "+00:00")
                    ),
                    completed_date=datetime.fromisoformat(
                        c["CompletedDateTime"].replace("Z", "+00:00")
                    ) if c.get("CompletedDateTime") else None,
                    target_count=c.get("TargetCount", 0),
                    success_count=c.get("SuccessCount", 0),
                    failed_count=c.get("FailedCount", 0),
                )
                for c in response["Commands"]
            ]
            return cmds, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list commands: {e}")
            raise

    def get_command_invocation(
        self,
        command_id: str,
        instance_id: str,
    ) -> CommandExecution:
        """
        Get command invocation details.

        Args:
            command_id: Command ID
            instance_id: Instance ID

        Returns:
            CommandExecution object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
            return CommandExecution(
                command_id=command_id,
                status=CommandStatus(response["Status"]),
                requested_date=datetime.fromisoformat(
                    response["RequestedDateTime"].replace("Z", "+00:00")
                ),
                completed_date=datetime.fromisoformat(
                    response["ExecutionDateTime"].replace("Z", "+00:00")
                ) if response.get("ExecutionDateTime") else None,
                instance_ids=[instance_id],
                output=response.get("StandardOutputContent"),
                error=response.get("StandardErrorContent"),
            )
        except ClientError as e:
            logger.error(f"Failed to get command invocation: {e}")
            raise

    def cancel_command(self, command_id: str) -> bool:
        """
        Cancel a command execution.

        Args:
            command_id: Command ID

        Returns:
            True if cancelled
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.cancel_command(CommandId=command_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to cancel command {command_id}: {e}")
            raise

    # =========================================================================
    # 4. Session Manager
    # =========================================================================

    def start_session(
        self,
        target: str,
        document_name: str = "SSM-Session",
        reason: str = "",
    ) -> SessionInfo:
        """
        Start a Session Manager session.

        Args:
            target: Instance ID to connect to
            document_name: Session document name
            reason: Reason for starting session

        Returns:
            SessionInfo object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"Target": target, "DocumentName": document_name}
        if reason:
            kwargs["Reason"] = reason

        try:
            response = self.ssm.start_session(**kwargs)
            return SessionInfo(
                session_id=response["SessionId"],
                target=target,
                status=SessionStatus.ACTIVE,
                start_date=datetime.now(),
            )
        except ClientError as e:
            logger.error(f"Failed to start session on {target}: {e}")
            raise

    def terminate_session(self, session_id: str) -> bool:
        """
        Terminate a session.

        Args:
            session_id: Session ID

        Returns:
            True if terminated
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.terminate_session(SessionId=session_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to terminate session {session_id}: {e}")
            raise

    def list_sessions(
        self,
        status: Optional[SessionStatus] = None,
        target: Optional[str] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[SessionInfo], Optional[str]]:
        """
        List Session Manager sessions.

        Args:
            status: Filter by status
            target: Filter by target instance
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (sessions list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if status:
            kwargs["Filters"] = [{"key": "Status", "value": status.value}]
        if target:
            if "Filters" not in kwargs:
                kwargs["Filters"] = []
            kwargs["Filters"].append({"key": "Target", "value": target})
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.list_sessions(**kwargs)
            sessions = [
                SessionInfo(
                    session_id=s["SessionId"],
                    target=s["Target"],
                    status=SessionStatus(s["Status"]),
                    start_date=datetime.fromisoformat(
                        s["StartDate"].replace("Z", "+00:00")
                    ),
                    end_date=datetime.fromisoformat(
                        s["EndDate"].replace("Z", "+00:00")
                    ) if s.get("EndDate") else None,
                    owner=s.get("Owner"),
                )
                for s in response["Sessions"]
            ]
            return sessions, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list sessions: {e}")
            raise

    # =========================================================================
    # 5. State Manager
    # =========================================================================

    def create_association(
        self,
        name: str,
        instance_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        targets: Optional[List[Dict]] = None,
        schedule: Optional[str] = None,
        location: Optional[Dict] = None,
    ) -> Association:
        """
        Create a State Manager association.

        Args:
            name: Document name
            instance_id: Instance ID
            parameters: Association parameters
            targets: Targets (alternative to instance_id)
            schedule: Cron schedule expression
            location: Target location

        Returns:
            Association object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "Name": name,
            "InstanceId": instance_id,
        }

        if parameters:
            kwargs["Parameters"] = parameters
        if targets:
            kwargs["Targets"] = targets
        if schedule:
            kwargs["ScheduleExpression"] = schedule
        if location:
            kwargs["Location"] = location

        try:
            response = self.ssm.create_association(**kwargs)
            assoc = response["AssociationDescription"]
            return Association(
                association_id=assoc["AssociationId"],
                name=assoc["Name"],
                instance_id=assoc["InstanceId"],
                status=AssociationStatus(assoc.get("AssociationStatusName", "Pending")),
                last_execution_date=datetime.fromisoformat(
                    assoc["LastExecutionDate"].replace("Z", "+00:00")
                ) if assoc.get("LastExecutionDate") else None,
            )
        except ClientError as e:
            logger.error(f"Failed to create association: {e}")
            raise

    def list_associations(
        self,
        association_filter: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[Association], Optional[str]]:
        """
        List State Manager associations.

        Args:
            association_filter: Filter criteria
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (associations list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if association_filter:
            kwargs["AssociationFilterList"] = association_filter
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.list_associations(**kwargs)
            assocs = [
                Association(
                    association_id=a["AssociationId"],
                    name=a["Name"],
                    instance_id=a["InstanceId"],
                    status=AssociationStatus(a.get("AssociationStatusName", "Pending")),
                    last_execution_date=datetime.fromisoformat(
                        a["LastExecutionDate"].replace("Z", "+00:00")
                    ) if a.get("LastExecutionDate") else None,
                )
                for a in response["AssociationList"]
            ]
            return assocs, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list associations: {e}")
            raise

    def describe_association(
        self,
        name: str,
        instance_id: str,
    ) -> Association:
        """
        Describe an association.

        Args:
            name: Document name
            instance_id: Instance ID

        Returns:
            Association object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.describe_association(
                Name=name,
                InstanceId=instance_id,
            )
            assoc = response["AssociationDescription"]
            return Association(
                association_id=assoc["AssociationId"],
                name=assoc["Name"],
                instance_id=assoc["InstanceId"],
                status=AssociationStatus(assoc.get("AssociationStatusName", "Pending")),
                last_execution_date=datetime.fromisoformat(
                    assoc["LastExecutionDate"].replace("Z", "+00:00")
                ) if assoc.get("LastExecutionDate") else None,
                last_successful_execution=datetime.fromisoformat(
                    assoc["LastSuccessfulExecutionDate"].replace("Z", "+00:00")
                ) if assoc.get("LastSuccessfulExecutionDate") else None,
                last_failed_execution=datetime.fromisoformat(
                    assoc["LastFailedExecutionDate"].replace("Z", "+00:00")
                ) if assoc.get("LastFailedExecutionDate") else None,
            )
        except ClientError as e:
            logger.error(f"Failed to describe association: {e}")
            raise

    def delete_association(self, name: str, instance_id: str) -> bool:
        """
        Delete an association.

        Args:
            name: Document name
            instance_id: Instance ID

        Returns:
            True if deleted
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.delete_association(Name=name, InstanceId=instance_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete association: {e}")
            raise

    # =========================================================================
    # 6. Maintenance Windows
    # =========================================================================

    def create_maintenance_window(
        self,
        name: str,
        start_time: datetime,
        end_time: datetime,
        schedule: str,
        duration: int,
        cutoff: int,
        description: str = "",
        enabled: bool = True,
        tags: Optional[Dict[str, str]] = None,
    ) -> MaintenanceWindow:
        """
        Create a maintenance window.

        Args:
            name: Window name
            start_time: Window start time
            end_time: Window end time
            schedule: Cron expression
            duration: Duration in hours
            cutoff: Cutoff time in hours before end
            description: Window description
            enabled: Whether window is enabled
            tags: Window tags

        Returns:
            MaintenanceWindow object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "Name": name,
            "StartTime": start_time.strftime("%H:%M"),
            "EndTime": end_time.strftime("%H:%M"),
            "Schedule": schedule,
            "Duration": duration,
            "Cutoff": cutoff,
            "Enabled": enabled,
        }

        if description:
            kwargs["Description"] = description
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

        try:
            response = self.ssm.create_maintenance_window(**kwargs)
            return MaintenanceWindow(
                window_id=response["WindowId"],
                name=name,
                description=description,
                start_time=start_time,
                end_time=end_time,
                schedule=schedule,
                duration=duration,
                cutoff=cutoff,
                enabled=enabled,
            )
        except ClientError as e:
            logger.error(f"Failed to create maintenance window: {e}")
            raise

    def get_maintenance_window(self, window_id: str) -> MaintenanceWindow:
        """
        Get a maintenance window.

        Args:
            window_id: Window ID

        Returns:
            MaintenanceWindow object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.get_maintenance_window(WindowId=window_id)
            w = response
            return MaintenanceWindow(
                window_id=w["WindowId"],
                name=w["Name"],
                description=w.get("Description", ""),
                start_time=datetime.fromisoformat(w["StartTime"]),
                end_time=datetime.fromisoformat(w["EndTime"]),
                schedule=w["Schedule"],
                duration=w["Duration"],
                cutoff=w["Cutoff"],
                enabled=w["Enabled"],
                target_count=w.get("TargetCount", 0),
            )
        except ClientError as e:
            logger.error(f"Failed to get maintenance window {window_id}: {e}")
            raise

    def list_maintenance_windows(
        self,
        filters: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[MaintenanceWindow], Optional[str]]:
        """
        List maintenance windows.

        Args:
            filters: Filter criteria
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (windows list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if filters:
            kwargs["Filters"] = filters
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.list_maintenance_windows(**kwargs)
            windows = [
                MaintenanceWindow(
                    window_id=w["WindowId"],
                    name=w["Name"],
                    description=w.get("Description", ""),
                    schedule=w.get("Schedule", ""),
                    start_time=datetime.fromisoformat(w["NextExecutionTime"])
                    if w.get("NextExecutionTime") else datetime.now(),
                    end_time=datetime.now(),
                    duration=w.get("Duration", 0),
                    cutoff=w.get("Cutoff", 0),
                    enabled=w.get("Enabled", True),
                )
                for w in response["WindowIdentities"]
            ]
            return windows, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list maintenance windows: {e}")
            raise

    def delete_maintenance_window(self, window_id: str) -> bool:
        """
        Delete a maintenance window.

        Args:
            window_id: Window ID

        Returns:
            True if deleted
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.delete_maintenance_window(WindowId=window_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete maintenance window {window_id}: {e}")
            raise

    def register_target(
        self,
        window_id: str,
        target: Dict,
        owner_information: str = "",
    ) -> str:
        """
        Register a target with a maintenance window.

        Args:
            window_id: Window ID
            target: Target definition
            owner_information: Owner info

        Returns:
            Target ID
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"WindowId": window_id, "Target": target}
        if owner_information:
            kwargs["OwnerInformation"] = owner_information

        try:
            response = self.ssm.register_target(**kwargs)
            return response["WindowTargetId"]
        except ClientError as e:
            logger.error(f"Failed to register target for window {window_id}: {e}")
            raise

    def register_task(
        self,
        window_id: str,
        task_type: str,
        task_arn: str,
        target: Dict,
        task_name: str,
        max_concurrency: Optional[str] = None,
        max_errors: Optional[str] = None,
        priority: int = 1,
    ) -> str:
        """
        Register a task with a maintenance window.

        Args:
            window_id: Window ID
            task_type: Task type (RUN_COMMAND, AUTOMATION, etc.)
            task_arn: Task ARN
            target: Target definition
            task_name: Task name
            max_concurrency: Max concurrency
            max_errors: Max errors
            priority: Task priority

        Returns:
            Task ID
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "WindowId": window_id,
            "TaskType": task_type,
            "TaskArn": task_arn,
            "Target": target,
            "Name": task_name,
            "Priority": priority,
        }

        if max_concurrency:
            kwargs["MaxConcurrency"] = max_concurrency
        if max_errors:
            kwargs["MaxErrors"] = max_errors

        try:
            response = self.ssm.register_task(**kwargs)
            return response["WindowTaskId"]
        except ClientError as e:
            logger.error(f"Failed to register task for window {window_id}: {e}")
            raise

    # =========================================================================
    # 7. OpsCenter
    # =========================================================================

    def create_ops_item(
        self,
        title: str,
        description: str = "",
        priority: int = 3,
        category: Optional[str] = None,
        severity: str = "Medium",
        operational_data: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> OpsItem:
        """
        Create an OpsCenter OpsItem.

        Args:
            title: OpsItem title
            description: OpsItem description
            priority: Priority (1-5)
            category: Category
            severity: Severity
            operational_data: Operational data
            tags: Tags

        Returns:
            OpsItem object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "Title": title,
            "Priority": priority,
        }

        if description:
            kwargs["Description"] = description
        if category:
            kwargs["Category"] = category
        if severity:
            kwargs["Severity"] = severity
        if operational_data:
            kwargs["OperationalData"] = {
                k: {"Type": "SearchableString", "Value": json.dumps(v)}
                for k, v in operational_data.items()
            }
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

        try:
            response = self.ssm.create_ops_item(**kwargs)
            return OpsItem(
                ops_item_id=response["OpsItemId"],
                title=title,
                status=OpsItemStatus.OPEN,
                priority=priority,
                category=category,
                severity=severity,
                operational_data=operational_data or {},
                tags=tags or {},
            )
        except ClientError as e:
            logger.error(f"Failed to create ops item: {e}")
            raise

    def get_ops_item(self, ops_item_id: str) -> OpsItem:
        """
        Get an OpsItem.

        Args:
            ops_item_id: OpsItem ID

        Returns:
            OpsItem object
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.get_ops_item(OpsItemId=ops_item_id)
            item = response["OpsItem"]
            return OpsItem(
                ops_item_id=item["OpsItemId"],
                title=item["Title"],
                status=OpsItemStatus(item["Status"]),
                priority=item.get("Priority", 3),
                category=item.get("Category"),
                severity=item.get("Severity", "Medium"),
                created_by=item.get("CreatedBy"),
                created_time=datetime.fromisoformat(
                    item["CreatedTime"].replace("Z", "+00:00")
                ) if item.get("CreatedTime") else None,
                modified_time=datetime.fromisoformat(
                    item["ModifiedTime"].replace("Z", "+00:00")
                ) if item.get("ModifiedTime") else None,
                operational_data={
                    k: json.loads(v["Value"])
                    for k, v in item.get("OperationalData", {}).items()
                },
            )
        except ClientError as e:
            logger.error(f"Failed to get ops item {ops_item_id}: {e}")
            raise

    def update_ops_item(
        self,
        ops_item_id: str,
        status: Optional[OpsItemStatus] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        priority: Optional[int] = None,
        operational_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Update an OpsItem.

        Args:
            ops_item_id: OpsItem ID
            status: New status
            title: New title
            description: New description
            priority: New priority
            operational_data: New operational data

        Returns:
            True if updated
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"OpsItemId": ops_item_id}

        if status:
            kwargs["Status"] = status.value
        if title:
            kwargs["Title"] = title
        if description:
            kwargs["Description"] = description
        if priority is not None:
            kwargs["Priority"] = priority
        if operational_data:
            kwargs["OperationalData"] = {
                k: {"Type": "SearchableString", "Value": json.dumps(v)}
                for k, v in operational_data.items()
            }

        try:
            self.ssm.update_ops_item(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to update ops item {ops_item_id}: {e}")
            raise

    def delete_ops_item(self, ops_item_id: str) -> bool:
        """
        Delete an OpsItem.

        Args:
            ops_item_id: OpsItem ID

        Returns:
            True if deleted
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.delete_ops_item(OpsItemId=ops_item_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete ops item {ops_item_id}: {e}")
            raise

    def list_ops_items(
        self,
        filters: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[OpsItem], Optional[str]]:
        """
        List OpsItems.

        Args:
            filters: Filter criteria
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (ops items list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if filters:
            kwargs["OpsFilterList"] = filters
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.describe_ops_items(**kwargs)
            items = [
                OpsItem(
                    ops_item_id=item["OpsItemId"],
                    title=item["Title"],
                    status=OpsItemStatus(item["Status"]),
                    priority=item.get("Priority", 3),
                    category=item.get("Category"),
                    severity=item.get("Severity", "Medium"),
                    created_time=datetime.fromisoformat(
                        item["CreatedTime"].replace("Z", "+00:00")
                    ) if item.get("CreatedTime") else None,
                    modified_time=datetime.fromisoformat(
                        item["ModifiedTime"].replace("Z", "+00:00")
                    ) if item.get("ModifiedTime") else None,
                )
                for item in response["OpsItemSummaries"]
            ]
            return items, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to list ops items: {e}")
            raise

    # =========================================================================
    # 8. Inventory
    # =========================================================================

    def get_inventory(
        self,
        instance_id: Optional[str] = None,
        filters: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[InventoryEntry], Optional[str]]:
        """
        Get inventory data.

        Args:
            instance_id: Filter by instance ID
            filters: Type name filters
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (inventory entries, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if instance_id:
            kwargs["InstanceId"] = instance_id
        if filters:
            kwargs["Filters"] = filters
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.get_inventory(**kwargs)
            entries = [
                InventoryEntry(
                    instance_id=e["Id"],
                    capture_time=datetime.fromisoformat(
                        e["CaptureTime"].replace("Z", "+00:00")
                    ),
                    entries=e.get("Entries", []),
                )
                for e in response["Entities"]
            ]
            return entries, response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to get inventory: {e}")
            raise

    def list_inventory_types(
        self,
        max_results: int = 50,
    ) -> List[Dict]:
        """
        List inventory types.

        Args:
            max_results: Maximum results

        Returns:
            List of inventory type schemas
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.list_inventory_types(MaxResults=max_results)
            return response["schemas"]
        except ClientError as e:
            logger.error(f"Failed to list inventory types: {e}")
            raise

    def put_inventory(
        self,
        instance_id: str,
        entries: List[Dict],
    ) -> bool:
        """
        Put custom inventory entries.

        Args:
            instance_id: Instance ID
            entries: Inventory entries

        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.put_inventory(
                InstanceId=instance_id,
                Entries=entries,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put inventory for {instance_id}: {e}")
            raise

    # =========================================================================
    # 9. Patch Manager
    # =========================================================================

    def describe_patch_baselines(
        self,
        filters: Optional[List[Dict]] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> tuple[List[Dict], Optional[str]]:
        """
        Describe patch baselines.

        Args:
            filters: Filter criteria
            max_results: Maximum results
            next_token: Pagination token

        Returns:
            Tuple of (baselines list, next token)
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"MaxResults": max_results}
        if filters:
            kwargs["Filters"] = filters
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            response = self.ssm.describe_patch_baselines(**kwargs)
            return response["BaselineIdentities"], response.get("NextToken")
        except ClientError as e:
            logger.error(f"Failed to describe patch baselines: {e}")
            raise

    def create_patch_baseline(
        self,
        name: str,
        operating_system: str = "WINDOWS",
        global_filters: Optional[Dict] = None,
        approval_rules: Optional[Dict] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Create a patch baseline.

        Args:
            name: Baseline name
            operating_system: Operating system
            global_filters: Global filters
            approval_rules: Approval rules
            description: Description
            tags: Tags

        Returns:
            Baseline ID
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {
            "Name": name,
            "OperatingSystem": operating_system,
        }

        if global_filters:
            kwargs["GlobalFilters"] = global_filters
        if approval_rules:
            kwargs["ApprovalRules"] = approval_rules
        if description:
            kwargs["Description"] = description
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]

        try:
            response = self.ssm.create_patch_baseline(**kwargs)
            return response["BaselineId"]
        except ClientError as e:
            logger.error(f"Failed to create patch baseline: {e}")
            raise

    def register_patch_baseline_for_patch_group(
        self,
        baseline_id: str,
        patch_group: str,
    ) -> bool:
        """
        Register a patch baseline for a patch group.

        Args:
            baseline_id: Baseline ID
            patch_group: Patch group

        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.ssm.register_patch_baseline_for_patch_group(
                BaselineId=baseline_id,
                PatchGroup=patch_group,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to register patch baseline: {e}")
            raise

    def describe_patch_groups(
        self,
        max_results: int = 50,
    ) -> List[Dict]:
        """
        Describe patch groups.

        Args:
            max_results: Maximum results

        Returns:
            List of patch groups
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.describe_patch_groups(MaxResults=max_results)
            return response["PatchGroups"]
        except ClientError as e:
            logger.error(f"Failed to describe patch groups: {e}")
            raise

    def describe_instance_patch_states(
        self,
        instance_ids: List[str],
    ) -> List[PatchState]:
        """
        Get patch states for instances.

        Args:
            instance_ids: Instance IDs

        Returns:
            List of PatchState objects
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            response = self.ssm.describe_instance_patch_states(
                InstanceIds=instance_ids,
            )
            return [
                PatchState(
                    instance_id=s["InstanceId"],
                    patch_group=s.get("PatchGroup"),
                    baseline_id=s.get("BaselineId"),
                    critical_count=s.get("CriticalCount", 0),
                    high_count=s.get("HighCount", 0),
                    medium_count=s.get("MediumCount", 0),
                    low_count=s.get("LowCount", 0),
                    other_count=s.get("OtherCount", 0),
                    last_operation_time=datetime.fromisoformat(
                        s["LastOperationTime"].replace("Z", "+00:00")
                    ) if s.get("LastOperationTime") else None,
                )
                for s in response["InstancePatchStates"]
            ]
        except ClientError as e:
            logger.error(f"Failed to describe patch states: {e}")
            raise

    def start_patch_baseline_update(
        self,
        instance_ids: List[str],
        baseline_id: Optional[str] = None,
    ) -> bool:
        """
        Start patch baseline update on instances.

        Args:
            instance_ids: Instance IDs
            baseline_id: Optional baseline ID override

        Returns:
            True if started
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"InstanceIds": instance_ids}
        if baseline_id:
            kwargs["BaselineId"] = baseline_id

        try:
            self.ssm.start_patch_baseline_update(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to start patch baseline update: {e}")
            raise

    # =========================================================================
    # 10. CloudWatch Integration
    # =========================================================================

    def put_log_events(
        self,
        log_group: str,
        log_stream: str,
        log_events: List[Dict],
    ) -> bool:
        """
        Put log events to CloudWatch Logs.

        Args:
            log_group: Log group name
            log_stream: Log stream name
            log_events: List of log events [{timestamp, message}]

        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.logs.put_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                logEvents=log_events,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put log events: {e}")
            raise

    def create_log_group(
        self,
        log_group: str,
        retention_days: Optional[int] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Create a CloudWatch log group.

        Args:
            log_group: Log group name
            retention_days: Log retention days
            tags: Tags

        Returns:
            True if created
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        kwargs = {"logGroupName": log_group}
        if retention_days:
            kwargs["retentionInDays"] = retention_days
        if tags:
            kwargs["tags"] = tags

        try:
            self.logs.create_log_group(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to create log group {log_group}: {e}")
            raise

    def create_log_stream(self, log_group: str, log_stream: str) -> bool:
        """
        Create a CloudWatch log stream.

        Args:
            log_group: Log group name
            log_stream: Log stream name

        Returns:
            True if created
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.logs.create_log_stream(
                logGroupName=log_group,
                logStreamName=log_stream,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to create log stream: {e}")
            raise

    def get_command_log(
        self,
        command_id: str,
        log_group: str = "/aws/ssm/AWS-RunCommand",
    ) -> Optional[str]:
        """
        Get command output from CloudWatch Logs.

        Args:
            command_id: Command ID
            log_group: Log group name

        Returns:
            Command output or None
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            log_stream = f"{command_id}/stdout"
            response = self.logs.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
            )
            return "\n".join(e["message"] for e in response["events"])
        except ClientError:
            return None

    def put_metric_data(
        self,
        namespace: str,
        metric_data: List[Dict],
    ) -> bool:
        """
        Put metric data to CloudWatch.

        Args:
            namespace: Metric namespace
            metric_data: List of metric data dictionaries

        Returns:
            True if successful
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            raise

    def record_command_metrics(
        self,
        command_id: str,
        status: CommandStatus,
        instance_count: int,
        success_count: int,
        failure_count: int,
        duration_seconds: float,
    ) -> bool:
        """
        Record command execution metrics to CloudWatch.

        Args:
            command_id: Command ID
            status: Command status
            instance_count: Target instance count
            success_count: Successful instance count
            failure_count: Failed instance count
            duration_seconds: Execution duration

        Returns:
            True if successful
        """
        metric_data = [
            {
                "MetricName": "CommandExecution",
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "CommandId", "Value": command_id},
                    {"Name": "Status", "Value": status.value},
                ],
            },
            {
                "MetricName": "TargetInstanceCount",
                "Value": instance_count,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "CommandId", "Value": command_id},
                ],
            },
            {
                "MetricName": "SuccessCount",
                "Value": success_count,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "CommandId", "Value": command_id},
                ],
            },
            {
                "MetricName": "FailureCount",
                "Value": failure_count,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": "CommandId", "Value": command_id},
                ],
            },
            {
                "MetricName": "ExecutionDuration",
                "Value": duration_seconds,
                "Unit": "Seconds",
                "Dimensions": [
                    {"Name": "CommandId", "Value": command_id},
                ],
            },
        ]

        return self.put_metric_data("AWS/SSM", metric_data)

    def create_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 1,
    ) -> bool:
        """
        Create a CloudWatch alarm.

        Args:
            alarm_name: Alarm name
            metric_name: Metric name
            namespace: Metric namespace
            threshold: Threshold value
            comparison_operator: Comparison operator
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods

        Returns:
            True if created
        """
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SSM integration")

        try:
            self.cloudwatch.put_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace=namespace,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                Period=period,
                EvaluationPeriods=evaluation_periods,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to create alarm {alarm_name}: {e}")
            raise
