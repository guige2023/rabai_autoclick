"""
AWS EventBridge Integration Module for Workflow System

Implements an EventBridgeIntegration class with:
1. Event bus management: Create/manage event buses
2. Rules management: Create/manage event rules
3. Targets: Configure rule targets
4. Events: Put and receive events
5. Archives: Archive events
6. Replays: Replay archived events
7. Schema discovery: Schema registry and discovery
8. API destinations: Configure API destinations
9. Pipes: EventBridge Pipes
10. CloudWatch integration: Metrics and logging

Commit: 'feat(aws-eventbridge): add AWS EventBridge with event bus, rules, targets, events, archives, replay, schema discovery, API destinations, Pipes, CloudWatch'
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
import re

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


class EventBusType(Enum):
    """EventBridge event bus types."""
    DEFAULT = "default"
    CUSTOM = "custom"
    PARTNER = "partner"


class RuleState(Enum):
    """Rule states."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class TargetType(Enum):
    """Target types for rules."""
    LAMBDA = "lambda"
    SNS = "sns"
    SQS = "sqs"
    KINESIS = "kinesis"
    FIREHOSE = "firehose"
    ECS = "ecs"
    STEP_FUNCTIONS = "stepfunctions"
    API_DESTINATION = "api-destination"
    REDSHIFT = "redshift"
    QUEUE = "queue"


class ArchiveState(Enum):
    """Archive states."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class ReplayState(Enum):
    """Replay states."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class SchemaOrigin(Enum):
    """Schema registry types."""
    AWS_EVENTBRIDGE = "AWS_EVENTBRIDGE"
    SNS = "SNS"
    CUSTOM = "CUSTOM"


class PipeState(Enum):
    """Pipe states."""
    CREATING = "CREATING"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"


class PipeSourceType(Enum):
    """Pipe source types."""
    KINESIS = "kinesis"
    SQS = "sqs"
    DYNAMODB = "dynamodb"
    MSK = "msk"
    KAFKA = "kafka"


class PipeTargetType(Enum):
    """Pipe target types."""
    LAMBDA = "lambda"
    SNS = "sns"
    SQS = "sqs"
    KINESIS = "kinesis"
    FIREHOSE = "firehose"
    STEP_FUNCTIONS = "stepfunctions"
    ECS = "ecs"
    API_DESTINATION = "api-destination"


@dataclass
class EventBridgeConfig:
    """Configuration for EventBridge connection."""
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
class EventBus:
    """Represents an EventBridge event bus."""
    arn: str
    name: str
    policy: Optional[str] = None
    event_count: int = 0
    num_rules: int = 0
    creation_time: Optional[datetime] = None


@dataclass
class EventRule:
    """Represents an EventBridge rule."""
    arn: str
    name: str
    event_bus_name: str
    state: RuleState = RuleState.ENABLED
    description: Optional[str] = None
    event_pattern: Optional[str] = None
    schedule_expression: Optional[str] = None
    role_arn: Optional[str] = None
    targets: List[Dict] = field(default_factory=list)
    created_on: Optional[datetime] = None
    last_modified_on: Optional[datetime] = None


@dataclass
class EventTarget:
    """Represents an EventBridge rule target."""
    id: str
    arn: str
    rule_name: str
    event_bus_name: str
    target_type: TargetType = TargetType.LAMBDA
    input_transformer: Optional[Dict] = None
    batch_config: Optional[Dict] = None
    constant_input: Optional[str] = None
    ecs_parameters: Optional[Dict] = None
    kinesis_parameters: Optional[Dict] = None
    redshift_parameters: Optional[Dict] = None
    retry_policy: Optional[Dict] = None
    dead_letter_config: Optional[Dict] = None


@dataclass
class ArchivedEvent:
    """Represents an archived event."""
    archive_name: str
    event_bus_arn: str
    retention_days: int
    state: ArchiveState = ArchiveState.ENABLED
    event_count: int = 0
    size_bytes: int = 0
    first_event_time: Optional[datetime] = None
    last_event_time: Optional[datetime] = None


@dataclass
class Replay:
    """Represents an event replay."""
    arn: str
    name: str
    state: ReplayState = ReplayState.RUNNING
    source_arn: str = ""
    destination_arn: str = ""
    event_count: int = 0
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None


@dataclass
class SchemaRegistry:
    """Represents a schema registry."""
    registry_name: str
    arn: str
    schema_count: int = 0
    description: Optional[str] = None


@dataclass
class Schema:
    """Represents a schema."""
    arn: str
    registry_name: str
    schema_name: str
    type: str = "JSONSchemaDraft4"
    version: str = "1"
    schema_version: str = "1"
    content: str = ""
    description: Optional[str] = None
    last_modified: Optional[datetime] = None


@dataclass
class APIDestination:
    """Represents an API destination."""
    arn: str
    name: str
    api_destination_url: str
    http_method: str = "GET"
    invocation_endpoint: Optional[str] = None
    invocation_rate_limit_per_second: int = 300
    connection_arn: Optional[str] = None


@dataclass
class Connection:
    """Represents an EventBridge connection."""
    arn: str
    name: str
    connection_type: str = "OAUTH"
    auth_parameters: Dict = field(default_factory=dict)
    state: str = "AUTHORIZED"


@dataclass
class Pipe:
    """Represents an EventBridge Pipe."""
    arn: str
    name: str
    source: str
    target: str
    state: PipeState = PipeState.ACTIVE
    description: Optional[str] = None
    enrichment: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EventBridgeIntegration:
    """
    AWS EventBridge Integration for workflow automation.
    
    Provides comprehensive integration with AWS EventBridge including:
    - Event bus management (create, list, delete event buses)
    - Rules management (create, update, delete rules)
    - Target configuration for rules
    - Event publishing and receiving
    - Event archiving and replay
    - Schema discovery and registry
    - API destinations for external integrations
    - EventBridge Pipes for source-to-target workflows
    - CloudWatch metrics and logging
    """
    
    def __init__(self, config: Optional[EventBridgeConfig] = None):
        """
        Initialize EventBridge integration.

        Args:
            config: EventBridge configuration. Uses default if not provided.
        """
        self.config = config or EventBridgeConfig()
        self._client = None
        self._schema_client = None
        self._pipelines_client = None
        self._lock = threading.RLock()
        self._local_event_history: List[Dict] = []
        self._event_handlers: Dict[str, Callable] = {}
        
    @property
    def client(self):
        """Get or create EventBridge client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    kwargs = self._get_client_kwargs()
                    self._client = boto3.client("events", **kwargs)
        return self._client
    
    @property
    def schema_client(self):
        """Get or create EventBridge Schemas client."""
        if self._schema_client is None:
            with self._lock:
                if self._schema_client is None:
                    kwargs = self._get_client_kwargs()
                    self._schema_client = boto3.client("schemas", **kwargs)
        return self._schema_client
    
    @property
    def pipes_client(self):
        """Get or create EventBridge Pipes client."""
        if self._pipelines_client is None:
            with self._lock:
                if self._pipelines_client is None:
                    kwargs = self._get_client_kwargs()
                    self._pipelines_client = boto3.client("eventbridge", **kwargs)
        return self._pipelines_client
    
    def _get_client_kwargs(self) -> Dict[str, Any]:
        """Get common client kwargs."""
        kwargs = {
            "region_name": self.config.region_name,
            "config": self.config.config,
        }
        if self.config.endpoint_url:
            kwargs["endpoint_url"] = self.config.endpoint_url
        if self.config.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
        if self.config.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
        if self.config.aws_session_token:
            kwargs["aws_session_token"] = self.config.aws_session_token
        if self.config.profile_name:
            kwargs["profile_name"] = self.config.profile_name
        return kwargs
    
    def _parse_event_bus_arn(self, arn: str) -> Dict[str, str]:
        """Parse EventBridge event bus ARN."""
        parts = arn.split(":")
        return {
            "partition": parts[1],
            "service": parts[2],
            "region": parts[3],
            "account_id": parts[4],
            "event_bus_name": parts[5] if len(parts) > 5 else parts[4]
        }
    
    # ==================== Event Bus Management ====================
    
    def create_event_bus(
        self,
        name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        kms_key_identifier: Optional[str] = None,
    ) -> EventBus:
        """
        Create an event bus.
        
        Args:
            name: Name of the event bus
            description: Description of the event bus
            tags: Tags to associate with the event bus
            kms_key_identifier: KMS key for encryption
            
        Returns:
            EventBus object with created event bus details
        """
        kwargs = {"Name": name}
        if description:
            kwargs["Description"] = description
        if kms_key_identifier:
            kwargs["KmsKeyIdentifier"] = kms_key_identifier
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        response = self.client.create_event_bus(**kwargs)
        
        return EventBus(
            arn=response["EventBusArn"],
            name=name,
            policy=None,
            creation_time=datetime.now()
        )
    
    def describe_event_bus(self, name: str) -> EventBus:
        """
        Get details of an event bus.
        
        Args:
            name: Name of the event bus
            
        Returns:
            EventBus object with event bus details
        """
        response = self.client.describe_event_bus(Name=name)
        
        return EventBus(
            arn=response["Arn"],
            name=name,
            policy=response.get("Policy"),
            event_count=response.get("EventCount", 0),
            num_rules=response.get("NumberOfRules", 0)
        )
    
    def list_event_buses(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List event buses.
        
        Args:
            prefix: Filter event buses by name prefix
            limit: Maximum number of event buses to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'event_buses' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if prefix:
            kwargs["NamePrefix"] = prefix
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_event_buses(**kwargs)
        
        event_buses = [
            EventBus(
                arn=eb["Arn"],
                name=eb["Name"],
                policy=eb.get("Policy"),
                event_count=eb.get("EventCount", 0),
                num_rules=eb.get("NumberOfRules", 0)
            )
            for eb in response.get("EventBuses", [])
        ]
        
        return {
            "event_buses": event_buses,
            "next_token": response.get("NextToken")
        }
    
    def delete_event_bus(self, name: str) -> bool:
        """
        Delete an event bus.
        
        Args:
            name: Name of the event bus to delete
            
        Returns:
            True if deletion was successful
        """
        self.client.delete_event_bus(Name=name)
        return True
    
    def put_event_bus_policy(
        self,
        event_bus_name: str,
        policy: str,
        statement_id: Optional[str] = None
    ) -> bool:
        """
        Set or update the permission policy for an event bus.
        
        Args:
            event_bus_name: Name of the event bus
            policy: JSON policy document
            statement_id: Optional statement ID for the policy
            
        Returns:
            True if policy was set successfully
        """
        kwargs = {
            "EventBusName": event_bus_name,
            "Policy": policy
        }
        if statement_id:
            kwargs["StatementId"] = statement_id
        
        self.client.put_permission(**kwargs)
        return True
    
    def get_event_bus_policy(self, event_bus_name: str) -> Optional[str]:
        """
        Get the permission policy for an event bus.
        
        Args:
            event_bus_name: Name of the event bus
            
        Returns:
            Policy document as JSON string, or None
        """
        response = self.client.describe_event_bus(Name=event_bus_name)
        return response.get("Policy")
    
    # ==================== Rules Management ====================
    
    def create_rule(
        self,
        name: str,
        event_bus_name: str = "default",
        event_pattern: Optional[str] = None,
        schedule_expression: Optional[str] = None,
        description: Optional[str] = None,
        state: RuleState = RuleState.ENABLED,
        role_arn: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> EventRule:
        """
        Create a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            event_pattern: Event pattern to match events
            schedule_expression: Cron or rate expression for scheduled rules
            description: Description of the rule
            state: Whether the rule is enabled or disabled
            role_arn: IAM role ARN for the rule
            tags: Tags to associate with the rule
            
        Returns:
            EventRule object with created rule details
        """
        kwargs = {
            "Name": name,
            "EventBusName": event_bus_name,
            "State": state.value,
        }
        if event_pattern:
            kwargs["EventPattern"] = event_pattern
        if schedule_expression:
            kwargs["ScheduleExpression"] = schedule_expression
        if description:
            kwargs["Description"] = description
        if role_arn:
            kwargs["RoleArn"] = role_arn
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        response = self.client.put_rule(**kwargs)
        
        return EventRule(
            arn=response["RuleArn"],
            name=name,
            event_bus_name=event_bus_name,
            state=state,
            description=description,
            event_pattern=event_pattern,
            schedule_expression=schedule_expression,
            role_arn=role_arn,
            created_on=datetime.now()
        )
    
    def describe_rule(self, name: str, event_bus_name: str = "default") -> EventRule:
        """
        Get details of a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            
        Returns:
            EventRule object with rule details
        """
        response = self.client.describe_rule(
            Name=name,
            EventBusName=event_bus_name
        )
        
        return EventRule(
            arn=response["Arn"],
            name=response["Name"],
            event_bus_name=event_bus_name,
            state=RuleState(response.get("State", "ENABLED")),
            description=response.get("Description"),
            event_pattern=response.get("EventPattern"),
            schedule_expression=response.get("ScheduleExpression"),
            role_arn=response.get("RoleArn"),
            created_on=response.get("CreationDate"),
            last_modified_on=response.get("LastModifiedTime")
        )
    
    def list_rules(
        self,
        event_bus_name: str = "default",
        prefix: Optional[str] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List rules for an event bus.
        
        Args:
            event_bus_name: Name of the event bus
            prefix: Filter rules by name prefix
            limit: Maximum number of rules to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'rules' list and 'next_token'
        """
        kwargs = {
            "EventBusName": event_bus_name,
            "Limit": limit
        }
        if prefix:
            kwargs["NamePrefix"] = prefix
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_rules(**kwargs)
        
        rules = [
            EventRule(
                arn=r["Arn"],
                name=r["Name"],
                event_bus_name=event_bus_name,
                state=RuleState(r.get("State", "ENABLED")),
                description=r.get("Description"),
                event_pattern=r.get("EventPattern"),
                schedule_expression=r.get("ScheduleExpression"),
                role_arn=r.get("RoleArn"),
                created_on=r.get("CreationDate"),
                last_modified_on=r.get("LastModifiedTime")
            )
            for r in response.get("Rules", [])
        ]
        
        return {
            "rules": rules,
            "next_token": response.get("NextToken")
        }
    
    def update_rule(
        self,
        name: str,
        event_bus_name: str = "default",
        event_pattern: Optional[str] = None,
        schedule_expression: Optional[str] = None,
        description: Optional[str] = None,
        state: Optional[RuleState] = None,
        role_arn: Optional[str] = None,
    ) -> bool:
        """
        Update a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            event_pattern: New event pattern
            schedule_expression: New schedule expression
            description: New description
            state: New state
            role_arn: New IAM role ARN
            
        Returns:
            True if update was successful
        """
        kwargs = {
            "Name": name,
            "EventBusName": event_bus_name,
        }
        if event_pattern is not None:
            kwargs["EventPattern"] = event_pattern
        if schedule_expression is not None:
            kwargs["ScheduleExpression"] = schedule_expression
        if description is not None:
            kwargs["Description"] = description
        if state is not None:
            kwargs["State"] = state.value
        if role_arn is not None:
            kwargs["RoleArn"] = role_arn
        
        self.client.put_rule(**kwargs)
        return True
    
    def delete_rule(self, name: str, event_bus_name: str = "default") -> bool:
        """
        Delete a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            
        Returns:
            True if deletion was successful
        """
        self.client.delete_rule(
            Name=name,
            EventBusName=event_bus_name
        )
        return True
    
    def enable_rule(self, name: str, event_bus_name: str = "default") -> bool:
        """
        Enable a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            
        Returns:
            True if enabling was successful
        """
        self.client.enable_rule(
            Name=name,
            EventBusName=event_bus_name
        )
        return True
    
    def disable_rule(self, name: str, event_bus_name: str = "default") -> bool:
        """
        Disable a rule.
        
        Args:
            name: Name of the rule
            event_bus_name: Name of the event bus
            
        Returns:
            True if disabling was successful
        """
        self.client.disable_rule(
            Name=name,
            EventBusName=event_bus_name
        )
        return True
    
    # ==================== Target Management ====================
    
    def put_target(
        self,
        rule_name: str,
        event_bus_name: str,
        target_id: str,
        target_arn: str,
        target_type: TargetType = TargetType.LAMBDA,
        input_transformer: Optional[Dict] = None,
        input_path: Optional[str] = None,
        input_template: Optional[str] = None,
        constant_input: Optional[str] = None,
        ecs_parameters: Optional[Dict] = None,
        kinesis_parameters: Optional[Dict] = None,
        batch_config: Optional[Dict] = None,
        retry_policy: Optional[Dict] = None,
        dead_letter_config: Optional[Dict] = None,
        run_command_parameters: Optional[Dict] = None,
    ) -> str:
        """
        Add or update a target for a rule.
        
        Args:
            rule_name: Name of the rule
            event_bus_name: Name of the event bus
            target_id: Unique identifier for the target
            target_arn: ARN of the target resource
            target_type: Type of the target
            input_transformer: Input transformer configuration
            input_path: JSON path to extract input
            input_template: Input template string
            constant_input: Constant JSON input
            ecs_parameters: ECS task parameters
            kinesis_parameters: Kinesis stream parameters
            batch_config: Batch job configuration
            retry_policy: Retry policy configuration
            dead_letter_config: Dead letter queue configuration
            run_command_parameters: SSM Run Command parameters
            
        Returns:
            Target ID
        """
        target = {
            "Id": target_id,
            "Arn": target_arn
        }
        
        if target_type == TargetType.LAMBDA:
            target["Type"] = "Lambda"
        elif target_type == TargetType.SQS:
            target["Type"] = "SQS"
        elif target_type == TargetType.SNS:
            target["Type"] = "SNS"
        elif target_type == TargetType.KINESIS:
            target["Type"] = "KinesisStream"
        elif target_type == TargetType.FIREHOSE:
            target["Type"] = "KinesisFirehose"
        elif target_type == TargetType.STEP_FUNCTIONS:
            target["Type"] = "StepFunctions"
        elif target_type == TargetType.ECS:
            target["Type"] = "ECS"
        elif target_type == TargetType.API_DESTINATION:
            target["Type"] = "ApiDestination"
        
        if input_transformer:
            target["InputTransformer"] = input_transformer
        elif input_path:
            target["InputPath"] = input_path
        elif input_template:
            target["InputTemplate"] = input_template
        elif constant_input:
            target["Input"] = constant_input
        
        if ecs_parameters:
            target["EcsParameters"] = ecs_parameters
        if kinesis_parameters:
            target["KinesisParameters"] = kinesis_parameters
        if batch_config:
            target["BatchParameters"] = batch_config
        if retry_policy:
            target["RetryPolicy"] = retry_policy
        if dead_letter_config:
            target["DeadLetterConfig"] = dead_letter_config
        if run_command_parameters:
            target["RunCommandParameters"] = run_command_parameters
        
        self.client.put_targets(
            Rule=rule_name,
            EventBusName=event_bus_name,
            Targets=[target]
        )
        
        return target_id
    
    def remove_target(
        self,
        rule_name: str,
        event_bus_name: str,
        target_ids: List[str],
        force: bool = False
    ) -> bool:
        """
        Remove targets from a rule.
        
        Args:
            rule_name: Name of the rule
            event_bus_name: Name of the event bus
            target_ids: List of target IDs to remove
            force: Force removal even if targets are in use
            
        Returns:
            True if removal was successful
        """
        self.client.remove_targets(
            Rule=rule_name,
            EventBusName=event_bus_name,
            Ids=target_ids,
            Force=force
        )
        return True
    
    def list_targets(
        self,
        rule_name: str,
        event_bus_name: str = "default"
    ) -> List[Dict]:
        """
        List targets for a rule.
        
        Args:
            rule_name: Name of the rule
            event_bus_name: Name of the event bus
            
        Returns:
            List of target configurations
        """
        response = self.client.list_targets_by_rule(
            Rule=rule_name,
            EventBusName=event_bus_name
        )
        
        return response.get("Targets", [])
    
    def list_target_rules(self, target_arn: str) -> List[Dict]:
        """
        List all rules that have a specific target.
        
        Args:
            target_arn: ARN of the target
            
        Returns:
            List of rules associated with the target
        """
        response = self.client.list_rules()
        matching_rules = []
        
        for rule in response.get("Rules", []):
            targets = self.list_targets(
                rule["Name"],
                rule.get("EventBusName", "default")
            )
            for target in targets:
                if target.get("Arn") == target_arn:
                    matching_rules.append({
                        "rule_name": rule["Name"],
                        "event_bus_name": rule.get("EventBusName", "default"),
                        "target_id": target["Id"]
                    })
        
        return matching_rules
    
    # ==================== Event Management ====================
    
    def put_events(
        self,
        events: List[Dict],
        event_bus_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Send events to an event bus.
        
        Args:
            events: List of event dictionaries
            event_bus_name: Name of the event bus
            
        Returns:
            Dict with 'success_count' and 'failed_count'
        """
        entries = []
        for event in events:
            entry = {
                "Source": event.get("source", "custom"),
                "DetailType": event.get("detail-type", event.get("detailType", "custom.event")),
                "Detail": json.dumps(event.get("detail", event)) if isinstance(event.get("detail", event), dict) else str(event.get("detail", event)),
            }
            if event.get("resources"):
                entry["Resources"] = event["resources"]
            
            entries.append(entry)
        
        response = self.client.put_events(
            Entries=entries,
            EndpointId=event_bus_name if event_bus_name != "default" else None
        )
        
        failed_count = len(response.get("FailedEntries", []))
        success_count = len(events) - failed_count
        
        return {
            "success_count": success_count,
            "failed_count": failed_count,
            "entries": response.get("Entries", []),
            "failed_entries": response.get("FailedEntries", [])
        }
    
    def put_event(
        self,
        source: str,
        detail_type: str,
        detail: Dict,
        event_bus_name: str = "default",
        resources: Optional[List[str]] = None,
        trace_header: Optional[str] = None,
    ) -> str:
        """
        Send a single event to an event bus.
        
        Args:
            source: Event source (e.g., 'my-app')
            detail_type: Event detail type (e.g., 'my-event')
            detail: Event detail payload
            event_bus_name: Name of the event bus
            resources: Optional list of resource ARNs
            trace_header: Optional X-Ray trace header
            
        Returns:
            Event ID
        """
        kwargs = {
            "Source": source,
            "DetailType": detail_type,
            "Detail": json.dumps(detail) if isinstance(detail, dict) else detail,
        }
        
        if event_bus_name and event_bus_name != "default":
            kwargs["EventBusName"] = event_bus_name
        if resources:
            kwargs["Resources"] = resources
        if trace_header:
            kwargs["TraceHeader"] = trace_header
        
        response = self.client.put_events(Entries=[kwargs])
        
        entry = response["Entries"][0]
        if entry.get("ErrorCode"):
            raise Exception(f"Failed to put event: {entry.get('ErrorMessage')}")
        
        return entry["EventId"]
    
    def receive_events(
        self,
        event_bus_name: str = "default",
        max_events: int = 10,
        wait_time_seconds: int = 20
    ) -> List[Dict]:
        """
        Receive events from an event bus (using GetEvents).
        
        Args:
            event_bus_name: Name of the event bus
            max_events: Maximum number of events to receive
            wait_time_seconds: How long to wait for events
            
        Returns:
            List of received events
        """
        response = self.client.get_events(
            EndpointId=event_bus_name if event_bus_name != "default" else None,
            Limit=max_events
        )
        
        events = []
        for event in response.get("Events", []):
            events.append({
                "id": event["EventId"],
                "source": event["Source"],
                "detail_type": event["DetailType"],
                "detail": json.loads(event["Detail"]) if event["Detail"] else {},
                "resources": event.get("Resources", []),
                "time": event.get("Time"),
                "region": event.get("AWSRegion")
            })
        
        return events
    
    def create_event_pattern(
        self,
        source: Optional[List[str]] = None,
        detail_type: Optional[List[str]] = None,
        detail: Optional[Dict] = None,
    ) -> str:
        """
        Create an event pattern for rule matching.
        
        Args:
            source: List of allowed event sources
            detail_type: List of allowed detail types
            detail: Nested event detail filters
            
        Returns:
            JSON string of the event pattern
        """
        pattern = {}
        
        if source:
            pattern["source"] = source if isinstance(source, list) else [source]
        if detail_type:
            pattern["detail-type"] = detail_type if isinstance(detail_type, list) else [detail_type]
        if detail:
            pattern["detail"] = detail
        
        return json.dumps(pattern)
    
    def test_event_pattern(
        self,
        event_pattern: str,
        event: Dict
    ) -> bool:
        """
        Test if an event matches an event pattern.
        
        Args:
            event_pattern: JSON event pattern
            event: Event to test
            
        Returns:
            True if event matches the pattern
        """
        response = self.client.test_event_pattern(
            EventPattern=event_pattern,
            Event=json.dumps(event)
        )
        
        return response.get("Result", False)
    
    # ==================== Archive Management ====================
    
    def create_archive(
        self,
        archive_name: str,
        event_bus_arn: str,
        retention_days: int = 0,
        description: Optional[str] = None,
        event_pattern: Optional[str] = None,
    ) -> ArchivedEvent:
        """
        Create an event archive.
        
        Args:
            archive_name: Name of the archive
            event_bus_arn: ARN of the source event bus
            retention_days: Number of days to retain events
            description: Description of the archive
            event_pattern: Filter which events to archive
            
        Returns:
            ArchivedEvent object with archive details
        """
        kwargs = {
            "ArchiveName": archive_name,
            "EventBusArn": event_bus_arn,
        }
        
        if retention_days > 0:
            kwargs["RetentionDays"] = retention_days
        if description:
            kwargs["Description"] = description
        if event_pattern:
            kwargs["EventPattern"] = event_pattern
        
        response = self.client.create_archive(**kwargs)
        
        return ArchivedEvent(
            archive_name=archive_name,
            event_bus_arn=event_bus_arn,
            retention_days=retention_days,
            state=ArchiveState.ENABLED,
            event_count=0,
            first_event_time=datetime.now()
        )
    
    def describe_archive(self, archive_name: str) -> ArchivedEvent:
        """
        Get details of an archive.
        
        Args:
            archive_name: Name of the archive
            
        Returns:
            ArchivedEvent object with archive details
        """
        response = self.client.describe_archive(ArchiveName=archive_name)
        
        return ArchivedEvent(
            archive_name=response["ArchiveName"],
            event_bus_arn=response["EventBusArn"],
            retention_days=response.get("RetentionDays", 0),
            state=ArchiveState(response.get("State", "ENABLED")),
            event_count=response.get("EventCount", 0),
            size_bytes=response.get("SizeBytes", 0),
            first_event_time=response.get("FirstEventTime"),
            last_event_time=response.get("LastEventTime")
        )
    
    def list_archives(
        self,
        event_bus_arn: Optional[str] = None,
        prefix: Optional[str] = None,
        state: Optional[ArchiveState] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List event archives.
        
        Args:
            event_bus_arn: Filter by source event bus ARN
            prefix: Filter archives by name prefix
            state: Filter by archive state
            limit: Maximum number of archives to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'archives' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if event_bus_arn:
            kwargs["EventBusArn"] = event_bus_arn
        if prefix:
            kwargs["NamePrefix"] = prefix
        if state:
            kwargs["State"] = state.value
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_archives(**kwargs)
        
        archives = [
            ArchivedEvent(
                archive_name=a["ArchiveName"],
                event_bus_arn=a["EventBusArn"],
                retention_days=a.get("RetentionDays", 0),
                state=ArchiveState(a.get("State", "ENABLED")),
                event_count=a.get("EventCount", 0),
                size_bytes=a.get("SizeBytes", 0),
                first_event_time=a.get("FirstEventTime"),
                last_event_time=a.get("LastEventTime")
            )
            for a in response.get("Archives", [])
        ]
        
        return {
            "archives": archives,
            "next_token": response.get("NextToken")
        }
    
    def update_archive(
        self,
        archive_name: str,
        retention_days: Optional[int] = None,
        description: Optional[str] = None,
        event_pattern: Optional[str] = None,
    ) -> bool:
        """
        Update an archive configuration.
        
        Args:
            archive_name: Name of the archive
            retention_days: New retention period in days
            description: New description
            event_pattern: New event pattern filter
            
        Returns:
            True if update was successful
        """
        kwargs = {"ArchiveName": archive_name}
        if retention_days is not None:
            kwargs["RetentionDays"] = retention_days
        if description is not None:
            kwargs["Description"] = description
        if event_pattern is not None:
            kwargs["EventPattern"] = event_pattern
        
        self.client.update_archive(**kwargs)
        return True
    
    def delete_archive(self, archive_name: str) -> bool:
        """
        Delete an event archive.
        
        Args:
            archive_name: Name of the archive to delete
            
        Returns:
            True if deletion was successful
        """
        self.client.delete_archive(ArchiveName=archive_name)
        return True
    
    def start_archive_replay(
        self,
        replay_name: str,
        source_arn: str,
        destination_arn: str,
        start_time: datetime,
        end_time: datetime,
        description: Optional[str] = None,
        event_pattern: Optional[str] = None,
    ) -> Replay:
        """
        Start replaying events from an archive.
        
        Args:
            replay_name: Name of the replay
            source_arn: ARN of the archive to replay from
            destination_arn: ARN of the event bus to replay to
            start_time: Start of time range to replay
            end_time: End of time range to replay
            description: Description of the replay
            event_pattern: Filter which events to replay
            
        Returns:
            Replay object with replay details
        """
        kwargs = {
            "ReplayName": replay_name,
            "SourceArn": source_arn,
            "DestinationArn": destination_arn,
            "EventTimeRange": {
                "StartingTime": start_time.isoformat(),
                "EndingTime": end_time.isoformat()
            }
        }
        if description:
            kwargs["Description"] = description
        if event_pattern:
            kwargs["EventPattern"] = event_pattern
        
        response = self.client.start_replay(**kwargs)
        
        return Replay(
            arn=response["ReplayArn"],
            name=replay_name,
            state=ReplayState(response.get("State", "RUNNING")),
            source_arn=source_arn,
            destination_arn=destination_arn,
            started_at=datetime.now()
        )
    
    def describe_replay(self, replay_name: str) -> Replay:
        """
        Get details of a replay.
        
        Args:
            replay_name: Name of the replay
            
        Returns:
            Replay object with replay details
        """
        response = self.client.describe_replay(ReplayName=replay_name)
        
        return Replay(
            arn=response["ReplayArn"],
            name=response["ReplayName"],
            state=ReplayState(response.get("State", "RUNNING")),
            source_arn=response["SourceArn"],
            destination_arn=response["DestinationArn"],
            event_count=response.get("EventCount", 0),
            started_at=response.get("ReplayStartTime"),
            ended_at=response.get("ReplayEndTime")
        )
    
    def list_replays(
        self,
        name_prefix: Optional[str] = None,
        source_arn: Optional[str] = None,
        state: Optional[ReplayState] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List event replays.
        
        Args:
            name_prefix: Filter replays by name prefix
            source_arn: Filter by source archive ARN
            state: Filter by replay state
            limit: Maximum number of replays to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'replays' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if name_prefix:
            kwargs["NamePrefix"] = name_prefix
        if source_arn:
            kwargs["SourceArn"] = source_arn
        if state:
            kwargs["State"] = state.value
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_replays(**kwargs)
        
        replays = [
            Replay(
                arn=r["ReplayArn"],
                name=r["ReplayName"],
                state=ReplayState(r.get("State", "RUNNING")),
                source_arn=r.get("SourceArn", ""),
                destination_arn=r.get("DestinationArn", ""),
                event_count=r.get("EventCount", 0),
                started_at=r.get("ReplayStartTime"),
                ended_at=r.get("ReplayEndTime")
            )
            for r in response.get("Replays", [])
        ]
        
        return {
            "replays": replays,
            "next_token": response.get("NextToken")
        }
    
    def cancel_replay(self, replay_name: str) -> bool:
        """
        Cancel a running replay.
        
        Args:
            replay_name: Name of the replay to cancel
            
        Returns:
            True if cancellation was successful
        """
        self.client.cancel_replay(ReplayName=replay_name)
        return True
    
    # ==================== Schema Discovery ====================
    
    def create_registry(
        self,
        registry_name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> SchemaRegistry:
        """
        Create a schema registry.
        
        Args:
            registry_name: Name of the registry
            description: Description of the registry
            tags: Tags to associate with the registry
            
        Returns:
            SchemaRegistry object with registry details
        """
        kwargs = {"RegistryName": registry_name}
        if description:
            kwargs["Description"] = description
        if tags:
            kwargs["Tags"] = tags
        
        response = self.schema_client.create_registry(**kwargs)
        
        return SchemaRegistry(
            registry_name=registry_name,
            arn=response["RegistryArn"],
            description=description
        )
    
    def describe_registry(self, registry_name: str) -> SchemaRegistry:
        """
        Get details of a schema registry.
        
        Args:
            registry_name: Name of the registry
            
        Returns:
            SchemaRegistry object with registry details
        """
        response = self.schema_client.describe_registry(RegistryName=registry_name)
        
        return SchemaRegistry(
            registry_name=response["RegistryName"],
            arn=response["RegistryArn"],
            schema_count=response.get("SchemaCount", 0),
            description=response.get("Description")
        )
    
    def list_registries(
        self,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List schema registries.
        
        Args:
            limit: Maximum number of registries to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'registries' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.schema_client.list_registries(**kwargs)
        
        registries = [
            SchemaRegistry(
                registry_name=r["RegistryName"],
                arn=r["RegistryArn"],
                schema_count=r.get("SchemaCount", 0),
                description=r.get("Description")
            )
            for r in response.get("Registries", [])
        ]
        
        return {
            "registries": registries,
            "next_token": response.get("NextToken")
        }
    
    def delete_registry(self, registry_name: str) -> bool:
        """
        Delete a schema registry.
        
        Args:
            registry_name: Name of the registry to delete
            
        Returns:
            True if deletion was successful
        """
        self.schema_client.delete_registry(RegistryName=registry_name)
        return True
    
    def create_schema(
        self,
        registry_name: str,
        schema_name: str,
        content: str,
        type: str = "JSONSchemaDraft4",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        schema_version: Optional[str] = None,
    ) -> Schema:
        """
        Create a schema.
        
        Args:
            registry_name: Name of the registry
            schema_name: Name of the schema
            content: Schema content (JSON)
            type: Schema type
            description: Description of the schema
            tags: Tags to associate with the schema
            schema_version: Initial schema version
            
        Returns:
            Schema object with schema details
        """
        kwargs = {
            "RegistryName": registry_name,
            "SchemaName": schema_name,
            "Content": content,
            "Type": type,
        }
        if description:
            kwargs["Description"] = description
        if tags:
            kwargs["Tags"] = tags
        if schema_version:
            kwargs["SchemaVersion"] = schema_version
        
        response = self.schema_client.create_schema(**kwargs)
        
        return Schema(
            arn=response["SchemaArn"],
            registry_name=registry_name,
            schema_name=schema_name,
            type=type,
            version=response.get("Version", "1"),
            schema_version=response.get("SchemaVersion", "1"),
            content=content,
            description=description
        )
    
    def describe_schema(self, registry_name: str, schema_name: str) -> Schema:
        """
        Get details of a schema.
        
        Args:
            registry_name: Name of the registry
            schema_name: Name of the schema
            
        Returns:
            Schema object with schema details
        """
        response = self.schema_client.describe_schema(
            RegistryName=registry_name,
            SchemaName=schema_name
        )
        
        return Schema(
            arn=response["SchemaArn"],
            registry_name=registry_name,
            schema_name=response["SchemaName"],
            type=response.get("Type", "JSONSchemaDraft4"),
            version=response.get("Version", "1"),
            schema_version=response.get("SchemaVersion", "1"),
            content=response.get("Content", ""),
            description=response.get("Description"),
            last_modified=response.get("LastModified")
        )
    
    def list_schemas(
        self,
        registry_name: str,
        prefix: Optional[str] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List schemas in a registry.
        
        Args:
            registry_name: Name of the registry
            prefix: Filter schemas by name prefix
            limit: Maximum number of schemas to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'schemas' list and 'next_token'
        """
        kwargs = {
            "RegistryName": registry_name,
            "Limit": limit
        }
        if prefix:
            kwargs["SchemaNamePrefix"] = prefix
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.schema_client.list_schemas(**kwargs)
        
        schemas = [
            Schema(
                arn=s["SchemaArn"],
                registry_name=registry_name,
                schema_name=s["SchemaName"],
                type=s.get("Type", "JSONSchemaDraft4"),
                version=s.get("Version", "1"),
                schema_version=s.get("SchemaVersion", "1"),
                description=s.get("Description")
            )
            for s in response.get("Schemas", [])
        ]
        
        return {
            "schemas": schemas,
            "next_token": response.get("NextToken")
        }
    
    def get_schema_version(
        self,
        registry_name: str,
        schema_name: str,
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a specific version of a schema.
        
        Args:
            registry_name: Name of the registry
            schema_name: Name of the schema
            version: Schema version (latest if not specified)
            
        Returns:
            Dict with schema content and metadata
        """
        kwargs = {
            "RegistryName": registry_name,
            "SchemaName": schema_name
        }
        if version:
            kwargs["SchemaVersion"] = version
        
        response = self.schema_client.get_schema_version(**kwargs)
        
        return {
            "content": response.get("Content", ""),
            "schema_arn": response.get("SchemaArn"),
            "version": response.get("VersionNumber", response.get("Version", "1")),
            "type": response.get("Type", "JSONSchemaDraft4")
        }
    
    def list_schema_versions(
        self,
        registry_name: str,
        schema_name: str,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List all versions of a schema.
        
        Args:
            registry_name: Name of the registry
            schema_name: Name of the schema
            limit: Maximum number of versions to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'versions' list and 'next_token'
        """
        kwargs = {
            "RegistryName": registry_name,
            "SchemaName": schema_name,
            "Limit": limit
        }
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.schema_client.list_schema_versions(**kwargs)
        
        return {
            "versions": response.get("SchemaVersions", []),
            "next_token": response.get("NextToken")
        }
    
    def delete_schema(self, registry_name: str, schema_name: str) -> bool:
        """
        Delete a schema and all its versions.
        
        Args:
            registry_name: Name of the registry
            schema_name: Name of the schema to delete
            
        Returns:
            True if deletion was successful
        """
        self.schema_client.delete_schema(
            RegistryName=registry_name,
            SchemaName=schema_name
        )
        return True
    
    def discover_schema(
        self,
        event: Dict,
        schema_type: SchemaOrigin = SchemaOrigin.AWS_EVENTBRIDGE
    ) -> str:
        """
        Discover a schema from an event.
        
        Args:
            event: Event to discover schema from
            schema_type: Origin type of the schema
            
        Returns:
            JSON schema content
        """
        response = self.schema_client.discover_schema(
            Event=json.dumps(event),
            Type=schema_type.value
        )
        
        return response.get("Schema", {})
    
    # ==================== API Destinations ====================
    
    def create_connection(
        self,
        name: str,
        connection_type: str = "OAUTH",
        auth_parameters: Optional[Dict] = None,
        description: Optional[str] = None,
        secret_arn: Optional[str] = None,
    ) -> Connection:
        """
        Create a connection for API destinations.
        
        Args:
            name: Name of the connection
            connection_type: Type of connection (OAUTH, API_KEY, BASIC)
            auth_parameters: Authentication parameters
            description: Description of the connection
            secret_arn: ARN of the secret containing credentials
            
        Returns:
            Connection object with connection details
        """
        kwargs = {
            "Name": name,
            "ConnectionType": connection_type,
        }
        if auth_parameters:
            kwargs["AuthParameters"] = auth_parameters
        if description:
            kwargs["Description"] = description
        if secret_arn:
            kwargs["SecretArn"] = secret_arn
        
        response = self.client.create_connection(**kwargs)
        
        return Connection(
            arn=response["ConnectionArn"],
            name=name,
            connection_type=connection_type,
            auth_parameters=auth_parameters or {},
            state=response.get("ConnectionState", "AUTHORIZED")
        )
    
    def describe_connection(self, name: str) -> Connection:
        """
        Get details of a connection.
        
        Args:
            name: Name of the connection
            
        Returns:
            Connection object with connection details
        """
        response = self.client.describe_connection(Name=name)
        
        return Connection(
            arn=response["ConnectionArn"],
            name=response["Name"],
            connection_type=response.get("ConnectionType", "OAUTH"),
            auth_parameters=response.get("AuthParameters", {}),
            state=response.get("ConnectionState", "AUTHORIZED")
        )
    
    def list_connections(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List connections.
        
        Args:
            prefix: Filter connections by name prefix
            limit: Maximum number of connections to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'connections' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if prefix:
            kwargs["NamePrefix"] = prefix
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_connections(**kwargs)
        
        connections = [
            Connection(
                arn=c["ConnectionArn"],
                name=c["Name"],
                connection_type=c.get("ConnectionType", "OAUTH"),
                auth_parameters=c.get("AuthParameters", {}),
                state=c.get("ConnectionState", "AUTHORIZED")
            )
            for c in response.get("Connections", [])
        ]
        
        return {
            "connections": connections,
            "next_token": response.get("NextToken")
        }
    
    def delete_connection(self, name: str) -> bool:
        """
        Delete a connection.
        
        Args:
            name: Name of the connection to delete
            
        Returns:
            True if deletion was successful
        """
        self.client.delete_connection(Name=name)
        return True
    
    def authorize_connection(self, name: str) -> bool:
        """
        Authorize a connection.
        
        Args:
            name: Name of the connection
            
        Returns:
            True if authorization was successful
        """
        self.client.authorize_connection(Name=name)
        return True
    
    def revoke_connection(self, name: str) -> bool:
        """
        Revoke authorization for a connection.
        
        Args:
            name: Name of the connection
            
        Returns:
            True if revocation was successful
        """
        self.client.revoke_connection(Name=name)
        return True
    
    def create_api_destination(
        self,
        name: str,
        api_destination_url: str,
        http_method: str = "GET",
        connection_name: Optional[str] = None,
        invocation_endpoint: Optional[str] = None,
        invocation_rate_limit_per_second: int = 300,
        description: Optional[str] = None,
    ) -> APIDestination:
        """
        Create an API destination.
        
        Args:
            name: Name of the API destination
            api_destination_url: URL of the API endpoint
            http_method: HTTP method to use
            connection_name: Name of the connection to use
            invocation_endpoint: Override endpoint for invocations
            invocation_rate_limit_per_second: Rate limit for invocations
            description: Description of the API destination
            
        Returns:
            APIDestination object with destination details
        """
        kwargs = {
            "Name": name,
            "ApiDestinationUrl": api_destination_url,
            "HttpMethod": http_method,
        }
        if connection_name:
            kwargs["ConnectionArn"] = self._get_connection_arn(connection_name)
        if invocation_endpoint:
            kwargs["InvocationEndpoint"] = invocation_endpoint
        kwargs["InvocationRateLimitPerSecond"] = invocation_rate_limit_per_second
        if description:
            kwargs["Description"] = description
        
        response = self.client.create_api_destination(**kwargs)
        
        return APIDestination(
            arn=response["ApiDestinationArn"],
            name=name,
            api_destination_url=api_destination_url,
            http_method=http_method,
            invocation_endpoint=invocation_endpoint,
            invocation_rate_limit_per_second=invocation_rate_limit_per_second,
            connection_arn=response.get("ConnectionArn")
        )
    
    def _get_connection_arn(self, connection_name: str) -> str:
        """Get connection ARN by name."""
        response = self.client.describe_connection(Name=connection_name)
        return response["ConnectionArn"]
    
    def describe_api_destination(self, name: str) -> APIDestination:
        """
        Get details of an API destination.
        
        Args:
            name: Name of the API destination
            
        Returns:
            APIDestination object with destination details
        """
        response = self.client.describe_api_destination(Name=name)
        
        return APIDestination(
            arn=response["ApiDestinationArn"],
            name=response["Name"],
            api_destination_url=response["ApiDestinationUrl"],
            http_method=response.get("HttpMethod", "GET"),
            invocation_endpoint=response.get("InvocationEndpoint"),
            invocation_rate_limit_per_second=response.get("InvocationRateLimitPerSecond", 300),
            connection_arn=response.get("ConnectionArn")
        )
    
    def list_api_destinations(
        self,
        prefix: Optional[str] = None,
        connection_arn: Optional[str] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List API destinations.
        
        Args:
            prefix: Filter by name prefix
            connection_arn: Filter by connection ARN
            limit: Maximum number of destinations to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'api_destinations' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if prefix:
            kwargs["NamePrefix"] = prefix
        if connection_arn:
            kwargs["ConnectionArn"] = connection_arn
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.client.list_api_destinations(**kwargs)
        
        destinations = [
            APIDestination(
                arn=d["ApiDestinationArn"],
                name=d["Name"],
                api_destination_url=d["ApiDestinationUrl"],
                http_method=d.get("HttpMethod", "GET"),
                invocation_endpoint=d.get("InvocationEndpoint"),
                invocation_rate_limit_per_second=d.get("InvocationRateLimitPerSecond", 300),
                connection_arn=d.get("ConnectionArn")
            )
            for d in response.get("ApiDestinations", [])
        ]
        
        return {
            "api_destinations": destinations,
            "next_token": response.get("NextToken")
        }
    
    def delete_api_destination(self, name: str) -> bool:
        """
        Delete an API destination.
        
        Args:
            name: Name of the API destination to delete
            
        Returns:
            True if deletion was successful
        """
        self.client.delete_api_destination(Name=name)
        return True
    
    # ==================== EventBridge Pipes ====================
    
    def create_pipe(
        self,
        name: str,
        source: str,
        target: str,
        enrichment: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        source_parameters: Optional[Dict] = None,
        enrichment_parameters: Optional[Dict] = None,
        target_parameters: Optional[Dict] = None,
        role_arn: Optional[str] = None,
    ) -> Pipe:
        """
        Create an EventBridge Pipe.
        
        Args:
            name: Name of the pipe
            source: Source resource ARN
            target: Target resource ARN
            enrichment: Enrichment resource ARN
            description: Description of the pipe
            tags: Tags to associate with the pipe
            source_parameters: Source configuration parameters
            enrichment_parameters: Enrichment configuration parameters
            target_parameters: Target configuration parameters
            role_arn: IAM role ARN for the pipe
            
        Returns:
            Pipe object with pipe details
        """
        kwargs = {
            "Name": name,
            "Source": source,
            "Target": target,
            "RoleArn": role_arn,
        }
        if enrichment:
            kwargs["Enrichment"] = enrichment
        if description:
            kwargs["Description"] = description
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        if source_parameters:
            kwargs["SourceParameters"] = source_parameters
        if enrichment_parameters:
            kwargs["EnrichmentParameters"] = enrichment_parameters
        if target_parameters:
            kwargs["TargetParameters"] = target_parameters
        
        response = self.pipes_client.create_pipe(**kwargs)
        
        return Pipe(
            arn=response["PipeArn"],
            name=name,
            source=source,
            target=target,
            enrichment=enrichment,
            description=description,
            state=PipeState(response.get("State", "ACTIVE")),
            created_at=datetime.now()
        )
    
    def describe_pipe(self, name: str) -> Pipe:
        """
        Get details of a pipe.
        
        Args:
            name: Name of the pipe
            
        Returns:
            Pipe object with pipe details
        """
        response = self.pipes_client.describe_pipe(Name=name)
        
        return Pipe(
            arn=response["PipeArn"],
            name=response["Name"],
            source=response["Source"],
            target=response["Target"],
            enrichment=response.get("Enrichment"),
            description=response.get("Description"),
            state=PipeState(response.get("State", "ACTIVE")),
            created_at=response.get("CreationTime"),
            updated_at=response.get("LastModifiedTime")
        )
    
    def list_pipes(
        self,
        prefix: Optional[str] = None,
        source_arn: Optional[str] = None,
        target_arn: Optional[str] = None,
        state: Optional[PipeState] = None,
        limit: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List EventBridge Pipes.
        
        Args:
            prefix: Filter pipes by name prefix
            source_arn: Filter by source ARN
            target_arn: Filter by target ARN
            state: Filter by pipe state
            limit: Maximum number of pipes to return
            next_token: Token for pagination
            
        Returns:
            Dict with 'pipes' list and 'next_token'
        """
        kwargs = {"Limit": limit}
        if prefix:
            kwargs["NamePrefix"] = prefix
        if source_arn:
            kwargs["SourceArn"] = source_arn
        if target_arn:
            kwargs["TargetArn"] = target_arn
        if state:
            kwargs["DesiredState"] = state.value
        if next_token:
            kwargs["NextToken"] = next_token
        
        response = self.pipes_client.list_pipes(**kwargs)
        
        pipes = [
            Pipe(
                arn=p["PipeArn"],
                name=p["Name"],
                source=p["Source"],
                target=p["Target"],
                enrichment=p.get("Enrichment"),
                description=p.get("Description"),
                state=PipeState(p.get("State", "ACTIVE")),
                created_at=p.get("CreationTime"),
                updated_at=p.get("LastModifiedTime")
            )
            for p in response.get("Pipes", [])
        ]
        
        return {
            "pipes": pipes,
            "next_token": response.get("NextToken")
        }
    
    def update_pipe(
        self,
        name: str,
        description: Optional[str] = None,
        enrichment: Optional[str] = None,
        target_parameters: Optional[Dict] = None,
        role_arn: Optional[str] = None,
        source_parameters: Optional[Dict] = None,
        enrichment_parameters: Optional[Dict] = None,
    ) -> bool:
        """
        Update a pipe.
        
        Args:
            name: Name of the pipe
            description: New description
            enrichment: New enrichment resource ARN
            target_parameters: New target parameters
            role_arn: New IAM role ARN
            source_parameters: New source parameters
            enrichment_parameters: New enrichment parameters
            
        Returns:
            True if update was successful
        """
        kwargs = {"Name": name}
        if description is not None:
            kwargs["Description"] = description
        if enrichment is not None:
            kwargs["Enrichment"] = enrichment
        if target_parameters is not None:
            kwargs["TargetParameters"] = target_parameters
        if role_arn is not None:
            kwargs["RoleArn"] = role_arn
        if source_parameters is not None:
            kwargs["SourceParameters"] = source_parameters
        if enrichment_parameters is not None:
            kwargs["EnrichmentParameters"] = enrichment_parameters
        
        self.pipes_client.update_pipe(**kwargs)
        return True
    
    def delete_pipe(self, name: str) -> bool:
        """
        Delete a pipe.
        
        Args:
            name: Name of the pipe to delete
            
        Returns:
            True if deletion was successful
        """
        self.pipes_client.delete_pipe(Name=name)
        return True
    
    def start_pipe(self, name: str) -> bool:
        """
        Start a pipe.
        
        Args:
            name: Name of the pipe to start
            
        Returns:
            True if starting was successful
        """
        self.pipes_client.start_pipe(Name=name)
        return True
    
    def stop_pipe(self, name: str) -> bool:
        """
        Stop a pipe.
        
        Args:
            name: Name of the pipe to stop
            
        Returns:
            True if stopping was successful
        """
        self.pipes_client.stop_pipe(Name=name)
        return True
    
    # ==================== CloudWatch Integration ====================
    
    def get_cloudwatch_metrics(
        self,
        namespace: str = "AWS/Events",
        metric_names: Optional[List[str]] = None,
        dimensions: Optional[Dict[str, str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 300,
        statistics: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metrics for EventBridge.
        
        Args:
            namespace: CloudWatch namespace
            metric_names: List of metric names to retrieve
            dimensions: Dimensions for the metrics
            start_time: Start of time range
            end_time: End of time range
            period: Period in seconds
            statistics: List of statistics to retrieve
            
        Returns:
            Dict with metric data points
        """
        cloudwatch = boto3.client("cloudwatch", **self._get_client_kwargs())
        
        kwargs = {
            "Namespace": namespace,
            "Period": period,
        }
        
        if metric_names:
            kwargs["MetricNames"] = metric_names
        if dimensions:
            kwargs["Dimensions"] = [{"Name": k, "Value": v} for k, v in dimensions.items()]
        if start_time:
            kwargs["StartTime"] = start_time
        if end_time:
            kwargs["EndTime"] = end_time
        if statistics:
            kwargs["Statistics"] = statistics
        
        response = cloudwatch.get_metric_statistics(**kwargs)
        
        return {
            "label": response.get("Label"),
            "datapoints": response.get("Datapoints", []),
            "metric_name": metric_names[0] if metric_names else None
        }
    
    def list_eventbridge_metrics(
        self,
        event_bus_name: str = "default",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, List]:
        """
        List common EventBridge metrics.
        
        Args:
            event_bus_name: Name of the event bus
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Dict with various metric data
        """
        dimensions = [{"Name": "EventBusName", "Value": event_bus_name}]
        
        metrics = {}
        
        incoming = self.get_cloudwatch_metrics(
            namespace="AWS/Events",
            metric_names=["IncomingEvents"],
            dimensions={"EventBusName": event_bus_name},
            start_time=start_time,
            end_time=end_time
        )
        metrics["incoming_events"] = incoming.get("datapoints", [])
        
        matched = self.get_cloudwatch_metrics(
            namespace="AWS/Events",
            metric_names=["Invocations", "TriggeredRules", "MatchedEvents"],
            dimensions={"EventBusName": event_bus_name},
            start_time=start_time,
            end_time=end_time
        )
        metrics["invocations"] = matched.get("datapoints", [])
        
        failed = self.get_cloudwatch_metrics(
            namespace="AWS/Events",
            metric_names=["FailedInvocations", "PutEvents", "PutEventsFailed"],
            dimensions={"EventBusName": event_bus_name},
            start_time=start_time,
            end_time=end_time
        )
        metrics["failed_invocations"] = failed.get("datapoints", [])
        
        return metrics
    
    def create_cloudwatch_dashboard(
        self,
        dashboard_name: str,
        event_bus_name: str = "default"
    ) -> str:
        """
        Create a CloudWatch dashboard for EventBridge monitoring.
        
        Args:
            dashboard_name: Name of the dashboard
            event_bus_name: Name of the event bus to monitor
            
        Returns:
            Dashboard body JSON
        """
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/Events", "IncomingEvents", "EventBusName", event_bus_name],
                            [".", "MatchedEvents", ".", "."],
                            [".", "TriggeredRules", ".", "."]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": self.config.region_name,
                        "title": "EventBridge Events"
                    }
                },
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            ["AWS/Events", "Invocations", "EventBusName", event_bus_name],
                            [".", "FailedInvocations", ".", "."],
                            [".", "PutEvents", ".", "."],
                            [".", "PutEventsFailed", ".", "."]
                        ],
                        "period": 300,
                        "stat": "Sum",
                        "region": self.config.region_name,
                        "title": "EventBridge Invocations"
                    }
                },
                {
                    "type": "log",
                    "properties": {
                        "logGroup": f"/aws/events/{event_bus_name}",
                        "lines": 50,
                        "title": f"EventBridge Logs - {event_bus_name}"
                    }
                }
            ]
        }
        
        return json.dumps(dashboard_body)
    
    def set_cloudwatch_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 1,
        period: int = 300,
        statistic: str = "Sum",
        event_bus_name: str = "default",
        sns_topic_arn: Optional[str] = None,
    ) -> bool:
        """
        Create a CloudWatch alarm for EventBridge metrics.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Name of the metric
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods
            period: Period in seconds
            statistic: Statistic type
            event_bus_name: Name of the event bus
            sns_topic_arn: Optional SNS topic ARN for notifications
            
        Returns:
            True if alarm creation was successful
        """
        cloudwatch = boto3.client("cloudwatch", **self._get_client_kwargs())
        
        kwargs = {
            "AlarmName": alarm_name,
            "MetricName": metric_name,
            "Namespace": "AWS/Events",
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "EvaluationPeriods": evaluation_periods,
            "Period": period,
            "Statistic": statistic,
            "Dimensions": [{"Name": "EventBusName", "Value": event_bus_name}]
        }
        
        if sns_topic_arn:
            kwargs["AlarmActions"] = [sns_topic_arn]
        
        cloudwatch.put_metric_alarm(**kwargs)
        return True
    
    def enable_cloudwatch_logs(
        self,
        event_bus_name: str = "default",
        log_group: Optional[str] = None
    ) -> bool:
        """
        Enable CloudWatch Logs for an event bus.
        
        Args:
            event_bus_name: Name of the event bus
            log_group: Optional custom log group name
            
        Returns:
            True if logging was enabled successfully
        """
        logs_client = boto3.client("logs", **self._get_client_kwargs())
        
        if not log_group:
            log_group = f"/aws/events/{event_bus_name}"
        
        try:
            logs_client.create_log_group(logGroupName=log_group)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass
        
        logs_client.put_resource_policy(
            policyName="EventBridgeLoggingPolicy",
            policyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "EventBridgeLogging",
                        "Effect": "Allow",
                        "Principal": {"Service": "events.amazonaws.com"},
                        "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                        "Resource": f"arn:aws:logs:*:*:log-group:{log_group}:*"
                    }
                ]
            })
        )
        
        self.client.put_logging_config(
            EventBusArn=f"arn:aws:events:{self.config.region_name}::{event_bus_name}",
            DestinationType="CloudWatchLogs",
            LogGroup=log_group
        )
        
        return True
    
    # ==================== Utility Methods ====================
    
    def generate_event_id(self) -> str:
        """Generate a unique event ID."""
        return f"{uuid.uuid4().hex[:8]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:12]}"
    
    def format_event(
        self,
        source: str,
        detail_type: str,
        detail: Dict,
        resources: Optional[List[str]] = None
    ) -> Dict:
        """
        Format an event according to EventBridge structure.
        
        Args:
            source: Event source
            detail_type: Detail type
            detail: Event detail
            resources: Optional resources
            
        Returns:
            Formatted event dictionary
        """
        event = {
            "source": source,
            "detail-type": detail_type,
            "detail": detail,
            "time": datetime.now().isoformat(),
            "id": self.generate_event_id()
        }
        if resources:
            event["resources"] = resources
        return event
    
    def validate_event_pattern(self, pattern: str) -> bool:
        """
        Validate an event pattern JSON structure.
        
        Args:
            pattern: JSON event pattern string
            
        Returns:
            True if pattern is valid
        """
        try:
            parsed = json.loads(pattern)
            if not isinstance(parsed, dict):
                return False
            valid_keys = {"source", "detail-type", "detail", "resources", "time", "id"}
            for key in parsed.keys():
                if key not in valid_keys:
                    return False
            return True
        except json.JSONDecodeError:
            return False
    
    def parse_arn(self, arn: str) -> Dict[str, str]:
        """
        Parse an EventBridge ARN.
        
        Args:
            arn: ARN to parse
            
        Returns:
            Dict with ARN components
        """
        parts = arn.split(":")
        result = {
            "partition": parts[1] if len(parts) > 1 else "",
            "service": parts[2] if len(parts) > 2 else "",
            "region": parts[3] if len(parts) > 3 else "",
            "account_id": parts[4] if len(parts) > 4 else "",
        }
        
        if len(parts) > 5:
            resource_parts = parts[5].split("/")
            result["resource_type"] = resource_parts[0] if len(resource_parts) > 1 else ""
            result["resource_id"] = "/".join(resource_parts[1:]) if len(resource_parts) > 1 else parts[5]
        
        return result
    
    def get_event_bus_arn(self, event_bus_name: str) -> str:
        """
        Get the ARN for an event bus.
        
        Args:
            event_bus_name: Name of the event bus
            
        Returns:
            Event bus ARN
        """
        sts_client = boto3.client("sts", **self._get_client_kwargs())
        account_id = sts_client.get_caller_identity()["Account"]
        return f"arn:aws:events:{self.config.region_name}:{account_id}:event-bus/{event_bus_name}"
    
    def tag_resource(
        self,
        resource_arn: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Tag an EventBridge resource.
        
        Args:
            resource_arn: ARN of the resource
            tags: Tags to apply
            
        Returns:
            True if tagging was successful
        """
        self.client.tag_resource(
            ResourceARN=resource_arn,
            Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
        )
        return True
    
    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: List[str]
    ) -> bool:
        """
        Remove tags from an EventBridge resource.
        
        Args:
            resource_arn: ARN of the resource
            tag_keys: Keys of tags to remove
            
        Returns:
            True if untagging was successful
        """
        self.client.untag_resource(
            ResourceARN=resource_arn,
            TagKeys=tag_keys
        )
        return True
    
    def list_tags_for_resource(self, resource_arn: str) -> Dict[str, str]:
        """
        List tags for an EventBridge resource.
        
        Args:
            resource_arn: ARN of the resource
            
        Returns:
            Dict of tag key-value pairs
        """
        response = self.client.list_tags_for_resource(ResourceARN=resource_arn)
        return {t["Key"]: t["Value"] for t in response.get("Tags", [])}
