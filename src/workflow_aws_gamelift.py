"""
AWS GameLift Integration Module for Workflow System

Implements a GameLiftIntegration class with:
1. Fleet management: Create/manage game fleets
2. Build management: Manage game builds
3. Game sessions: Manage game sessions
4. Player sessions: Manage player sessions
5. Aliases: Fleet aliases
6. Locations: Multi-location deployments
7. Game server groups: Game server groups
8. Queues: Placement queues
9. Matchmaking: FlexMatch matchmaking
10. CloudWatch integration: Fleet and matchmaking metrics

Commit: 'feat(aws-gamelift): add Amazon GameLift with fleet management, builds, game sessions, player sessions, aliases, locations, game server groups, queues, FlexMatch matchmaking, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
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


class FleetType(Enum):
    """GameLift fleet types."""
    ON_DEMAND = "ON_DEMAND"
    SPOT = "SPOT"


class OperatingSystem(Enum):
    """GameLift operating systems."""
    WINDOWS_2012 = "WINDOWS_2012"
    WINDOWS_2016 = "WINDOWS_2016"
    AMAZON_LINUX = "AMAZON_LINUX"
    AMAZON_LINUX_2 = "AMAZON_LINUX_2"


class BuildStatus(Enum):
    """GameLift build statuses."""
    INITIALIZED = "INITIALIZED"
    BUILDING = "BUILDING"
    BUILD_COMPLETE = "BUILD_COMPLETE"
    BUILD_FAILED = "BUILD_FAILED"
    DELETED = "DELETED"


class FleetStatus(Enum):
    """GameLift fleet statuses."""
    NEW = "NEW"
    DOWNLOADING = "DOWNLOADING"
    VALIDATING = "VALIDATING"
    BUILDING = "BUILDING"
    ACTIVATING = "ACTIVATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    DELETED = "DELETED"
    ERROR = "ERROR"


class GameSessionStatus(Enum):
    """Game session statuses."""
    ACTIVE = "ACTIVE"
    ACTIVATING = "ACTIVATING"
    TERMINATED = "TERMINATED"
    TERMINATING = "TERMINATING"
    CREATING = "CREATING"
    CREATED = "CREATED"


class PlayerSessionStatus(Enum):
    """Player session statuses."""
    ACTIVE = "ACTIVE"
    TERMINATED = "TERMINATED"
    RESERVED = "RESERVED"


class AliasType(Enum):
    """Alias types."""
    SIMPLE = "SIMPLE"
    TERMINAL = "TERMINAL"


class MetricType(Enum):
    """CloudWatch metric types for GameLift."""
    ACTIVE_SERVER_PROCESSES = "ActiveServerProcesses"
    ACTIVE_GAME_SESSIONS = "ActiveGameSessions"
    AVAILABLE_SERVER_PROCESSES = "AvailableServerProcesses"
    CURRENT_PLAYER_SESSIONS = "CurrentPlayerSessions"
    MAX_ACTIVE_SERVER_PROCESSES = "MaxActiveServerProcesses"
    PLACEMENTS_CANCELLED = "PlacementsCancelled"
    PLACEMENTS_FAILED = "PlacementsFailed"
    PLACEMENTS_STARTED = "PlacementsStarted"
    PLACEMENTS_SUCCEEDED = "PlacementsSucceeded"
    QUEUE_DEPTH = "QueueDepth"
    MATCHMAKING_CONFIGURATIONS_ACTIVE = "MatchmakingConfigurationsActive"
    MATCHMAKING_GAMES_STARTED = "MatchmakingGamesStarted"
    MATCHMAKING_SETUP_TIME = "MatchmakingSetupTime"
    MATCHMAKING_TICKETS = "MatchmakingTickets"


@dataclass
class GameLiftConfig:
    """GameLift configuration."""
    region: str = "us-west-2"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    endpoint_url: Optional[str] = None
    verify: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None
    game_session_termination_time: int = 300
    max_concurrent_game_session_activations: int = 2147483647
    game_session_creation_retries: int = 5
    game_session_creation_timeout: int = 600
    enable_auto_scaling: bool = False
    backup_region: Optional[str] = None
    fallback_regions: List[str] = field(default_factory=list)


class Fleet:
    """Represents a GameLift fleet."""
    
    def __init__(
        self,
        fleet_id: str,
        fleet_name: str,
        fleet_type: FleetType = FleetType.ON_DEMAND,
        operating_system: OperatingSystem = OperatingSystem.AMAZON_LINUX_2,
        status: FleetStatus = FleetStatus.NEW,
        build_id: Optional[str] = None,
        build_name: Optional[str] = None,
        server_executable: Optional[str] = None,
        launch_parameters: Optional[str] = None,
        instance_type: Optional[str] = None,
        desired_ec2_instances: int = 0,
        min_size: int = 0,
        max_size: int = 10,
        locations: Optional[List[str]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.fleet_id = fleet_id
        self.fleet_name = fleet_name
        self.fleet_type = fleet_type
        self.operating_system = operating_system
        self.status = status
        self.build_id = build_id
        self.build_name = build_name
        self.server_executable = server_executable
        self.launch_parameters = launch_parameters
        self.instance_type = instance_type
        self.desired_ec2_instances = desired_ec2_instances
        self.min_size = min_size
        self.max_size = max_size
        self.locations = locations or ["us-west-2"]
        self.metrics = metrics or {}
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "FleetId": self.fleet_id,
            "FleetName": self.fleet_name,
            "FleetType": self.fleet_type.value,
            "OperatingSystem": self.operating_system.value,
            "Status": self.status.value,
            "BuildId": self.build_id,
            "ServerExecutable": self.server_executable,
            "LaunchParameters": self.launch_parameters,
            "InstanceType": self.instance_type,
            "DesiredEC2Instances": self.desired_ec2_instances,
            "Locations": self.locations,
            "Metrics": self.metrics,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "UpdatedAt": self.updated_at.isoformat() if self.updated_at else None
        }


class Build:
    """Represents a GameLift build."""
    
    def __init__(
        self,
        build_id: str,
        build_name: Optional[str] = None,
        build_version: Optional[str] = None,
        status: BuildStatus = BuildStatus.INITIALIZED,
        operating_system: Optional[OperatingSystem] = None,
        storage_location: Optional[Dict[str, Any]] = None,
        total_server_count: int = 0,
        server_build: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.build_id = build_id
        self.build_name = build_name
        self.build_version = build_version
        self.status = status
        self.operating_system = operating_system
        self.storage_location = storage_location
        self.total_server_count = total_server_count
        self.server_build = server_build
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "BuildId": self.build_id,
            "BuildName": self.build_name,
            "BuildVersion": self.build_version,
            "Status": self.status.value,
            "OperatingSystem": self.operating_system.value if self.operating_system else None,
            "StorageLocation": self.storage_location,
            "TotalServerCount": self.total_server_count,
            "ServerBuild": self.server_build,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "UpdatedAt": self.updated_at.isoformat() if self.updated_at else None
        }


class GameSession:
    """Represents a GameLift game session."""
    
    def __init__(
        self,
        game_session_id: str,
        fleet_id: str,
        fleet_name: Optional[str] = None,
        name: Optional[str] = None,
        status: GameSessionStatus = GameSessionStatus.CREATING,
        maximum_player_session_count: int = 10,
        current_player_session_count: int = 0,
        location: Optional[str] = None,
        dns_name: Optional[str] = None,
        ip_address: Optional[str] = None,
        port: int = 1935,
        game_properties: Optional[Dict[str, str]] = None,
        game_session_data: Optional[str] = None,
        matchmaker_data: Optional[str] = None,
        created_at: Optional[datetime] = None,
        creator_id: Optional[str] = None
    ):
        self.game_session_id = game_session_id
        self.fleet_id = fleet_id
        self.fleet_name = fleet_name
        self.name = name
        self.status = status
        self.maximum_player_session_count = maximum_player_session_count
        self.current_player_session_count = current_player_session_count
        self.location = location
        self.dns_name = dns_name
        self.ip_address = ip_address
        self.port = port
        self.game_properties = game_properties or {}
        self.game_session_data = game_session_data
        self.matchmaker_data = matchmaker_data
        self.created_at = created_at or datetime.now()
        self.creator_id = creator_id
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "GameSessionId": self.game_session_id,
            "FleetId": self.fleet_id,
            "FleetName": self.fleet_name,
            "Name": self.name,
            "Status": self.status.value,
            "MaximumPlayerSessionCount": self.maximum_player_session_count,
            "CurrentPlayerSessionCount": self.current_player_session_count,
            "Location": self.location,
            "DnsName": self.dns_name,
            "IpAddress": self.ip_address,
            "Port": self.port,
            "GameProperties": self.game_properties,
            "GameSessionData": self.game_session_data,
            "MatchmakerData": self.matchmaker_data,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "CreatorId": self.creator_id
        }


class PlayerSession:
    """Represents a GameLift player session."""
    
    def __init__(
        self,
        player_session_id: str,
        player_id: str,
        game_session_id: str,
        fleet_id: str,
        status: PlayerSessionStatus = PlayerSessionStatus.RESERVED,
        ip_address: Optional[str] = None,
        dns_name: Optional[str] = None,
        port: Optional[int] = None,
        player_data: Optional[str] = None,
        created_at: Optional[datetime] = None,
        termination_time: Optional[datetime] = None
    ):
        self.player_session_id = player_session_id
        self.player_id = player_id
        self.game_session_id = game_session_id
        self.fleet_id = fleet_id
        self.status = status
        self.ip_address = ip_address
        self.dns_name = dns_name
        self.port = port
        self.player_data = player_data
        self.created_at = created_at or datetime.now()
        self.termination_time = termination_time
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "PlayerSessionId": self.player_session_id,
            "PlayerId": self.player_id,
            "GameSessionId": self.game_session_id,
            "FleetId": self.fleet_id,
            "Status": self.status.value,
            "IpAddress": self.ip_address,
            "DnsName": self.dns_name,
            "Port": self.port,
            "PlayerData": self.player_data,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "TerminationTime": self.termination_time.isoformat() if self.termination_time else None
        }


class Alias:
    """Represents a GameLift alias."""
    
    def __init__(
        self,
        alias_id: str,
        name: str,
        alias_type: AliasType = AliasType.SIMPLE,
        description: Optional[str] = None,
        fleet_id: Optional[str] = None,
        fleet_name: Optional[str] = None,
        routing_strategy: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.alias_id = alias_id
        self.name = name
        self.alias_type = alias_type
        self.description = description
        self.fleet_id = fleet_id
        self.fleet_name = fleet_name
        self.routing_strategy = routing_strategy or {}
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "AliasId": self.alias_id,
            "Name": self.name,
            "AliasType": self.alias_type.value,
            "Description": self.description,
            "FleetId": self.fleet_id,
            "FleetName": self.fleet_name,
            "RoutingStrategy": self.routing_strategy,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "UpdatedAt": self.updated_at.isoformat() if self.updated_at else None
        }


class GameServerGroup:
    """Represents an GameLift game server group."""
    
    def __init__(
        self,
        game_server_group_name: str,
        game_server_group_arn: str,
        fleet_id: Optional[str] = None,
        status: Optional[str] = None,
        instance_type: Optional[str] = None,
        ec2_instance_ids: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.game_server_group_name = game_server_group_name
        self.game_server_group_arn = game_server_group_arn
        self.fleet_id = fleet_id
        self.status = status
        self.instance_type = instance_type
        self.ec2_instance_ids = ec2_instance_ids or []
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "GameServerGroupName": self.game_server_group_name,
            "GameServerGroupArn": self.game_server_group_arn,
            "FleetId": self.fleet_id,
            "Status": self.status,
            "InstanceType": self.instance_type,
            "Ec2InstanceIds": self.ec2_instance_ids,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "UpdatedAt": self.updated_at.isoformat() if self.updated_at else None
        }


class PlacementQueue:
    """Represents a GameLift placement queue."""
    
    def __init__(
        self,
        queue_name: str,
        queue_arn: str,
        destinations: Optional[List[Dict[str, Any]]] = None,
        player_latency_policies: Optional[List[Dict[str, Any]]] = None,
        priority_configurations: Optional[List[Dict[str, Any]]] = None,
        custom_event_data: Optional[str] = None,
        filter_configuration: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None
    ):
        self.queue_name = queue_name
        self.queue_arn = queue_arn
        self.destinations = destinations or []
        self.player_latency_policies = player_latency_policies or []
        self.priority_configurations = priority_configurations or []
        self.custom_event_data = custom_event_data
        self.filter_configuration = filter_configuration
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "QueueName": self.queue_name,
            "QueueArn": self.queue_arn,
            "Destinations": self.destinations,
            "PlayerLatencyPolicies": self.player_latency_policies,
            "PriorityConfigurations": self.priority_configurations,
            "CustomEventData": self.custom_event_data,
            "FilterConfiguration": self.filter_configuration,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None
        }


class MatchmakingConfiguration:
    """Represents a FlexMatch matchmaking configuration."""
    
    def __init__(
        self,
        name: str,
        configuration_arn: str,
        description: Optional[str] = None,
        game_session_queue_arn: Optional[str] = None,
        request_timeout_seconds: int = 300,
        acceptance_timeout_seconds: int = 300,
        acceptance_required: bool = True,
        rule_set_name: Optional[str] = None,
        rule_set_arn: Optional[str] = None,
        backfill_mode: Optional[str] = None,
        notification_setting: Optional[Dict[str, Any]] = None,
        additional_player_count: Optional[int] = None,
        flex_match_mode: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.name = name
        self.configuration_arn = configuration_arn
        self.description = description
        self.game_session_queue_arn = game_session_queue_arn
        self.request_timeout_seconds = request_timeout_seconds
        self.acceptance_timeout_seconds = acceptance_timeout_seconds
        self.acceptance_required = acceptance_required
        self.rule_set_name = rule_set_name
        self.rule_set_arn = rule_set_arn
        self.backfill_mode = backfill_mode
        self.notification_setting = notification_setting
        self.additional_player_count = additional_player_count
        self.flex_match_mode = flex_match_mode
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "ConfigurationArn": self.configuration_arn,
            "Description": self.description,
            "GameSessionQueueArn": self.game_session_queue_arn,
            "RequestTimeoutSeconds": self.request_timeout_seconds,
            "AcceptanceTimeoutSeconds": self.acceptance_timeout_seconds,
            "AcceptanceRequired": self.acceptance_required,
            "RuleSetName": self.rule_set_name,
            "RuleSetArn": self.rule_set_arn,
            "BackfillMode": self.backfill_mode,
            "NotificationSetting": self.notification_setting,
            "AdditionalPlayerCount": self.additional_player_count,
            "FlexMatchMode": self.flex_match_mode,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None,
            "UpdatedAt": self.updated_at.isoformat() if self.updated_at else None
        }


class MatchmakingRuleSet:
    """Represents a FlexMatch matchmaking rule set."""
    
    def __init__(
        self,
        rule_set_name: str,
        rule_set_arn: str,
        rule_set_body: str,
        tags: Optional[Dict[str, str]] = None,
        created_at: Optional[datetime] = None
    ):
        self.rule_set_name = rule_set_name
        self.rule_set_arn = rule_set_arn
        self.rule_set_body = rule_set_body
        self.tags = tags or {}
        self.created_at = created_at or datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "RuleSetName": self.rule_set_name,
            "RuleSetArn": self.rule_set_arn,
            "RuleSetBody": self.rule_set_body,
            "Tags": self.tags,
            "CreatedAt": self.created_at.isoformat() if self.created_at else None
        }


class MatchmakingTicket:
    """Represents a matchmaking ticket."""
    
    def __init__(
        self,
        ticket_id: str,
        configuration_name: str,
        status: str = "SEARCHING",
        players: Optional[List[Dict[str, Any]]] = None,
        game_session_connection_info: Optional[Dict[str, Any]] = None,
        estimated_wait_time: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ):
        self.ticket_id = ticket_id
        self.configuration_name = configuration_name
        self.status = status
        self.players = players or []
        self.game_session_connection_info = game_session_connection_info
        self.estimated_wait_time = estimated_wait_time
        self.start_time = start_time or datetime.now()
        self.end_time = end_time
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "TicketId": self.ticket_id,
            "ConfigurationName": self.configuration_name,
            "Status": self.status,
            "Players": self.players,
            "GameSessionConnectionInfo": self.game_session_connection_info,
            "EstimatedWaitTime": self.estimated_wait_time,
            "StartTime": self.start_time.isoformat() if self.start_time else None,
            "EndTime": self.end_time.isoformat() if self.end_time else None
        }


class GameLiftIntegration:
    """
    GameLift Integration class providing:
    1. Fleet management: Create/manage game fleets
    2. Build management: Manage game builds
    3. Game sessions: Manage game sessions
    4. Player sessions: Manage player sessions
    5. Aliases: Fleet aliases
    6. Locations: Multi-location deployments
    7. Game server groups: Game server groups
    8. Queues: Placement queues
    9. Matchmaking: FlexMatch matchmaking
    10. CloudWatch integration: Fleet and matchmaking metrics
    """
    
    def __init__(self, config: Optional[GameLiftConfig] = None):
        """Initialize GameLift integration."""
        self.config = config or GameLiftConfig()
        self.client = None
        self.cloudwatch_client = None
        self.resource_groups_client = None
        self._initialize_clients()
        self._fleets: Dict[str, Fleet] = {}
        self._builds: Dict[str, Build] = {}
        self._game_sessions: Dict[str, GameSession] = {}
        self._player_sessions: Dict[str, PlayerSession] = {}
        self._aliases: Dict[str, Alias] = {}
        self._game_server_groups: Dict[str, GameServerGroup] = {}
        self._queues: Dict[str, PlacementQueue] = {}
        self._matchmaking_configurations: Dict[str, MatchmakingConfiguration] = {}
        self._matchmaking_rule_sets: Dict[str, MatchmakingRuleSet] = {}
        self._matchmaking_tickets: Dict[str, MatchmakingTicket] = {}
        self._lock = threading.RLock()
        self._metrics_cache: Dict[str, Dict[str, Any]] = {}
        self._metrics_cache_time: Dict[str, datetime] = {}
    
    def _initialize_clients(self):
        """Initialize AWS clients."""
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available. GameLift operations will use mock mode.")
            return
        
        client_kwargs = {
            "region_name": self.config.region
        }
        
        if self.config.aws_access_key_id:
            client_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
        if self.config.aws_secret_access_key:
            client_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
        if self.config.aws_session_token:
            client_kwargs["aws_session_token"] = self.config.aws_session_token
        if self.config.endpoint_url:
            client_kwargs["endpoint_url"] = self.config.endpoint_url
        if self.config.verify is not None:
            client_kwargs["verify"] = self.config.verify
        
        try:
            self.client = boto3.client("gamelift", **client_kwargs)
            self.cloudwatch_client = boto3.client("cloudwatch", **client_kwargs)
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to initialize GameLift client: {e}")
            self.client = None
            self.cloudwatch_client = None
    
    def _generate_id(self, prefix: str) -> str:
        """Generate a unique ID."""
        return f"{prefix}-{uuid.uuid4().hex[:12]}"
    
    def _get_current_time(self) -> datetime:
        """Get current timestamp."""
        return datetime.now()
    
    def _update_fleet_metrics(self, fleet_id: str):
        """Update cached metrics for a fleet."""
        if not self.client:
            return
        
        try:
            response = self.client.describe_fleet_attributes(FleetIds=[fleet_id])
            if response.get("Fleets"):
                fleet_data = response["Fleets"][0]
                self._metrics_cache[fleet_id] = {
                    "ActiveGameSessions": fleet_data.get("FleetConfig", {}).get("Metrics", []),
                    "ActiveServerProcesses": 0,
                    "CurrentPlayerSessions": 0,
                    "AvailableServerProcesses": 0
                }
                self._metrics_cache_time[fleet_id] = self._get_current_time()
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to update fleet metrics: {e}")
    
    # =========================================================================
    # FLEET MANAGEMENT
    # =========================================================================
    
    def create_fleet(
        self,
        name: str,
        build_id: str,
        fleet_type: FleetType = FleetType.ON_DEMAND,
        operating_system: OperatingSystem = OperatingSystem.AMAZON_LINUX_2,
        instance_type: str = "c5.large",
        server_executable: Optional[str] = None,
        launch_parameters: Optional[str] = None,
        locations: Optional[List[str]] = None,
        min_size: int = 0,
        max_size: int = 10,
        tags: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Fleet:
        """
        Create a new GameLift fleet.
        
        Args:
            name: Fleet name
            build_id: Build ID to deploy
            fleet_type: ON_DEMAND or SPOT fleet
            operating_system: Operating system for instances
            instance_type: EC2 instance type
            server_executable: Path to server executable
            launch_parameters: Additional launch parameters
            locations: List of locations for multi-location deployment
            min_size: Minimum fleet size
            max_size: Maximum fleet size
            tags: Resource tags
            **kwargs: Additional parameters
        
        Returns:
            Created Fleet object
        """
        fleet_id = self._generate_id("fleet")
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "Name": name,
                        "FleetType": fleet_type.value,
                        "ComputeType": "ON_DEMAND",
                        "RuntimeConfiguration": {
                            "ServerProcesses": [
                                {
                                    "LaunchPath": server_executable or "/local/game/server",
                                    "ConcurrentExecutions": 100,
                                    "Parameters": launch_parameters or ""
                                }
                            ]
                        },
                        "LocationsConfig": {
                            loc: {"MinSize": min_size, "MaxSize": max_size}
                            for loc in (locations or [self.config.region])
                        },
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    
                    if instance_type:
                        params["InstanceType"] = instance_type
                    
                    response = self.client.create_fleet(**params)
                    fleet_data = response.get("FleetAttributes", {})
                    fleet_id = response.get("FleetId", fleet_id)
                    
                    fleet = Fleet(
                        fleet_id=fleet_id,
                        fleet_name=name,
                        fleet_type=fleet_type,
                        operating_system=operating_system,
                        status=FleetStatus.NEW,
                        build_id=build_id,
                        server_executable=server_executable,
                        launch_parameters=launch_parameters,
                        instance_type=instance_type,
                        locations=locations or [self.config.region],
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create fleet via API: {e}")
                    fleet = Fleet(
                        fleet_id=fleet_id,
                        fleet_name=name,
                        fleet_type=fleet_type,
                        operating_system=operating_system,
                        status=FleetStatus.ACTIVE,
                        build_id=build_id,
                        server_executable=server_executable,
                        launch_parameters=launch_parameters,
                        instance_type=instance_type,
                        locations=locations or [self.config.region],
                        tags=tags
                    )
            else:
                fleet = Fleet(
                    fleet_id=fleet_id,
                    fleet_name=name,
                    fleet_type=fleet_type,
                    operating_system=operating_system,
                    status=FleetStatus.ACTIVE,
                    build_id=build_id,
                    server_executable=server_executable,
                    launch_parameters=launch_parameters,
                    instance_type=instance_type,
                    locations=locations or [self.config.region],
                    tags=tags
                )
            
            self._fleets[fleet_id] = fleet
            return fleet
    
    def get_fleet(self, fleet_id: str) -> Optional[Fleet]:
        """Get fleet by ID."""
        return self._fleets.get(fleet_id)
    
    def list_fleets(self) -> List[Fleet]:
        """List all fleets."""
        if self.client:
            try:
                response = self.client.list_fleets()
                fleet_ids = response.get("FleetIds", [])
                
                if fleet_ids:
                    attr_response = self.client.describe_fleet_attributes(FleetIds=fleet_ids)
                    for fleet_data in attr_response.get("Fleets", []):
                        fid = fleet_data.get("FleetId")
                        if fid and fid not in self._fleets:
                            self._fleets[fid] = Fleet(
                                fleet_id=fid,
                                fleet_name=fleet_data.get("Name", ""),
                                fleet_type=FleetType(fleet_data.get("FleetType", "ON_DEMAND")),
                                status=FleetStatus(fleet_data.get("Status", "NEW")),
                                tags={t["Key"]: t["Value"] for t in fleet_data.get("Tags", [])}
                            )
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list fleets: {e}")
        
        return list(self._fleets.values())
    
    def delete_fleet(self, fleet_id: str) -> bool:
        """Delete a fleet."""
        with self._lock:
            if self.client:
                try:
                    self.client.delete_fleet(FleetId=fleet_id)
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to delete fleet: {e}")
                    return False
            
            if fleet_id in self._fleets:
                del self._fleets[fleet_id]
            return True
    
    def update_fleet_capacity(
        self,
        fleet_id: str,
        desired_instances: int,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None
    ) -> bool:
        """Update fleet capacity."""
        if fleet_id not in self._fleets:
            return False
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "FleetId": fleet_id,
                        "DesiredEC2Instances": desired_instances
                    }
                    if min_size is not None:
                        params["MinSize"] = min_size
                    if max_size is not None:
                        params["MaxSize"] = max_size
                    
                    self.client.update_fleet_capacity(**params)
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to update fleet capacity: {e}")
                    return False
            
            self._fleets[fleet_id].desired_ec2_instances = desired_instances
            if min_size is not None:
                self._fleets[fleet_id].min_size = min_size
            if max_size is not None:
                self._fleets[fleet_id].max_size = max_size
            return True
    
    def get_fleet_metrics(self, fleet_id: str) -> Dict[str, Any]:
        """Get fleet metrics."""
        self._update_fleet_metrics(fleet_id)
        return self._metrics_cache.get(fleet_id, {})
    
    def fleet_exists(self, fleet_id: str) -> bool:
        """Check if fleet exists."""
        return fleet_id in self._fleets
    
    # =========================================================================
    # BUILD MANAGEMENT
    # =========================================================================
    
    def create_build(
        self,
        name: Optional[str] = None,
        build_version: Optional[str] = None,
        operating_system: OperatingSystem = OperatingSystem.AMAZON_LINUX_2,
        tags: Optional[Dict[str, str]] = None
    ) -> Build:
        """
        Create a new GameLift build.
        
        Args:
            name: Build name
            build_version: Build version
            operating_system: Operating system
            tags: Resource tags
        
        Returns:
            Created Build object
        """
        build_id = self._generate_id("build")
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "OperatingSystem": operating_system.value,
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    if name:
                        params["Name"] = name
                    if build_version:
                        params["BuildVersion"] = build_version
                    
                    response = self.client.create_build(**params)
                    build_data = response.get("Build", {})
                    build_id = build_data.get("BuildId", build_id)
                    
                    build = Build(
                        build_id=build_id,
                        build_name=name,
                        build_version=build_version,
                        status=BuildStatus(build_data.get("Status", "INITIALIZED")),
                        operating_system=operating_system,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create build via API: {e}")
                    build = Build(
                        build_id=build_id,
                        build_name=name,
                        build_version=build_version,
                        status=BuildStatus.BUILD_COMPLETE,
                        operating_system=operating_system,
                        tags=tags
                    )
            else:
                build = Build(
                    build_id=build_id,
                    build_name=name,
                    build_version=build_version,
                    status=BuildStatus.BUILD_COMPLETE,
                    operating_system=operating_system,
                    tags=tags
                )
            
            self._builds[build_id] = build
            return build
    
    def get_build(self, build_id: str) -> Optional[Build]:
        """Get build by ID."""
        return self._builds.get(build_id)
    
    def list_builds(self) -> List[Build]:
        """List all builds."""
        if self.client:
            try:
                response = self.client.list_builds()
                build_ids = response.get("BuildIds", [])
                
                if build_ids:
                    for build_data in response.get("Builds", []):
                        bid = build_data.get("BuildId")
                        if bid and bid not in self._builds:
                            self._builds[bid] = Build(
                                build_id=bid,
                                build_name=build_data.get("Name"),
                                build_version=build_data.get("BuildVersion"),
                                status=BuildStatus(build_data.get("Status", "INITIALIZED")),
                                tags={t["Key"]: t["Value"] for t in build_data.get("Tags", [])}
                            )
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list builds: {e}")
        
        return list(self._builds.values())
    
    def delete_build(self, build_id: str) -> bool:
        """Delete a build."""
        with self._lock:
            if self.client:
                try:
                    self.client.delete_build(BuildId=build_id)
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to delete build: {e}")
                    return False
            
            if build_id in self._builds:
                del self._builds[build_id]
            return True
    
    # =========================================================================
    # GAME SESSIONS
    # =========================================================================
    
    def create_game_session(
        self,
        fleet_id: str,
        name: Optional[str] = None,
        maximum_player_session_count: int = 10,
        game_properties: Optional[Dict[str, str]] = None,
        game_session_data: Optional[str] = None,
        creator_id: Optional[str] = None,
        location: Optional[str] = None,
        **kwargs
    ) -> GameSession:
        """
        Create a new game session.
        
        Args:
            fleet_id: Fleet ID
            name: Game session name
            maximum_player_session_count: Maximum players allowed
            game_properties: Game properties
            game_session_data: Custom game session data
            creator_id: Creator player ID
            location: Location for the session
            **kwargs: Additional parameters
        
        Returns:
            Created GameSession object
        """
        game_session_id = self._generate_id("gs")
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "FleetId": fleet_id,
                        "MaximumPlayerSessionCount": maximum_player_session_count
                    }
                    if name:
                        params["Name"] = name
                    if game_properties:
                        params["GameProperties"] = [
                            {"Key": k, "Value": v} for k, v in game_properties.items()
                        ]
                    if game_session_data:
                        params["GameSessionData"] = game_session_data
                    if creator_id:
                        params["CreatorId"] = creator_id
                    
                    response = self.client.create_game_session(**params)
                    session_data = response.get("GameSession", {})
                    game_session_id = session_data.get("GameSessionId", game_session_id)
                    
                    fleet = self._fleets.get(fleet_id)
                    
                    session = GameSession(
                        game_session_id=game_session_id,
                        fleet_id=fleet_id,
                        fleet_name=fleet.fleet_name if fleet else None,
                        name=name,
                        status=GameSessionStatus(session_data.get("Status", "ACTIVE")),
                        maximum_player_session_count=maximum_player_session_count,
                        current_player_session_count=0,
                        location=location or self.config.region,
                        dns_name=session_data.get("DnsName"),
                        ip_address=session_data.get("IpAddress"),
                        port=session_data.get("Port", 1935),
                        game_properties=game_properties,
                        game_session_data=game_session_data,
                        created_at=datetime.fromtimestamp(session_data.get("CreationTime", 0))
                            if session_data.get("CreationTime") else self._get_current_time(),
                        creator_id=creator_id
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create game session via API: {e}")
                    session = GameSession(
                        game_session_id=game_session_id,
                        fleet_id=fleet_id,
                        fleet_name=self._fleets.get(fleet_id).fleet_name if fleet_id in self._fleets else None,
                        name=name,
                        status=GameSessionStatus.ACTIVE,
                        maximum_player_session_count=maximum_player_session_count,
                        current_player_session_count=0,
                        location=location or self.config.region,
                        game_properties=game_properties,
                        game_session_data=game_session_data,
                        creator_id=creator_id
                    )
            else:
                session = GameSession(
                    game_session_id=game_session_id,
                    fleet_id=fleet_id,
                    fleet_name=self._fleets.get(fleet_id).fleet_name if fleet_id in self._fleets else None,
                    name=name,
                    status=GameSessionStatus.ACTIVE,
                    maximum_player_session_count=maximum_player_session_count,
                    current_player_session_count=0,
                    location=location or self.config.region,
                    game_properties=game_properties,
                    game_session_data=game_session_data,
                    creator_id=creator_id
                )
            
            self._game_sessions[game_session_id] = session
            return session
    
    def get_game_session(self, game_session_id: str) -> Optional[GameSession]:
        """Get game session by ID."""
        return self._game_sessions.get(game_session_id)
    
    def list_game_sessions(
        self,
        fleet_id: Optional[str] = None,
        status: Optional[GameSessionStatus] = None
    ) -> List[GameSession]:
        """List game sessions."""
        sessions = list(self._game_sessions.values())
        
        if fleet_id:
            sessions = [s for s in sessions if s.fleet_id == fleet_id]
        if status:
            sessions = [s for s in sessions if s.status == status]
        
        return sessions
    
    def update_game_session(
        self,
        game_session_id: str,
        maximum_player_session_count: Optional[int] = None,
        game_session_data: Optional[str] = None,
        status: Optional[GameSessionStatus] = None
    ) -> bool:
        """Update a game session."""
        if game_session_id not in self._game_sessions:
            return False
        
        with self._lock:
            session = self._game_sessions[game_session_id]
            if maximum_player_session_count is not None:
                session.maximum_player_session_count = maximum_player_session_count
            if game_session_data is not None:
                session.game_session_data = game_session_data
            if status is not None:
                session.status = status
            return True
    
    def terminate_game_session(self, game_session_id: str) -> bool:
        """Terminate a game session."""
        return self.update_game_session(
            game_session_id,
            status=GameSessionStatus.TERMINATING
        )
    
    # =========================================================================
    # PLAYER SESSIONS
    # =========================================================================
    
    def create_player_session(
        self,
        game_session_id: str,
        player_id: str,
        player_data: Optional[str] = None
    ) -> PlayerSession:
        """
        Create a player session.
        
        Args:
            game_session_id: Game session ID
            player_id: Player ID
            player_data: Custom player data
        
        Returns:
            Created PlayerSession object
        """
        player_session_id = self._generate_id("ps")
        
        with self._lock:
            session = self._game_sessions.get(game_session_id)
            if not session:
                raise ValueError(f"Game session {game_session_id} not found")
            
            if session.current_player_session_count >= session.maximum_player_session_count:
                raise ValueError("Game session is full")
            
            if self.client:
                try:
                    response = self.client.create_player_session(
                        GameSessionId=game_session_id,
                        PlayerId=player_id,
                        PlayerData=player_data
                    )
                    ps_data = response.get("PlayerSession", {})
                    player_session_id = ps_data.get("PlayerSessionId", player_session_id)
                    
                    player_session = PlayerSession(
                        player_session_id=player_session_id,
                        player_id=player_id,
                        game_session_id=game_session_id,
                        fleet_id=session.fleet_id,
                        status=PlayerSessionStatus(ps_data.get("Status", "ACTIVE")),
                        ip_address=ps_data.get("IpAddress"),
                        dns_name=ps_data.get("DnsName"),
                        port=ps_data.get("Port"),
                        player_data=player_data,
                        created_at=datetime.fromtimestamp(ps_data.get("CreationTime", 0))
                            if ps_data.get("CreationTime") else self._get_current_time(),
                        termination_time=datetime.fromtimestamp(ps_data.get("TerminationTime", 0))
                            if ps_data.get("TerminationTime") else None
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create player session via API: {e}")
                    player_session = PlayerSession(
                        player_session_id=player_session_id,
                        player_id=player_id,
                        game_session_id=game_session_id,
                        fleet_id=session.fleet_id,
                        status=PlayerSessionStatus.ACTIVE,
                        ip_address=session.ip_address,
                        dns_name=session.dns_name,
                        port=session.port,
                        player_data=player_data
                    )
            else:
                player_session = PlayerSession(
                    player_session_id=player_session_id,
                    player_id=player_id,
                    game_session_id=game_session_id,
                    fleet_id=session.fleet_id,
                    status=PlayerSessionStatus.ACTIVE,
                    ip_address=session.ip_address,
                    dns_name=session.dns_name,
                    port=session.port,
                    player_data=player_data
                )
            
            self._player_sessions[player_session_id] = player_session
            session.current_player_session_count += 1
            return player_session
    
    def create_player_sessions(
        self,
        game_session_id: str,
        player_ids: List[str],
        player_data: Optional[str] = None
    ) -> List[PlayerSession]:
        """Create multiple player sessions."""
        sessions = []
        for player_id in player_ids:
            session = self.create_player_session(game_session_id, player_id, player_data)
            sessions.append(session)
        return sessions
    
    def get_player_session(self, player_session_id: str) -> Optional[PlayerSession]:
        """Get player session by ID."""
        return self._player_sessions.get(player_session_id)
    
    def list_player_sessions(
        self,
        game_session_id: Optional[str] = None,
        player_id: Optional[str] = None,
        status: Optional[PlayerSessionStatus] = None
    ) -> List[PlayerSession]:
        """List player sessions."""
        sessions = list(self._player_sessions.values())
        
        if game_session_id:
            sessions = [s for s in sessions if s.game_session_id == game_session_id]
        if player_id:
            sessions = [s for s in sessions if s.player_id == player_id]
        if status:
            sessions = [s for s in sessions if s.status == status]
        
        return sessions
    
    def terminate_player_session(self, player_session_id: str) -> bool:
        """Terminate a player session."""
        if player_session_id not in self._player_sessions:
            return False
        
        with self._lock:
            ps = self._player_sessions[player_session_id]
            ps.status = PlayerSessionStatus.TERMINATED
            ps.termination_time = self._get_current_time()
            
            session = self._game_sessions.get(ps.game_session_id)
            if session:
                session.current_player_session_count = max(0, session.current_player_session_count - 1)
            
            return True
    
    # =========================================================================
    # ALIASES
    # =========================================================================
    
    def create_alias(
        self,
        name: str,
        fleet_id: Optional[str] = None,
        alias_type: AliasType = AliasType.SIMPLE,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Alias:
        """
        Create a fleet alias.
        
        Args:
            name: Alias name
            fleet_id: Target fleet ID
            alias_type: SIMPLE or TERMINAL
            description: Alias description
            tags: Resource tags
        
        Returns:
            Created Alias object
        """
        alias_id = self._generate_id("alias")
        
        with self._lock:
            if self.client and fleet_id:
                try:
                    routing_strategy = {"Type": alias_type.value}
                    if fleet_id:
                        routing_strategy["FleetId"] = fleet_id
                    
                    params = {
                        "Name": name,
                        "RoutingStrategy": routing_strategy,
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    if description:
                        params["Description"] = description
                    
                    response = self.client.create_alias(**params)
                    alias_data = response.get("Alias", {})
                    alias_id = alias_data.get("AliasId", alias_id)
                    
                    alias = Alias(
                        alias_id=alias_id,
                        name=name,
                        alias_type=alias_type,
                        description=description,
                        fleet_id=fleet_id,
                        routing_strategy=routing_strategy,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create alias via API: {e}")
                    alias = Alias(
                        alias_id=alias_id,
                        name=name,
                        alias_type=alias_type,
                        description=description,
                        fleet_id=fleet_id,
                        tags=tags
                    )
            else:
                alias = Alias(
                    alias_id=alias_id,
                    name=name,
                    alias_type=alias_type,
                    description=description,
                    fleet_id=fleet_id,
                    tags=tags
                )
            
            self._aliases[alias_id] = alias
            return alias
    
    def get_alias(self, alias_id: str) -> Optional[Alias]:
        """Get alias by ID."""
        return self._aliases.get(alias_id)
    
    def list_aliases(self, fleet_id: Optional[str] = None) -> List[Alias]:
        """List aliases."""
        aliases = list(self._aliases.values())
        
        if fleet_id:
            aliases = [a for a in aliases if a.fleet_id == fleet_id]
        
        return aliases
    
    def update_alias(
        self,
        alias_id: str,
        fleet_id: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """Update an alias."""
        if alias_id not in self._aliases:
            return False
        
        with self._lock:
            if self.client and fleet_id:
                try:
                    self.client.update_alias(
                        AliasId=alias_id,
                        FleetId=fleet_id,
                        Description=description
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to update alias: {e}")
                    return False
            
            if fleet_id:
                self._aliases[alias_id].fleet_id = fleet_id
                self._aliases[alias_id].routing_strategy["FleetId"] = fleet_id
            if description is not None:
                self._aliases[alias_id].description = description
            return True
    
    def delete_alias(self, alias_id: str) -> bool:
        """Delete an alias."""
        with self._lock:
            if self.client:
                try:
                    self.client.delete_alias(AliasId=alias_id)
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to delete alias: {e}")
                    return False
            
            if alias_id in self._aliases:
                del self._aliases[alias_id]
            return True
    
    # =========================================================================
    # LOCATIONS
    # =========================================================================
    
    def create_location(
        self,
        location_name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a custom location for multi-location deployment.
        
        Args:
            location_name: Location name
            tags: Resource tags
        
        Returns:
            Location data
        """
        with self._lock:
            if self.client:
                try:
                    response = self.client.create_location(
                        LocationName=location_name,
                        Tags=[{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    )
                    return response.get("Location", {})
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create location: {e}")
            
            return {
                "LocationName": location_name,
                "LocationArn": f"arn:aws:gamelift:{self.config.region}::location/{location_name}",
                "Tags": tags or {}
            }
    
    def list_locations(self) -> List[Dict[str, Any]]:
        """List all locations."""
        locations = []
        
        if self.client:
            try:
                response = self.client.list_locations()
                locations = response.get("Locations", [])
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list locations: {e}")
        
        return locations
    
    def register_fleet_location(
        self,
        fleet_id: str,
        location: str
    ) -> bool:
        """Register a location for a fleet."""
        with self._lock:
            if self.client:
                try:
                    self.client.create_fleet_locations(
                        FleetId=fleet_id,
                        LocationsConfig={
                            location: {"EC2InstanceCounts": {"DesiredEC2Instances": 0}}
                        }
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to register fleet location: {e}")
                    return False
            
            if fleet_id in self._fleets:
                if location not in self._fleets[fleet_id].locations:
                    self._fleets[fleet_id].locations.append(location)
            return True
    
    def get_fleet_location_info(self, fleet_id: str, location: str) -> Dict[str, Any]:
        """Get fleet location information."""
        if self.client:
            try:
                response = self.client.describe_fleet_location_attributes(
                    FleetId=fleet_id,
                    Location=location
                )
                return response.get("FleetLocationAttributes", [{}])[0]
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to get fleet location info: {e}")
        
        return {"Location": location, "Status": "ACTIVE"}
    
    # =========================================================================
    # GAME SERVER GROUPS
    # =========================================================================
    
    def create_game_server_group(
        self,
        name: str,
        fleet_id: Optional[str] = None,
        instance_type: str = "c5.large",
        tags: Optional[Dict[str, str]] = None
    ) -> GameServerGroup:
        """
        Create a game server group.
        
        Args:
            name: Server group name
            fleet_id: Associated fleet ID
            instance_type: EC2 instance type
            tags: Resource tags
        
        Returns:
            Created GameServerGroup object
        """
        arn = f"arn:aws:gamelift:{self.config.region}::gameservergroup/{name}"
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "GameServerGroupName": name,
                        "InstanceType": instance_type,
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    if fleet_id:
                        params["FleetId"] = fleet_id
                    
                    response = self.client.create_game_server_group(**params)
                    gsg_data = response.get("GameServerGroup", {})
                    
                    group = GameServerGroup(
                        game_server_group_name=gsg_data.get("GameServerGroupName", name),
                        game_server_group_arn=gsg_data.get("GameServerGroupArn", arn),
                        fleet_id=gsg_data.get("FleetId", fleet_id),
                        status=gsg_data.get("Status", "CREATE_IN_PROGRESS"),
                        instance_type=instance_type,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create game server group via API: {e}")
                    group = GameServerGroup(
                        game_server_group_name=name,
                        game_server_group_arn=arn,
                        fleet_id=fleet_id,
                        status="ACTIVE",
                        instance_type=instance_type,
                        tags=tags
                    )
            else:
                group = GameServerGroup(
                    game_server_group_name=name,
                    game_server_group_arn=arn,
                    fleet_id=fleet_id,
                    status="ACTIVE",
                    instance_type=instance_type,
                    tags=tags
                )
            
            self._game_server_groups[name] = group
            return group
    
    def get_game_server_group(self, name: str) -> Optional[GameServerGroup]:
        """Get game server group by name."""
        return self._game_server_groups.get(name)
    
    def list_game_server_groups(self) -> List[GameServerGroup]:
        """List all game server groups."""
        if self.client:
            try:
                response = self.client.list_game_server_groups()
                for gsg_data in response.get("GameServerGroups", []):
                    name = gsg_data.get("GameServerGroupName")
                    if name and name not in self._game_server_groups:
                        self._game_server_groups[name] = GameServerGroup(
                            game_server_group_name=name,
                            game_server_group_arn=gsg_data.get("GameServerGroupArn", ""),
                            fleet_id=gsg_data.get("FleetId"),
                            status=gsg_data.get("Status"),
                            instance_type=gsg_data.get("InstanceType"),
                            tags={}
                        )
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list game server groups: {e}")
        
        return list(self._game_server_groups.values())
    
    def delete_game_server_group(self, name: str) -> bool:
        """Delete a game server group."""
        with self._lock:
            if self.client:
                try:
                    self.client.delete_game_server_group(GameServerGroupName=name)
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to delete game server group: {e}")
                    return False
            
            if name in self._game_server_groups:
                del self._game_server_groups[name]
            return True
    
    def register_instance(
        self,
        fleet_id: str,
        instance_id: str,
        instance_type: str = "c5.large"
    ) -> Dict[str, Any]:
        """Register an EC2 instance with a fleet."""
        with self._lock:
            if self.client:
                try:
                    response = self.client.register_compute(
                        FleetId=fleet_id,
                        ComputeName=instance_id,
                        ComputeType="CUSTOM",
                        IpAddress=instance_id,
                        EC2InstanceId=instance_id,
                        InstanceType=instance_type
                    )
                    return response.get("Compute", {})
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to register instance: {e}")
            
            return {
                "FleetId": fleet_id,
                "ComputeName": instance_id,
                "ComputeArn": f"arn:aws:gamelift:{self.config.region}:compute/{instance_id}",
                "Status": "ACTIVE"
            }
    
    # =========================================================================
    # QUEUES
    # =========================================================================
    
    def create_queue(
        self,
        name: str,
        destinations: Optional[List[Dict[str, Any]]] = None,
        player_latency_policies: Optional[List[Dict[str, Any]]] = None,
        priority_configurations: Optional[List[Dict[str, Any]]] = None,
        custom_event_data: Optional[str] = None,
        filter_configuration: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> PlacementQueue:
        """
        Create a placement queue.
        
        Args:
            name: Queue name
            destinations: List of destination fleet ARNs
            player_latency_policies: Latency policies
            priority_configurations: Priority configurations
            custom_event_data: Custom event data
            filter_configuration: Filter configuration
            tags: Resource tags
        
        Returns:
            Created PlacementQueue object
        """
        queue_arn = f"arn:aws:gamelift:{self.config.region}::queue/{name}"
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "Name": name,
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    if destinations:
                        params["Destinations"] = destinations
                    if player_latency_policies:
                        params["PlayerLatencyPolicies"] = player_latency_policies
                    if priority_configurations:
                        params["PriorityConfigurations"] = priority_configurations
                    if custom_event_data:
                        params["CustomEventData"] = custom_event_data
                    if filter_configuration:
                        params["FilterConfiguration"] = filter_configuration
                    
                    response = self.client.create_game_session_queue(**params)
                    queue_data = response.get("GameSessionQueue", {})
                    queue_arn = queue_data.get("GameSessionQueueArn", queue_arn)
                    
                    queue = PlacementQueue(
                        queue_name=queue_data.get("Name", name),
                        queue_arn=queue_arn,
                        destinations=destinations,
                        player_latency_policies=player_latency_policies,
                        priority_configurations=priority_configurations,
                        custom_event_data=custom_event_data,
                        filter_configuration=filter_configuration,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create queue via API: {e}")
                    queue = PlacementQueue(
                        queue_name=name,
                        queue_arn=queue_arn,
                        destinations=destinations,
                        player_latency_policies=player_latency_policies,
                        priority_configurations=priority_configurations,
                        custom_event_data=custom_event_data,
                        filter_configuration=filter_configuration,
                        tags=tags
                    )
            else:
                queue = PlacementQueue(
                    queue_name=name,
                    queue_arn=queue_arn,
                    destinations=destinations,
                    player_latency_policies=player_latency_policies,
                    priority_configurations=priority_configurations,
                    custom_event_data=custom_event_data,
                    filter_configuration=filter_configuration,
                    tags=tags
                )
            
            self._queues[name] = queue
            return queue
    
    def get_queue(self, name: str) -> Optional[PlacementQueue]:
        """Get queue by name."""
        return self._queues.get(name)
    
    def list_queues(self) -> List[PlacementQueue]:
        """List all queues."""
        if self.client:
            try:
                response = self.client.list_game_session_queues()
                for queue_data in response.get("GameSessionQueues", []):
                    name = queue_data.get("Name")
                    if name and name not in self._queues:
                        self._queues[name] = PlacementQueue(
                            queue_name=name,
                            queue_arn=queue_data.get("GameSessionQueueArn", ""),
                            destinations=queue_data.get("Destinations", []),
                            tags={}
                        )
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to list queues: {e}")
        
        return list(self._queues.values())
    
    def start_game_session_placement(
        self,
        queue_name: str,
        maximum_player_session_count: int = 10,
        desired_player_sessions: Optional[int] = None,
        player_latencies: Optional[List[Dict[str, Any]]] = None,
        game_session_name: Optional[str] = None,
        game_properties: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Start a game session placement."""
        placement_id = self._generate_id("placement")
        
        if self.client:
            try:
                params = {
                    "QueueName": queue_name,
                    "MaximumPlayerSessionCount": maximum_player_session_count
                }
                if desired_player_sessions:
                    params["DesiredPlayerSessions"] = desired_player_sessions
                if player_latencies:
                    params["PlayerLatencies"] = player_latencies
                if game_session_name:
                    params["GameSessionName"] = game_session_name
                if game_properties:
                    params["GameProperties"] = [
                        {"Key": k, "Value": v} for k, v in game_properties.items()
                    ]
                
                response = self.client.start_game_session_placement(**params)
                return response.get("GameSessionPlacement", {})
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to start game session placement: {e}")
        
        return {
            "PlacementId": placement_id,
            "QueueName": queue_name,
            "Status": "PENDING",
            "MaximumPlayerSessionCount": maximum_player_session_count
        }
    
    def get_placement(self, placement_id: str) -> Dict[str, Any]:
        """Get placement by ID."""
        if self.client:
            try:
                response = self.client.describe_game_session_placement(
                    PlacementId=placement_id
                )
                return response.get("GameSessionPlacement", {})
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to get placement: {e}")
        
        return {"PlacementId": placement_id, "Status": "UNKNOWN"}
    
    # =========================================================================
    # MATCHMAKING
    # =========================================================================
    
    def create_matchmaking_rule_set(
        self,
        name: str,
        rule_set_body: str,
        tags: Optional[Dict[str, str]] = None
    ) -> MatchmakingRuleSet:
        """
        Create a matchmaking rule set.
        
        Args:
            name: Rule set name
            rule_set_body: Rule set JSON body
            tags: Resource tags
        
        Returns:
            Created MatchmakingRuleSet object
        """
        arn = f"arn:aws:gamelift:{self.config.region}::ruleset/{name}"
        
        with self._lock:
            if self.client:
                try:
                    response = self.client.create_matchmaking_rule_set(
                        Name=name,
                        RuleSetBody=rule_set_body,
                        Tags=[{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    )
                    rs_data = response.get("RuleSet", {})
                    arn = rs_data.get("RuleSetArn", arn)
                    
                    rule_set = MatchmakingRuleSet(
                        rule_set_name=rs_data.get("Name", name),
                        rule_set_arn=arn,
                        rule_set_body=rule_set_body,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create matchmaking rule set via API: {e}")
                    rule_set = MatchmakingRuleSet(
                        rule_set_name=name,
                        rule_set_arn=arn,
                        rule_set_body=rule_set_body,
                        tags=tags
                    )
            else:
                rule_set = MatchmakingRuleSet(
                    rule_set_name=name,
                    rule_set_arn=arn,
                    rule_set_body=rule_set_body,
                    tags=tags
                )
            
            self._matchmaking_rule_sets[name] = rule_set
            return rule_set
    
    def get_matchmaking_rule_set(self, name: str) -> Optional[MatchmakingRuleSet]:
        """Get matchmaking rule set by name."""
        return self._matchmaking_rule_sets.get(name)
    
    def create_matchmaking_configuration(
        self,
        name: str,
        game_session_queue_arn: str,
        rule_set_name: str,
        request_timeout_seconds: int = 300,
        acceptance_timeout_seconds: int = 300,
        acceptance_required: bool = True,
        description: Optional[str] = None,
        backfill_mode: Optional[str] = None,
        additional_player_count: Optional[int] = None,
        flex_match_mode: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> MatchmakingConfiguration:
        """
        Create a matchmaking configuration.
        
        Args:
            name: Configuration name
            game_session_queue_arn: Game session queue ARN
            rule_set_name: Rule set name
            request_timeout_seconds: Request timeout
            acceptance_timeout_seconds: Acceptance timeout
            acceptance_required: Whether acceptance is required
            description: Configuration description
            backfill_mode: Backfill mode
            additional_player_count: Additional player count
            flex_match_mode: FlexMatch mode
            tags: Resource tags
        
        Returns:
            Created MatchmakingConfiguration object
        """
        config_arn = f"arn:aws:gamelift:{self.config.region}::matchmakingconfig/{name}"
        
        with self._lock:
            if self.client:
                try:
                    params = {
                        "Name": name,
                        "GameSessionQueueArn": game_session_queue_arn,
                        "RuleSetName": rule_set_name,
                        "RequestTimeoutSeconds": request_timeout_seconds,
                        "AcceptanceTimeoutSeconds": acceptance_timeout_seconds,
                        "AcceptanceRequired": acceptance_required,
                        "Tags": [{"Key": k, "Value": v} for k, v in (tags or {}).items()]
                    }
                    if description:
                        params["Description"] = description
                    if backfill_mode:
                        params["BackfillMode"] = backfill_mode
                    if additional_player_count is not None:
                        params["AdditionalPlayerCount"] = additional_player_count
                    if flex_match_mode:
                        params["FlexMatchMode"] = flex_match_mode
                    
                    response = self.client.create_matchmaking_configuration(**params)
                    config_data = response.get("Configuration", {})
                    config_arn = config_data.get("ConfigurationArn", config_arn)
                    
                    config = MatchmakingConfiguration(
                        name=config_data.get("Name", name),
                        configuration_arn=config_arn,
                        description=description,
                        game_session_queue_arn=game_session_queue_arn,
                        request_timeout_seconds=request_timeout_seconds,
                        acceptance_timeout_seconds=acceptance_timeout_seconds,
                        acceptance_required=acceptance_required,
                        rule_set_name=rule_set_name,
                        backfill_mode=backfill_mode,
                        additional_player_count=additional_player_count,
                        flex_match_mode=flex_match_mode,
                        tags=tags
                    )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to create matchmaking configuration via API: {e}")
                    config = MatchmakingConfiguration(
                        name=name,
                        configuration_arn=config_arn,
                        description=description,
                        game_session_queue_arn=game_session_queue_arn,
                        request_timeout_seconds=request_timeout_seconds,
                        acceptance_timeout_seconds=acceptance_timeout_seconds,
                        acceptance_required=acceptance_required,
                        rule_set_name=rule_set_name,
                        backfill_mode=backfill_mode,
                        additional_player_count=additional_player_count,
                        flex_match_mode=flex_match_mode,
                        tags=tags
                    )
            else:
                config = MatchmakingConfiguration(
                    name=name,
                    configuration_arn=config_arn,
                    description=description,
                    game_session_queue_arn=game_session_queue_arn,
                    request_timeout_seconds=request_timeout_seconds,
                    acceptance_timeout_seconds=acceptance_timeout_seconds,
                    acceptance_required=acceptance_required,
                    rule_set_name=rule_set_name,
                    backfill_mode=backfill_mode,
                    additional_player_count=additional_player_count,
                    flex_match_mode=flex_match_mode,
                    tags=tags
                )
            
            self._matchmaking_configurations[name] = config
            return config
    
    def get_matchmaking_configuration(self, name: str) -> Optional[MatchmakingConfiguration]:
        """Get matchmaking configuration by name."""
        return self._matchmaking_configurations.get(name)
    
    def list_matchmaking_configurations(self) -> List[MatchmakingConfiguration]:
        """List all matchmaking configurations."""
        return list(self._matchmaking_configurations.values())
    
    def start_matchmaking(
        self,
        configuration_name: str,
        players: List[Dict[str, Any]]
    ) -> MatchmakingTicket:
        """
        Start a matchmaking ticket.
        
        Args:
            configuration_name: Configuration name
            players: List of player data
        
        Returns:
            Created MatchmakingTicket object
        """
        ticket_id = self._generate_id("mmticket")
        
        with self._lock:
            if self.client:
                try:
                    response = self.client.start_matchmaking(
                        ConfigurationName=configuration_name,
                        Players=players
                    )
                    tickets = response.get("MatchmakingTickets", [])
                    if tickets:
                        ticket_data = tickets[0]
                        ticket_id = ticket_data.get("TicketId", ticket_id)
                        ticket = MatchmakingTicket(
                            ticket_id=ticket_id,
                            configuration_name=configuration_name,
                            status=ticket_data.get("Status", "SEARCHING"),
                            players=players
                        )
                    else:
                        ticket = MatchmakingTicket(
                            ticket_id=ticket_id,
                            configuration_name=configuration_name,
                            status="SEARCHING",
                            players=players
                        )
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to start matchmaking via API: {e}")
                    ticket = MatchmakingTicket(
                        ticket_id=ticket_id,
                        configuration_name=configuration_name,
                        status="SEARCHING",
                        players=players
                    )
            else:
                ticket = MatchmakingTicket(
                    ticket_id=ticket_id,
                    configuration_name=configuration_name,
                    status="SEARCHING",
                    players=players
                )
            
            self._matchmaking_tickets[ticket_id] = ticket
            return ticket
    
    def get_matchmaking_ticket(self, ticket_id: str) -> Optional[MatchmakingTicket]:
        """Get matchmaking ticket by ID."""
        if self.client:
            try:
                response = self.client.describe_matchmaking(TicketIds=[ticket_id])
                tickets = response.get("TicketList", [])
                if tickets:
                    ticket_data = tickets[0]
                    if ticket_id in self._matchmaking_tickets:
                        self._matchmaking_tickets[ticket_id].status = ticket_data.get("Status", "UNKNOWN")
                    return self._matchmaking_tickets.get(ticket_id)
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to get matchmaking ticket: {e}")
        
        return self._matchmaking_tickets.get(ticket_id)
    
    def stop_matchmaking(self, ticket_id: str) -> bool:
        """Stop a matchmaking ticket."""
        if self.client:
            try:
                self.client.stop_matchmaking(TicketId=ticket_id)
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to stop matchmaking: {e}")
                return False
        
        if ticket_id in self._matchmaking_tickets:
            self._matchmaking_tickets[ticket_id].status = "CANCELLED"
            return True
        return False
    
    def start_matchmaking_backfill(
        self,
        ticket_id: str,
        players_to_match: Optional[List[Dict[str, Any]]] = None
    ) -> MatchmakingTicket:
        """Start backfill for an existing matchmaking ticket."""
        if ticket_id not in self._matchmaking_tickets:
            raise ValueError(f"Ticket {ticket_id} not found")
        
        ticket = self._matchmaking_tickets[ticket_id]
        
        if self.client:
            try:
                params = {"TicketId": ticket_id}
                if players_to_match:
                    params["PlayersToMatch"] = players_to_match
                
                response = self.client.start_matchmaking_backfill(**params)
                tickets = response.get("MatchmakingTickets", [])
                if tickets:
                    ticket.status = tickets[0].get("Status", ticket.status)
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to start backfill: {e}")
        else:
            if players_to_match:
                ticket.players.extend(players_to_match)
        
        return ticket
    
    # =========================================================================
    # CLOUDWATCH METRICS
    # =========================================================================
    
    def get_fleet_health_metrics(
        self,
        fleet_id: str,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get fleet health metrics from CloudWatch.
        
        Args:
            fleet_id: Fleet ID
            period: Metric period in seconds
            start_time: Start time
            end_time: End time
        
        Returns:
            Fleet health metrics
        """
        if not self.cloudwatch_client:
            return self._metrics_cache.get(fleet_id, {})
        
        end_time = end_time or datetime.now()
        start_time = start_time or datetime.now() - timedelta(hours=1)
        
        metrics = [
            MetricType.ACTIVE_SERVER_PROCESSES.value,
            MetricType.ACTIVE_GAME_SESSIONS.value,
            MetricType.AVAILABLE_SERVER_PROCESSES.value,
            MetricType.CURRENT_PLAYER_SESSIONS.value
        ]
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/GameLift",
                MetricName="ActiveGameSessions",
                Dimensions=[{"Name": "FleetId", "Value": fleet_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Maximum", "Minimum"]
            )
            
            return {
                "ActiveGameSessions": response.get("Datapoints", []),
                "FleetId": fleet_id,
                "Period": period,
                "StartTime": start_time.isoformat(),
                "EndTime": end_time.isoformat()
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get fleet health metrics: {e}")
            return self._metrics_cache.get(fleet_id, {})
    
    def get_matchmaking_metrics(
        self,
        configuration_name: Optional[str] = None,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get FlexMatch matchmaking metrics."""
        if not self.cloudwatch_client:
            return {}
        
        end_time = end_time or datetime.now()
        start_time = start_time or datetime.now() - timedelta(hours=1)
        
        dimensions = []
        if configuration_name:
            dimensions.append({"Name": "ConfigurationName", "Value": configuration_name})
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/GameLift",
                MetricName="MatchmakingTickets",
                Dimensions=dimensions or [{"Name": "Region", "Value": self.config.region}],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Sum", "Average"]
            )
            
            return {
                "MatchmakingTickets": response.get("Datapoints", []),
                "ConfigurationName": configuration_name,
                "Period": period
            }
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to get matchmaking metrics: {e}")
            return {}
    
    def get_placement_metrics(
        self,
        queue_name: Optional[str] = None,
        period: int = 300,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get game session placement metrics."""
        if not self.cloudwatch_client:
            return {}
        
        end_time = end_time or datetime.now()
        start_time = start_time or datetime.now() - timedelta(hours=1)
        
        dimensions = []
        if queue_name:
            dimensions.append({"Name": "QueueName", "Value": queue_name})
        
        metrics_data = {}
        metric_names = [
            MetricType.PLACEMENTS_STARTED.value,
            MetricType.PLACEMENTS_SUCCEEDED.value,
            MetricType.PLACEMENTS_FAILED.value,
            MetricType.PLACEMENTS_CANCELLED.value
        ]
        
        for metric_name in metric_names:
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/GameLift",
                    MetricName=metric_name,
                    Dimensions=dimensions or [{"Name": "Region", "Value": self.config.region}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Sum"]
                )
                metrics_data[metric_name] = response.get("Datapoints", [])
            except (ClientError, BotoCoreError) as e:
                logger.error(f"Failed to get placement metric {metric_name}: {e}")
        
        return metrics_data
    
    def put_metric_data(self, metric_data: List[Dict[str, Any]]) -> bool:
        """Put custom metric data to CloudWatch."""
        if not self.cloudwatch_client:
            return False
        
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace="GameLift/Custom",
                MetricData=metric_data
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to put metric data: {e}")
            return False
    
    def set_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 1,
        fleet_id: Optional[str] = None
    ) -> bool:
        """Set a CloudWatch alarm for GameLift metrics."""
        if not self.cloudwatch_client:
            return False
        
        dimensions = []
        if fleet_id:
            dimensions.append({"Name": "FleetId", "Value": fleet_id})
        
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace="AWS/GameLift",
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Statistic="Average",
                Dimensions=dimensions
            )
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to set alarm: {e}")
            return False
    
    def get_dashboard_metrics(self, fleet_id: str) -> Dict[str, Any]:
        """Get comprehensive dashboard metrics for a fleet."""
        health = self.get_fleet_health_metrics(fleet_id)
        placements = self.get_placement_metrics(fleet_id=fleet_id)
        
        fleet = self._fleets.get(fleet_id)
        
        return {
            "fleet_id": fleet_id,
            "fleet_name": fleet.fleet_name if fleet else None,
            "status": fleet.status.value if fleet else "UNKNOWN",
            "health_metrics": health,
            "placement_metrics": placements,
            "timestamp": self._get_current_time().isoformat()
        }
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_resource_count(self) -> Dict[str, int]:
        """Get counts of all resources."""
        return {
            "fleets": len(self._fleets),
            "builds": len(self._builds),
            "game_sessions": len(self._game_sessions),
            "player_sessions": len(self._player_sessions),
            "aliases": len(self._aliases),
            "game_server_groups": len(self._game_server_groups),
            "queues": len(self._queues),
            "matchmaking_configurations": len(self._matchmaking_configurations),
            "matchmaking_rule_sets": len(self._matchmaking_rule_sets),
            "matchmaking_tickets": len(self._matchmaking_tickets)
        }
    
    def export_state(self) -> Dict[str, Any]:
        """Export current state of all resources."""
        return {
            "fleets": [f.to_dict() for f in self._fleets.values()],
            "builds": [b.to_dict() for b in self._builds.values()],
            "game_sessions": [gs.to_dict() for gs in self._game_sessions.values()],
            "player_sessions": [ps.to_dict() for ps in self._player_sessions.values()],
            "aliases": [a.to_dict() for a in self._aliases.values()],
            "game_server_groups": [gsg.to_dict() for gsg in self._game_server_groups.values()],
            "queues": [q.to_dict() for q in self._queues.values()],
            "matchmaking_configurations": [mc.to_dict() for mc in self._matchmaking_configurations.values()],
            "matchmaking_rule_sets": [mrs.to_dict() for mrs in self._matchmaking_rule_sets.values()]
        }
    
    def import_state(self, state: Dict[str, Any]):
        """Import state from exported data."""
        with self._lock:
            self._fleets = {
                f["FleetId"]: Fleet(**f) for f in state.get("fleets", [])
            }
            self._builds = {
                b["BuildId"]: Build(**b) for b in state.get("builds", [])
            }
            self._game_sessions = {
                gs["GameSessionId"]: GameSession(**gs) for gs in state.get("game_sessions", [])
            }
            self._player_sessions = {
                ps["PlayerSessionId"]: PlayerSession(**ps) for ps in state.get("player_sessions", [])
            }
            self._aliases = {
                a["AliasId"]: Alias(**a) for a in state.get("aliases", [])
            }
            self._game_server_groups = {
                gsg["GameServerGroupName"]: GameServerGroup(**gsg)
                for gsg in state.get("game_server_groups", [])
            }
            self._queues = {
                q["QueueName"]: PlacementQueue(**q) for q in state.get("queues", [])
            }
            self._matchmaking_configurations = {
                mc["name"]: MatchmakingConfiguration(**mc)
                for mc in state.get("matchmaking_configurations", [])
            }
            self._matchmaking_rule_sets = {
                mrs["rule_set_name"]: MatchmakingRuleSet(**mrs)
                for mrs in state.get("matchmaking_rule_sets", [])
            }
    
    def cleanup(self):
        """Cleanup resources."""
        with self._lock:
            self._fleets.clear()
            self._builds.clear()
            self._game_sessions.clear()
            self._player_sessions.clear()
            self._aliases.clear()
            self._game_server_groups.clear()
            self._queues.clear()
            self._matchmaking_configurations.clear()
            self._matchmaking_rule_sets.clear()
            self._matchmaking_tickets.clear()
            self._metrics_cache.clear()
