"""
AWS ECS Container Orchestration Module for Workflow System

Implements an ECSIntegration class with:
1. Cluster management: Create/manage ECS clusters
2. Task definition: Register/manage task definitions
3. Task management: Run/manage tasks
4. Service management: Create/manage services
5. Container management: Manage containers
6. Service discovery: Service discovery integration
7. Auto scaling: Service auto scaling
8. Load balancing: ALB/NLB integration
9. IAM roles: Task execution and task roles
10. CloudWatch integration: Monitoring and logging

Commit: 'feat(aws-ecs): add AWS ECS integration with cluster management, task definitions, tasks, services, containers, service discovery, auto scaling, load balancing, IAM, CloudWatch'
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


class ClusterStatus(Enum):
    """ECS cluster status values."""
    ACTIVE = "ACTIVE"
    PROVISIONING = "PROVISIONING"
    DEPROVISIONING = "DEPROVISIONING"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"


class TaskStatus(Enum):
    """ECS task status values."""
    PROVISIONING = "PROVISIONING"
    PENDING = "PENDING"
    ACTIVATING = "ACTIVATING"
    RUNNING = "RUNNING"
    DEACTIVATING = "DEACTIVATING"
    STOPPING = "STOPPING"
    DEPROVISIONING = "DEPROVISIONING"
    STOPPED = "STOPPED"


class ServiceStatus(Enum):
    """ECS service status values."""
    ACTIVE = "ACTIVE"
    PROVISIONING = "PROVISIONING"
    DEPROVISIONING = "DEPROVISIONING"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"


class LaunchType(Enum):
    """ECS launch types."""
    EC2 = "EC2"
    FARGATE = "FARGATE"
    EXTERNAL = "EXTERNAL"


class NetworkMode(Enum):
    """Docker network modes for task definitions."""
    NONE = "none"
    BRIDGE = "bridge"
    HOST = "host"
    AWS_VPC = "awsvpc"
    NATS = "nats"


class LogDriver(Enum):
    """Docker log drivers."""
    JSON_FILE = "json-file"
    SYSLOG = "syslog"
    JOURNALD = "journald"
    GELF = "gelf"
    FLUENTD = "fluentd"
    AWSLOGS = "awslogs"
    SPLUNK = "splunk"
    NONE = "none"


class HealthCheckType(Enum):
    """Container health check types."""
    ECS = "ECS"
    ELB = "ELB"
    ALB = "ALB"
    NLB = "NLB"


class PlacementStrategyType(Enum):
    """Task placement strategy types."""
    RANDOM = "random"
    SPREAD = "spread"
    BINPACK = "binpack"


class SortOrder(Enum):
    """Sort order for listing operations."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class ContainerDefinition:
    """Container definition configuration."""
    name: str
    image: str
    essential: bool = True
    cpu: int = 0
    memory: int = 256
    memory_reservation: int = 0
    command: List[str] = field(default_factory=list)
    entry_point: List[str] = field(default_factory=list)
    working_directory: str = ""
    environment: Dict[str, str] = field(default_factory=dict)
    environment_files: List[Dict[str, str]] = field(default_factory=list)
    secrets: List[Dict[str, str]] = field(default_factory=list)
    port_mappings: List[Dict[str, Any]] = field(default_factory=list)
    health_check: Dict[str, Any] = field(default_factory=dict)
    log_configuration: Dict[str, Any] = field(default_factory=dict)
    mount_points: List[Dict[str, Any]] = field(default_factory=list)
    volumes_from: List[Dict[str, Any]] = field(default_factory=list)
    depends_on: List[Dict[str, str]] = field(default_factory=list)
    readonly_root_filesystem: bool = False
    privileged: bool = False
    user: str = ""
    docker_labels: Dict[str, str] = field(default_factory=dict)
    ulimits: List[Dict[str, Any]] = field(default_factory=list)
    linux_parameters: Dict[str, Any] = field(default_factory=dict)
    repository_credentials: Dict[str, str] = field(default_factory=dict)


@dataclass
class VolumeDefinition:
    """Volume definition for task definitions."""
    name: str
    host_path: str = ""
    docker_volume_configuration: Dict[str, Any] = field(default_factory=dict)
    fsx_windows_file_server_volume_configuration: Dict[str, Any] = field(default_factory=dict)
    efs_volume_configuration: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskDefinition:
    """Task definition configuration."""
    family: str
    containers: List[ContainerDefinition]
    volumes: List[VolumeDefinition] = field(default_factory=list)
    network_mode: NetworkMode = NetworkMode.AWS_VPC
    execution_role_arn: str = ""
    task_role_arn: str = ""
    cpu: str = "256"
    memory: str = "512"
    requires_compatibilities: List[LaunchType] = field(default_factory=list)
    runtime_platform: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    ipc_mode: str = ""
    pid_mode: str = ""
    proxy_configuration: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceDiscoveryConfig:
    """Service discovery configuration."""
    name: str
    dns_config: Dict[str, Any] = field(default_factory=dict)
    health_check_config: Dict[str, Any] = field(default_factory=dict)
    service_discovery_service: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AutoScalingConfig:
    """Auto scaling configuration for services."""
    min_capacity: int = 1
    max_capacity: int = 10
    target_cpu_utilization: float = 70.0
    target_memory_utilization: float = 80.0
    target_scaling_metric: Dict[str, Any] = field(default_factory=dict)
    scale_in_cooldown: int = 300
    scale_out_cooldown: int = 300


@dataclass
class LoadBalancerConfig:
    """Load balancer configuration."""
    target_group_arn: str
    container_name: str
    container_port: int
    load_balancer_type: str = "application"  # application, network
    load_balancer_arn: str = ""
    target_group_name: str = ""


@dataclass
class ClusterInfo:
    """ECS cluster information."""
    cluster_arn: str
    cluster_name: str
    status: ClusterStatus = ClusterStatus.ACTIVE
    registered_container_instances: int = 0
    running_tasks: int = 0
    pending_tasks: int = 0
    active_services: int = 0
    statistics: List[Dict[str, str]] = field(default_factory=list)
    settings: List[Dict[str, str]] = field(default_factory=list)
    capacity_providers: List[str] = field(default_factory=list)
    default_capacity_provider_strategy: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TaskInfo:
    """ECS task information."""
    task_arn: str
    task_definition_arn: str
    cluster_arn: str
    status: TaskStatus = TaskStatus.RUNNING
    desired_status: str = "RUNNING"
    launch_type: LaunchType = LaunchType.FARGATE
    container_instances: List[str] = field(default_factory=list)
    started_by: str = ""
    tags: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ServiceInfo:
    """ECS service information."""
    service_arn: str
    service_name: str
    cluster_arn: str
    status: ServiceStatus = ServiceStatus.ACTIVE
    desired_count: int = 1
    running_count: int = 0
    pending_count: int = 0
    task_definition: str = ""
    deployment_controller: Dict[str, str] = field(default_factory=dict)
    deployments: List[Dict[str, Any]] = field(default_factory=list)
    load_balancers: List[Dict[str, Any]] = field(default_factory=list)
    service_discovery_config: Dict[str, Any] = field(default_factory=dict)
    auto_scaling_config: AutoScalingConfig = field(default_factory=AutoScalingConfig)


class ECSIntegration:
    """
    AWS ECS Container Orchestration Integration.
    
    Provides comprehensive ECS cluster, task, and service management
    with support for Fargate, EC2, service discovery, auto scaling,
    load balancing, IAM roles, and CloudWatch monitoring.
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        profile: Optional[str] = None,
        cluster_name: Optional[str] = None
    ):
        """
        Initialize ECS integration.
        
        Args:
            region: AWS region for ECS operations
            profile: AWS profile name for credentials
            cluster_name: Default cluster name for operations
        """
        self.region = region
        self.profile = profile
        self.cluster_name = cluster_name or "default"
        self.ecs_client = None
        self.elbv2_client = None
        self.iam_client = None
        self.cloudwatch_client = None
        self.servicediscovery_client = None
        self.application_autoscaling_client = None
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS clients."""
        try:
            session_kwargs = {"region_name": self.region}
            if self.profile:
                session_kwargs["profile_name"] = self.profile
            
            session = boto3.Session(**session_kwargs)
            
            self.ecs_client = session.client("ecs")
            self.elbv2_client = session.client("elbv2")
            self.iam_client = session.client("iam")
            self.cloudwatch_client = session.client("cloudwatch")
            
            try:
                self.servicediscovery_client = session.client("servicediscovery")
            except Exception:
                logger.warning("Service Discovery client not available")
            
            try:
                self.application_autoscaling_client = session.client("application-autoscaling")
            except Exception:
                logger.warning("Application Auto Scaling client not available")
                
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
    
    def ensure_aws_credentials(func):
        """Decorator to ensure AWS credentials are available."""
        def wrapper(self, *args, **kwargs):
            if not BOTO3_AVAILABLE:
                raise ImportError("boto3 is required for AWS ECS operations")
            if not self.ecs_client:
                self._initialize_clients()
            return func(self, *args, **kwargs)
        return wrapper

    # =========================================================================
    # Cluster Management
    # =========================================================================
    
    @ensure_aws_credentials
    def create_cluster(
        self,
        cluster_name: str,
        settings: Optional[Dict[str, bool]] = None,
        capacity_providers: Optional[List[str]] = None,
        default_capacity_provider_strategy: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Dict[str, str]] = None,
        cluster_configuration: Optional[Dict[str, Any]] = None
    ) -> ClusterInfo:
        """
        Create an ECS cluster.
        
        Args:
            cluster_name: Name for the cluster
            settings: Cluster settings (e.g., containerInsights)
            capacity_providers: List of capacity providers
            default_capacity_provider_strategy: Default capacity provider strategy
            tags: Tags to apply to the cluster
            cluster_configuration: Cluster configuration details
            
        Returns:
            ClusterInfo object with cluster details
        """
        kwargs = {"clusterName": cluster_name}
        
        if settings:
            kwargs["settings"] = [
                {"name": k, "value": str(v).lower()} for k, v in settings.items()
            ]
        
        if capacity_providers:
            kwargs["capacityProviders"] = capacity_providers
        
        if default_capacity_provider_strategy:
            kwargs["defaultCapacityProviderStrategy"] = default_capacity_provider_strategy
        
        if tags:
            kwargs["tags"] = [{"key": k, "value": v} for k, v in tags.items()]
        
        if cluster_configuration:
            kwargs["clusterConfiguration"] = cluster_configuration
        
        try:
            response = self.ecs_client.create_cluster(**kwargs)
            cluster = response["cluster"]
            
            return ClusterInfo(
                cluster_arn=cluster["clusterArn"],
                cluster_name=cluster["clusterName"],
                status=ClusterStatus(cluster.get("status", "ACTIVE")),
                registered_container_instances=cluster.get("registeredContainerInstancesCount", 0),
                running_tasks=cluster.get("runningTasksCount", 0),
                pending_tasks=cluster.get("pendingTasksCount", 0),
                active_services=cluster.get("activeServicesCount", 0),
                statistics=cluster.get("statistics", []),
                settings=cluster.get("settings", []),
                capacity_providers=cluster.get("capacityProviders", []),
                default_capacity_provider_strategy=cluster.get("defaultCapacityProviderStrategy", [])
            )
        except ClientError as e:
            logger.error(f"Failed to create cluster: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_clusters(
        self,
        clusters: Optional[List[str]] = None,
        include: Optional[List[str]] = None
    ) -> List[ClusterInfo]:
        """
        Describe ECS clusters.
        
        Args:
            clusters: List of cluster names or ARNs
            include: Additional information to include
            
        Returns:
            List of ClusterInfo objects
        """
        kwargs = {}
        if clusters:
            kwargs["clusters"] = clusters
        if include:
            kwargs["include"] = include
        
        response = self.ecs_client.describe_clusters(**kwargs)
        
        return [
            ClusterInfo(
                cluster_arn=c["clusterArn"],
                cluster_name=c["clusterName"],
                status=ClusterStatus(c.get("status", "ACTIVE")),
                registered_container_instances=c.get("registeredContainerInstancesCount", 0),
                running_tasks=c.get("runningTasksCount", 0),
                pending_tasks=c.get("pendingTasksCount", 0),
                active_services=c.get("activeServicesCount", 0),
                statistics=c.get("statistics", []),
                settings=c.get("settings", []),
                capacity_providers=c.get("capacityProviders", []),
                default_capacity_provider_strategy=c.get("defaultCapacityProviderStrategy", [])
            )
            for c in response.get("clusters", [])
        ]
    
    @ensure_aws_credentials
    def list_clusters(self, max_results: int = 100) -> List[str]:
        """
        List all ECS cluster ARNs.
        
        Args:
            max_results: Maximum number of results to return
            
        Returns:
            List of cluster ARNs
        """
        cluster_arns = []
        paginator = self.ecs_client.get_paginator("list_clusters")
        
        for page in paginator.paginate(maxResults=max_results):
            cluster_arns.extend(page.get("clusterArns", []))
        
        return cluster_arns
    
    @ensure_aws_credentials
    def delete_cluster(self, cluster: str) -> bool:
        """
        Delete an ECS cluster.
        
        Args:
            cluster: Cluster name or ARN
            
        Returns:
            True if deletion was successful
        """
        try:
            self.ecs_client.delete_cluster(cluster=cluster)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete cluster: {e}")
            return False
    
    @ensure_aws_credentials
    def update_cluster(
        self,
        cluster: str,
        settings: Optional[Dict[str, str]] = None,
        capacity_providers: Optional[List[str]] = None,
        default_capacity_provider_strategy: Optional[List[Dict[str, Any]]] = None
    ) -> ClusterInfo:
        """
        Update an ECS cluster.
        
        Args:
            cluster: Cluster name or ARN
            settings: Updated cluster settings
            capacity_providers: Updated capacity providers
            default_capacity_provider_strategy: Updated capacity provider strategy
            
        Returns:
            Updated ClusterInfo object
        """
        kwargs = {"cluster": cluster}
        
        if settings:
            kwargs["settings"] = [
                {"name": k, "value": v} for k, v in settings.items()
            ]
        
        if capacity_providers is not None:
            kwargs["capacityProviders"] = capacity_providers
        
        if default_capacity_provider_strategy is not None:
            kwargs["defaultCapacityProviderStrategy"] = default_capacity_provider_strategy
        
        response = self.ecs_client.update_cluster(**kwargs)
        cluster = response["cluster"]
        
        return ClusterInfo(
            cluster_arn=cluster["clusterArn"],
            cluster_name=cluster["clusterName"],
            status=ClusterStatus(cluster.get("status", "ACTIVE")),
            registered_container_instances=cluster.get("registeredContainerInstancesCount", 0),
            running_tasks=cluster.get("runningTasksCount", 0),
            pending_tasks=cluster.get("pendingTasksCount", 0),
            active_services=cluster.get("activeServicesCount", 0)
        )

    # =========================================================================
    # Task Definition Management
    # =========================================================================
    
    @ensure_aws_credentials
    def register_task_definition(
        self,
        task_definition: TaskDefinition
    ) -> Dict[str, Any]:
        """
        Register a new task definition.
        
        Args:
            task_definition: TaskDefinition object with configuration
            
        Returns:
            Task definition registration response
        """
        container_definitions = []
        for container in task_definition.containers:
            container_def = {
                "name": container.name,
                "image": container.image,
                "essential": container.essential
            }
            
            if container.cpu > 0:
                container_def["cpu"] = container.cpu
            
            if container.memory > 0:
                container_def["memory"] = container.memory
            
            if container.memory_reservation > 0:
                container_def["memoryReservation"] = container.memory_reservation
            
            if container.command:
                container_def["command"] = container.command
            
            if container.entry_point:
                container_def["entryPoint"] = container.entry_point
            
            if container.working_directory:
                container_def["workingDirectory"] = container.working_directory
            
            if container.environment:
                container_def["environment"] = [
                    {"name": k, "value": v} for k, v in container.environment.items()
                ]
            
            if container.port_mappings:
                container_def["portMappings"] = container.port_mappings
            
            if container.health_check:
                container_def["healthCheck"] = container.health_check
            
            if container.log_configuration:
                container_def["logConfiguration"] = container.log_configuration
            
            if container.mount_points:
                container_def["mountPoints"] = container.mount_points
            
            if container.volumes_from:
                container_def["volumesFrom"] = container.volumes_from
            
            if container.depends_on:
                container_def["dependsOn"] = container.depends_on
            
            if container.readonly_root_filesystem:
                container_def["readonlyRootFilesystem"] = True
            
            if container.privileged:
                container_def["privileged"] = True
            
            if container.user:
                container_def["user"] = container.user
            
            if container.docker_labels:
                container_def["dockerLabels"] = container.docker_labels
            
            if container.ulimits:
                container_def["ulimits"] = container.ulimits
            
            if container.linux_parameters:
                container_def["linuxParameters"] = container.linux_parameters
            
            container_definitions.append(container_def)
        
        volumes = []
        for volume in task_definition.volumes:
            vol_def = {"name": volume.name}
            
            if volume.host_path:
                vol_def["host"] = {"sourcePath": volume.host_path}
            
            if volume.docker_volume_configuration:
                vol_def["dockerVolumeConfiguration"] = volume.docker_volume_configuration
            
            if volume.efs_volume_configuration:
                vol_def["efsVolumeConfiguration"] = volume.efs_volume_configuration
            
            volumes.append(vol_def)
        
        kwargs = {
            "family": task_definition.family,
            "containerDefinitions": container_definitions,
            "networkMode": task_definition.network_mode.value
        }
        
        if volumes:
            kwargs["volumes"] = volumes
        
        if task_definition.execution_role_arn:
            kwargs["executionRoleArn"] = task_definition.execution_role_arn
        
        if task_definition.task_role_arn:
            kwargs["taskRoleArn"] = task_definition.task_role_arn
        
        if task_definition.cpu:
            kwargs["cpu"] = task_definition.cpu
        
        if task_definition.memory:
            kwargs["memory"] = task_definition.memory
        
        if task_definition.requires_compatibilities:
            kwargs["requiresCompatibilities"] = [
                lt.value for lt in task_definition.requires_compatibilities
            ]
        
        if task_definition.runtime_platform:
            kwargs["runtimePlatform"] = task_definition.runtime_platform
        
        if task_definition.ipc_mode:
            kwargs["ipcMode"] = task_definition.ipc_mode
        
        if task_definition.pid_mode:
            kwargs["pidMode"] = task_definition.pid_mode
        
        if task_definition.proxy_configuration:
            kwargs["proxyConfiguration"] = task_definition.proxy_configuration
        
        try:
            response = self.ecs_client.register_task_definition(**kwargs)
            return response["taskDefinition"]
        except ClientError as e:
            logger.error(f"Failed to register task definition: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_task_definition(
        self,
        task_definition: str
    ) -> Dict[str, Any]:
        """
        Describe a task definition.
        
        Args:
            task_definition: Task definition family:revision or ARN
            
        Returns:
            Task definition details
        """
        response = self.ecs_client.describe_task_definition(
            taskDefinition=task_definition
        )
        return response["taskDefinition"]
    
    @ensure_aws_credentials
    def list_task_definitions(
        self,
        family_prefix: Optional[str] = None,
        status: Optional[str] = None,
        sort: SortOrder = SortOrder.DESC,
        max_results: int = 100
    ) -> List[str]:
        """
        List task definition ARNs.
        
        Args:
            family_prefix: Filter by family name prefix
            status: Filter by status (ACTIVE, INACTIVE, etc.)
            sort: Sort order for revisions
            max_results: Maximum results to return
            
        Returns:
            List of task definition ARNs
        """
        kwargs = {"maxResults": max_results, "sort": sort.value}
        
        if family_prefix:
            kwargs["familyPrefix"] = family_prefix
        
        if status:
            kwargs["status"] = status
        
        task_definition_arns = []
        paginator = self.ecs_client.get_paginator("list_task_definitions")
        
        for page in paginator.paginate(**kwargs):
            task_definition_arns.extend(page.get("taskDefinitionArns", []))
        
        return task_definition_arns
    
    @ensure_aws_credentials
    def deregister_task_definition(
        self,
        task_definition: str
    ) -> bool:
        """
        Deregister a task definition.
        
        Args:
            task_definition: Task definition family:revision or ARN
            
        Returns:
            True if deregistration was successful
        """
        try:
            self.ecs_client.deregister_task_definition(
                taskDefinition=task_definition
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to deregister task definition: {e}")
            return False
    
    @ensure_aws_credentials
    def create_task_definition_revision(
        self,
        task_definition: str,
        containers: Optional[List[ContainerDefinition]] = None,
        volumes: Optional[List[VolumeDefinition]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a new revision of an existing task definition.
        
        Args:
            task_definition: Current task definition
            containers: Updated container definitions
            volumes: Updated volume definitions
            **kwargs: Additional task definition attributes
            
        Returns:
            New task definition revision
        """
        current = self.describe_task_definition(task_definition)
        
        new_containers = containers or [
            ContainerDefinition(
                name=c["name"],
                image=c["image"],
                essential=c.get("essential", True),
                cpu=c.get("cpu", 0),
                memory=c.get("memory", 256)
            )
            for c in current.get("containerDefinitions", [])
        ]
        
        new_volumes = volumes or []
        if not new_volumes and current.get("volumes"):
            new_volumes = [
                VolumeDefinition(name=v["name"])
                for v in current.get("volumes", [])
            ]
        
        task_def = TaskDefinition(
            family=current["family"],
            containers=new_containers,
            volumes=new_volumes,
            network_mode=NetworkMode(current.get("networkMode", "awsvpc")),
            execution_role_arn=current.get("executionRoleArn", ""),
            task_role_arn=current.get("taskRoleArn", ""),
            cpu=current.get("cpu", "256"),
            memory=current.get("memory", "512")
        )
        
        for key, value in kwargs.items():
            if hasattr(task_def, key):
                setattr(task_def, key, value)
        
        return self.register_task_definition(task_def)

    # =========================================================================
    # Task Management
    # =========================================================================
    
    @ensure_aws_credentials
    def run_task(
        self,
        task_definition: str,
        cluster: Optional[str] = None,
        count: int = 1,
        launch_type: LaunchType = LaunchType.FARGATE,
        platform_version: str = "LATEST",
        network_configuration: Optional[Dict[str, Any]] = None,
        overrides: Optional[Dict[str, Any]] = None,
        placement_constraints: Optional[List[Dict[str, Any]]] = None,
        placement_strategy: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Dict[str, str]] = None,
        started_by: str = ""
    ) -> List[TaskInfo]:
        """
        Run a task.
        
        Args:
            task_definition: Task definition family:revision or ARN
            cluster: Cluster to run the task in
            count: Number of tasks to run
            launch_type: Launch type (EC2, FARGATE, EXTERNAL)
            platform_version: Platform version for Fargate
            network_configuration: Network configuration for awsvpc mode
            overrides: Task overrides
            placement_constraints: Placement constraints
            placement_strategy: Placement strategy
            tags: Tags to apply to tasks
            started_by: Identifier for who started the task
            
        Returns:
            List of TaskInfo objects
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {
            "taskDefinition": task_definition,
            "cluster": cluster,
            "count": count,
            "launchType": launch_type.value
        }
        
        if launch_type == LaunchType.FARGATE:
            kwargs["platformVersion"] = platform_version
        
        if network_configuration:
            kwargs["networkConfiguration"] = network_configuration
        
        if overrides:
            kwargs["overrides"] = overrides
        
        if placement_constraints:
            kwargs["placementConstraints"] = placement_constraints
        
        if placement_strategy:
            kwargs["placementStrategy"] = placement_strategy
        
        if tags:
            kwargs["tags"] = [{"key": k, "value": v} for k, v in tags.items()]
        
        if started_by:
            kwargs["startedBy"] = started_by
        
        try:
            response = self.ecs_client.run_task(**kwargs)
            
            return [
                TaskInfo(
                    task_arn=t["taskArn"],
                    task_definition_arn=t["taskDefinitionArn"],
                    cluster_arn=t["clusterArn"],
                    status=TaskStatus(t.get("lastStatus", "RUNNING")),
                    desired_status=t.get("desiredStatus", "RUNNING"),
                    launch_type=LaunchType(t.get("launchType", "FARGATE")),
                    container_instances=t.get("containerInstanceArn", ""),
                    started_by=t.get("startedBy", ""),
                    tags=t.get("tags", [])
                )
                for t in response.get("tasks", [])
            ]
        except ClientError as e:
            logger.error(f"Failed to run task: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_tasks(
        self,
        tasks: List[str],
        cluster: Optional[str] = None,
        include: Optional[List[str]] = None
    ) -> List[TaskInfo]:
        """
        Describe tasks.
        
        Args:
            tasks: List of task ARNs or IDs
            cluster: Cluster containing the tasks
            include: Additional information to include
            
        Returns:
            List of TaskInfo objects
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"tasks": tasks, "cluster": cluster}
        
        if include:
            kwargs["include"] = include
        
        response = self.ecs_client.describe_tasks(**kwargs)
        
        return [
            TaskInfo(
                task_arn=t["taskArn"],
                task_definition_arn=t["taskDefinitionArn"],
                cluster_arn=t["clusterArn"],
                status=TaskStatus(t.get("lastStatus", "RUNNING")),
                desired_status=t.get("desiredStatus", "RUNNING"),
                launch_type=LaunchType(t.get("launchType", "FARGATE")),
                container_instances=t.get("containerInstanceArn", ""),
                started_by=t.get("startedBy", ""),
                tags=t.get("tags", [])
            )
            for t in response.get("tasks", [])
        ]
    
    @ensure_aws_credentials
    def list_tasks(
        self,
        cluster: Optional[str] = None,
        container_instance: Optional[str] = None,
        family: Optional[str] = None,
        launch_type: Optional[LaunchType] = None,
        desired_status: str = "RUNNING",
        max_results: int = 100
    ) -> List[str]:
        """
        List tasks in a cluster.
        
        Args:
            cluster: Cluster to list tasks from
            container_instance: Filter by container instance
            family: Filter by task definition family
            launch_type: Filter by launch type
            desired_status: Filter by desired status
            max_results: Maximum results to return
            
        Returns:
            List of task ARNs
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"cluster": cluster, "maxResults": max_results}
        
        if container_instance:
            kwargs["containerInstance"] = container_instance
        
        if family:
            kwargs["family"] = family
        
        if launch_type:
            kwargs["launchType"] = launch_type.value
        
        if desired_status:
            kwargs["desiredStatus"] = desired_status
        
        task_arns = []
        paginator = self.ecs_client.get_paginator("list_tasks")
        
        for page in paginator.paginate(**kwargs):
            task_arns.extend(page.get("taskArns", []))
        
        return task_arns
    
    @ensure_aws_credentials
    def stop_task(
        self,
        task: str,
        cluster: Optional[str] = None,
        reason: str = ""
    ) -> TaskInfo:
        """
        Stop a task.
        
        Args:
            task: Task ARN or ID
            cluster: Cluster containing the task
            reason: Reason for stopping the task
            
        Returns:
            TaskInfo for the stopped task
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"task": task, "cluster": cluster}
        
        if reason:
            kwargs["reason"] = reason
        
        try:
            response = self.ecs_client.stop_task(**kwargs)
            t = response["task"]
            
            return TaskInfo(
                task_arn=t["taskArn"],
                task_definition_arn=t["taskDefinitionArn"],
                cluster_arn=t["clusterArn"],
                status=TaskStatus(t.get("lastStatus", "STOPPED")),
                desired_status=t.get("desiredStatus", "STOPPED"),
                launch_type=LaunchType(t.get("launchType", "FARGATE")),
                started_by=t.get("startedBy", ""),
                tags=t.get("tags", [])
            )
        except ClientError as e:
            logger.error(f"Failed to stop task: {e}")
            raise
    
    @ensure_aws_credentials
    def submit_task_state_change(
        self,
        task: str,
        cluster: Optional[str] = None,
        status: str = "DRAINING",
        reason: str = ""
    ) -> bool:
        """
        Submit task state change.
        
        Args:
            task: Task ARN or ID
            cluster: Cluster containing the task
            status: New status
            reason: Reason for the status change
            
        Returns:
            True if submission was successful
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {
            "tasks": [task],
            "cluster": cluster,
            "status": status
        }
        
        if reason:
            kwargs["reason"] = reason
        
        try:
            self.ecs_client.submit_task_state_change(**kwargs)
            return True
        except ClientError as e:
            logger.error(f"Failed to submit task state change: {e}")
            return False

    # =========================================================================
    # Service Management
    # =========================================================================
    
    @ensure_aws_credentials
    def create_service(
        self,
        service_name: str,
        task_definition: str,
        cluster: Optional[str] = None,
        desired_count: int = 1,
        launch_type: Optional[LaunchType] = None,
        platform_version: str = "LATEST",
        network_configuration: Optional[Dict[str, Any]] = None,
        load_balancers: Optional[List[LoadBalancerConfig]] = None,
        service_discovery_config: Optional[ServiceDiscoveryConfig] = None,
        auto_scaling_config: Optional[AutoScalingConfig] = None,
        deployment_controller: Optional[Dict[str, str]] = None,
        deployment_maximum_percent: int = 200,
        deployment_minimum_healthy_percent: int = 100,
        health_check_grace_period_seconds: int = 0,
        placement_constraints: Optional[List[Dict[str, Any]]] = None,
        placement_strategy: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Dict[str, str]] = None,
        enable_ecs_managed_tags: bool = False,
        propagate_tags: str = ""
    ) -> ServiceInfo:
        """
        Create an ECS service.
        
        Args:
            service_name: Name for the service
            task_definition: Task definition to use
            cluster: Cluster to create the service in
            desired_count: Desired number of tasks
            launch_type: Launch type
            platform_version: Platform version for Fargate
            network_configuration: Network configuration for awsvpc mode
            load_balancers: Load balancer configurations
            service_discovery_config: Service discovery configuration
            auto_scaling_config: Auto scaling configuration
            deployment_controller: Deployment controller type
            deployment_maximum_percent: Max percent during deployment
            deployment_minimum_healthy_percent: Min healthy percent
            health_check_grace_period_seconds: Grace period for health checks
            placement_constraints: Placement constraints
            placement_strategy: Placement strategy
            tags: Tags to apply
            enable_ecs_managed_tags: Enable ECS managed tags
            propagate_tags: Propagate tags from task definition or service
            
        Returns:
            ServiceInfo object
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {
            "serviceName": service_name,
            "taskDefinition": task_definition,
            "cluster": cluster,
            "desiredCount": desired_count
        }
        
        if launch_type:
            kwargs["launchType"] = launch_type.value
        
        if network_configuration:
            kwargs["networkConfiguration"] = network_configuration
        
        if load_balancers:
            kwargs["loadBalancers"] = [
                {
                    "targetGroupArn": lb.target_group_arn,
                    "containerName": lb.container_name,
                    "containerPort": lb.container_port
                }
                for lb in load_balancers
            ]
        
        if service_discovery_config:
            kwargs["serviceRegistries"] = [
                {
                    "registryArn": service_discovery_config.service_discovery_service.get("arn", ""),
                    "port": service_discovery_config.service_discovery_service.get("port", 0),
                    "containerName": service_discovery_config.service_discovery_service.get("containerName", ""),
                    "containerPort": service_discovery_config.service_discovery_service.get("containerPort", 0)
                }
            ]
        
        if deployment_controller:
            kwargs["deploymentController"] = deployment_controller
        
        kwargs["deploymentConfiguration"] = {
            "maximumPercent": deployment_maximum_percent,
            "minimumHealthyPercent": deployment_minimum_healthy_percent
        }
        
        if health_check_grace_period_seconds > 0:
            kwargs["healthCheckGracePeriodSeconds"] = health_check_grace_period_seconds
        
        if placement_constraints:
            kwargs["placementConstraints"] = placement_constraints
        
        if placement_strategy:
            kwargs["placementStrategy"] = placement_strategy
        
        if tags:
            kwargs["tags"] = [{"key": k, "value": v} for k, v in tags.items()]
        
        if enable_ecs_managed_tags:
            kwargs["enableECSManagedTags"] = True
        
        if propagate_tags:
            kwargs["propagateTags"] = propagate_tags
        
        try:
            response = self.ecs_client.create_service(**kwargs)
            service = response["service"]
            
            service_info = ServiceInfo(
                service_arn=service["serviceArn"],
                service_name=service["serviceName"],
                cluster_arn=service["clusterArn"],
                status=ServiceStatus(service.get("status", "ACTIVE")),
                desired_count=service.get("desiredCount", 0),
                running_count=service.get("runningCount", 0),
                pending_count=service.get("pendingCount", 0),
                task_definition=service.get("taskDefinition", ""),
                deployment_controller=service.get("deploymentController", {}),
                deployments=service.get("deployments", []),
                load_balancers=service.get("loadBalancers", [])
            )
            
            if auto_scaling_config:
                self.configure_service_auto_scaling(
                    service=service_info.service_arn,
                    cluster=cluster,
                    config=auto_scaling_config
                )
                service_info.auto_scaling_config = auto_scaling_config
            
            return service_info
            
        except ClientError as e:
            logger.error(f"Failed to create service: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_services(
        self,
        services: List[str],
        cluster: Optional[str] = None,
        include: Optional[List[str]] = None
    ) -> List[ServiceInfo]:
        """
        Describe services.
        
        Args:
            services: List of service names or ARNs
            cluster: Cluster containing the services
            include: Additional information to include
            
        Returns:
            List of ServiceInfo objects
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"services": services, "cluster": cluster}
        
        if include:
            kwargs["include"] = include
        
        response = self.ecs_client.describe_services(**kwargs)
        
        return [
            ServiceInfo(
                service_arn=s["serviceArn"],
                service_name=s["serviceName"],
                cluster_arn=s["clusterArn"],
                status=ServiceStatus(s.get("status", "ACTIVE")),
                desired_count=s.get("desiredCount", 0),
                running_count=s.get("runningCount", 0),
                pending_count=s.get("pendingCount", 0),
                task_definition=s.get("taskDefinition", ""),
                deployment_controller=s.get("deploymentController", {}),
                deployments=s.get("deployments", []),
                load_balancers=s.get("loadBalancers", []),
                service_discovery_config=s.get("serviceRegistries", [])
            )
            for s in response.get("services", [])
        ]
    
    @ensure_aws_credentials
    def list_services(
        self,
        cluster: Optional[str] = None,
        launch_type: Optional[LaunchType] = None,
        scheduling_strategy: Optional[str] = None,
        max_results: int = 100
    ) -> List[str]:
        """
        List services in a cluster.
        
        Args:
            cluster: Cluster to list services from
            launch_type: Filter by launch type
            scheduling_strategy: Filter by scheduling strategy
            max_results: Maximum results to return
            
        Returns:
            List of service ARNs
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"cluster": cluster, "maxResults": max_results}
        
        if launch_type:
            kwargs["launchType"] = launch_type.value
        
        if scheduling_strategy:
            kwargs["schedulingStrategy"] = scheduling_strategy
        
        service_arns = []
        paginator = self.ecs_client.get_paginator("list_services")
        
        for page in paginator.paginate(**kwargs):
            service_arns.extend(page.get("serviceArns", []))
        
        return service_arns
    
    @ensure_aws_credentials
    def update_service(
        self,
        service: str,
        cluster: Optional[str] = None,
        desired_count: Optional[int] = None,
        task_definition: Optional[str] = None,
        deployment_controller: Optional[Dict[str, str]] = None,
        deployment_maximum_percent: Optional[int] = None,
        deployment_minimum_healthy_percent: Optional[int] = None,
        health_check_grace_period_seconds: Optional[int] = None,
        force_new_deployment: bool = False
    ) -> ServiceInfo:
        """
        Update an ECS service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            desired_count: New desired count
            task_definition: New task definition
            deployment_controller: Updated deployment controller
            deployment_maximum_percent: Updated max percent
            deployment_minimum_healthy_percent: Updated min healthy percent
            health_check_grace_period_seconds: Updated grace period
            force_new_deployment: Force a new deployment
            
        Returns:
            Updated ServiceInfo object
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"service": service, "cluster": cluster}
        
        if desired_count is not None:
            kwargs["desiredCount"] = desired_count
        
        if task_definition:
            kwargs["taskDefinition"] = task_definition
        
        if deployment_controller:
            kwargs["deploymentController"] = deployment_controller
        
        if deployment_maximum_percent is not None or deployment_minimum_healthy_percent is not None:
            kwargs["deploymentConfiguration"] = {}
            if deployment_maximum_percent is not None:
                kwargs["deploymentConfiguration"]["maximumPercent"] = deployment_maximum_percent
            if deployment_minimum_healthy_percent is not None:
                kwargs["deploymentConfiguration"]["minimumHealthyPercent"] = deployment_minimum_healthy_percent
        
        if health_check_grace_period_seconds is not None:
            kwargs["healthCheckGracePeriodSeconds"] = health_check_grace_period_seconds
        
        if force_new_deployment:
            kwargs["forceNewDeployment"] = True
        
        try:
            response = self.ecs_client.update_service(**kwargs)
            s = response["service"]
            
            return ServiceInfo(
                service_arn=s["serviceArn"],
                service_name=s["serviceName"],
                cluster_arn=s["clusterArn"],
                status=ServiceStatus(s.get("status", "ACTIVE")),
                desired_count=s.get("desiredCount", 0),
                running_count=s.get("runningCount", 0),
                pending_count=s.get("pendingCount", 0),
                task_definition=s.get("taskDefinition", ""),
                deployment_controller=s.get("deploymentController", {}),
                deployments=s.get("deployments", []),
                load_balancers=s.get("loadBalancers", [])
            )
        except ClientError as e:
            logger.error(f"Failed to update service: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_service(
        self,
        service: str,
        cluster: Optional[str] = None,
        force: bool = False
    ) -> bool:
        """
        Delete an ECS service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            force: Force deletion even if there are running tasks
            
        Returns:
            True if deletion was successful
        """
        cluster = cluster or self.cluster_name
        
        try:
            self.ecs_client.delete_service(
                service=service,
                cluster=cluster,
                force=force
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete service: {e}")
            return False
    
    @ensure_aws_credentials
    def update_service_primary_task_set(
        self,
        service: str,
        cluster: Optional[str] = None,
        task_set: str = ""
    ) -> Dict[str, Any]:
        """
        Update the primary task set for a service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            task_set: Task set ARN to make primary
            
        Returns:
            Updated task set
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {
            "service": service,
            "cluster": cluster,
            "taskSet": task_set
        }
        
        try:
            response = self.ecs_client.update_service_primary_task_set(**kwargs)
            return response["taskSet"]
        except ClientError as e:
            logger.error(f"Failed to update task set: {e}")
            raise

    # =========================================================================
    # Container Management
    # =========================================================================
    
    @ensure_aws_credentials
    def describe_container_instances(
        self,
        container_instances: List[str],
        cluster: Optional[str] = None,
        include: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe container instances.
        
        Args:
            container_instances: List of container instance ARNs
            cluster: Cluster containing the container instances
            include: Additional information to include
            
        Returns:
            List of container instance details
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {
            "containerInstances": container_instances,
            "cluster": cluster
        }
        
        if include:
            kwargs["include"] = include
        
        response = self.ecs_client.describe_container_instances(**kwargs)
        return response.get("containerInstances", [])
    
    @ensure_aws_credentials
    def list_container_instances(
        self,
        cluster: Optional[str] = None,
        filter: Optional[str] = None,
        max_results: int = 100
    ) -> List[str]:
        """
        List container instances in a cluster.
        
        Args:
            cluster: Cluster to list container instances from
            filter: Filter expression
            max_results: Maximum results to return
            
        Returns:
            List of container instance ARNs
        """
        cluster = cluster or self.cluster_name
        
        kwargs = {"cluster": cluster, "maxResults": max_results}
        
        if filter:
            kwargs["filter"] = filter
        
        container_instance_arns = []
        paginator = self.ecs_client.get_paginator("list_container_instances")
        
        for page in paginator.paginate(**kwargs):
            container_instance_arns.extend(page.get("containerInstanceArns", []))
        
        return container_instance_arns
    
    @ensure_aws_credentials
    def update_container_instances_state(
        self,
        container_instances: List[str],
        cluster: Optional[str] = None,
        status: str = "DRAINING"
    ) -> bool:
        """
        Update container instance state.
        
        Args:
            container_instances: List of container instance ARNs
            cluster: Cluster containing the container instances
            status: New status (ACTIVE, DRAINING)
            
        Returns:
            True if update was successful
        """
        cluster = cluster or self.cluster_name
        
        try:
            self.ecs_client.update_container_instances_state(
                cluster=cluster,
                containerInstances=container_instances,
                status=status
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update container instances state: {e}")
            return False
    
    @ensure_aws_credentials
    def put_account_setting(
        self,
        name: str,
        value: str,
        principal_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Put an account setting.
        
        Args:
            name: Setting name
            value: Setting value
            principal_arn: Principal ARN to apply setting to
            
        Returns:
            Account setting response
        """
        kwargs = {"name": name, "value": value}
        
        if principal_arn:
            kwargs["principalArn"] = principal_arn
        
        try:
            response = self.ecs_client.put_account_setting(**kwargs)
            return response["setting"]
        except ClientError as e:
            logger.error(f"Failed to put account setting: {e}")
            raise

    # =========================================================================
    # Service Discovery
    # =========================================================================
    
    @ensure_aws_credentials
    def create_service_discovery_service(
        self,
        name: str,
        cluster: Optional[str] = None,
        dns_config: Optional[Dict[str, Any]] = None,
        health_check_config: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a Cloud Map service discovery service.
        
        Args:
            name: Service name
            cluster: Cluster to associate with the service
            dns_config: DNS configuration
            health_check_config: Health check configuration
            tags: Tags to apply
            
        Returns:
            Service discovery service details
        """
        if not self.servicediscovery_client:
            raise ImportError("Service Discovery client not available")
        
        kwargs = {"Name": name}
        
        if cluster:
            kwargs["Description"] = f"ECS Service Discovery for {cluster}"
        
        if dns_config:
            kwargs["DnsConfig"] = dns_config
        
        if health_check_config:
            kwargs["HealthCheckConfig"] = health_check_config
        
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        try:
            response = self.servicediscovery_client.create_service(**kwargs)
            return response["Service"]
        except ClientError as e:
            logger.error(f"Failed to create service discovery service: {e}")
            raise
    
    @ensure_aws_credentials
    def get_service_discovery_service(
        self,
        service_id: str
    ) -> Dict[str, Any]:
        """
        Get a service discovery service.
        
        Args:
            service_id: Service ID
            
        Returns:
            Service discovery service details
        """
        if not self.servicediscovery_client:
            raise ImportError("Service Discovery client not available")
        
        try:
            response = self.servicediscovery_client.get_service(Id=service_id)
            return response["Service"]
        except ClientError as e:
            logger.error(f"Failed to get service discovery service: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_service_discovery_service(
        self,
        service_id: str
    ) -> bool:
        """
        Delete a service discovery service.
        
        Args:
            service_id: Service ID
            
        Returns:
            True if deletion was successful
        """
        if not self.servicediscovery_client:
            raise ImportError("Service Discovery client not available")
        
        try:
            self.servicediscovery_client.delete_service(Id=service_id)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete service discovery service: {e}")
            return False

    # =========================================================================
    # Auto Scaling
    # =========================================================================
    
    @ensure_aws_credentials
    def configure_service_auto_scaling(
        self,
        service: str,
        cluster: Optional[str] = None,
        config: Optional[AutoScalingConfig] = None
    ) -> bool:
        """
        Configure auto scaling for an ECS service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            config: Auto scaling configuration
            
        Returns:
            True if configuration was successful
        """
        if not self.application_autoscaling_client:
            raise ImportError("Application Auto Scaling client not available")
        
        cluster = cluster or self.cluster_name
        
        service_arn = service if service.startswith("arn:") else f"arn:aws:ecs:{self.region}:*:service/{cluster}/{service}"
        
        if not config:
            config = AutoScalingConfig()
        
        try:
            self.application_autoscaling_client.register_scalable_target(
                ServiceNamespace="ecs",
                ResourceId=service_arn,
                ScalableDimension="ecs:service:DesiredCount",
                MinCapacity=config.min_capacity,
                MaxCapacity=config.max_capacity
            )
            
            target_tracking_policies = []
            
            if config.target_cpu_utilization > 0:
                target_tracking_policies.append({
                    "TargetValue": config.target_cpu_utilization,
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
                    },
                    "ScaleInCooldown": config.scale_in_cooldown,
                    "ScaleOutCooldown": config.scale_out_cooldown
                })
            
            if config.target_memory_utilization > 0:
                target_tracking_policies.append({
                    "TargetValue": config.target_memory_utilization,
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ECSServiceAverageMemoryUtilization"
                    },
                    "ScaleInCooldown": config.scale_in_cooldown,
                    "ScaleOutCooldown": config.scale_out_cooldown
                })
            
            for i, policy in enumerate(target_tracking_policies):
                self.application_autoscaling_client.put_scaling_policy(
                    ServiceNamespace="ecs",
                    ResourceId=service_arn,
                    ScalableDimension="ecs:service:DesiredCount",
                    PolicyName=f"TargetTrackingPolicy{i+1}",
                    PolicyType="TargetTrackingScaling",
                    TargetTrackingScalingPolicyConfiguration=policy
                )
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to configure auto scaling: {e}")
            return False
    
    @ensure_aws_credentials
    def describe_scaling_policies(
        self,
        service: str,
        cluster: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe scaling policies for a service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            
        Returns:
            List of scaling policies
        """
        if not self.application_autoscaling_client:
            raise ImportError("Application Auto Scaling client not available")
        
        cluster = cluster or self.cluster_name
        service_arn = service if service.startswith("arn:") else f"arn:aws:ecs:{self.region}:*:service/{cluster}/{service}"
        
        try:
            response = self.application_autoscaling_client.describe_scaling_policies(
                ServiceNamespace="ecs",
                ResourceId=service_arn,
                ScalableDimension="ecs:service:DesiredCount"
            )
            return response.get("ScalingPolicies", [])
        except ClientError as e:
            logger.error(f"Failed to describe scaling policies: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_scaling_policies(
        self,
        service: str,
        cluster: Optional[str] = None
    ) -> bool:
        """
        Delete scaling policies for a service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            
        Returns:
            True if deletion was successful
        """
        if not self.application_autoscaling_client:
            raise ImportError("Application Auto Scaling client not available")
        
        cluster = cluster or self.cluster_name
        service_arn = service if service.startswith("arn:") else f"arn:aws:ecs:{self.region}:*:service/{cluster}/{service}"
        
        try:
            self.application_autoscaling_client.delete_scaling_policy(
                ServiceNamespace="ecs",
                ResourceId=service_arn,
                ScalableDimension="ecs:service:DesiredCount",
                PolicyName="TargetTrackingPolicy1"
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete scaling policies: {e}")
            return False
    
    @ensure_aws_credentials
    def deregister_scalable_target(
        self,
        service: str,
        cluster: Optional[str] = None
    ) -> bool:
        """
        Deregister a scalable target.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            
        Returns:
            True if deregistration was successful
        """
        if not self.application_autoscaling_client:
            raise ImportError("Application Auto Scaling client not available")
        
        cluster = cluster or self.cluster_name
        service_arn = service if service.startswith("arn:") else f"arn:aws:ecs:{self.region}:*:service/{cluster}/{service}"
        
        try:
            self.application_autoscaling_client.deregister_scalable_target(
                ServiceNamespace="ecs",
                ResourceId=service_arn,
                ScalableDimension="ecs:service:DesiredCount"
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to deregister scalable target: {e}")
            return False

    # =========================================================================
    # Load Balancing
    # =========================================================================
    
    @ensure_aws_credentials
    def create_target_group(
        self,
        name: str,
        port: int,
        protocol: str = "HTTP",
        vpc_id: Optional[str] = None,
        health_check_path: str = "/",
        health_check_interval_seconds: int = 30,
        health_check_timeout_seconds: int = 5,
        healthy_threshold_count: int = 2,
        unhealthy_threshold_count: int = 2,
        target_type: str = "ip"
    ) -> Dict[str, Any]:
        """
        Create an ALB/NLB target group.
        
        Args:
            name: Target group name
            port: Target group port
            protocol: Target group protocol (HTTP, HTTPS, TCP, etc.)
            vpc_id: VPC ID
            health_check_path: Health check path
            health_check_interval_seconds: Health check interval
            health_check_timeout_seconds: Health check timeout
            healthy_threshold_count: Healthy threshold count
            unhealthy_threshold_count: Unhealthy threshold count
            target_type: Target type (instance, ip, lambda)
            
        Returns:
            Target group details
        """
        kwargs = {
            "Name": name,
            "Port": port,
            "Protocol": protocol,
            "TargetType": target_type
        }
        
        if vpc_id:
            kwargs["VpcId"] = vpc_id
        
        if protocol in ["HTTP", "HTTPS"]:
            kwargs["HealthCheckPath"] = health_check_path
            kwargs["HealthCheckIntervalSeconds"] = health_check_interval_seconds
            kwargs["HealthCheckTimeoutSeconds"] = health_check_timeout_seconds
            kwargs["HealthyThresholdCount"] = healthy_threshold_count
            kwargs["UnhealthyThresholdCount"] = unhealthy_threshold_count
        
        try:
            response = self.elbv2_client.create_target_group(**kwargs)
            return response["TargetGroups"][0]
        except ClientError as e:
            logger.error(f"Failed to create target group: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_target_groups(
        self,
        names: Optional[List[str]] = None,
        target_group_arns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe target groups.
        
        Args:
            names: Target group names
            target_group_arns: Target group ARNs
            
        Returns:
            List of target group details
        """
        kwargs = {}
        
        if names:
            kwargs["Names"] = names
        
        if target_group_arns:
            kwargs["TargetGroupArns"] = target_group_arns
        
        try:
            response = self.elbv2_client.describe_target_groups(**kwargs)
            return response.get("TargetGroups", [])
        except ClientError as e:
            logger.error(f"Failed to describe target groups: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_target_group(
        self,
        target_group_arn: str
    ) -> bool:
        """
        Delete a target group.
        
        Args:
            target_group_arn: Target group ARN
            
        Returns:
            True if deletion was successful
        """
        try:
            self.elbv2_client.delete_target_group(TargetGroupArn=target_group_arn)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete target group: {e}")
            return False
    
    @ensure_aws_credentials
    def register_targets(
        self,
        target_group_arn: str,
        targets: List[Dict[str, Any]]
    ) -> bool:
        """
        Register targets with a target group.
        
        Args:
            target_group_arn: Target group ARN
            targets: List of targets to register
            
        Returns:
            True if registration was successful
        """
        try:
            self.elbv2_client.register_targets(
                TargetGroupArn=target_group_arn,
                Targets=targets
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to register targets: {e}")
            return False
    
    @ensure_aws_credentials
    def deregister_targets(
        self,
        target_group_arn: str,
        targets: List[Dict[str, Any]]
    ) -> bool:
        """
        Deregister targets from a target group.
        
        Args:
            target_group_arn: Target group ARN
            targets: List of targets to deregister
            
        Returns:
            True if deregistration was successful
        """
        try:
            self.elbv2_client.deregister_targets(
                TargetGroupArn=target_group_arn,
                Targets=targets
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to deregister targets: {e}")
            return False
    
    @ensure_aws_credentials
    def create_load_balancer(
        self,
        name: str,
        subnets: List[str],
        security_groups: Optional[List[str]] = None,
        load_balancer_type: str = "application",
        scheme: str = "internet-facing",
        tags: Optional[Dict[str, str]] = None,
        ip_address_type: str = "ipv4"
    ) -> Dict[str, Any]:
        """
        Create an ALB/NLB.
        
        Args:
            name: Load balancer name
            subnets: List of subnet IDs
            security_groups: List of security group IDs
            load_balancer_type: Type (application, network)
            scheme: Scheme (internet-facing, internal)
            tags: Tags to apply
            ip_address_type: IP address type
            
        Returns:
            Load balancer details
        """
        kwargs = {
            "Name": name,
            "Subnets": subnets,
            "Type": load_balancer_type,
            "Scheme": scheme,
            "IpAddressType": ip_address_type
        }
        
        if security_groups:
            kwargs["SecurityGroups"] = security_groups
        
        if tags:
            kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
        
        try:
            response = self.elbv2_client.create_load_balancer(**kwargs)
            return response["LoadBalancers"][0]
        except ClientError as e:
            logger.error(f"Failed to create load balancer: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_load_balancers(
        self,
        names: Optional[List[str]] = None,
        load_balancer_arns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Describe load balancers.
        
        Args:
            names: Load balancer names
            load_balancer_arns: Load balancer ARNs
            
        Returns:
            List of load balancer details
        """
        kwargs = {}
        
        if names:
            kwargs["Names"] = names
        
        if load_balancer_arns:
            kwargs["LoadBalancerArns"] = load_balancer_arns
        
        try:
            response = self.elbv2_client.describe_load_balancers(**kwargs)
            return response.get("LoadBalancers", [])
        except ClientError as e:
            logger.error(f"Failed to describe load balancers: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_load_balancer(
        self,
        load_balancer_arn: str
    ) -> bool:
        """
        Delete a load balancer.
        
        Args:
            load_balancer_arn: Load balancer ARN
            
        Returns:
            True if deletion was successful
        """
        try:
            self.elbv2_client.delete_load_balancer(
                LoadBalancerArn=load_balancer_arn
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete load balancer: {e}")
            return False
    
    @ensure_aws_credentials
    def create_listener(
        self,
        load_balancer_arn: str,
        protocol: str,
        port: int,
        default_actions: List[Dict[str, Any]],
        ssl_policy: Optional[str] = None,
        certificates: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a listener for a load balancer.
        
        Args:
            load_balancer_arn: Load balancer ARN
            protocol: Listener protocol
            port: Listener port
            default_actions: Default actions
            ssl_policy: SSL policy for HTTPS
            certificates: Certificates for HTTPS
            
        Returns:
            Listener details
        """
        kwargs = {
            "LoadBalancerArn": load_balancer_arn,
            "Protocol": protocol,
            "Port": port,
            "DefaultActions": default_actions
        }
        
        if ssl_policy:
            kwargs["SslPolicy"] = ssl_policy
        
        if certificates:
            kwargs["Certificates"] = certificates
        
        try:
            response = self.elbv2_client.create_listener(**kwargs)
            return response["Listeners"][0]
        except ClientError as e:
            logger.error(f"Failed to create listener: {e}")
            raise

    # =========================================================================
    # IAM Roles
    # =========================================================================
    
    @ensure_aws_credentials
    def create_execution_role(
        self,
        role_name: str,
        path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an IAM role for task execution.
        
        Args:
            role_name: Name for the role
            path: Path for the role
            
        Returns:
            Role details
        """
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        kwargs = {
            "RoleName": role_name,
            "AssumeRolePolicyDocument": json.dumps(trust_policy),
            "Description": "ECS Task Execution Role"
        }
        
        if path:
            kwargs["Path"] = path
        
        try:
            response = self.iam_client.create_role(**kwargs)
            role = response["Role"]
            
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
            )
            
            return role
        except ClientError as e:
            logger.error(f"Failed to create execution role: {e}")
            raise
    
    @ensure_aws_credentials
    def create_task_role(
        self,
        role_name: str,
        policy_arns: Optional[List[str]] = None,
        path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an IAM role for task execution.
        
        Args:
            role_name: Name for the role
            policy_arns: List of policy ARNs to attach
            path: Path for the role
            
        Returns:
            Role details
        """
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        kwargs = {
            "RoleName": role_name,
            "AssumeRolePolicyDocument": json.dumps(trust_policy),
            "Description": "ECS Task Role"
        }
        
        if path:
            kwargs["Path"] = path
        
        try:
            response = self.iam_client.create_role(**kwargs)
            role = response["Role"]
            
            if policy_arns:
                for policy_arn in policy_arns:
                    self.iam_client.attach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy_arn
                    )
            
            return role
        except ClientError as e:
            logger.error(f"Failed to create task role: {e}")
            raise
    
    @ensure_aws_credentials
    def get_role(self, role_name: str) -> Optional[Dict[str, Any]]:
        """
        Get an IAM role.
        
        Args:
            role_name: Name of the role
            
        Returns:
            Role details or None if not found
        """
        try:
            response = self.iam_client.get_role(RoleName=role_name)
            return response["Role"]
        except ClientError as e:
            if "NoSuchEntity" in str(e):
                return None
            logger.error(f"Failed to get role: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_role(self, role_name: str) -> bool:
        """
        Delete an IAM role.
        
        Args:
            role_name: Name of the role
            
        Returns:
            True if deletion was successful
        """
        try:
            self.iam_client.delete_role(RoleName=role_name)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete role: {e}")
            return False

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    @ensure_aws_credentials
    def put_cluster_metrics(
        self,
        cluster: str
    ) -> bool:
        """
        Enable CloudWatch metrics for a cluster.
        
        Args:
            cluster: Cluster name
            
        Returns:
            True if enabled successfully
        """
        try:
            self.ecs_client.put_cluster_capacity_settings(
                cluster=cluster,
                clusterMetrics=[
                    {"name": "CWSEvents", "value": "ENABLED"}
                ]
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put cluster metrics: {e}")
            return False
    
    @ensure_aws_credentials
    def get_metric_data(
        self,
        metric_queries: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Get CloudWatch metric data.
        
        Args:
            metric_queries: List of metric queries
            start_time: Start time
            end_time: End time
            
        Returns:
            Metric data results
        """
        try:
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time
            )
            return response
        except ClientError as e:
            logger.error(f"Failed to get metric data: {e}")
            raise
    
    @ensure_aws_credentials
    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        dimensions: List[Dict[str, str]],
        start_time: datetime,
        end_time: datetime,
        period: int = 60,
        statistics: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metric statistics.
        
        Args:
            namespace: CloudWatch namespace
            metric_name: Metric name
            dimensions: Metric dimensions
            start_time: Start time
            end_time: End time
            period: Metric period in seconds
            statistics: Statistics to retrieve
            
        Returns:
            Metric statistics
        """
        if statistics is None:
            statistics = ["Average", "Sum", "Maximum", "Minimum"]
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics
            )
            return response.get("Datapoints", [])
        except ClientError as e:
            logger.error(f"Failed to get metric statistics: {e}")
            raise
    
    @ensure_aws_credentials
    def list_metrics(
        self,
        namespace: Optional[str] = None,
        metric_name: Optional[str] = None,
        dimensions: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List CloudWatch metrics.
        
        Args:
            namespace: Metric namespace
            metric_name: Metric name
            dimensions: Metric dimensions
            
        Returns:
            List of metrics
        """
        kwargs = {}
        
        if namespace:
            kwargs["Namespace"] = namespace
        
        if metric_name:
            kwargs["MetricName"] = metric_name
        
        if dimensions:
            kwargs["Dimensions"] = dimensions
        
        try:
            metrics = []
            paginator = self.cloudwatch_client.get_paginator("list_metrics")
            
            for page in paginator.paginate(**kwargs):
                metrics.extend(page.get("Metrics", []))
            
            return metrics
        except ClientError as e:
            logger.error(f"Failed to list metrics: {e}")
            raise
    
    @ensure_aws_credentials
    def put_metric_data(
        self,
        namespace: str,
        metric_data: List[Dict[str, Any]]
    ) -> bool:
        """
        Put metric data to CloudWatch.
        
        Args:
            namespace: Metric namespace
            metric_data: List of metric data points
            
        Returns:
            True if successful
        """
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to put metric data: {e}")
            return False
    
    @ensure_aws_credentials
    def create_log_delivery(
        self,
        cluster: str,
        log_group: str
    ) -> bool:
        """
        Create log delivery for ECS clusters.
        
        Args:
            cluster: Cluster name
            log_group: CloudWatch log group
            
        Returns:
            True if successful
        """
        try:
            self.ecs_client.put_cluster_capacity_settings(
                cluster=cluster,
                clusterMetrics=[
                    {
                        "name": "CWSEvents",
                        "value": "ENABLED"
                    }
                ]
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to create log delivery: {e}")
            return False
    
    @ensure_aws_credentials
    def get_tasks_for_service(
        self,
        service: str,
        cluster: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all tasks for a service.
        
        Args:
            service: Service name or ARN
            cluster: Cluster containing the service
            
        Returns:
            List of task details
        """
        cluster = cluster or self.cluster_name
        
        response = self.describe_services([service], cluster)
        
        if not response:
            return []
        
        service_info = response[0]
        task_def = service_info.task_definition
        
        tasks = self.list_tasks(
            cluster=cluster,
            family=task_def.split("/")[-1].rsplit(":", 1)[0] if ":" in task_def else task_def
        )
        
        if tasks:
            return self.describe_tasks(tasks, cluster)
        
        return []

    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    @ensure_aws_credentials
    def get_cluster_capacity_providers(
        self,
        cluster: str
    ) -> Dict[str, Any]:
        """
        Get capacity providers for a cluster.
        
        Args:
            cluster: Cluster name
            
        Returns:
            Capacity provider details
        """
        try:
            response = self.ecs_client.describe_clusters(
                clusters=[cluster],
                include=["CAPACITY_PROVIDERS"]
            )
            
            if response["clusters"]:
                c = response["clusters"][0]
                return {
                    "capacity_providers": c.get("capacityProviders", []),
                    "default_capacityProviderStrategy": c.get("defaultCapacityProviderStrategy", [])
                }
            return {}
        except ClientError as e:
            logger.error(f"Failed to get capacity providers: {e}")
            raise
    
    @ensure_aws_credentials
    def list_account_settings(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
        principal_arn: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List account settings.
        
        Args:
            name: Setting name filter
            value: Setting value filter
            principal_arn: Principal ARN filter
            
        Returns:
            List of account settings
        """
        kwargs = {}
        
        if name:
            kwargs["name"] = name
        
        if value:
            kwargs["value"] = value
        
        if principal_arn:
            kwargs["principalArn"] = principal_arn
        
        try:
            response = self.ecs_client.list_account_settings(**kwargs)
            return response.get("settings", [])
        except ClientError as e:
            logger.error(f"Failed to list account settings: {e}")
            raise
    
    @ensure_aws_credentials
    def validate_task_definition(
        self,
        task_definition: TaskDefinition
    ) -> bool:
        """
        Validate a task definition configuration.
        
        Args:
            task_definition: TaskDefinition to validate
            
        Returns:
            True if valid
        """
        if not task_definition.family:
            raise ValueError("Task definition family is required")
        
        if not task_definition.containers:
            raise ValueError("At least one container definition is required")
        
        for container in task_definition.containers:
            if not container.name:
                raise ValueError("Container name is required")
            
            if not container.image:
                raise ValueError(f"Container image is required for {container.name}")
        
        return True
