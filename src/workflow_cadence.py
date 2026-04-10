"""
Cadence Workflow Engine Integration Module

Implements a CadenceManager class with:
1. Domain management: Create/manage Cadence domains
2. Workflow management: Start/signal/cancel workflows
3. Activity registration: Register activities
4. Workflow types: Support multiple workflow types
5. Task list: Manage task lists
6. Cross-dc: Cross-data center replication
7. Visibility: Search workflows
8. Archival: Archive workflow histories
9. Retry policies: Configure retry policies
10. Domain failover: Handle domain failover

Commit: 'feat(cadence): add Cadence workflow engine integration with domain management, workflow management, activities, task lists, cross-dc, visibility, archival, retry policies, failover'
"""

import uuid
import json
import threading
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, TypeVar, Generic
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from enum import Enum
import copy
import logging

logger = logging.getLogger(__name__)


class DomainStatus(Enum):
    """Cadence domain status."""
    REGISTERED = "registered"
    DEPRECATED = "deprecated"
    DELETED = "deleted"


class WorkflowStatus(Enum):
    """Workflow execution status."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    CONTINUED_AS_NEW = "continued_as_new"
    TIMED_OUT = "timed_out"


class ActivityStatus(Enum):
    """Activity execution status."""
    SCHEDULED = "scheduled"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TIMED_OUT = "timed_out"


class ReplicationStatus(Enum):
    """Cross-dc replication status."""
    ACTIVE = "active"
    STANDBY = "standby"
    FAILOVER = "failover"
    REPLICATION_LAG = "replication_lag"


@dataclass
class RetryPolicy:
    """Retry policy configuration for workflows and activities."""
    initial_interval: int = 1  # seconds
    backoff_coefficient: float = 2.0
    maximum_interval: int = 100  # seconds
    maximum_attempts: int = 5
    non_retryable_errors: List[str] = field(default_factory=list)
    expiration_interval: int = 600  # seconds

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RetryPolicy':
        return cls(**data)


@dataclass
class DomainConfig:
    """Cadence domain configuration."""
    name: str
    description: str = ""
    owner_email: str = ""
    global_domain: bool = False
    data: Dict[str, str] = field(default_factory=dict)
    workflow_execution_retention_period: int = 7  # days
    archival_bucket: str = ""
    archival_enabled: bool = False
    active_cluster: str = "primary"
    clusters: List[str] = field(default_factory=list)
    security_token: str = ""
    status: DomainStatus = DomainStatus.REGISTERED

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['status'] = self.status.value
        return result


@dataclass
class WorkflowType:
    """Workflow type definition."""
    name: str
    version: str
    task_list: str = "default"
    workflow_id_reuse_policy: str = "AllowDuplicate"
    execution_timeout: int = 0  # 0 = infinite
    run_timeout: int = 0  # 0 = infinite
    task_timeout: int = 10  # seconds
    retry_policy: Optional[RetryPolicy] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.retry_policy:
            result['retry_policy'] = self.retry_policy.to_dict()
        return result


@dataclass
class ActivityType:
    """Activity type definition."""
    name: str
    version: str = "1.0"
    task_list: str = "default"
    timeout: int = 60  # seconds
    heartbeat_timeout: int = 10  # seconds
    schedule_to_close_timeout: int = 300  # seconds
    schedule_to_start_timeout: int = 60  # seconds
    start_to_close_timeout: int = 60  # seconds
    retry_policy: Optional[RetryPolicy] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.retry_policy:
            result['retry_policy'] = self.retry_policy.to_dict()
        return result


@dataclass
class WorkflowExecution:
    """Workflow execution information."""
    workflow_id: str
    run_id: str
    workflow_type: str
    status: WorkflowStatus = WorkflowStatus.RUNNING
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    parent_domain: Optional[str] = None
    parent_workflow_id: Optional[str] = None
    parent_run_id: Optional[str] = None
    task_list: str = "default"
    input: Any = None
    output: Any = None
    error: Optional[str] = None
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    memo: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    header: Dict[str, Any] = field(default_factory=dict)
    next_event_id: int = 1
    last_processed_event_id: int = 0
    attempt: int = 1
    max_attempts: int = 1

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['status'] = self.status.value
        result['start_time'] = self.start_time.isoformat() if self.start_time else None
        result['end_time'] = self.end_time.isoformat() if self.end_time else None
        result['close_time'] = self.close_time.isoformat() if self.close_time else None
        return result


@dataclass
class ActivityExecution:
    """Activity execution information."""
    activity_id: str
    activity_type: str
    workflow_id: str
    run_id: str
    status: ActivityStatus = ActivityStatus.SCHEDULED
    scheduled_time: datetime = field(default_factory=datetime.utcnow)
    started_time: Optional[datetime] = None
    completed_time: Optional[datetime] = None
    failed_time: Optional[datetime] = None
    task_token: Optional[str] = None
    attempt: int = 0
    max_attempts: int = 3
    heartbeat_details: Any = None
    result: Any = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['status'] = self.status.value
        result['scheduled_time'] = self.scheduled_time.isoformat() if self.scheduled_time else None
        result['started_time'] = self.started_time.isoformat() if self.started_time else None
        result['completed_time'] = self.completed_time.isoformat() if self.completed_time else None
        result['failed_time'] = self.failed_time.isoformat() if self.failed_time else None
        return result


@dataclass
class TaskListConfig:
    """Task list configuration."""
    name: str
    kind: str = "Normal"  # Normal, Sticky, System
    partition_config: Dict[str, Any] = field(default_factory=dict)
    max_tasks_per_flow: int = 1000
    backlog_size: int = 0
    reader_count: int = 1
    writer_count: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CrossDCConfig:
    """Cross-data center replication configuration."""
    enabled: bool = True
    source_cluster: str = "primary"
    target_clusters: List[str] = field(default_factory=list)
    replication_policy: str = "ASYNC"  # ASYNC, SYNC
    replicationlag_threshold: int = 5000  # milliseconds
    failover_timeout: int = 30  # seconds
    active_cluster: str = "primary"
    standby_clusters: List[str] = field(default_factory=list)
    replication_states: Dict[str, ReplicationStatus] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['replication_states'] = {k: v.value for k, v in self.replication_states.items()}
        return result


@dataclass
class ArchivalConfig:
    """Workflow history archival configuration."""
    enabled: bool = False
    provider: str = "s3"  # s3, gcs, minio, custom
    bucket: str = ""
    path_prefix: str = "cadence/archive/"
    retention_days: int = 365
    compression: str = "gzip"  # none, gzip, lz4
    encrypt: bool = True
    privacy_mask: bool = True
    archival_events: List[str] = field(default_factory=lambda: ["WorkflowExecutionCompleted", "WorkflowExecutionFailed"])

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class VisibilityQuery:
    """Visibility query for workflow search."""
    domain: str
    workflow_id: Optional[str] = None
    workflow_type: Optional[str] = None
    status: Optional[WorkflowStatus] = None
    start_time_min: Optional[datetime] = None
    start_time_max: Optional[datetime] = None
    close_time_min: Optional[datetime] = None
    close_time_max: Optional[datetime] = None
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    tag_filters: List[Dict[str, str]] = field(default_factory=list)
    limit: int = 100
    next_page_token: Optional[str] = None


@dataclass
class FailoverConfig:
    """Domain failover configuration."""
    enabled: bool = True
    failover_timeout: int = 30  # seconds
    primary_cluster: str = "primary"
    standby_clusters: List[str] = field(default_factory=list)
    promotion_policy: str = "automatic"  # automatic, manual
    health_check_interval: int = 5  # seconds
    unhealth_threshold: int = 3
    graceful_failover: bool = True


class CadenceManager:
    """
    Cadence workflow engine integration manager.

    Provides comprehensive management of:
    - Domain lifecycle and configuration
    - Workflow execution (start, signal, cancel, terminate)
    - Activity registration and heartbeating
    - Multiple workflow types
    - Task list management
    - Cross-data center replication
    - Workflow visibility and search
    - History archival
    - Retry policy configuration
    - Domain failover handling
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Cadence manager."""
        self.config = config or {}
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 7933)
        self.domain = self.config.get('domain', 'default')

        # State storage
        self._domains: Dict[str, DomainConfig] = {}
        self._workflow_types: Dict[str, WorkflowType] = {}
        self._activity_types: Dict[str, ActivityType] = {}
        self._workflows: Dict[str, WorkflowExecution] = {}
        self._activities: Dict[str, ActivityExecution] = {}
        self._task_lists: Dict[str, TaskListConfig] = {}
        self._cross_dc_configs: Dict[str, CrossDCConfig] = {}
        self._archival_configs: Dict[str, ArchivalConfig] = {}
        self._failover_configs: Dict[str, FailoverConfig] = {}

        # Activity registry
        self._activity_handlers: Dict[str, Callable] = {}

        # Workflow registry
        self._workflow_handlers: Dict[str, Callable] = {}

        # Event callbacks
        self._workflow_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        self._activity_callbacks: Dict[str, List[Callable]] = defaultdict(list)

        # Thread safety
        self._lock = threading.RLock()

        # Replication state
        self._replication_state: Dict[str, ReplicationStatus] = {}
        self._failover_in_progress: Dict[str, bool] = {}

        # Initialize default configurations
        self._initialize_defaults()

    def _initialize_defaults(self):
        """Initialize default domain and configurations."""
        default_domain = DomainConfig(
            name="default",
            description="Default Cadence domain",
            owner_email="admin@example.com",
            global_domain=False,
            clusters=["primary"],
            active_cluster="primary"
        )
        self._domains["default"] = default_domain

        default_task_list = TaskListConfig(name="default")
        self._task_lists["default"] = default_task_list

    # =========================================================================
    # DOMAIN MANAGEMENT
    # =========================================================================

    def register_domain(
        self,
        name: str,
        description: str = "",
        owner_email: str = "",
        global_domain: bool = False,
        clusters: Optional[List[str]] = None,
        workflow_execution_retention_period: int = 7,
        archival_config: Optional[ArchivalConfig] = None,
        cross_dc_config: Optional[CrossDCConfig] = None,
        failover_config: Optional[FailoverConfig] = None,
    ) -> DomainConfig:
        """
        Register a new Cadence domain.

        Args:
            name: Domain name
            description: Domain description
            owner_email: Owner email for the domain
            global_domain: Whether this is a global domain
            clusters: List of clusters for the domain
            workflow_execution_retention_period: Retention period in days
            archival_config: Archival configuration
            cross_dc_config: Cross-dc replication configuration
            failover_config: Failover configuration

        Returns:
            Created DomainConfig
        """
        with self._lock:
            if name in self._domains:
                raise ValueError(f"Domain {name} already exists")

            clusters = clusters or ["primary"]

            domain_config = DomainConfig(
                name=name,
                description=description,
                owner_email=owner_email,
                global_domain=global_domain,
                clusters=clusters,
                active_cluster=clusters[0] if clusters else "primary",
                workflow_execution_retention_period=workflow_execution_retention_period,
                status=DomainStatus.REGISTERED
            )

            self._domains[name] = domain_config

            # Initialize associated configurations
            if archival_config:
                self._archival_configs[name] = archival_config
            else:
                self._archival_configs[name] = ArchivalConfig()

            if cross_dc_config:
                self._cross_dc_configs[name] = cross_dc_config
            else:
                self._cross_dc_configs[name] = CrossDCConfig(
                    enabled=False,
                    clusters=clusters,
                    active_cluster=clusters[0] if clusters else "primary",
                    standby_clusters=clusters[1:] if len(clusters) > 1 else []
                )

            if failover_config:
                self._failover_configs[name] = failover_config
            else:
                self._failover_configs[name] = FailoverConfig(
                    primary_cluster=clusters[0] if clusters else "primary",
                    standby_clusters=clusters[1:] if len(clusters) > 1 else []
                )

            logger.info(f"Registered domain: {name}")
            return domain_config

    def get_domain(self, name: str) -> Optional[DomainConfig]:
        """Get domain configuration by name."""
        return self._domains.get(name)

    def list_domains(self, include_deprecated: bool = False) -> List[DomainConfig]:
        """List all registered domains."""
        domains = list(self._domains.values())
        if not include_deprecated:
            domains = [d for d in domains if d.status != DomainStatus.DEPRECATED]
        return domains

    def update_domain(
        self,
        name: str,
        description: Optional[str] = None,
        owner_email: Optional[str] = None,
        workflow_execution_retention_period: Optional[int] = None,
        active_cluster: Optional[str] = None,
        archival_enabled: Optional[bool] = None,
    ) -> DomainConfig:
        """Update domain configuration."""
        with self._lock:
            if name not in self._domains:
                raise ValueError(f"Domain {name} not found")

            domain = self._domains[name]

            if description is not None:
                domain.description = description
            if owner_email is not None:
                domain.owner_email = owner_email
            if workflow_execution_retention_period is not None:
                domain.workflow_execution_retention_period = workflow_execution_retention_period
            if active_cluster is not None:
                domain.active_cluster = active_cluster
            if archival_enabled is not None:
                domain.archival_enabled = archival_enabled

            logger.info(f"Updated domain: {name}")
            return domain

    def deprecate_domain(self, name: str) -> bool:
        """Deprecate a domain (soft delete)."""
        with self._lock:
            if name not in self._domains:
                raise ValueError(f"Domain {name} not found")

            self._domains[name].status = DomainStatus.DEPRECATED
            logger.info(f"Deprecated domain: {name}")
            return True

    def delete_domain(self, name: str) -> bool:
        """Delete a domain (hard delete)."""
        with self._lock:
            if name not in self._domains:
                raise ValueError(f"Domain {name} not found")

            self._domains[name].status = DomainStatus.DELETED
            del self._domains[name]
            logger.info(f"Deleted domain: {name}")
            return True

    # =========================================================================
    # WORKFLOW TYPE MANAGEMENT
    # =========================================================================

    def register_workflow_type(
        self,
        name: str,
        version: str = "1.0",
        task_list: str = "default",
        retry_policy: Optional[RetryPolicy] = None,
        execution_timeout: int = 0,
        run_timeout: int = 0,
        task_timeout: int = 10,
        description: str = "",
    ) -> WorkflowType:
        """
        Register a workflow type.

        Args:
            name: Workflow type name
            version: Workflow version
            task_list: Task list for workflow execution
            retry_policy: Retry policy for workflow failures
            execution_timeout: Overall execution timeout in seconds
            run_timeout: Individual run timeout in seconds
            task_timeout: Decision task timeout in seconds
            description: Workflow description

        Returns:
            Created WorkflowType
        """
        with self._lock:
            key = f"{name}:{version}"
            workflow_type = WorkflowType(
                name=name,
                version=version,
                task_list=task_list,
                retry_policy=retry_policy,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                description=description
            )

            self._workflow_types[key] = workflow_type
            logger.info(f"Registered workflow type: {key}")
            return workflow_type

    def get_workflow_type(self, name: str, version: str = "1.0") -> Optional[WorkflowType]:
        """Get workflow type by name and version."""
        key = f"{name}:{version}"
        return self._workflow_types.get(key)

    def list_workflow_types(self, task_list: Optional[str] = None) -> List[WorkflowType]:
        """List all registered workflow types."""
        types_list = list(self._workflow_types.values())
        if task_list:
            types_list = [t for t in types_list if t.task_list == task_list]
        return types_list

    def register_workflow_handler(self, name: str, version: str, handler: Callable):
        """Register a workflow implementation handler."""
        key = f"{name}:{version}"
        self._workflow_handlers[key] = handler
        logger.info(f"Registered workflow handler: {key}")

    # =========================================================================
    # ACTIVITY TYPE MANAGEMENT
    # =========================================================================

    def register_activity_type(
        self,
        name: str,
        version: str = "1.0",
        task_list: str = "default",
        timeout: int = 60,
        heartbeat_timeout: int = 10,
        schedule_to_close_timeout: int = 300,
        schedule_to_start_timeout: int = 60,
        start_to_close_timeout: int = 60,
        retry_policy: Optional[RetryPolicy] = None,
        description: str = "",
    ) -> ActivityType:
        """
        Register an activity type.

        Args:
            name: Activity type name
            version: Activity version
            task_list: Task list for activity execution
            timeout: Activity timeout in seconds
            heartbeat_timeout: Heartbeat timeout in seconds
            schedule_to_close_timeout: Schedule to close timeout
            schedule_to_start_timeout: Schedule to start timeout
            start_to_close_timeout: Start to close timeout
            retry_policy: Retry policy for activity failures
            description: Activity description

        Returns:
            Created ActivityType
        """
        with self._lock:
            key = f"{name}:{version}"
            activity_type = ActivityType(
                name=name,
                version=version,
                task_list=task_list,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                retry_policy=retry_policy,
                description=description
            )

            self._activity_types[key] = activity_type
            logger.info(f"Registered activity type: {key}")
            return activity_type

    def get_activity_type(self, name: str, version: str = "1.0") -> Optional[ActivityType]:
        """Get activity type by name and version."""
        key = f"{name}:{version}"
        return self._activity_types.get(key)

    def list_activity_types(self, task_list: Optional[str] = None) -> List[ActivityType]:
        """List all registered activity types."""
        types_list = list(self._activity_types.values())
        if task_list:
            types_list = [t for t in types_list if t.task_list == task_list]
        return types_list

    def register_activity_handler(self, name: str, version: str, handler: Callable):
        """Register an activity implementation handler."""
        key = f"{name}:{version}"
        self._activity_handlers[key] = handler
        logger.info(f"Registered activity handler: {key}")

    # =========================================================================
    # WORKFLOW MANAGEMENT
    # =========================================================================

    def start_workflow(
        self,
        workflow_type: str,
        workflow_id: Optional[str] = None,
        task_list: str = "default",
        input_data: Any = None,
        retry_policy: Optional[RetryPolicy] = None,
        memo: Optional[Dict[str, Any]] = None,
        header: Optional[Dict[str, Any]] = None,
        search_attributes: Optional[Dict[str, Any]] = None,
        domain: Optional[str] = None,
    ) -> WorkflowExecution:
        """
        Start a new workflow execution.

        Args:
            workflow_type: Type name of the workflow
            workflow_id: Unique workflow ID (auto-generated if not provided)
            task_list: Task list for the workflow
            input_data: Workflow input data
            retry_policy: Retry policy for the workflow
            memo: Workflow memo
            header: Workflow header
            search_attributes: Searchable attributes
            domain: Domain name

        Returns:
            WorkflowExecution instance
        """
        with self._lock:
            workflow_id = workflow_id or str(uuid.uuid4())
            run_id = str(uuid.uuid4())
            domain = domain or self.domain

            execution = WorkflowExecution(
                workflow_id=workflow_id,
                run_id=run_id,
                workflow_type=workflow_type,
                status=WorkflowStatus.RUNNING,
                task_list=task_list,
                input=input_data,
                memo=memo or {},
                header=header or {},
                search_attributes=search_attributes or {},
                start_time=datetime.utcnow()
            )

            self._workflows[workflow_id] = execution
            logger.info(f"Started workflow: {workflow_id} (run: {run_id})")

            # Trigger callbacks
            self._trigger_workflow_callback('started', execution)

            return execution

    def signal_workflow(
        self,
        workflow_id: str,
        signal_name: str,
        signal_input: Any = None,
        run_id: Optional[str] = None,
    ) -> bool:
        """
        Send a signal to a workflow execution.

        Args:
            workflow_id: Workflow ID
            signal_name: Signal name
            signal_input: Signal input data
            run_id: Specific run ID (latest if not provided)

        Returns:
            True if signal was sent successfully
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            if execution.status != WorkflowStatus.RUNNING:
                logger.warning(f"Cannot signal non-running workflow: {workflow_id}")
                return False

            event = {
                'event_type': 'WorkflowSignaled',
                'signal_name': signal_name,
                'signal_input': signal_input,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(event)
            execution.next_event_id += 1

            logger.info(f"Signaled workflow: {workflow_id} with signal: {signal_name}")
            self._trigger_workflow_callback('signaled', execution, signal_name, signal_input)

            return True

    def cancel_workflow(self, workflow_id: str, run_id: Optional[str] = None, reason: str = "") -> bool:
        """
        Cancel a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Specific run ID
            reason: Cancellation reason

        Returns:
            True if cancellation was successful
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            if execution.status != WorkflowStatus.RUNNING:
                logger.warning(f"Cannot cancel non-running workflow: {workflow_id}")
                return False

            execution.status = WorkflowStatus.CANCELED
            execution.end_time = datetime.utcnow()
            execution.close_time = datetime.utcnow()

            event = {
                'event_type': 'WorkflowCanceled',
                'reason': reason,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(event)
            execution.next_event_id += 1

            logger.info(f"Canceled workflow: {workflow_id}")
            self._trigger_workflow_callback('canceled', execution, reason)

            return True

    def terminate_workflow(
        self,
        workflow_id: str,
        reason: str = "",
        details: Any = None,
        run_id: Optional[str] = None,
    ) -> bool:
        """
        Terminate a workflow execution immediately.

        Args:
            workflow_id: Workflow ID
            reason: Termination reason
            details: Additional termination details
            run_id: Specific run ID

        Returns:
            True if termination was successful
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            execution.status = WorkflowStatus.TERMINATED
            execution.end_time = datetime.utcnow()
            execution.close_time = datetime.utcnow()
            execution.error = reason

            event = {
                'event_type': 'WorkflowTerminated',
                'reason': reason,
                'details': details,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(event)
            execution.next_event_id += 1

            logger.info(f"Terminated workflow: {workflow_id}")
            self._trigger_workflow_callback('terminated', execution, reason, details)

            return True

    def get_workflow_execution(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """Get workflow execution by ID."""
        return self._workflows.get(workflow_id)

    def complete_workflow(self, workflow_id: str, output: Any = None) -> bool:
        """
        Mark a workflow as completed.

        Args:
            workflow_id: Workflow ID
            output: Workflow output data

        Returns:
            True if workflow was completed
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            execution.status = WorkflowStatus.COMPLETED
            execution.output = output
            execution.end_time = datetime.utcnow()
            execution.close_time = datetime.utcnow()

            event = {
                'event_type': 'WorkflowExecutionCompleted',
                'result': output,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(event)
            execution.next_event_id += 1

            logger.info(f"Completed workflow: {workflow_id}")
            self._trigger_workflow_callback('completed', execution, output)

            return True

    def fail_workflow(self, workflow_id: str, error: str, details: Any = None) -> bool:
        """
        Mark a workflow as failed.

        Args:
            workflow_id: Workflow ID
            error: Error message
            details: Additional error details

        Returns:
            True if workflow was marked as failed
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            execution.status = WorkflowStatus.FAILED
            execution.error = error
            execution.end_time = datetime.utcnow()
            execution.close_time = datetime.utcnow()

            event = {
                'event_type': 'WorkflowExecutionFailed',
                'error': error,
                'details': details,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(event)
            execution.next_event_id += 1

            logger.info(f"Failed workflow: {workflow_id} - {error}")
            self._trigger_workflow_callback('failed', execution, error, details)

            return True

    def list_workflow_executions(
        self,
        domain: Optional[str] = None,
        status: Optional[WorkflowStatus] = None,
        workflow_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[WorkflowExecution]:
        """List workflow executions with optional filtering."""
        executions = list(self._workflows.values())

        if status:
            executions = [e for e in executions if e.status == status]
        if workflow_type:
            executions = [e for e in executions if e.workflow_type == workflow_type]

        return executions[:limit]

    def add_workflow_callback(
        self,
        event: str,
        workflow_id: str,
        callback: Callable[[WorkflowExecution, ...], None],
    ):
        """Add a callback for workflow events."""
        key = f"{workflow_id}:{event}"
        self._workflow_callbacks[key].append(callback)

    def _trigger_workflow_callback(self, event: str, execution: WorkflowExecution, *args, **kwargs):
        """Trigger callbacks for workflow events."""
        key = f"{execution.workflow_id}:{event}"
        for callback in self._workflow_callbacks.get(key, []):
            try:
                callback(execution, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in workflow callback: {e}")

        # Also trigger global event callbacks
        for callback in self._workflow_callbacks.get(event, []):
            try:
                callback(execution, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in workflow callback: {e}")

    # =========================================================================
    # ACTIVITY MANAGEMENT
    # =========================================================================

    def schedule_activity(
        self,
        activity_type: str,
        workflow_id: str,
        run_id: str,
        task_list: str = "default",
        input_data: Any = None,
        retry_policy: Optional[RetryPolicy] = None,
        activity_id: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> ActivityExecution:
        """
        Schedule an activity for execution.

        Args:
            activity_type: Activity type name
            workflow_id: Associated workflow ID
            run_id: Associated workflow run ID
            task_list: Task list for the activity
            input_data: Activity input data
            retry_policy: Retry policy for the activity
            activity_id: Unique activity ID (auto-generated if not provided)
            domain: Domain name

        Returns:
            ActivityExecution instance
        """
        with self._lock:
            activity_id = activity_id or str(uuid.uuid4())
            domain = domain or self.domain

            activity = ActivityExecution(
                activity_id=activity_id,
                activity_type=activity_type,
                workflow_id=workflow_id,
                run_id=run_id,
                status=ActivityStatus.SCHEDULED,
                scheduled_time=datetime.utcnow(),
                attempt=1
            )

            self._activities[activity_id] = activity
            logger.info(f"Scheduled activity: {activity_id} for workflow: {workflow_id}")

            return activity

    def start_activity(self, activity_id: str) -> bool:
        """Mark an activity as started."""
        with self._lock:
            if activity_id not in self._activities:
                raise ValueError(f"Activity {activity_id} not found")

            activity = self._activities[activity_id]
            activity.status = ActivityStatus.STARTED
            activity.started_time = datetime.utcnow()
            activity.task_token = str(uuid.uuid4())

            logger.info(f"Started activity: {activity_id}")
            self._trigger_activity_callback('started', activity)

            return True

    def complete_activity(self, activity_id: str, result: Any = None) -> bool:
        """Mark an activity as completed."""
        with self._lock:
            if activity_id not in self._activities:
                raise ValueError(f"Activity {activity_id} not found")

            activity = self._activities[activity_id]
            activity.status = ActivityStatus.COMPLETED
            activity.completed_time = datetime.utcnow()
            activity.result = result

            logger.info(f"Completed activity: {activity_id}")
            self._trigger_activity_callback('completed', activity, result)

            return True

    def fail_activity(self, activity_id: str, error: str, details: Any = None) -> bool:
        """Mark an activity as failed."""
        with self._lock:
            if activity_id not in self._activities:
                raise ValueError(f"Activity {activity_id} not found")

            activity = self._activities[activity_id]
            activity.status = ActivityStatus.FAILED
            activity.failed_time = datetime.utcnow()
            activity.error = error

            logger.info(f"Failed activity: {activity_id} - {error}")
            self._trigger_activity_callback('failed', activity, error, details)

            return True

    def heartbeat_activity(self, activity_id: str, details: Any = None) -> bool:
        """Record activity heartbeat."""
        with self._lock:
            if activity_id not in self._activities:
                raise ValueError(f"Activity {activity_id} not found")

            activity = self._activities[activity_id]
            activity.heartbeat_details = details

            logger.debug(f"Heartbeat for activity: {activity_id}")
            self._trigger_activity_callback('heartbeat', activity, details)

            return True

    def cancel_activity(self, activity_id: str, reason: str = "") -> bool:
        """Cancel an activity."""
        with self._lock:
            if activity_id not in self._activities:
                raise ValueError(f"Activity {activity_id} not found")

            activity = self._activities[activity_id]
            activity.status = ActivityStatus.CANCELED

            logger.info(f"Canceled activity: {activity_id}")
            self._trigger_activity_callback('canceled', activity, reason)

            return True

    def get_activity_execution(self, activity_id: str) -> Optional[ActivityExecution]:
        """Get activity execution by ID."""
        return self._activities.get(activity_id)

    def list_activity_executions(
        self,
        workflow_id: Optional[str] = None,
        status: Optional[ActivityStatus] = None,
        limit: int = 100,
    ) -> List[ActivityExecution]:
        """List activity executions with optional filtering."""
        activities = list(self._activities.values())

        if workflow_id:
            activities = [a for a in activities if a.workflow_id == workflow_id]
        if status:
            activities = [a for a in activities if a.status == status]

        return activities[:limit]

    def add_activity_callback(
        self,
        event: str,
        activity_id: str,
        callback: Callable[[ActivityExecution, ...], None],
    ):
        """Add a callback for activity events."""
        key = f"{activity_id}:{event}"
        self._activity_callbacks[key].append(callback)

    def _trigger_activity_callback(self, event: str, activity: ActivityExecution, *args, **kwargs):
        """Trigger callbacks for activity events."""
        key = f"{activity.activity_id}:{event}"
        for callback in self._activity_callbacks.get(key, []):
            try:
                callback(activity, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in activity callback: {e}")

        # Also trigger global event callbacks
        for callback in self._activity_callbacks.get(event, []):
            try:
                callback(activity, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in activity callback: {e}")

    # =========================================================================
    # TASK LIST MANAGEMENT
    # =========================================================================

    def register_task_list(
        self,
        name: str,
        kind: str = "Normal",
        max_tasks_per_flow: int = 1000,
        partition_config: Optional[Dict[str, Any]] = None,
    ) -> TaskListConfig:
        """
        Register a task list.

        Args:
            name: Task list name
            kind: Task list kind (Normal, Sticky, System)
            max_tasks_per_flow: Maximum tasks per flow
            partition_config: Partition configuration

        Returns:
            TaskListConfig instance
        """
        with self._lock:
            task_list = TaskListConfig(
                name=name,
                kind=kind,
                max_tasks_per_flow=max_tasks_per_flow,
                partition_config=partition_config or {}
            )

            self._task_lists[name] = task_list
            logger.info(f"Registered task list: {name}")
            return task_list

    def get_task_list(self, name: str) -> Optional[TaskListConfig]:
        """Get task list configuration by name."""
        return self._task_lists.get(name)

    def list_task_lists(self, kind: Optional[str] = None) -> List[TaskListConfig]:
        """List all task lists."""
        task_lists = list(self._task_lists.values())
        if kind:
            task_lists = [t for t in task_lists if t.kind == kind]
        return task_lists

    def update_task_list(self, name: str, **kwargs) -> TaskListConfig:
        """Update task list configuration."""
        with self._lock:
            if name not in self._task_lists:
                raise ValueError(f"Task list {name} not found")

            task_list = self._task_lists[name]
            for key, value in kwargs.items():
                if hasattr(task_list, key):
                    setattr(task_list, key, value)

            logger.info(f"Updated task list: {name}")
            return task_list

    def poll_task_list(
        self,
        name: str,
        task_type: str = "activity",
        timeout: int = 30,
    ) -> Optional[Dict[str, Any]]:
        """
        Poll a task list for pending tasks.

        Args:
            name: Task list name
            task_type: Type of task to poll (activity, decision)
            timeout: Poll timeout in seconds

        Returns:
            Task data or None
        """
        # Simulate polling - in real implementation would connect to Cadence
        logger.debug(f"Polling task list: {name} for {task_type} tasks")
        return None

    # =========================================================================
    # CROSS-DATA CENTER REPLICATION
    # =========================================================================

    def configure_cross_dc(
        self,
        domain: str,
        enabled: bool = True,
        source_cluster: str = "primary",
        target_clusters: Optional[List[str]] = None,
        replication_policy: str = "ASYNC",
        replicationlag_threshold: int = 5000,
    ) -> CrossDCConfig:
        """
        Configure cross-data center replication for a domain.

        Args:
            domain: Domain name
            enabled: Whether replication is enabled
            source_cluster: Source cluster name
            target_clusters: Target cluster names
            replication_policy: Replication policy (ASYNC, SYNC)
            replicationlag_threshold: Replication lag threshold in milliseconds

        Returns:
            CrossDCConfig instance
        """
        with self._lock:
            target_clusters = target_clusters or []

            config = CrossDCConfig(
                enabled=enabled,
                source_cluster=source_cluster,
                target_clusters=target_clusters,
                replication_policy=replication_policy,
                replicationlag_threshold=replicationlag_threshold,
                active_cluster=source_cluster,
                standby_clusters=target_clusters
            )

            self._cross_dc_configs[domain] = config
            self._replication_state[domain] = ReplicationStatus.ACTIVE

            logger.info(f"Configured cross-dc for domain: {domain}")
            return config

    def get_cross_dc_config(self, domain: str) -> Optional[CrossDCConfig]:
        """Get cross-dc configuration for a domain."""
        return self._cross_dc_configs.get(domain)

    def get_replication_status(self, domain: str) -> ReplicationStatus:
        """Get replication status for a domain."""
        return self._replication_state.get(domain, ReplicationStatus.ACTIVE)

    def replicate_workflow(self, workflow_id: str, target_cluster: str) -> bool:
        """
        Replicate a workflow to a target cluster.

        Args:
            workflow_id: Workflow ID to replicate
            target_cluster: Target cluster name

        Returns:
            True if replication was successful
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            replication_event = {
                'event_type': 'WorkflowReplicated',
                'target_cluster': target_cluster,
                'source_cluster': self._cross_dc_configs.get(
                    self.domain, CrossDCConfig()
                ).source_cluster,
                'timestamp': datetime.utcnow().isoformat(),
            }
            execution.history.append(replication_event)

            logger.info(f"Replicated workflow: {workflow_id} to cluster: {target_cluster}")
            return True

    def get_replication_lag(self, domain: str) -> int:
        """
        Get current replication lag for a domain.

        Returns:
            Replication lag in milliseconds
        """
        # Simulate lag calculation
        return 0

    # =========================================================================
    # VISIBILITY (SEARCH)
    # =========================================================================

    def search_workflows(self, query: VisibilityQuery) -> List[WorkflowExecution]:
        """
        Search workflows using visibility API.

        Args:
            query: VisibilityQuery with search criteria

        Returns:
            List of matching WorkflowExecution instances
        """
        executions = list(self._workflows.values())

        # Apply filters
        if query.workflow_id:
            executions = [e for e in executions if query.workflow_id in e.workflow_id]
        if query.workflow_type:
            executions = [e for e in executions if e.workflow_type == query.workflow_type]
        if query.status:
            executions = [e for e in executions if e.status == query.status]
        if query.start_time_min:
            executions = [e for e in executions if e.start_time >= query.start_time_min]
        if query.start_time_max:
            executions = [e for e in executions if e.start_time <= query.start_time_max]
        if query.search_attributes:
            for key, value in query.search_attributes.items():
                executions = [
                    e for e in executions
                    if e.search_attributes.get(key) == value
                ]

        # Apply limit
        return executions[:query.limit]

    def list_workflow_executions_by_type(
        self,
        workflow_type: str,
        status: Optional[WorkflowStatus] = None,
        limit: int = 100,
    ) -> List[WorkflowExecution]:
        """List all workflow executions of a specific type."""
        executions = [
            e for e in self._workflows.values()
            if e.workflow_type == workflow_type
        ]

        if status:
            executions = [e for e in executions if e.status == status]

        return executions[:limit]

    def list_closed_workflow_executions(
        self,
        domain: Optional[str] = None,
        start_time_min: Optional[datetime] = None,
        start_time_max: Optional[datetime] = None,
        close_time_min: Optional[datetime] = None,
        close_time_max: Optional[datetime] = None,
        workflow_type: Optional[str] = None,
        status: Optional[WorkflowStatus] = None,
        limit: int = 100,
    ) -> List[WorkflowExecution]:
        """List closed (completed, failed, canceled, terminated) workflow executions."""
        closed_statuses = {
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELED,
            WorkflowStatus.TERMINATED,
            WorkflowStatus.TIMED_OUT,
        }

        executions = [
            e for e in self._workflows.values()
            if e.status in closed_statuses
        ]

        if workflow_type:
            executions = [e for e in executions if e.workflow_type == workflow_type]
        if status:
            executions = [e for e in executions if e.status == status]
        if start_time_min:
            executions = [e for e in executions if e.start_time >= start_time_min]
        if start_time_max:
            executions = [e for e in executions if e.start_time <= start_time_max]
        if close_time_min:
            executions = [e for e in executions if e.close_time and e.close_time >= close_time_min]
        if close_time_max:
            executions = [e for e in executions if e.close_time and e.close_time <= close_time_max]

        return executions[:limit]

    # =========================================================================
    # ARCHIVAL
    # =========================================================================

    def configure_archival(
        self,
        domain: str,
        enabled: bool = False,
        provider: str = "s3",
        bucket: str = "",
        path_prefix: str = "cadence/archive/",
        retention_days: int = 365,
        compression: str = "gzip",
        encrypt: bool = True,
        privacy_mask: bool = True,
    ) -> ArchivalConfig:
        """
        Configure archival settings for a domain.

        Args:
            domain: Domain name
            enabled: Whether archival is enabled
            provider: Archival provider (s3, gcs, minio, custom)
            bucket: Storage bucket name
            path_prefix: Path prefix for archived histories
            retention_days: Retention period in days
            compression: Compression algorithm
            encrypt: Whether to encrypt archived data
            privacy_mask: Whether to mask sensitive data

        Returns:
            ArchivalConfig instance
        """
        with self._lock:
            config = ArchivalConfig(
                enabled=enabled,
                provider=provider,
                bucket=bucket,
                path_prefix=path_prefix,
                retention_days=retention_days,
                compression=compression,
                encrypt=encrypt,
                privacy_mask=privacy_mask
            )

            self._archival_configs[domain] = config

            # Update domain config
            if domain in self._domains:
                self._domains[domain].archival_enabled = enabled
                self._domains[domain].archival_bucket = bucket

            logger.info(f"Configured archival for domain: {domain}")
            return config

    def get_archival_config(self, domain: str) -> Optional[ArchivalConfig]:
        """Get archival configuration for a domain."""
        return self._archival_configs.get(domain)

    def archive_workflow(self, workflow_id: str) -> bool:
        """
        Archive a workflow's history.

        Args:
            workflow_id: Workflow ID to archive

        Returns:
            True if archival was initiated
        """
        with self._lock:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")

            execution = self._workflows[workflow_id]

            archival_event = {
                'event_type': 'WorkflowArchived',
                'archived_time': datetime.utcnow().isoformat(),
                'history_size': len(execution.history),
            }
            execution.history.append(archival_event)

            logger.info(f"Archived workflow: {workflow_id}")
            return True

    def get_archived_history(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve archived workflow history.

        Args:
            workflow_id: Workflow ID
            run_id: Specific run ID

        Returns:
            Archived history or None
        """
        # In real implementation would retrieve from storage
        if workflow_id in self._workflows:
            return self._workflows[workflow_id].history
        return None

    # =========================================================================
    # RETRY POLICIES
    # =========================================================================

    def create_retry_policy(
        self,
        initial_interval: int = 1,
        backoff_coefficient: float = 2.0,
        maximum_interval: int = 100,
        maximum_attempts: int = 5,
        non_retryable_errors: Optional[List[str]] = None,
        expiration_interval: int = 600,
    ) -> RetryPolicy:
        """
        Create a retry policy.

        Args:
            initial_interval: Initial retry interval in seconds
            backoff_coefficient: Exponential backoff coefficient
            maximum_interval: Maximum retry interval in seconds
            maximum_attempts: Maximum number of retry attempts
            non_retryable_errors: List of non-retryable error types
            expiration_interval: Retry expiration interval in seconds

        Returns:
            RetryPolicy instance
        """
        return RetryPolicy(
            initial_interval=initial_interval,
            backoff_coefficient=backoff_coefficient,
            maximum_interval=maximum_interval,
            maximum_attempts=maximum_attempts,
            non_retryable_errors=non_retryable_errors or [],
            expiration_interval=expiration_interval
        )

    def calculate_retry_delay(
        self,
        policy: RetryPolicy,
        attempt: int,
    ) -> Optional[float]:
        """
        Calculate the delay for a retry attempt.

        Args:
            policy: Retry policy
            attempt: Current attempt number (1-based)

        Returns:
            Delay in seconds, or None if retries exhausted
        """
        if attempt >= policy.maximum_attempts:
            return None

        delay = policy.initial_interval * (policy.backoff_coefficient ** (attempt - 1))
        return min(delay, policy.maximum_interval)

    def should_retry(
        self,
        policy: RetryPolicy,
        attempt: int,
        error: str,
    ) -> bool:
        """
        Determine if an operation should be retried.

        Args:
            policy: Retry policy
            attempt: Current attempt number
            error: Error message

        Returns:
            True if should retry
        """
        if attempt >= policy.maximum_attempts:
            return False

        # Check non-retryable errors
        for non_retryable in policy.non_retryable_errors:
            if non_retryable.lower() in error.lower():
                return False

        return True

    # =========================================================================
    # DOMAIN FAILOVER
    # =========================================================================

    def configure_failover(
        self,
        domain: str,
        enabled: bool = True,
        primary_cluster: str = "primary",
        standby_clusters: Optional[List[str]] = None,
        promotion_policy: str = "automatic",
        failover_timeout: int = 30,
        health_check_interval: int = 5,
        unhealth_threshold: int = 3,
        graceful_failover: bool = True,
    ) -> FailoverConfig:
        """
        Configure domain failover settings.

        Args:
            domain: Domain name
            enabled: Whether failover is enabled
            primary_cluster: Primary cluster name
            standby_clusters: Standby cluster names
            promotion_policy: Promotion policy (automatic, manual)
            failover_timeout: Failover timeout in seconds
            health_check_interval: Health check interval in seconds
            unhealth_threshold: Unhealthy threshold count
            graceful_failover: Whether to perform graceful failover

        Returns:
            FailoverConfig instance
        """
        with self._lock:
            config = FailoverConfig(
                enabled=enabled,
                primary_cluster=primary_cluster,
                standby_clusters=standby_clusters or [],
                promotion_policy=promotion_policy,
                failover_timeout=failover_timeout,
                health_check_interval=health_check_interval,
                unhealth_threshold=unhealth_threshold,
                graceful_failover=graceful_failover
            )

            self._failover_configs[domain] = config

            logger.info(f"Configured failover for domain: {domain}")
            return config

    def get_failover_config(self, domain: str) -> Optional[FailoverConfig]:
        """Get failover configuration for a domain."""
        return self._failover_configs.get(domain)

    def initiate_failover(
        self,
        domain: str,
        target_cluster: Optional[str] = None,
    ) -> bool:
        """
        Initiate domain failover.

        Args:
            domain: Domain name
            target_cluster: Target cluster (auto-selected if not provided)

        Returns:
            True if failover was initiated
        """
        with self._lock:
            if domain not in self._domains:
                raise ValueError(f"Domain {domain} not found")

            config = self._failover_configs.get(domain)
            if not config or not config.enabled:
                logger.warning(f"Failover not enabled for domain: {domain}")
                return False

            if self._failover_in_progress.get(domain, False):
                logger.warning(f"Failover already in progress for domain: {domain}")
                return False

            # Determine target cluster
            if not target_cluster:
                target_cluster = config.standby_clusters[0] if config.standby_clusters else None

            if not target_cluster:
                logger.error(f"No target cluster available for failover: {domain}")
                return False

            self._failover_in_progress[domain] = True

            # Update domain active cluster
            self._domains[domain].active_cluster = target_cluster

            # Update cross-dc config
            if domain in self._cross_dc_configs:
                self._cross_dc_configs[domain].active_cluster = target_cluster

            # Update replication state
            self._replication_state[domain] = ReplicationStatus.FAILOVER

            logger.info(f"Initiated failover for domain: {domain} to cluster: {target_cluster}")
            return True

    def complete_failover(self, domain: str) -> bool:
        """
        Complete domain failover.

        Args:
            domain: Domain name

        Returns:
            True if failover was completed
        """
        with self._lock:
            if domain not in self._domains:
                raise ValueError(f"Domain {domain} not found")

            if not self._failover_in_progress.get(domain, False):
                logger.warning(f"No failover in progress for domain: {domain}")
                return False

            self._failover_in_progress[domain] = False
            self._replication_state[domain] = ReplicationStatus.ACTIVE

            logger.info(f"Completed failover for domain: {domain}")
            return True

    def cancel_failover(self, domain: str) -> bool:
        """
        Cancel an in-progress failover.

        Args:
            domain: Domain name

        Returns:
            True if failover was canceled
        """
        with self._lock:
            if domain not in self._domains:
                raise ValueError(f"Domain {domain} not found")

            self._failover_in_progress[domain] = False

            # Revert to previous state
            if domain in self._failover_configs:
                config = self._failover_configs[domain]
                self._domains[domain].active_cluster = config.primary_cluster

            self._replication_state[domain] = ReplicationStatus.ACTIVE

            logger.info(f"Canceled failover for domain: {domain}")
            return True

    def get_failover_status(self, domain: str) -> Dict[str, Any]:
        """
        Get failover status for a domain.

        Returns:
            Dictionary with failover status information
        """
        return {
            'domain': domain,
            'in_progress': self._failover_in_progress.get(domain, False),
            'replication_status': self._replication_state.get(domain, ReplicationStatus.ACTIVE).value,
            'active_cluster': self._domains.get(domain, DomainConfig(name=domain)).active_cluster if domain in self._domains else None,
        }

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get manager statistics."""
        return {
            'domains': len(self._domains),
            'workflow_types': len(self._workflow_types),
            'activity_types': len(self._activity_types),
            'active_workflows': len([
                w for w in self._workflows.values()
                if w.status == WorkflowStatus.RUNNING
            ]),
            'total_workflows': len(self._workflows),
            'active_activities': len([
                a for a in self._activities.values()
                if a.status == ActivityStatus.STARTED
            ]),
            'total_activities': len(self._activities),
            'task_lists': len(self._task_lists),
        }

    def reset(self):
        """Reset all state (for testing)."""
        with self._lock:
            self._domains.clear()
            self._workflow_types.clear()
            self._activity_types.clear()
            self._workflows.clear()
            self._activities.clear()
            self._task_lists.clear()
            self._cross_dc_configs.clear()
            self._archival_configs.clear()
            self._failover_configs.clear()
            self._activity_handlers.clear()
            self._workflow_handlers.clear()
            self._workflow_callbacks.clear()
            self._activity_callbacks.clear()
            self._replication_state.clear()
            self._failover_in_progress.clear()
            self._initialize_defaults()
            logger.info("CadenceManager state reset")

    def to_dict(self) -> Dict[str, Any]:
        """Export manager state as dictionary."""
        return {
            'domains': {k: v.to_dict() for k, v in self._domains.items()},
            'workflow_types': {k: v.to_dict() for k, v in self._workflow_types.items()},
            'activity_types': {k: v.to_dict() for k, v in self._activity_types.items()},
            'workflows': {k: v.to_dict() for k, v in self._workflows.items()},
            'activities': {k: v.to_dict() for k, v in self._activities.items()},
            'task_lists': {k: v.to_dict() for k, v in self._task_lists.items()},
            'cross_dc_configs': {k: v.to_dict() for k, v in self._cross_dc_configs.items()},
            'archival_configs': {k: v.to_dict() for k, v in self._archival_configs.items()},
            'failover_configs': {k: v.to_dict() for k, v in self._failover_configs.items()},
        }
