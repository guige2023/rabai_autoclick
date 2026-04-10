"""
Temporal Workflow Engine Integration v22
Comprehensive Temporal integration with workflow registration, execution,
activities, child workflows, queries, search attributes, namespaces, task queues,
history replay, and Temporal web UI integration
"""
import json
import time
import threading
import uuid
import asyncio
from typing import Dict, List, Optional, Any, Tuple, Callable, Type, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import copy
import logging
import traceback

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status"""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    CONTINUED_AS_NEW = "continued_as_new"
    PENDING = "pending"


class ActivityStatus(Enum):
    """Activity execution status"""
    SCHEDULED = "scheduled"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    HEARTBEAT = "heartbeat"


class SignalType(Enum):
    """Workflow signal types"""
    START = "start"
    STOP = "stop"
    PAUSE = "pause"
    RESUME = "resume"
    UPDATE = "update"
    EXTERNAL = "external"


class ChildWorkflowPolicy(Enum):
    """Child workflow execution policy"""
    ALLOW_PARALLEL = "allow_parallel"
    ALLOW_RETRY = "allow_retry"
    WAIT_FOR_COMPLETION = "wait_for_completion"
    WAIT_FOR_CANCELLATION = "wait_for_cancellation"


@dataclass
class WorkflowRegistration:
    """Workflow registration details"""
    name: str
    version: str
    workflow_type: str
    description: str = ""
    task_queue: str = "default"
    workflow_id: Optional[str] = None
    parent_workflow_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    retry_policy: Optional[Dict[str, Any]] = None
    timeout_config: Dict[str, Any] = field(default_factory=dict)
    registered_at: Optional[datetime] = None
    last_execution: Optional[datetime] = None
    execution_count: int = 0


@dataclass
class WorkflowExecution:
    """Workflow execution details"""
    execution_id: str
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    task_queue: str = "default"
    parent_execution_id: Optional[str] = None
    root_execution_id: Optional[str] = None
    child_executions: List[str] = field(default_factory=list)
    signals: List[Dict[str, Any]] = field(default_factory=list)
    queries: List[Dict[str, Any]] = field(default_factory=list)
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    runtime_config: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    result: Optional[Any] = None
    history_length: int = 0


@dataclass
class ActivityRegistration:
    """Activity registration details"""
    name: str
    version: str
    activity_type: str
    task_queue: str = "default"
    description: str = ""
    retry_policy: Optional[Dict[str, Any]] = None
    timeout_config: Dict[str, Any] = field(default_factory=dict)
    heartbeat_timeout: int = 30
    schedule_to_start_timeout: int = 60
    schedule_to_close_timeout: int = 120
    start_to_close_timeout: int = 120
    registered_at: Optional[datetime] = None
    execution_count: int = 0


@dataclass
class ActivityExecution:
    """Activity execution details"""
    execution_id: str
    activity_id: str
    activity_name: str
    status: ActivityStatus
    schedule_time: datetime
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    task_queue: str = "default"
    attempt: int = 1
    max_attempts: int = 3
    heartbeat_details: Optional[Dict[str, Any]] = None
    input: Optional[Any] = None
    output: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class ChildWorkflowExecution:
    """Child workflow execution details"""
    child_execution_id: str
    parent_execution_id: str
    child_workflow_id: str
    child_workflow_name: str
    status: WorkflowStatus
    policy: ChildWorkflowPolicy
    start_time: datetime
    end_time: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class WorkflowQuery:
    """Workflow query details"""
    query_type: str
    query_id: str
    workflow_execution_id: str
    query_handler: Optional[Callable] = None
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SearchAttribute:
    """Search attribute definition"""
    name: str
    value_type: str
    indexed: bool = True
    value: Optional[Any] = None


@dataclass
class NamespaceConfig:
    """Temporal namespace configuration"""
    name: str
    description: str = ""
    retention_days: int = 7
    active_bins: int = 1
    prometheus_metrics: bool = False
    is_default: bool = False
    is_global: bool = False
    history_shard_count: int = 1
    cluster_name: str = "default"


@dataclass
class TaskQueueConfig:
    """Task queue configuration"""
    name: str
    task_queue_type: str = "normal"
    build_id: Optional[str] = None
    max_tasks_per_second: Optional[float] = None
    max_concurrent_workflow_tasks: Optional[int] = None
    max_concurrent_activity_tasks: Optional[int] = None
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HistoryEvent:
    """Workflow history event"""
    event_id: int
    event_type: str
    timestamp: datetime
    workflow_execution_id: str
    event_data: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TemporalUIConfig:
    """Temporal web UI configuration"""
    host: str = "localhost"
    port: int = 8088
    base_url: str = "http://localhost:8088"
    namespace: str = "default"
    auth_enabled: bool = False
    tls_enabled: bool = False
    tls_cert_path: Optional[str] = None
    tls_key_path: Optional[str] = None


class WorkflowReplayResult:
    """Result of workflow replay"""
    success: bool
    replayed_events: int
    mismatches: List[Dict[str, Any]]
    error: Optional[str]
    duration_seconds: float


class TemporalManager:
    """
    Temporal Workflow Engine Manager
    
    Provides comprehensive integration with Temporal workflow engine including:
    - Workflow registration and management
    - Workflow execution (start, signal, cancel)
    - Activity registration and execution
    - Child workflow management
    - Query handling
    - Search attributes management
    - Namespace management
    - Task queue management
    - Workflow history access and replay
    - Temporal web UI integration
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 7233,
        namespace: str = "default",
        tls_enabled: bool = False,
        tls_cert_path: Optional[str] = None,
        tls_key_path: Optional[str] = None
    ):
        """
        Initialize TemporalManager
        
        Args:
            host: Temporal server host
            port: Temporal server port
            namespace: Default namespace
            tls_enabled: Enable TLS connection
            tls_cert_path: Path to TLS certificate
            tls_key_path: Path to TLS key
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.tls_enabled = tls_enabled
        self.tls_cert_path = tls_cert_path
        self.tls_key_path = tls_key_path
        
        # Connection state
        self._connected = False
        self._connection_id = str(uuid.uuid4())
        self._last_ping: Optional[datetime] = None
        
        # Registered workflows
        self._workflows: Dict[str, WorkflowRegistration] = {}
        self._workflow_implementations: Dict[str, Type] = {}
        
        # Active executions
        self._executions: Dict[str, WorkflowExecution] = {}
        self._execution_history: Dict[str, List[HistoryEvent]] = defaultdict(list)
        
        # Registered activities
        self._activities: Dict[str, ActivityRegistration] = {}
        self._activity_implementations: Dict[str, Callable] = {}
        self._activity_executions: Dict[str, ActivityExecution] = {}
        
        # Child workflows
        self._child_workflows: Dict[str, ChildWorkflowExecution] = {}
        
        # Queries
        self._query_handlers: Dict[str, Callable] = {}
        self._pending_queries: Dict[str, WorkflowQuery] = {}
        
        # Search attributes
        self._search_attribute_definitions: Dict[str, SearchAttribute] = {}
        self._search_attribute_indexes: Dict[str, List[str]] = defaultdict(list)
        
        # Namespaces
        self._namespaces: Dict[str, NamespaceConfig] = {}
        self._active_namespace = namespace
        
        # Task queues
        self._task_queues: Dict[str, TaskQueueConfig] = {}
        
        # UI config
        self._ui_config = TemporalUIConfig(namespace=namespace)
        
        # Event callbacks
        self._workflow_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        self._activity_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        # Background tasks
        self._background_tasks: Dict[str, threading.Event] = {}
        self._polling_thread: Optional[threading.Thread] = None
        self._running = False
        
        logger.info(f"TemporalManager initialized for {host}:{port}, namespace={namespace}")
    
    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================
    
    async def connect(self) -> bool:
        """
        Connect to Temporal server
        
        Returns:
            True if connection successful
        """
        with self._lock:
            try:
                # Simulate connection
                logger.info(f"Connecting to Temporal at {self.host}:{self.port}")
                
                # In real implementation, this would use temporalio client SDK
                # from temporalio.client import Client
                # self._client = await Client.connect(f"{self.host}:{self.port}")
                
                self._connected = True
                self._last_ping = datetime.now()
                logger.info("Successfully connected to Temporal server")
                return True
                
            except Exception as e:
                logger.error(f"Failed to connect to Temporal: {e}")
                self._connected = False
                return False
    
    async def disconnect(self) -> bool:
        """
        Disconnect from Temporal server
        
        Returns:
            True if disconnection successful
        """
        with self._lock:
            try:
                self._running = False
                if self._polling_thread and self._polling_thread.is_alive():
                    self._polling_thread.join(timeout=5)
                
                self._connected = False
                logger.info("Disconnected from Temporal server")
                return True
                
            except Exception as e:
                logger.error(f"Error disconnecting from Temporal: {e}")
                return False
    
    def is_connected(self) -> bool:
        """Check if connected to Temporal server"""
        return self._connected
    
    async def ping(self) -> bool:
        """
        Ping Temporal server to check connectivity
        
        Returns:
            True if server responds
        """
        try:
            # In real implementation, this would check server health
            # response = await self._client.health()
            # self._last_ping = datetime.now()
            # return response.ok
            self._last_ping = datetime.now()
            return self._connected
        except Exception as e:
            logger.warning(f"Ping failed: {e}")
            return False
    
    # =========================================================================
    # WORKFLOW REGISTRATION
    # =========================================================================
    
    def register_workflow(
        self,
        name: str,
        version: str,
        workflow_type: str = "standard",
        task_queue: str = "default",
        description: str = "",
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_config: Optional[Dict[str, Any]] = None
    ) -> WorkflowRegistration:
        """
        Register a workflow type with Temporal
        
        Args:
            name: Workflow name
            version: Workflow version
            workflow_type: Type of workflow
            task_queue: Task queue for workflow
            description: Workflow description
            retry_policy: Retry policy configuration
            timeout_config: Timeout configuration
            
        Returns:
            WorkflowRegistration object
        """
        with self._lock:
            registration = WorkflowRegistration(
                name=name,
                version=version,
                workflow_type=workflow_type,
                task_queue=task_queue,
                description=description,
                retry_policy=retry_policy,
                timeout_config=timeout_config or {},
                registered_at=datetime.now()
            )
            
            key = f"{name}:{version}"
            self._workflows[key] = registration
            
            logger.info(f"Registered workflow: {key}")
            return registration
    
    def register_workflow_implementation(
        self,
        name: str,
        version: str,
        implementation: Type
    ) -> bool:
        """
        Register workflow implementation class
        
        Args:
            name: Workflow name
            version: Workflow version
            implementation: Workflow implementation class
            
        Returns:
            True if registration successful
        """
        with self._lock:
            key = f"{name}:{version}"
            self._workflow_implementations[key] = implementation
            logger.debug(f"Registered workflow implementation: {key}")
            return True
    
    def get_workflow_registration(
        self,
        name: str,
        version: Optional[str] = None
    ) -> Optional[WorkflowRegistration]:
        """
        Get workflow registration details
        
        Args:
            name: Workflow name
            version: Workflow version (latest if not specified)
            
        Returns:
            WorkflowRegistration if found
        """
        with self._lock:
            if version:
                key = f"{name}:{version}"
                return self._workflows.get(key)
            else:
                # Find latest version
                matching = {k: v for k, v in self._workflows.items() if k.startswith(f"{name}:")}
                if matching:
                    return matching[sorted(matching.keys())[-1]]
            return None
    
    def list_workflows(
        self,
        task_queue: Optional[str] = None,
        workflow_type: Optional[str] = None
    ) -> List[WorkflowRegistration]:
        """
        List registered workflows
        
        Args:
            task_queue: Filter by task queue
            workflow_type: Filter by workflow type
            
        Returns:
            List of WorkflowRegistration objects
        """
        with self._lock:
            result = list(self._workflows.values())
            if task_queue:
                result = [w for w in result if w.task_queue == task_queue]
            if workflow_type:
                result = [w for w in result if w.workflow_type == workflow_type]
            return result
    
    def unregister_workflow(self, name: str, version: str) -> bool:
        """
        Unregister a workflow
        
        Args:
            name: Workflow name
            version: Workflow version
            
        Returns:
            True if unregistered successfully
        """
        with self._lock:
            key = f"{name}:{version}"
            if key in self._workflows:
                del self._workflows[key]
                if key in self._workflow_implementations:
                    del self._workflow_implementations[key]
                logger.info(f"Unregistered workflow: {key}")
                return True
            return False
    
    # =========================================================================
    # WORKFLOW EXECUTION
    # =========================================================================
    
    async def start_workflow(
        self,
        workflow_name: str,
        workflow_version: Optional[str] = None,
        workflow_id: Optional[str] = None,
        task_queue: Optional[str] = None,
        input_args: Tuple = (),
        input_kwargs: Dict[str, Any] = None,
        parent_execution_id: Optional[str] = None,
        search_attributes: Optional[Dict[str, Any]] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_config: Optional[Dict[str, Any]] = None
    ) -> WorkflowExecution:
        """
        Start a workflow execution
        
        Args:
            workflow_name: Name of workflow to start
            workflow_version: Version of workflow
            workflow_id: Specific workflow ID (generated if not provided)
            task_queue: Task queue to use
            input_args: Workflow input arguments
            input_kwargs: Workflow input keyword arguments
            parent_execution_id: Parent workflow execution ID for child workflows
            search_attributes: Search attributes for the workflow
            retry_policy: Override retry policy
            timeout_config: Override timeout configuration
            
        Returns:
            WorkflowExecution object
        """
        with self._lock:
            execution_id = workflow_id or f"{workflow_name}-{uuid.uuid4().hex[:8]}"
            
            # Get workflow registration
            registration = self.get_workflow_registration(workflow_name, workflow_version)
            queue = task_queue or (registration.task_queue if registration else "default")
            
            execution = WorkflowExecution(
                execution_id=execution_id,
                workflow_id=execution_id,
                workflow_name=workflow_name,
                status=WorkflowStatus.RUNNING,
                start_time=datetime.now(),
                task_queue=queue,
                parent_execution_id=parent_execution_id,
                root_execution_id=parent_execution_id or execution_id,
                search_attributes=search_attributes or {},
                runtime_config={
                    "input_args": input_args,
                    "input_kwargs": input_kwargs or {},
                    "retry_policy": retry_policy,
                    "timeout_config": timeout_config or {}
                }
            )
            
            self._executions[execution_id] = execution
            
            # Update registration stats
            if registration:
                registration.last_execution = datetime.now()
                registration.execution_count += 1
            
            logger.info(f"Started workflow: {workflow_name} (id={execution_id})")
            
            # Trigger callbacks
            await self._trigger_workflow_callback("started", execution)
            
            # Start background task for this execution
            self._start_execution_task(execution_id)
            
            return execution
    
    async def signal_workflow(
        self,
        execution_id: str,
        signal_name: str,
        signal_data: Any = None
    ) -> bool:
        """
        Send a signal to a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            signal_name: Name of the signal
            signal_data: Signal payload
            
        Returns:
            True if signal sent successfully
        """
        with self._lock:
            if execution_id not in self._executions:
                logger.warning(f"Workflow execution not found: {execution_id}")
                return False
            
            execution = self._executions[execution_id]
            
            signal_record = {
                "signal_name": signal_name,
                "signal_data": signal_data,
                "timestamp": datetime.now().isoformat(),
                "type": SignalType.EXTERNAL.value
            }
            
            execution.signals.append(signal_record)
            
            logger.info(f"Signal sent to workflow {execution_id}: {signal_name}")
            await self._trigger_workflow_callback("signaled", execution, signal_name, signal_data)
            
            return True
    
    async def cancel_workflow(
        self,
        execution_id: str,
        reason: Optional[str] = None
    ) -> bool:
        """
        Cancel a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            reason: Cancellation reason
            
        Returns:
            True if cancellation successful
        """
        with self._lock:
            if execution_id not in self._executions:
                logger.warning(f"Workflow execution not found: {execution_id}")
                return False
            
            execution = self._executions[execution_id]
            execution.status = WorkflowStatus.CANCELED
            execution.end_time = datetime.now()
            
            logger.info(f"Canceled workflow: {execution_id}, reason={reason}")
            await self._trigger_workflow_callback("canceled", execution, reason)
            
            return True
    
    async def terminate_workflow(
        self,
        execution_id: str,
        reason: Optional[str] = None,
        error: Optional[str] = None
    ) -> bool:
        """
        Terminate a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            reason: Termination reason
            error: Error details
            
        Returns:
            True if termination successful
        """
        with self._lock:
            if execution_id not in self._executions:
                logger.warning(f"Workflow execution not found: {execution_id}")
                return False
            
            execution = self._executions[execution_id]
            execution.status = WorkflowStatus.TERMINATED
            execution.end_time = datetime.now()
            execution.error = error
            
            # Cancel child workflows
            for child_id in execution.child_executions:
                if child_id in self._executions:
                    await self.cancel_workflow(child_id, "Parent terminated")
            
            logger.info(f"Terminated workflow: {execution_id}, reason={reason}")
            await self._trigger_workflow_callback("terminated", execution, reason, error)
            
            return True
    
    def get_workflow_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """
        Get workflow execution details
        
        Args:
            execution_id: Workflow execution ID
            
        Returns:
            WorkflowExecution if found
        """
        with self._lock:
            return self._executions.get(execution_id)
    
    def list_workflow_executions(
        self,
        status: Optional[WorkflowStatus] = None,
        workflow_name: Optional[str] = None,
        limit: int = 100
    ) -> List[WorkflowExecution]:
        """
        List workflow executions
        
        Args:
            status: Filter by status
            workflow_name: Filter by workflow name
            limit: Maximum number of results
            
        Returns:
            List of WorkflowExecution objects
        """
        with self._lock:
            result = list(self._executions.values())
            
            if status:
                result = [e for e in result if e.status == status]
            if workflow_name:
                result = [e for e in result if e.workflow_name == workflow_name]
            
            # Sort by start time descending
            result.sort(key=lambda e: e.start_time, reverse=True)
            
            return result[:limit]
    
    async def complete_workflow(
        self,
        execution_id: str,
        result: Any = None
    ) -> bool:
        """
        Complete a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            result: Workflow result
            
        Returns:
            True if completed successfully
        """
        with self._lock:
            if execution_id not in self._executions:
                return False
            
            execution = self._executions[execution_id]
            execution.status = WorkflowStatus.COMPLETED
            execution.end_time = datetime.now()
            execution.result = result
            
            logger.info(f"Completed workflow: {execution_id}")
            await self._trigger_workflow_callback("completed", execution, result)
            
            return True
    
    async def fail_workflow(
        self,
        execution_id: str,
        error: str
    ) -> bool:
        """
        Mark a workflow as failed
        
        Args:
            execution_id: Workflow execution ID
            error: Error message
            
        Returns:
            True if marked as failed
        """
        with self._lock:
            if execution_id not in self._executions:
                return False
            
            execution = self._executions[execution_id]
            execution.status = WorkflowStatus.FAILED
            execution.end_time = datetime.now()
            execution.error = error
            
            logger.error(f"Workflow failed: {execution_id}, error={error}")
            await self._trigger_workflow_callback("failed", execution, error)
            
            return True
    
    # =========================================================================
    # ACTIVITY REGISTRATION
    # =========================================================================
    
    def register_activity(
        self,
        name: str,
        version: str,
        activity_type: str = "standard",
        task_queue: str = "default",
        description: str = "",
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_config: Optional[Dict[str, Any]] = None,
        heartbeat_timeout: int = 30,
        schedule_to_start_timeout: int = 60,
        schedule_to_close_timeout: int = 120,
        start_to_close_timeout: int = 120
    ) -> ActivityRegistration:
        """
        Register an activity type with Temporal
        
        Args:
            name: Activity name
            version: Activity version
            activity_type: Type of activity
            task_queue: Task queue for activity
            description: Activity description
            retry_policy: Retry policy configuration
            timeout_config: Timeout configuration
            heartbeat_timeout: Heartbeat timeout in seconds
            schedule_to_start_timeout: Schedule to start timeout
            schedule_to_close_timeout: Schedule to close timeout
            start_to_close_timeout: Start to close timeout
            
        Returns:
            ActivityRegistration object
        """
        with self._lock:
            registration = ActivityRegistration(
                name=name,
                version=version,
                activity_type=activity_type,
                task_queue=task_queue,
                description=description,
                retry_policy=retry_policy,
                timeout_config=timeout_config or {},
                heartbeat_timeout=heartbeat_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                schedule_to_close_timeout=schedule_to_close_timeout,
                start_to_close_timeout=start_to_close_timeout,
                registered_at=datetime.now()
            )
            
            key = f"{name}:{version}"
            self._activities[key] = registration
            
            logger.info(f"Registered activity: {key}")
            return registration
    
    def register_activity_implementation(
        self,
        name: str,
        version: str,
        implementation: Callable
    ) -> bool:
        """
        Register activity implementation function
        
        Args:
            name: Activity name
            version: Activity version
            implementation: Activity implementation function
            
        Returns:
            True if registration successful
        """
        with self._lock:
            key = f"{name}:{version}"
            self._activity_implementations[key] = implementation
            logger.debug(f"Registered activity implementation: {key}")
            return True
    
    def get_activity_registration(
        self,
        name: str,
        version: Optional[str] = None
    ) -> Optional[ActivityRegistration]:
        """
        Get activity registration details
        
        Args:
            name: Activity name
            version: Activity version
            
        Returns:
            ActivityRegistration if found
        """
        with self._lock:
            if version:
                key = f"{name}:{version}"
                return self._activities.get(key)
            else:
                matching = {k: v for k, v in self._activities.items() if k.startswith(f"{name}:")}
                if matching:
                    return matching[sorted(matching.keys())[-1]]
            return None
    
    def list_activities(
        self,
        task_queue: Optional[str] = None,
        activity_type: Optional[str] = None
    ) -> List[ActivityRegistration]:
        """
        List registered activities
        
        Args:
            task_queue: Filter by task queue
            activity_type: Filter by activity type
            
        Returns:
            List of ActivityRegistration objects
        """
        with self._lock:
            result = list(self._activities.values())
            if task_queue:
                result = [a for a in result if a.task_queue == task_queue]
            if activity_type:
                result = [a for a in result if a.activity_type == activity_type]
            return result
    
    def unregister_activity(self, name: str, version: str) -> bool:
        """
        Unregister an activity
        
        Args:
            name: Activity name
            version: Activity version
            
        Returns:
            True if unregistered successfully
        """
        with self._lock:
            key = f"{name}:{version}"
            if key in self._activities:
                del self._activities[key]
                if key in self._activity_implementations:
                    del self._activity_implementations[key]
                logger.info(f"Unregistered activity: {key}")
                return True
            return False
    
    # =========================================================================
    # ACTIVITY EXECUTION
    # =========================================================================
    
    async def execute_activity(
        self,
        activity_name: str,
        activity_version: Optional[str] = None,
        activity_id: Optional[str] = None,
        task_queue: Optional[str] = None,
        input_args: Tuple = (),
        input_kwargs: Dict[str, Any] = None,
        workflow_execution_id: Optional[str] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
        heartbeat_callback: Optional[Callable] = None
    ) -> ActivityExecution:
        """
        Execute an activity
        
        Args:
            activity_name: Name of activity to execute
            activity_version: Version of activity
            activity_id: Specific activity ID
            task_queue: Task queue to use
            input_args: Activity input arguments
            input_kwargs: Activity input keyword arguments
            workflow_execution_id: Associated workflow execution ID
            retry_policy: Override retry policy
            heartbeat_callback: Callback for heartbeats
            
        Returns:
            ActivityExecution object
        """
        with self._lock:
            execution_id = activity_id or f"{activity_name}-{uuid.uuid4().hex[:8]}"
            
            # Get activity registration
            registration = self.get_activity_registration(activity_name, activity_version)
            queue = task_queue or (registration.task_queue if registration else "default")
            
            execution = ActivityExecution(
                execution_id=execution_id,
                activity_id=execution_id,
                activity_name=activity_name,
                status=ActivityStatus.SCHEDULED,
                schedule_time=datetime.now(),
                task_queue=queue,
                attempt=1,
                max_attempts=retry_policy.get("max_attempts", 3) if retry_policy else 3,
                input=(input_args, input_kwargs or {})
            )
            
            self._activity_executions[execution_id] = execution
            
            # Update registration stats
            if registration:
                registration.execution_count += 1
            
            logger.info(f"Scheduled activity: {activity_name} (id={execution_id})")
            
            # Simulate activity execution in background
            asyncio.create_task(self._run_activity(execution, heartbeat_callback))
            
            return execution
    
    async def _run_activity(
        self,
        execution: ActivityExecution,
        heartbeat_callback: Optional[Callable] = None
    ):
        """Internal method to run an activity"""
        try:
            # Mark as started
            execution.status = ActivityStatus.STARTED
            execution.start_time = datetime.now()
            
            # Get implementation
            key = f"{execution.activity_name}"
            implementation = self._activity_implementations.get(key)
            
            if implementation:
                args, kwargs = execution.input or ((), {})
                result = await implementation(*args, **kwargs)
                execution.output = result
                execution.status = ActivityStatus.COMPLETED
            else:
                # Simulate execution
                await asyncio.sleep(0.1)
                execution.output = {"status": "completed", "simulated": True}
                execution.status = ActivityStatus.COMPLETED
            
            execution.end_time = datetime.now()
            
        except Exception as e:
            execution.status = ActivityStatus.FAILED
            execution.error = str(e)
            execution.end_time = datetime.now()
            logger.error(f"Activity failed: {execution.activity_id}, error={e}")
    
    async def record_activity_heartbeat(
        self,
        execution_id: str,
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Record activity heartbeat
        
        Args:
            execution_id: Activity execution ID
            details: Heartbeat details
            
        Returns:
            True if heartbeat recorded
        """
        with self._lock:
            if execution_id not in self._activity_executions:
                return False
            
            execution = self._activity_executions[execution_id]
            execution.status = ActivityStatus.HEARTBEAT
            execution.heartbeat_details = details
            
            return True
    
    def get_activity_execution(self, execution_id: str) -> Optional[ActivityExecution]:
        """
        Get activity execution details
        
        Args:
            execution_id: Activity execution ID
            
        Returns:
            ActivityExecution if found
        """
        with self._lock:
            return self._activity_executions.get(execution_id)
    
    # =========================================================================
    # CHILD WORKFLOWS
    # =========================================================================
    
    async def start_child_workflow(
        self,
        parent_execution_id: str,
        workflow_name: str,
        workflow_version: Optional[str] = None,
        workflow_id: Optional[str] = None,
        task_queue: Optional[str] = None,
        input_args: Tuple = (),
        input_kwargs: Dict[str, Any] = None,
        policy: ChildWorkflowPolicy = ChildWorkflowPolicy.ALLOW_PARALLEL,
        search_attributes: Optional[Dict[str, Any]] = None
    ) -> ChildWorkflowExecution:
        """
        Start a child workflow from a parent workflow
        
        Args:
            parent_execution_id: Parent workflow execution ID
            workflow_name: Child workflow name
            workflow_version: Child workflow version
            workflow_id: Specific workflow ID
            task_queue: Task queue to use
            input_args: Workflow input arguments
            input_kwargs: Workflow input keyword arguments
            policy: Child workflow policy
            search_attributes: Search attributes for child workflow
            
        Returns:
            ChildWorkflowExecution object
        """
        with self._lock:
            parent = self._executions.get(parent_execution_id)
            if not parent:
                raise ValueError(f"Parent execution not found: {parent_execution_id}")
            
            # Start child workflow
            child_execution_id = workflow_id or f"{workflow_name}-{uuid.uuid4().hex[:8]}"
            
            child = ChildWorkflowExecution(
                child_execution_id=child_execution_id,
                parent_execution_id=parent_execution_id,
                child_workflow_id=child_execution_id,
                child_workflow_name=workflow_name,
                status=WorkflowStatus.RUNNING,
                policy=policy,
                start_time=datetime.now()
            )
            
            self._child_workflows[child_execution_id] = child
            
            # Add to parent's child executions
            parent.child_executions.append(child_execution_id)
            
            # Start actual workflow execution
            execution = await self.start_workflow(
                workflow_name=workflow_name,
                workflow_version=workflow_version,
                workflow_id=child_execution_id,
                task_queue=task_queue,
                input_args=input_args,
                input_kwargs=input_kwargs,
                parent_execution_id=parent_execution_id,
                search_attributes=search_attributes
            )
            
            logger.info(f"Started child workflow: {workflow_name} (id={child_execution_id})")
            
            return child
    
    async def wait_for_child_workflow(
        self,
        child_execution_id: str,
        timeout_seconds: Optional[float] = None
    ) -> Any:
        """
        Wait for a child workflow to complete
        
        Args:
            child_execution_id: Child workflow execution ID
            timeout_seconds: Timeout in seconds
            
        Returns:
            Child workflow result
        """
        start_time = time.time()
        
        while True:
            with self._lock:
                child = self._child_workflows.get(child_execution_id)
                if not child:
                    raise ValueError(f"Child workflow not found: {child_execution_id}")
                
                if child.status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, 
                                   WorkflowStatus.CANCELED, WorkflowStatus.TERMINATED]:
                    if child.error:
                        raise Exception(child.error)
                    return child.result
            
            if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                raise TimeoutError(f"Timeout waiting for child workflow: {child_execution_id}")
            
            await asyncio.sleep(0.1)
    
    def get_child_workflow(self, child_execution_id: str) -> Optional[ChildWorkflowExecution]:
        """
        Get child workflow execution details
        
        Args:
            child_execution_id: Child workflow execution ID
            
        Returns:
            ChildWorkflowExecution if found
        """
        with self._lock:
            return self._child_workflows.get(child_execution_id)
    
    def list_child_workflows(
        self,
        parent_execution_id: str
    ) -> List[ChildWorkflowExecution]:
        """
        List child workflows for a parent
        
        Args:
            parent_execution_id: Parent workflow execution ID
            
        Returns:
            List of ChildWorkflowExecution objects
        """
        with self._lock:
            return [c for c in self._child_workflows.values() 
                   if c.parent_execution_id == parent_execution_id]
    
    async def cancel_child_workflow(
        self,
        child_execution_id: str,
        reason: Optional[str] = None
    ) -> bool:
        """
        Cancel a child workflow
        
        Args:
            child_execution_id: Child workflow execution ID
            reason: Cancellation reason
            
        Returns:
            True if cancellation successful
        """
        with self._lock:
            child = self._child_workflows.get(child_execution_id)
            if not child:
                return False
            
            # Cancel the actual workflow
            success = await self.cancel_workflow(child_execution_id, reason)
            
            if success:
                child.status = WorkflowStatus.CANCELED
                child.end_time = datetime.now()
            
            return success
    
    # =========================================================================
    # QUERY HANDLING
    # =========================================================================
    
    def register_query_handler(
        self,
        query_type: str,
        handler: Callable,
        description: str = ""
    ) -> bool:
        """
        Register a workflow query handler
        
        Args:
            query_type: Type of query
            handler: Query handler function
            description: Query description
            
        Returns:
            True if registration successful
        """
        with self._lock:
            self._query_handlers[query_type] = handler
            logger.debug(f"Registered query handler: {query_type}")
            return True
    
    async def handle_query(
        self,
        execution_id: str,
        query_type: str,
        query_args: Tuple = (),
        query_kwargs: Dict[str, Any] = None
    ) -> Any:
        """
        Handle a workflow query
        
        Args:
            execution_id: Workflow execution ID
            query_type: Type of query
            query_args: Query arguments
            query_kwargs: Query keyword arguments
            
        Returns:
            Query result
        """
        with self._lock:
            execution = self._executions.get(execution_id)
            if not execution:
                raise ValueError(f"Workflow execution not found: {execution_id}")
            
            # Create query record
            query_id = f"{execution_id}:{query_type}:{uuid.uuid4().hex[:8]}"
            query = WorkflowQuery(
                query_type=query_type,
                query_id=query_id,
                workflow_execution_id=execution_id,
                args=query_args,
                kwargs=query_kwargs or {}
            )
            
            self._pending_queries[query_id] = query
            execution.queries.append({
                "query_type": query_type,
                "query_id": query_id,
                "timestamp": datetime.now().isoformat()
            })
            
            logger.info(f"Handling query: {query_type} for workflow {execution_id}")
            
            # Get handler
            handler = self._query_handlers.get(query_type)
            
            if handler:
                try:
                    result = await handler(*query_args, **(query_kwargs or {}))
                    query.result = result
                    return result
                except Exception as e:
                    query.error = str(e)
                    raise
            else:
                # Return default execution state
                return {
                    "execution_id": execution_id,
                    "status": execution.status.value,
                    "start_time": execution.start_time.isoformat(),
                    "signals": len(execution.signals),
                    "child_executions": len(execution.child_executions)
                }
    
    def get_pending_queries(self, execution_id: str) -> List[WorkflowQuery]:
        """
        Get pending queries for a workflow
        
        Args:
            execution_id: Workflow execution ID
            
        Returns:
            List of pending queries
        """
        with self._lock:
            return [q for q in self._pending_queries.values() 
                   if q.workflow_execution_id == execution_id]
    
    # =========================================================================
    # SEARCH ATTRIBUTES
    # =========================================================================
    
    def define_search_attribute(
        self,
        name: str,
        value_type: str,
        indexed: bool = True
    ) -> SearchAttribute:
        """
        Define a search attribute
        
        Args:
            name: Attribute name
            value_type: Type of value (string, keyword, int, float, bool, datetime)
            indexed: Whether to index the attribute
            
        Returns:
            SearchAttribute object
        """
        with self._lock:
            attr = SearchAttribute(
                name=name,
                value_type=value_type,
                indexed=indexed
            )
            self._search_attribute_definitions[name] = attr
            
            if indexed:
                self._search_attribute_indexes[name] = []
            
            logger.info(f"Defined search attribute: {name} ({value_type})")
            return attr
    
    def set_search_attribute(
        self,
        execution_id: str,
        attribute_name: str,
        value: Any
    ) -> bool:
        """
        Set a search attribute for a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            attribute_name: Name of attribute
            value: Attribute value
            
        Returns:
            True if set successfully
        """
        with self._lock:
            execution = self._executions.get(execution_id)
            if not execution:
                return False
            
            execution.search_attributes[attribute_name] = value
            
            # Update index
            if attribute_name in self._search_attribute_indexes:
                self._search_attribute_indexes[attribute_name].append(execution_id)
            
            return True
    
    def get_search_attribute(
        self,
        execution_id: str,
        attribute_name: str
    ) -> Optional[Any]:
        """
        Get a search attribute for a workflow execution
        
        Args:
            execution_id: Workflow execution ID
            attribute_name: Attribute name
            
        Returns:
            Attribute value if found
        """
        with self._lock:
            execution = self._executions.get(execution_id)
            if not execution:
                return None
            return execution.search_attributes.get(attribute_name)
    
    def search_executions(
        self,
        query: Dict[str, Any],
        namespace: Optional[str] = None,
        limit: int = 100
    ) -> List[WorkflowExecution]:
        """
        Search workflow executions by search attributes
        
        Args:
            query: Search query (attribute: value pairs)
            namespace: Namespace to search in
            limit: Maximum results
            
        Returns:
            List of matching WorkflowExecution objects
        """
        with self._lock:
            results = []
            
            for execution in self._executions.values():
                if namespace and self.namespace != namespace:
                    continue
                
                match = True
                for attr, value in query.items():
                    if execution.search_attributes.get(attr) != value:
                        match = False
                        break
                
                if match:
                    results.append(execution)
                
                if len(results) >= limit:
                    break
            
            return results
    
    def list_search_attributes(self) -> List[SearchAttribute]:
        """
        List all defined search attributes
        
        Returns:
            List of SearchAttribute objects
        """
        with self._lock:
            return list(self._search_attribute_definitions.values())
    
    # =========================================================================
    # NAMESPACES
    # =========================================================================
    
    def register_namespace(
        self,
        name: str,
        description: str = "",
        retention_days: int = 7,
        active_bins: int = 1,
        prometheus_metrics: bool = False,
        is_default: bool = False,
        is_global: bool = False
    ) -> NamespaceConfig:
        """
        Register a Temporal namespace
        
        Args:
            name: Namespace name
            description: Namespace description
            retention_days: History retention period
            active_bins: Number of active bins
            prometheus_metrics: Enable Prometheus metrics
            is_default: Set as default namespace
            is_global: Is a global namespace
            
        Returns:
            NamespaceConfig object
        """
        with self._lock:
            namespace = NamespaceConfig(
                name=name,
                description=description,
                retention_days=retention_days,
                active_bins=active_bins,
                prometheus_metrics=prometheus_metrics,
                is_default=is_default,
                is_global=is_global
            )
            
            self._namespaces[name] = namespace
            
            if is_default:
                # Unset other defaults
                for ns in self._namespaces.values():
                    ns.is_default = False
                namespace.is_default = True
            
            logger.info(f"Registered namespace: {name}")
            return namespace
    
    def get_namespace(self, name: str) -> Optional[NamespaceConfig]:
        """
        Get namespace configuration
        
        Args:
            name: Namespace name
            
        Returns:
            NamespaceConfig if found
        """
        with self._lock:
            return self._namespaces.get(name)
    
    def list_namespaces(self) -> List[NamespaceConfig]:
        """
        List all namespaces
        
        Returns:
            List of NamespaceConfig objects
        """
        with self._lock:
            return list(self._namespaces.values())
    
    def set_active_namespace(self, name: str) -> bool:
        """
        Set the active namespace
        
        Args:
            name: Namespace name
            
        Returns:
            True if set successfully
        """
        with self._lock:
            if name in self._namespaces or name == "default":
                self._active_namespace = name
                logger.info(f"Switched to namespace: {name}")
                return True
            return False
    
    def get_active_namespace(self) -> str:
        """
        Get the active namespace
        
        Returns:
            Active namespace name
        """
        return self._active_namespace
    
    async def describe_namespace(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed namespace information
        
        Args:
            name: Namespace name
            
        Returns:
            Namespace details
        """
        namespace = self.get_namespace(name) or (NamespaceConfig(name=name) if name == "default" else None)
        if namespace:
            return {
                "name": namespace.name,
                "description": namespace.description,
                "retention_days": namespace.retention_days,
                "active_bins": namespace.active_bins,
                "prometheus_metrics": namespace.prometheus_metrics,
                "is_default": namespace.is_default,
                "is_global": namespace.is_global,
                "workflow_count": len([e for e in self._executions.values()]),
                "task_queue_count": len(self._task_queues)
            }
        return None
    
    # =========================================================================
    # TASK QUEUES
    # =========================================================================
    
    def register_task_queue(
        self,
        name: str,
        task_queue_type: str = "normal",
        build_id: Optional[str] = None,
        max_tasks_per_second: Optional[float] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activity_tasks: Optional[int] = None
    ) -> TaskQueueConfig:
        """
        Register a task queue
        
        Args:
            name: Task queue name
            task_queue_type: Type of task queue
            build_id: Build ID for versioned queues
            max_tasks_per_second: Rate limiting
            max_concurrent_workflow_tasks: Max concurrent workflow tasks
            max_concurrent_activity_tasks: Max concurrent activity tasks
            
        Returns:
            TaskQueueConfig object
        """
        with self._lock:
            config = TaskQueueConfig(
                name=name,
                task_queue_type=task_queue_type,
                build_id=build_id,
                max_tasks_per_second=max_tasks_per_second,
                max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
                max_concurrent_activity_tasks=max_concurrent_activity_tasks
            )
            
            self._task_queues[name] = config
            logger.info(f"Registered task queue: {name}")
            return config
    
    def get_task_queue(self, name: str) -> Optional[TaskQueueConfig]:
        """
        Get task queue configuration
        
        Args:
            name: Task queue name
            
        Returns:
            TaskQueueConfig if found
        """
        with self._lock:
            return self._task_queues.get(name)
    
    def list_task_queues(self) -> List[TaskQueueConfig]:
        """
        List all task queues
        
        Returns:
            List of TaskQueueConfig objects
        """
        with self._lock:
            return list(self._task_queues.values())
    
    async def describe_task_queue(self, name: str) -> Dict[str, Any]:
        """
        Get detailed task queue information
        
        Args:
            name: Task queue name
            
        Returns:
            Task queue details
        """
        config = self.get_task_queue(name)
        if not config:
            config = TaskQueueConfig(name=name)
        
        with self._lock:
            # Count executions using this queue
            workflow_count = len([e for e in self._executions.values() if e.task_queue == name])
            activity_count = len([e for e in self._activity_executions.values() if e.task_queue == name])
            
            return {
                "name": config.name,
                "task_queue_type": config.task_queue_type,
                "build_id": config.build_id,
                "max_tasks_per_second": config.max_tasks_per_second,
                "max_concurrent_workflow_tasks": config.max_concurrent_workflow_tasks,
                "max_concurrent_activity_tasks": config.max_concurrent_activity_tasks,
                "workflow_executions": workflow_count,
                "activity_executions": activity_count,
                "pollers": [],
                "backlog": 0
            }
    
    async def update_task_queue(
        self,
        name: str,
        max_tasks_per_second: Optional[float] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activity_tasks: Optional[int] = None
    ) -> bool:
        """
        Update task queue configuration
        
        Args:
            name: Task queue name
            max_tasks_per_second: New rate limit
            max_concurrent_workflow_tasks: New max workflow tasks
            max_concurrent_activity_tasks: New max activity tasks
            
        Returns:
            True if updated successfully
        """
        with self._lock:
            if name not in self._task_queues:
                return False
            
            config = self._task_queues[name]
            if max_tasks_per_second is not None:
                config.max_tasks_per_second = max_tasks_per_second
            if max_concurrent_workflow_tasks is not None:
                config.max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
            if max_concurrent_activity_tasks is not None:
                config.max_concurrent_activity_tasks = max_concurrent_activity_tasks
            
            config.version += 1
            return True
    
    # =========================================================================
    # WORKFLOW HISTORY
    # =========================================================================
    
    def record_history_event(
        self,
        execution_id: str,
        event_type: str,
        event_data: Dict[str, Any] = None
    ) -> HistoryEvent:
        """
        Record a history event for a workflow
        
        Args:
            execution_id: Workflow execution ID
            event_type: Type of event
            event_data: Event data
            
        Returns:
            HistoryEvent object
        """
        with self._lock:
            history = self._execution_history[execution_id]
            
            event_id = len(history) + 1
            event = HistoryEvent(
                event_id=event_id,
                event_type=event_type,
                timestamp=datetime.now(),
                workflow_execution_id=execution_id,
                event_data=event_data or {}
            )
            
            history.append(event)
            
            # Update execution history length
            if execution_id in self._executions:
                self._executions[execution_id].history_length = len(history)
            
            return event
    
    def get_workflow_history(
        self,
        execution_id: str,
        event_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[HistoryEvent]:
        """
        Get workflow history
        
        Args:
            execution_id: Workflow execution ID
            event_type: Filter by event type
            limit: Maximum number of events
            
        Returns:
            List of HistoryEvent objects
        """
        with self._lock:
            history = self._execution_history.get(execution_id, [])
            
            if event_type:
                history = [e for e in history if e.event_type == event_type]
            
            if limit:
                history = history[-limit:]
            
            return history
    
    async def replay_workflow(
        self,
        execution_id: str
    ) -> WorkflowReplayResult:
        """
        Replay a workflow from its history
        
        Args:
            execution_id: Workflow execution ID
            
        Returns:
            WorkflowReplayResult object
        """
        start_time = time.time()
        mismatches = []
        
        history = self.get_workflow_history(execution_id)
        
        if not history:
            return WorkflowReplayResult(
                success=False,
                replayed_events=0,
                mismatches=[],
                error="No history found for execution",
                duration_seconds=0
            )
        
        # In a real implementation, this would use Temporal's replay SDK
        # to replay the workflow and compare results
        
        logger.info(f"Replaying workflow: {execution_id} ({len(history)} events)")
        
        return WorkflowReplayResult(
            success=True,
            replayed_events=len(history),
            mismatches=mismatches,
            error=None,
            duration_seconds=time.time() - start_time
        )
    
    async def get_workflow_history_events(
        self,
        execution_id: str,
        start_event_id: int = 1,
        maximum_events: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get workflow history events in paginated form
        
        Args:
            execution_id: Workflow execution ID
            start_event_id: Starting event ID
            maximum_events: Maximum events to return
            
        Returns:
            List of event dictionaries
        """
        history = self.get_workflow_history(execution_id)
        
        events = []
        for event in history:
            if event.event_id >= start_event_id and len(events) < maximum_events:
                events.append({
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "timestamp": event.timestamp.isoformat(),
                    "attributes": event.attributes
                })
        
        return events
    
    def get_event_types(self) -> List[str]:
        """
        Get list of all possible history event types
        
        Returns:
            List of event type strings
        """
        return [
            "WORKFLOW_EXECUTION_STARTED",
            "WORKFLOW_EXECUTION_COMPLETED",
            "WORKFLOW_EXECUTION_FAILED",
            "WORKFLOW_EXECUTION_CANCELED",
            "WORKFLOW_EXECUTION_TERMINATED",
            "WORKFLOW_EXECUTION_CONTINUED_AS_NEW",
            "WORKFLOW_EXECUTION_TIMED_OUT",
            "ACTIVITY_TASK_SCHEDULED",
            "ACTIVITY_TASK_STARTED",
            "ACTIVITY_TASK_COMPLETED",
            "ACTIVITY_TASK_FAILED",
            "ACTIVITY_TASK_CANCELED",
            "ACTIVITY_TASK_TIMEOUT",
            "ACTIVITY_TASK_HEARTBEAT",
            "TIMER_STARTED",
            "TIMER_FIRED",
            "TIMER_CANCELED",
            "CHILD_WORKFLOW_EXECUTION_STARTED",
            "CHILD_WORKFLOW_EXECUTION_COMPLETED",
            "CHILD_WORKFLOW_EXECUTION_FAILED",
            "CHILD_WORKFLOW_EXECUTION_CANCELED",
            "CHILD_WORKFLOW_EXECUTION_TERMINATED",
            "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED",
            "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED",
            "REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED",
            "REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED",
            "MARKER_RECORDED",
            "UPSERT_WORKFLOW_SEARCH_ATTRIBUTES",
            "WORKFLOW_PROPERTIES_MODIFIED"
        ]
    
    # =========================================================================
    # TEMPORAL UI
    # =========================================================================
    
    def configure_ui(
        self,
        host: str = "localhost",
        port: int = 8088,
        base_url: Optional[str] = None,
        namespace: Optional[str] = None,
        auth_enabled: bool = False,
        tls_enabled: bool = False,
        tls_cert_path: Optional[str] = None,
        tls_key_path: Optional[str] = None
    ) -> TemporalUIConfig:
        """
        Configure Temporal web UI
        
        Args:
            host: UI host
            port: UI port
            base_url: Base URL override
            namespace: Default namespace
            auth_enabled: Enable authentication
            tls_enabled: Enable TLS
            tls_cert_path: TLS certificate path
            tls_key_path: TLS key path
            
        Returns:
            TemporalUIConfig object
        """
        self._ui_config.host = host
        self._ui_config.port = port
        if base_url:
            self._ui_config.base_url = base_url
        else:
            self._ui_config.base_url = f"http://{host}:{port}"
        if namespace:
            self._ui_config.namespace = namespace
        self._ui_config.auth_enabled = auth_enabled
        self._ui_config.tls_enabled = tls_enabled
        self._ui_config.tls_cert_path = tls_cert_path
        self._ui_config.tls_key_path = tls_key_path
        
        logger.info(f"Configured Temporal UI: {self._ui_config.base_url}")
        return self._ui_config
    
    def get_ui_config(self) -> TemporalUIConfig:
        """
        Get Temporal UI configuration
        
        Returns:
            TemporalUIConfig object
        """
        return self._ui_config
    
    def get_ui_url(self, path: str = "") -> str:
        """
        Get URL for Temporal UI
        
        Args:
            path: UI path
            
        Returns:
            Full URL string
        """
        base = self._ui_config.base_url
        if path:
            return f"{base}{path}" if path.startswith("/") else f"{base}/{path}"
        return base
    
    def get_namespace_ui_url(self, namespace: str) -> str:
        """
        Get namespace URL for Temporal UI
        
        Args:
            namespace: Namespace name
            
        Returns:
            Namespace URL
        """
        return self.get_ui_url(f"/namespaces/{namespace}")
    
    def get_workflow_ui_url(self, execution_id: str, namespace: Optional[str] = None) -> str:
        """
        Get workflow URL for Temporal UI
        
        Args:
            execution_id: Workflow execution ID
            namespace: Namespace name
            
        Returns:
            Workflow URL
        """
        ns = namespace or self._active_namespace
        return self.get_ui_url(f"/namespaces/{ns}/workflows/{execution_id}")
    
    def get_task_queue_ui_url(self, task_queue: str, namespace: Optional[str] = None) -> str:
        """
        Get task queue URL for Temporal UI
        
        Args:
            task_queue: Task queue name
            namespace: Namespace name
            
        Returns:
            Task queue URL
        """
        ns = namespace or self._active_namespace
        return self.get_ui_url(f"/namespaces/{ns}/task-queues/{task_queue}")
    
    async def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Get dashboard summary for UI
        
        Returns:
            Dashboard summary data
        """
        with self._lock:
            running = len([e for e in self._executions.values() if e.status == WorkflowStatus.RUNNING])
            completed = len([e for e in self._executions.values() if e.status == WorkflowStatus.COMPLETED])
            failed = len([e for e in self._executions.values() if e.status == WorkflowStatus.FAILED])
            
            return {
                "namespace": self._active_namespace,
                "url": self.get_ui_url(),
                "timestamp": datetime.now().isoformat(),
                "executions": {
                    "total": len(self._executions),
                    "running": running,
                    "completed": completed,
                    "failed": failed
                },
                "workflows": {
                    "registered": len(self._workflows)
                },
                "activities": {
                    "registered": len(self._activities),
                    "active": len([e for e in self._activity_executions.values() 
                                  if e.status == ActivityStatus.STARTED])
                },
                "task_queues": {
                    "registered": len(self._task_queues)
                },
                "namespaces": {
                    "total": len(self._namespaces),
                    "active": self._active_namespace
                }
            }
    
    # =========================================================================
    # CALLBACKS AND EVENTS
    # =========================================================================
    
    def register_workflow_callback(
        self,
        event: str,
        callback: Callable
    ) -> bool:
        """
        Register a workflow event callback
        
        Args:
            event: Event name (started, completed, failed, canceled, terminated, signaled)
            callback: Callback function
            
        Returns:
            True if registration successful
        """
        with self._lock:
            self._workflow_callbacks[event].append(callback)
            return True
    
    def register_activity_callback(
        self,
        event: str,
        callback: Callable
    ) -> bool:
        """
        Register an activity event callback
        
        Args:
            event: Event name
            callback: Callback function
            
        Returns:
            True if registration successful
        """
        with self._lock:
            self._activity_callbacks[event].append(callback)
            return True
    
    async def _trigger_workflow_callback(
        self,
        event: str,
        execution: WorkflowExecution,
        *args,
        **kwargs
    ):
        """Internal method to trigger workflow callbacks"""
        callbacks = self._workflow_callbacks.get(event, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(execution, *args, **kwargs)
                else:
                    callback(execution, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in workflow callback: {e}")
    
    async def _trigger_activity_callback(
        self,
        event: str,
        execution: ActivityExecution,
        *args,
        **kwargs
    ):
        """Internal method to trigger activity callbacks"""
        callbacks = self._activity_callbacks.get(event, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(execution, *args, **kwargs)
                else:
                    callback(execution, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in activity callback: {e}")
    
    # =========================================================================
    # BACKGROUND TASKS
    # =========================================================================
    
    def _start_execution_task(self, execution_id: str):
        """Start background task for an execution"""
        event = threading.Event()
        self._background_tasks[execution_id] = event
        
        def task():
            while not event.is_set() and execution_id in self._executions:
                execution = self._executions.get(execution_id)
                if not execution or execution.status != WorkflowStatus.RUNNING:
                    break
                
                # Simulate workflow heartbeat
                self.record_history_event(execution_id, "WORKFLOW_EXECUTION_UPDATE", {
                    "timestamp": datetime.now().isoformat(),
                    "pending_activities": 0,
                    "pending_children": len(execution.child_executions)
                })
                
                # Sleep for a bit
                event.wait(timeout=5)
        
        thread = threading.Thread(target=task, daemon=True)
        thread.start()
    
    def _stop_execution_task(self, execution_id: str):
        """Stop background task for an execution"""
        if execution_id in self._background_tasks:
            self._background_tasks[execution_id].set()
            del self._background_tasks[execution_id]
    
    def start_polling(self, interval_seconds: int = 30):
        """
        Start background polling for workflow updates
        
        Args:
            interval_seconds: Polling interval
        """
        self._running = True
        
        def poll():
            while self._running:
                try:
                    asyncio.run(self._poll_updates())
                except Exception as e:
                    logger.error(f"Polling error: {e}")
                time.sleep(interval_seconds)
        
        self._polling_thread = threading.Thread(target=poll, daemon=True)
        self._polling_thread.start()
        logger.info("Started polling thread")
    
    async def _poll_updates(self):
        """Internal polling method"""
        # Check connection
        if not await self.ping():
            logger.warning("Temporal connection lost")
        
        # Check for stalled executions
        with self._lock:
            for execution in self._executions.values():
                if execution.status == WorkflowStatus.RUNNING:
                    # Record polling event
                    self.record_history_event(execution.execution_id, "POLLING_UPDATE")
    
    def stop_polling(self):
        """Stop background polling"""
        self._running = False
        if self._polling_thread:
            self._polling_thread.join(timeout=5)
        logger.info("Stopped polling thread")
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get Temporal manager statistics
        
        Returns:
            Statistics dictionary
        """
        with self._lock:
            return {
                "connected": self._connected,
                "namespace": self._active_namespace,
                "workflows": {
                    "registered": len(self._workflows),
                    "executions": len(self._executions),
                    "running": len([e for e in self._executions.values() 
                                   if e.status == WorkflowStatus.RUNNING]),
                    "completed": len([e for e in self._executions.values() 
                                     if e.status == WorkflowStatus.COMPLETED]),
                    "failed": len([e for e in self._executions.values() 
                                  if e.status == WorkflowStatus.FAILED])
                },
                "activities": {
                    "registered": len(self._activities),
                    "executions": len(self._activity_executions),
                    "running": len([e for e in self._activity_executions.values() 
                                   if e.status == ActivityStatus.STARTED])
                },
                "child_workflows": {
                    "total": len(self._child_workflows),
                    "running": len([c for c in self._child_workflows.values() 
                                   if c.status == WorkflowStatus.RUNNING])
                },
                "namespaces": len(self._namespaces),
                "task_queues": len(self._task_queues),
                "search_attributes": len(self._search_attribute_definitions),
                "history_events": sum(len(h) for h in self._execution_history.values())
            }
    
    def export_state(self) -> Dict[str, Any]:
        """
        Export current state for serialization
        
        Returns:
            State dictionary
        """
        with self._lock:
            return {
                "connected": self._connected,
                "host": self.host,
                "port": self.port,
                "namespace": self.namespace,
                "workflows": {k: asdict(v) for k, v in self._workflows.items()},
                "executions": {k: asdict(v) for k, v in self._executions.items()},
                "activities": {k: asdict(v) for k, v in self._activities.items()},
                "namespaces": {k: asdict(v) for k, v in self._namespaces.items()},
                "task_queues": {k: asdict(v) for k, v in self._task_queues.items()},
                "search_attributes": {k: asdict(v) for k, v in self._search_attribute_definitions.items()}
            }
    
    async def reset(self):
        """Reset the manager state"""
        with self._lock:
            self._executions.clear()
            self._execution_history.clear()
            self._activity_executions.clear()
            self._child_workflows.clear()
            self._pending_queries.clear()
            self._background_tasks.clear()
            logger.info("TemporalManager state reset")
    
    def __repr__(self) -> str:
        return f"TemporalManager(host={self.host}, port={self.port}, namespace={self._active_namespace}, connected={self._connected})"


# Factory function
def create_temporal_manager(
    host: str = "localhost",
    port: int = 7233,
    namespace: str = "default",
    tls_enabled: bool = False,
    tls_cert_path: Optional[str] = None,
    tls_key_path: Optional[str] = None
) -> TemporalManager:
    """
    Create a TemporalManager instance
    
    Args:
        host: Temporal server host
        port: Temporal server port
        namespace: Default namespace
        tls_enabled: Enable TLS
        tls_cert_path: TLS certificate path
        tls_key_path: TLS key path
        
    Returns:
        TemporalManager instance
    """
    return TemporalManager(
        host=host,
        port=port,
        namespace=namespace,
        tls_enabled=tls_enabled,
        tls_cert_path=tls_cert_path,
        tls_key_path=tls_key_path
    )
