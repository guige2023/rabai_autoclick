"""
Edge Computing Module for Distributed Workflow Execution

Provides edge node registration, orchestration, task distribution,
data locality, offline execution, edge-to-edge sync, latency and
bandwidth optimization, edge caching, and security at edge nodes.
"""

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict
import asyncio
import copy

logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    SYNCING = "syncing"
    BUSY = "busy"
    UNAVAILABLE = "unavailable"


class TaskStatus(Enum):
    PENDING = "pending"
    DISTRIBUTED = "distributed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    OFFLINE_READY = "offline_ready"


class EncryptionMode(Enum):
    NONE = "none"
    AES_256 = "aes_256"
    END_TO_END = "end_to_end"


@dataclass
class EdgeNode:
    """Represents an edge device/node in the distributed system."""
    node_id: str
    name: str
    host: str
    port: int
    status: NodeStatus = NodeStatus.OFFLINE
    capabilities: List[str] = field(default_factory=list)
    location: Dict[str, float] = field(default_factory=dict)  # lat, lon
    last_heartbeat: float = field(default_factory=time.time)
    current_load: float = 0.0
    max_load: float = 100.0
    bandwidth_mbps: float = 100.0
    latency_ms: float = 0.0
    cached_workflows: Set[str] = field(default_factory=set)
    cached_assets: Dict[str, str] = field(default_factory=dict)  # asset_id -> hash
    storage_available_mb: float = 1000.0
    encryption: EncryptionMode = EncryptionMode.AES_256
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_available(self) -> bool:
        """Check if node can accept new tasks."""
        return (
            self.status in (NodeStatus.ONLINE, NodeStatus.OFFLINE_READY)
            and self.current_load < self.max_load
        )

    def distance_to(self, other: 'EdgeNode') -> float:
        """Calculate approximate distance to another node using Haversine-like metric."""
        if not self.location or not other.location:
            return float('inf')
        lat1, lon1 = self.location.get('lat', 0), self.location.get('lon', 0)
        lat2, lon2 = other.location.get('lat', 0), other.location.get('lon', 0)
        return ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5


@dataclass
class Task:
    """Represents a workflow task to be executed on edge nodes."""
    task_id: str
    workflow_id: str
    task_definition: Dict[str, Any]
    source_node: Optional[str] = None
    assigned_node: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    priority: int = 5
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    data_requirements: Dict[str, Any] = field(default_factory=dict)
    estimated_execution_time_ms: float = 1000.0
    retry_count: int = 0
    max_retries: int = 3

    def can_execute(self, completed_tasks: Set[str]) -> bool:
        """Check if all dependencies are satisfied."""
        return all(dep in completed_tasks for dep in self.dependencies)


@dataclass
class SyncState:
    """Represents synchronization state between edge nodes."""
    state_id: str
    workflow_id: str
    node_id: str
    task_states: Dict[str, str] = field(default_factory=dict)  # task_id -> status
    shared_data: Dict[str, Any] = field(default_factory=dict)
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    checksum: str = ""


@dataclass
class CachedWorkflow:
    """Represents a workflow cached at an edge node."""
    workflow_id: str
    definition: Dict[str, Any]
    assets: Dict[str, str] = field(default_factory=dict)  # asset_id -> path
    cached_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    size_mb: float = 0.0
    version: str = "1.0.0"


class EdgeComputing:
    """
    Edge Computing Module for Distributed Workflow Execution.

    Features:
    1. Edge node registration: Register edge devices as execution nodes
    2. Edge orchestration: Coordinate execution across edge devices
    3. Task distribution: Distribute workflow tasks to edge nodes
    4. Data locality: Process data close to where it is generated
    5. Offline execution: Execute workflows without cloud connectivity
    6. Edge-to-edge sync: Sync state between edge devices
    7. Latency optimization: Route tasks to nearest edge node
    8. Bandwidth optimization: Minimize data transfer
    9. Edge caching: Cache workflows and assets at edge
    10. Security at edge: Encrypt data at edge nodes
    """

    def __init__(self, node_id: Optional[str] = None, node_name: Optional[str] = None):
        """Initialize the Edge Computing module."""
        self.local_node_id = node_id or str(uuid.uuid4())
        self.local_node_name = node_name or f"edge_{self.local_node_id[:8]}"

        # Node registry
        self.nodes: Dict[str, EdgeNode] = {}
        self._node_lock = threading.RLock()

        # Task management
        self.tasks: Dict[str, Task] = {}
        self.task_queue: List[str] = []
        self._task_lock = threading.RLock()

        # Sync state
        self.sync_states: Dict[str, SyncState] = {}
        self._sync_lock = threading.RLock()

        # Cached workflows
        self.cached_workflows: Dict[str, CachedWorkflow] = {}
        self._cache_lock = threading.RLock()

        # Offline execution support
        self.offline_mode = False
        self.offline_queue: List[str] = []
        self._offline_lock = threading.RLock()

        # Encryption
        self.encryption_key: Optional[bytes] = None
        self.encryption_mode = EncryptionMode.AES_256

        # Callbacks
        self._task_callbacks: Dict[str, Callable] = {}
        self._sync_callbacks: List[Callable] = []

        # Background threads
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._sync_thread: Optional[threading.Thread] = None
        self._running = False

        # Data locality - map data sources to optimal nodes
        self.data_source_map: Dict[str, str] = {}  # data_source_id -> node_id

        # Register self as local node
        self._register_local_node()

    # =========================================================================
    # 1. Edge Node Registration
    # =========================================================================

    def _register_local_node(self):
        """Register the local edge node."""
        local_node = EdgeNode(
            node_id=self.local_node_id,
            name=self.local_node_name,
            host="127.0.0.1",
            port=0,  # Local execution
            status=NodeStatus.ONLINE,
            capabilities=["local_execution", "workflow_execution", "data_processing"],
            location={},
            encryption=self.encryption_mode
        )
        with self._node_lock:
            self.nodes[self.local_node_id] = local_node
        logger.info(f"Registered local edge node: {self.local_node_id}")

    def register_node(self, node: EdgeNode) -> bool:
        """
        Register an edge device as an execution node.

        Args:
            node: EdgeNode object with node details

        Returns:
            bool: True if registration successful
        """
        with self._node_lock:
            if node.node_id in self.nodes:
                logger.warning(f"Node {node.node_id} already registered, updating...")
                self.nodes[node.node_id].status = node.status
                self.nodes[node.node_id].capabilities = node.capabilities
                self.nodes[node.node_id].location = node.location
                self.nodes[node.node_id].bandwidth_mbps = node.bandwidth_mbps
                self.nodes[node.node_id].metadata = node.metadata
                return True

            self.nodes[node.node_id] = node
            logger.info(f"Registered edge node: {node.node_id} ({node.name})")
            return True

    def unregister_node(self, node_id: str) -> bool:
        """Unregister an edge node."""
        with self._node_lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                logger.info(f"Unregistered edge node: {node_id}")
                return True
            return False

    def get_node(self, node_id: str) -> Optional[EdgeNode]:
        """Get node details by ID."""
        with self._node_lock:
            return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[EdgeNode]:
        """Get all registered nodes."""
        with self._node_lock:
            return list(self.nodes.values())

    def get_available_nodes(self) -> List[EdgeNode]:
        """Get all available (online and not overloaded) nodes."""
        with self._node_lock:
            return [n for n in self.nodes.values() if n.is_available()]

    def update_node_status(self, node_id: str, status: NodeStatus) -> bool:
        """Update the status of an edge node."""
        with self._node_lock:
            if node_id in self.nodes:
                self.nodes[node_id].status = status
                self.nodes[node_id].last_heartbeat = time.time()
                return True
            return False

    def update_node_load(self, node_id: str, load: float) -> bool:
        """Update the current load of an edge node."""
        with self._node_lock:
            if node_id in self.nodes:
                self.nodes[node_id].current_load = min(load, self.nodes[node_id].max_load)
                return True
            return False

    # =========================================================================
    # 2. Edge Orchestration
    # =========================================================================

    def start_orchestration(self):
        """Start the orchestration services."""
        self._running = True
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._heartbeat_thread.start()
        self._sync_thread.start()
        logger.info("Edge orchestration started")

    def stop_orchestration(self):
        """Stop the orchestration services."""
        self._running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
        if self._sync_thread:
            self._sync_thread.join(timeout=5)
        logger.info("Edge orchestration stopped")

    def _heartbeat_loop(self):
        """Background loop to send/receive heartbeats."""
        while self._running:
            try:
                self._send_heartbeat()
                self._check_node_health()
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
            time.sleep(10)

    def _sync_loop(self):
        """Background loop for edge-to-edge synchronization."""
        while self._running:
            try:
                if not self.offline_mode:
                    self._sync_with_peers()
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
            time.sleep(30)

    def _send_heartbeat(self):
        """Send heartbeat to other nodes."""
        with self._node_lock:
            local_node = self.nodes.get(self.local_node_id)
            if local_node:
                local_node.last_heartbeat = time.time()

    def _check_node_health(self):
        """Check health of all nodes based on last heartbeat."""
        threshold = 60  # seconds
        current_time = time.time()
        with self._node_lock:
            for node_id, node in self.nodes.items():
                if node_id == self.local_node_id:
                    continue
                if current_time - node.last_heartbeat > threshold:
                    if node.status != NodeStatus.OFFLINE:
                        logger.warning(f"Node {node_id} marked offline (no heartbeat)")
                        node.status = NodeStatus.OFFLINE

    def orchestrate_workflow(self, workflow_id: str, tasks: List[Task]) -> Dict[str, Any]:
        """
        Coordinate execution of a workflow across edge nodes.

        Args:
            workflow_id: Unique workflow identifier
            tasks: List of tasks to execute

        Returns:
            Dict with orchestration results
        """
        results = {
            "workflow_id": workflow_id,
            "status": "orchestrated",
            "task_assignments": {},
            "execution_order": []
        }

        with self._task_lock:
            for task in tasks:
                task.workflow_id = workflow_id
                self.tasks[task.task_id] = task
                self.task_queue.append(task.task_id)

        # Determine execution order based on dependencies
        execution_order = self._resolve_execution_order(workflow_id)
        results["execution_order"] = execution_order

        # Assign tasks to optimal nodes
        for task_id in execution_order:
            task = self.tasks.get(task_id)
            if task:
                assigned = self._assign_task_to_node(task)
                results["task_assignments"][task_id] = assigned

        return results

    def _resolve_execution_order(self, workflow_id: str) -> List[str]:
        """Resolve the execution order of tasks based on dependencies."""
        with self._task_lock:
            workflow_tasks = [t for t in self.tasks.values() if t.workflow_id == workflow_id]

        # Topological sort based on dependencies
        resolved = []
        remaining = set(t.task_id for t in workflow_tasks)
        completed = set()

        while remaining:
            progress = False
            for task_id in list(remaining):
                task = self.tasks[task_id]
                if task.can_execute(completed):
                    resolved.append(task_id)
                    completed.add(task_id)
                    remaining.remove(task_id)
                    progress = True

            if not progress:
                # Circular dependency or missing dependency
                logger.error(f"Cannot resolve execution order, remaining: {remaining}")
                resolved.extend(list(remaining))
                break

        return resolved

    # =========================================================================
    # 3. Task Distribution
    # =========================================================================

    def _assign_task_to_node(self, task: Task) -> Optional[str]:
        """
        Assign a task to the optimal edge node based on latency,
        load, and capabilities.

        Args:
            task: Task to assign

        Returns:
            Node ID where task was assigned, or None if no suitable node
        """
        available_nodes = self.get_available_nodes()
        if not available_nodes:
            logger.warning(f"No available nodes for task {task.task_id}")
            return None

        # Score nodes based on multiple factors
        scored_nodes = []
        for node in available_nodes:
            score = self._calculate_node_score(node, task)
            scored_nodes.append((node.node_id, score))

        # Sort by score (higher is better)
        scored_nodes.sort(key=lambda x: x[1], reverse=True)
        best_node_id = scored_nodes[0][0]

        # Assign task
        with self._task_lock:
            task.assigned_node = best_node_id
            task.status = TaskStatus.DISTRIBUTED

        with self._node_lock:
            if best_node_id in self.nodes:
                self.nodes[best_node_id].current_load += task.priority

        logger.info(f"Task {task.task_id} assigned to node {best_node_id}")
        return best_node_id

    def _calculate_node_score(self, node: EdgeNode, task: Task) -> float:
        """Calculate a score for node-task assignment."""
        score = 100.0

        # Capability match (bonus)
        if task.task_definition.get("required_capability"):
            if task.task_definition["required_capability"] in node.capabilities:
                score += 50
            else:
                score -= 100

        # Load factor (lower load = higher score)
        load_factor = 1 - (node.current_load / node.max_load)
        score *= load_factor

        # Latency factor (lower latency = higher score)
        if node.latency_ms > 0:
            latency_factor = max(0, 1 - (node.latency_ms / 1000))
            score *= (0.5 + 0.5 * latency_factor)

        # Bandwidth factor
        bandwidth_factor = node.bandwidth_mbps / 1000
        score *= (0.5 + 0.5 * min(1, bandwidth_factor))

        # Data locality bonus
        data_source = task.data_requirements.get("source_id")
        if data_source and self.data_source_map.get(data_source) == node.node_id:
            score *= 1.5

        # Offline-ready bonus
        if node.status == NodeStatus.OFFLINE_READY:
            score *= 1.2

        return score

    def submit_task(self, task: Task) -> str:
        """
        Submit a task for distributed execution.

        Args:
            task: Task object

        Returns:
            Task ID
        """
        with self._task_lock:
            self.tasks[task.task_id] = task
            self.task_queue.append(task.task_id)

        if self.offline_mode:
            with self._offline_lock:
                self.offline_queue.append(task.task_id)
            task.status = TaskStatus.OFFLINE_READY

        logger.info(f"Task {task.task_id} submitted")
        return task.task_id

    def submit_tasks(self, tasks: List[Task]) -> List[str]:
        """Submit multiple tasks."""
        task_ids = []
        for task in tasks:
            task_ids.append(self.submit_task(task))
        return task_ids

    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the current status of a task."""
        with self._task_lock:
            task = self.tasks.get(task_id)
            return task.status if task else None

    def update_task_status(self, task_id: str, status: TaskStatus,
                          result: Optional[Dict[str, Any]] = None,
                          error: Optional[str] = None) -> bool:
        """Update task status and optionally set result."""
        with self._task_lock:
            task = self.tasks.get(task_id)
            if not task:
                return False

            task.status = status
            if result:
                task.result = result
            if error:
                task.error = error

            if status == TaskStatus.RUNNING and not task.started_at:
                task.started_at = time.time()
            elif status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                task.completed_at = time.time()

                # Release node load
                if task.assigned_node:
                    with self._node_lock:
                        if task.assigned_node in self.nodes:
                            self.nodes[task.assigned_node].current_load -= task.priority

            # Trigger callbacks
            if task_id in self._task_callbacks:
                try:
                    self._task_callbacks[task_id](task)
                except Exception as e:
                    logger.error(f"Task callback error: {e}")

        return True

    def retry_task(self, task_id: str) -> bool:
        """Retry a failed task."""
        with self._task_lock:
            task = self.tasks.get(task_id)
            if not task:
                return False

            if task.retry_count >= task.max_retries:
                logger.error(f"Task {task_id} exceeded max retries")
                return False

            task.retry_count += 1
            task.status = TaskStatus.PENDING
            task.error = None
            self.task_queue.append(task_id)

        logger.info(f"Task {task_id} queued for retry (attempt {task.retry_count})")
        return True

    def register_task_callback(self, task_id: str, callback: Callable[[Task], None]):
        """Register a callback for task completion."""
        self._task_callbacks[task_id] = callback

    # =========================================================================
    # 4. Data Locality
    # =========================================================================

    def register_data_source(self, data_source_id: str, node_id: str,
                            location: Optional[Dict[str, float]] = None):
        """
        Register a data source and map it to the nearest edge node.

        Args:
            data_source_id: Unique data source identifier
            node_id: Edge node ID to associate with this data source
            location: Optional geographic location of data source
        """
        self.data_source_map[data_source_id] = node_id
        logger.info(f"Data source {data_source_id} registered to node {node_id}")

    def get_optimal_node_for_data(self, data_source_id: str) -> Optional[EdgeNode]:
        """
        Get the optimal node for processing data from a specific source.

        Args:
            data_source_id: Data source identifier

        Returns:
            EdgeNode that should process data from this source
        """
        mapped_node_id = self.data_source_map.get(data_source_id)
        if mapped_node_id:
            with self._node_lock:
                return self.nodes.get(mapped_node_id)

        # Fallback: find nearest available node
        available = self.get_available_nodes()
        if not available:
            return None

        # Return first available (would use location in real impl)
        return available[0] if available else None

    def process_data_locally(self, data_source_id: str,
                            processor: Callable[[Any], Any]) -> Optional[Any]:
        """
        Process data at the node closest to its source.

        Args:
            data_source_id: Source of the data
            processor: Function to process the data

        Returns:
            Processing result or None if no suitable node
        """
        optimal_node = self.get_optimal_node_for_data(data_source_id)
        if not optimal_node:
            logger.warning(f"No optimal node found for data source {data_source_id}")
            return None

        # If optimal node is not local, would dispatch to that node
        if optimal_node.node_id != self.local_node_id:
            logger.info(f"Would dispatch processing to node {optimal_node.node_id}")
            # In real implementation, would send to remote node
            return None

        # Process locally
        logger.info(f"Processing data from {data_source_id} locally")
        return None  # Actual processing would happen here

    # =========================================================================
    # 5. Offline Execution
    # =========================================================================

    def enable_offline_mode(self):
        """Enable offline execution mode."""
        self.offline_mode = True
        logger.info("Offline mode enabled")

    def disable_offline_mode(self):
        """Disable offline execution mode and sync."""
        self.offline_mode = False
        logger.info("Offline mode disabled")
        self._sync_offline_queue()

    def _sync_offline_queue(self):
        """Sync queued tasks when coming back online."""
        with self._offline_lock:
            queued_tasks = list(self.offline_queue)
            self.offline_queue.clear()

        for task_id in queued_tasks:
            task = self.tasks.get(task_id)
            if task:
                task.status = TaskStatus.PENDING
                self.task_queue.append(task_id)
                logger.info(f"Offline task {task_id} re-queued for execution")

    def get_offline_queue_size(self) -> int:
        """Get number of tasks in offline queue."""
        with self._offline_lock:
            return len(self.offline_queue)

    def cache_for_offline(self, workflow_id: str, tasks: List[Task]):
        """
        Cache workflow and dependencies for offline execution.

        Args:
            workflow_id: Workflow to cache
            tasks: Tasks comprising the workflow
        """
        with self._cache_lock:
            cached = CachedWorkflow(
                workflow_id=workflow_id,
                definition={"tasks": [t.task_definition for t in tasks]},
                cached_at=time.time()
            )
            self.cached_workflows[workflow_id] = cached

        with self._node_lock:
            if self.local_node_id in self.nodes:
                self.nodes[self.local_node_id].cached_workflows.add(workflow_id)

        logger.info(f"Workflow {workflow_id} cached for offline execution")

    def is_workflow_available_offline(self, workflow_id: str) -> bool:
        """Check if a workflow is cached for offline execution."""
        with self._cache_lock:
            return workflow_id in self.cached_workflows

    # =========================================================================
    # 6. Edge-to-Edge Sync
    # =========================================================================

    def create_sync_state(self, workflow_id: str) -> SyncState:
        """
        Create a synchronization state for a workflow.

        Args:
            workflow_id: Workflow identifier

        Returns:
            SyncState object
        """
        sync_state = SyncState(
            state_id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            node_id=self.local_node_id,
            timestamp=time.time()
        )

        with self._sync_lock:
            self.sync_states[sync_state.state_id] = sync_state

        return sync_state

    def update_sync_state(self, state_id: str, task_id: str, status: str):
        """Update a task status within a sync state."""
        with self._sync_lock:
            if state_id in self.sync_states:
                state = self.sync_states[state_id]
                state.task_states[task_id] = status
                state.version += 1
                state.timestamp = time.time()
                state.checksum = self._calculate_state_checksum(state)

    def _calculate_state_checksum(self, state: SyncState) -> str:
        """Calculate checksum for sync state verification."""
        content = json.dumps({
            "task_states": state.task_states,
            "shared_data": state.shared_data,
            "version": state.version
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def sync_with_node(self, target_node_id: str) -> bool:
        """
        Synchronize state with a specific edge node.

        Args:
            target_node_id: Node to sync with

        Returns:
            True if sync successful
        """
        with self._node_lock:
            target_node = self.nodes.get(target_node_id)
            if not target_node or target_node.status == NodeStatus.OFFLINE:
                return False

        # In real implementation, would send/receive state updates
        logger.info(f"Syncing with node {target_node_id}")
        return True

    def _sync_with_peers(self):
        """Background sync with all connected peers."""
        with self._node_lock:
            peer_nodes = [n for n in self.nodes.values()
                         if n.node_id != self.local_node_id
                         and n.status == NodeStatus.ONLINE]

        for peer in peer_nodes:
            try:
                self.sync_with_node(peer.node_id)
            except Exception as e:
                logger.error(f"Sync with {peer.node_id} failed: {e}")

    def merge_sync_state(self, remote_state: SyncState) -> bool:
        """
        Merge a remote sync state with local state.
        Uses vector clocks or last-write-wins for conflict resolution.

        Args:
            remote_state: State to merge

        Returns:
            True if merge successful
        """
        with self._sync_lock:
            # Find matching state
            local_state = None
            for state in self.sync_states.values():
                if state.workflow_id == remote_state.workflow_id:
                    local_state = state
                    break

            if not local_state:
                self.sync_states[remote_state.state_id] = remote_state
                return True

            # Conflict resolution: higher version wins
            if remote_state.version > local_state.version:
                # Merge task states
                for task_id, status in remote_state.task_states.items():
                    if task_id not in local_state.task_states:
                        local_state.task_states[task_id] = status
                    elif status == TaskStatus.COMPLETED.value:
                        local_state.task_states[task_id] = status

                # Merge shared data (last-write-wins by key)
                for key, value in remote_state.shared_data.items():
                    local_state.shared_data[key] = value

                local_state.version = max(local_state.version, remote_state.version) + 1
                local_state.timestamp = time.time()
                local_state.checksum = self._calculate_state_checksum(local_state)

        return True

    def get_sync_state(self, workflow_id: str) -> Optional[SyncState]:
        """Get current sync state for a workflow."""
        with self._sync_lock:
            for state in self.sync_states.values():
                if state.workflow_id == workflow_id:
                    return state
        return None

    def register_sync_callback(self, callback: Callable[[SyncState], None]):
        """Register a callback for sync events."""
        self._sync_callbacks.append(callback)

    # =========================================================================
    # 7. Latency Optimization
    # =========================================================================

    def find_nearest_node(self, location: Dict[str, float]) -> Optional[EdgeNode]:
        """
        Find the nearest edge node to a given location.

        Args:
            location: Dict with 'lat' and 'lon' keys

        Returns:
            Nearest EdgeNode or None
        """
        available = self.get_available_nodes()
        if not available:
            return None

        min_distance = float('inf')
        nearest = None

        for node in available:
            if node.location:
                distance = ((location.get('lat', 0) - node.location.get('lat', 0)) ** 2 +
                          (location.get('lon', 0) - node.location.get('lon', 0)) ** 2) ** 0.5
                if distance < min_distance:
                    min_distance = distance
                    nearest = node

        return nearest

    def find_lowest_latency_node(self) -> Optional[EdgeNode]:
        """
        Find the edge node with lowest latency.

        Returns:
            EdgeNode with lowest latency or None
        """
        available = self.get_available_nodes()
        if not available:
            return None

        return min(available, key=lambda n: n.latency_ms)

    def optimize_route(self, task: Task) -> Optional[EdgeNode]:
        """
        Optimize routing for a task based on latency and other factors.

        Args:
            task: Task to route

        Returns:
            Optimal node for this task
        """
        # Consider multiple factors
        candidates = self.get_available_nodes()
        if not candidates:
            return None

        # If data locality is important, prefer node near data source
        if task.data_requirements.get("source_id"):
            source_node = self.get_optimal_node_for_data(task.data_requirements["source_id"])
            if source_node and source_node in candidates:
                return source_node

        # Otherwise, find lowest latency node
        return self.find_lowest_latency_node()

    def measure_latency(self, node_id: str) -> float:
        """
        Measure latency to a specific node (placeholder for real measurement).

        Args:
            node_id: Target node ID

        Returns:
            Latency in milliseconds
        """
        with self._node_lock:
            node = self.nodes.get(node_id)
            if node:
                return node.latency_ms
        return float('inf')

    # =========================================================================
    # 8. Bandwidth Optimization
    # =========================================================================

    def estimate_transfer_size(self, task: Task) -> int:
        """
        Estimate data size that needs to be transferred for a task.

        Args:
            task: Task to estimate

        Returns:
            Estimated size in bytes
        """
        size = len(json.dumps(task.task_definition).encode())
        size += len(json.dumps(task.data_requirements).encode())
        return size

    def should_process_remote(self, task: Task, source_node: EdgeNode,
                              target_node: EdgeNode) -> bool:
        """
        Determine if a task should be processed remotely vs local execution.

        Args:
            task: Task to consider
            source_node: Node where data is located
            target_node: Node being evaluated

        Returns:
            True if remote processing is better
        """
        transfer_size = self.estimate_transfer_size(task)
        bandwidth = min(source_node.bandwidth_mbps, target_node.bandwidth_mbps)

        # Estimate transfer time
        transfer_time_ms = (transfer_size * 8) / (bandwidth * 1_000_000) * 1000

        # If transfer time exceeds threshold, local may be better
        return transfer_time_ms < task.estimated_execution_time_ms * 0.5

    def compress_for_transfer(self, data: Dict[str, Any]) -> bytes:
        """
        Compress data for efficient transfer.
        Uses simple serialization (would use zlib/msgpack in production).

        Args:
            data: Data to compress

        Returns:
            Compressed bytes
        """
        import zlib
        json_data = json.dumps(data).encode()
        return zlib.compress(json_data)

    def decompress_from_transfer(self, data: bytes) -> Dict[str, Any]:
        """Decompress data received from transfer."""
        import zlib
        json_data = zlib.decompress(data)
        return json.loads(json_data.decode())

    def optimize_bandwidth_usage(self, tasks: List[Task]) -> List[Task]:
        """
        Optimize task distribution to minimize bandwidth usage.

        Args:
            tasks: Tasks to optimize

        Returns:
            Reordered list of tasks for minimal bandwidth
        """
        # Group tasks by data source to minimize data movement
        by_source = defaultdict(list)
        for task in tasks:
            source = task.data_requirements.get("source_id", "default")
            by_source[source].append(task)

        # Process tasks from same source together
        optimized = []
        for source, source_tasks in by_source.items():
            optimized.extend(source_tasks)

        return optimized

    # =========================================================================
    # 9. Edge Caching
    # =========================================================================

    def cache_workflow(self, workflow_id: str, definition: Dict[str, Any],
                      assets: Optional[Dict[str, str]] = None):
        """
        Cache a workflow at the local edge node.

        Args:
            workflow_id: Unique workflow identifier
            definition: Workflow definition
            assets: Optional asset references
        """
        assets = assets or {}
        size_mb = len(json.dumps(definition).encode()) / (1024 * 1024)

        cached = CachedWorkflow(
            workflow_id=workflow_id,
            definition=definition,
            assets=assets,
            cached_at=time.time(),
            last_used=time.time(),
            size_mb=size_mb
        )

        with self._cache_lock:
            self.cached_workflows[workflow_id] = cached

        with self._node_lock:
            if self.local_node_id in self.nodes:
                self.nodes[self.local_node_id].cached_workflows.add(workflow_id)
                for asset_id in assets:
                    self.nodes[self.local_node_id].cached_assets[asset_id] = hashlib.md5(
                        asset_id.encode()).hexdigest()

        logger.info(f"Cached workflow {workflow_id} ({size_mb:.2f} MB)")

    def get_cached_workflow(self, workflow_id: str) -> Optional[CachedWorkflow]:
        """Get a cached workflow by ID."""
        with self._cache_lock:
            cached = self.cached_workflows.get(workflow_id)
            if cached:
                cached.last_used = time.time()
            return cached

    def cache_asset(self, asset_id: str, content: bytes, node_id: Optional[str] = None):
        """
        Cache an asset at edge node(s).

        Args:
            asset_id: Unique asset identifier
            content: Asset content bytes
            node_id: Specific node to cache at (None for local)
        """
        node_id = node_id or self.local_node_id
        content_hash = hashlib.sha256(content).hexdigest()

        with self._node_lock:
            if node_id in self.nodes:
                self.nodes[node_id].cached_assets[asset_id] = content_hash

        logger.info(f"Cached asset {asset_id} at node {node_id}")

    def is_asset_cached(self, asset_id: str, node_id: Optional[str] = None) -> bool:
        """Check if an asset is cached at a node."""
        node_id = node_id or self.local_node_id
        with self._node_lock:
            node = self.nodes.get(node_id)
            return node and asset_id in node.cached_assets

    def evict_cache(self, workflow_id: Optional[str] = None, max_age_hours: int = 24):
        """
        Evict cached workflows older than max_age_hours.

        Args:
            workflow_id: Specific workflow to evict (or None for all old)
            max_age_hours: Maximum age before eviction
        """
        cutoff = time.time() - (max_age_hours * 3600)

        with self._cache_lock:
            if workflow_id:
                cached = self.cached_workflows.get(workflow_id)
                if cached and cached.cached_at < cutoff:
                    del self.cached_workflows[workflow_id]
                    logger.info(f"Evicted workflow {workflow_id} from cache")
            else:
                to_evict = [wid for wid, cw in self.cached_workflows.items()
                           if cw.cached_at < cutoff]
                for wid in to_evict:
                    del self.cached_workflows[wid]
                logger.info(f"Evicted {len(to_evict)} workflows from cache")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._cache_lock:
            total_size = sum(cw.size_mb for cw in self.cached_workflows.values())
            return {
                "cached_workflows": len(self.cached_workflows),
                "total_size_mb": total_size,
                "workflows": [
                    {
                        "workflow_id": cw.workflow_id,
                        "size_mb": cw.size_mb,
                        "cached_at": datetime.fromtimestamp(cw.cached_at).isoformat(),
                        "last_used": datetime.fromtimestamp(cw.last_used).isoformat()
                    }
                    for cw in self.cached_workflows.values()
                ]
            }

    # =========================================================================
    # 10. Security at Edge
    # =========================================================================

    def set_encryption_key(self, key: bytes):
        """
        Set the encryption key for data at rest and in transit.

        Args:
            key: 32-byte encryption key
        """
        if len(key) < 32:
            raise ValueError("Encryption key must be at least 32 bytes")
        self.encryption_key = key[:32]
        logger.info("Encryption key set")

    def set_encryption_mode(self, mode: EncryptionMode):
        """Set the encryption mode for edge nodes."""
        self.encryption_mode = mode
        with self._node_lock:
            for node in self.nodes.values():
                node.encryption = mode
        logger.info(f"Encryption mode set to {mode.value}")

    def encrypt_data(self, data: Dict[str, Any]) -> bytes:
        """
        Encrypt data using AES-256.

        Args:
            data: Data to encrypt

        Returns:
            Encrypted bytes
        """
        if not self.encryption_key:
            logger.warning("No encryption key set, returning plain data")
            return json.dumps(data).encode()

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend
        import os

        # Generate IV
        iv = os.urandom(16)
        cipher = Cipher(
            algorithms.AES(self.encryption_key),
            modes.CBC(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()

        # Pad data
        json_data = json.dumps(data).encode()
        padding = 16 - (len(json_data) % 16)
        json_data += bytes([padding]) * padding

        encrypted = encryptor.update(json_data) + encryptor.finalize()
        return iv + encrypted

    def decrypt_data(self, encrypted_data: bytes) -> Dict[str, Any]:
        """
        Decrypt data.

        Args:
            encrypted_data: Encrypted bytes

        Returns:
            Decrypted data dict
        """
        if not self.encryption_key:
            logger.warning("No encryption key set, returning as plain data")
            return json.loads(encrypted_data.decode())

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend

        iv = encrypted_data[:16]
        encrypted = encrypted_data[16:]

        cipher = Cipher(
            algorithms.AES(self.encryption_key),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()

        decrypted = decryptor.update(encrypted) + decryptor.finalize()

        # Remove padding
        padding = decrypted[-1]
        decrypted = decrypted[:-padding]

        return json.loads(decrypted.decode())

    def encrypt_task_data(self, task: Task) -> Task:
        """Encrypt sensitive data within a task."""
        if self.encryption_mode == EncryptionMode.NONE:
            return task

        encrypted_task = copy.deepcopy(task)

        # Encrypt task definition if it contains sensitive data
        if task.task_definition.get("sensitive"):
            encrypted_task.task_definition["_encrypted_data"] = self.encrypt_data(
                task.task_definition
            ).hex()
            del encrypted_task.task_definition["sensitive"]

        return encrypted_task

    def decrypt_task_data(self, task: Task) -> Task:
        """Decrypt sensitive data within a task."""
        if self.encryption_mode == EncryptionMode.NONE:
            return task

        decrypted_task = copy.deepcopy(task)

        if "_encrypted_data" in task.task_definition:
            encrypted_hex = task.task_definition["_encrypted_data"]
            decrypted_task.task_definition = self.decrypt_data(
                bytes.fromhex(encrypted_hex)
            )

        return decrypted_task

    def secure_transfer(self, data: Dict[str, Any], target_node_id: str) -> bytes:
        """
        Prepare data for secure transfer to another edge node.

        Args:
            data: Data to transfer
            target_node_id: Destination node

        Returns:
            Encrypted data bytes
        """
        # Add node-specific encryption
        payload = {
            "source": self.local_node_id,
            "target": target_node_id,
            "data": data,
            "timestamp": time.time()
        }

        return self.encrypt_data(payload)

    def verify_node_identity(self, node_id: str, expected_fingerprint: str) -> bool:
        """
        Verify the identity of an edge node.

        Args:
            node_id: Node to verify
            expected_fingerprint: Expected node fingerprint

        Returns:
            True if verified
        """
        with self._node_lock:
            node = self.nodes.get(node_id)
            if not node:
                return False

            # Generate fingerprint from node metadata
            fingerprint_input = f"{node.node_id}:{node.name}:{node.host}:{node.port}"
            fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()[:16]

            return fingerprint == expected_fingerprint

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive edge computing statistics."""
        with self._node_lock:
            total_nodes = len(self.nodes)
            available_nodes = len([n for n in self.nodes.values() if n.is_available()])

        with self._task_lock:
            total_tasks = len(self.tasks)
            pending_tasks = len([t for t in self.tasks.values()
                                if t.status == TaskStatus.PENDING])
            running_tasks = len([t for t in self.tasks.values()
                                if t.status == TaskStatus.RUNNING])
            completed_tasks = len([t for t in self.tasks.values()
                                  if t.status == TaskStatus.COMPLETED])

        return {
            "nodes": {
                "total": total_nodes,
                "available": available_nodes,
                "by_status": self._count_nodes_by_status()
            },
            "tasks": {
                "total": total_tasks,
                "pending": pending_tasks,
                "running": running_tasks,
                "completed": completed_tasks,
                "offline_queue": self.get_offline_queue_size()
            },
            "cache": self.get_cache_stats(),
            "offline_mode": self.offline_mode
        }

    def _count_nodes_by_status(self) -> Dict[str, int]:
        """Count nodes by status."""
        counts = defaultdict(int)
        with self._node_lock:
            for node in self.nodes.values():
                counts[node.status.value] += 1
        return dict(counts)

    def shutdown(self):
        """Shutdown edge computing module gracefully."""
        logger.info("Shutting down edge computing module...")
        self.stop_orchestration()

        # Clear sensitive data
        self.encryption_key = None

        logger.info("Edge computing module shut down")
