"""
Prefect workflow orchestration v1
Flow registration, execution, deployments, schedules, tasks, blocks, work queues, cloud integration, result storage, notifications
"""
import json
import time
import threading
import os
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime
import tempfile


class FlowState(Enum):
    """Flow state"""
    REGISTERED = "registered"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DeploymentState(Enum):
    """Deployment state"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"


class WorkQueueState(Enum):
    """Work queue state"""
    READY = "ready"
    PAUSED = "paused"
    FULL = "full"


@dataclass
class FlowDefinition:
    """Flow definition"""
    flow_id: str
    flow_name: str
    flow_data: Dict[str, Any]
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    version: str = "1.0.0"
    tags: List[str] = field(default_factory=list)
    state: FlowState = FlowState.REGISTERED
    created_at: float = field(default_factory=time.time)
    last_run_at: Optional[float] = None


@dataclass
class FlowRun:
    """Flow run"""
    run_id: str
    flow_id: str
    flow_name: str
    parameters: Dict[str, Any]
    state: FlowState
    run_start_time: float
    run_end_time: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Deployment:
    """Deployment"""
    deployment_id: str
    flow_id: str
    flow_name: str
    deployment_name: str
    version: str = "1.0.0"
    state: DeploymentState = DeploymentState.ACTIVE
    schedule: Optional[str] = None
    schedule_enabled: bool = False
    work_queue_name: str = "default"
    tags: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    storage_block: Optional[str] = None
    infrastructure_block: Optional[str] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class TaskDefinition:
    """Task definition"""
    task_id: str
    task_name: str
    task_func: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    tags: List[str] = field(default_factory=list)
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    timeout: Optional[int] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class WorkQueue:
    """Work queue"""
    queue_id: str
    queue_name: str
    concurrency: int = 10
    state: WorkQueueState = WorkQueueState.READY
    priority: int = 5
    description: str = ""
    created_at: float = field(default_factory=time.time)


@dataclass
class Block:
    """Prefect block"""
    block_id: str
    block_name: str
    block_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    created_at: float = field(default_factory=time.time)


@dataclass
class NotificationHook:
    """Notification hook"""
    hook_id: str
    name: str
    event_type: str
    channel_type: str
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class ResultStorage:
    """Result storage configuration"""
    storage_id: str
    storage_type: str
    location: str
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


class PrefectManager:
    """Prefect workflow orchestration manager"""

    def __init__(self, data_dir: str = "./data", notification_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.notification_callback = notification_callback

        # Flow storage
        self.flows: Dict[str, FlowDefinition] = {}

        # Flow runs
        self.flow_runs: Dict[str, FlowRun] = {}
        self._running_flows: Dict[str, threading.Thread] = {}

        # Deployments
        self.deployments: Dict[str, Deployment] = {}

        # Tasks
        self.tasks: Dict[str, TaskDefinition] = {}

        # Work queues
        self.work_queues: Dict[str, WorkQueue] = {}

        # Blocks
        self.blocks: Dict[str, Block] = {}

        # Notifications
        self.notification_hooks: Dict[str, NotificationHook] = {}

        # Result storage
        self.result_storage: Dict[str, ResultStorage] = {}

        # Cloud config
        self.cloud_config: Dict[str, Any] = {}

        # Scheduler state
        self._scheduler_running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Create data directory
        os.makedirs(data_dir, exist_ok=True)

        # Load data
        self._load_data()

    def _load_data(self) -> None:
        """Load persisted data"""
        # Load flows
        try:
            with open(f"{self.data_dir}/prefect_flows.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for flow_id, flow_data in data.items():
                    if "state" in flow_data:
                        flow_data["state"] = FlowState(flow_data["state"])
                    self.flows[flow_id] = FlowDefinition(**flow_data)
        except FileNotFoundError:
            pass

        # Load deployments
        try:
            with open(f"{self.data_dir}/prefect_deployments.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for dep_id, dep_data in data.items():
                    if "state" in dep_data:
                        dep_data["state"] = DeploymentState(dep_data["state"])
                    self.deployments[dep_id] = Deployment(**dep_data)
        except FileNotFoundError:
            pass

        # Load tasks
        try:
            with open(f"{self.data_dir}/prefect_tasks.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for task_id, task_data in data.items():
                    self.tasks[task_id] = TaskDefinition(**task_data)
        except FileNotFoundError:
            pass

        # Load work queues
        try:
            with open(f"{self.data_dir}/prefect_work_queues.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for queue_id, queue_data in data.items():
                    if "state" in queue_data:
                        queue_data["state"] = WorkQueueState(queue_data["state"])
                    self.work_queues[queue_id] = WorkQueue(**queue_data)
        except FileNotFoundError:
            pass

        # Load blocks
        try:
            with open(f"{self.data_dir}/prefect_blocks.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for block_id, block_data in data.items():
                    self.blocks[block_id] = Block(**block_data)
        except FileNotFoundError:
            pass

        # Load notification hooks
        try:
            with open(f"{self.data_dir}/prefect_notifications.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for hook_id, hook_data in data.items():
                    self.notification_hooks[hook_id] = NotificationHook(**hook_data)
        except FileNotFoundError:
            pass

        # Load result storage
        try:
            with open(f"{self.data_dir}/prefect_result_storage.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for storage_id, storage_data in data.items():
                    self.result_storage[storage_id] = ResultStorage(**storage_data)
        except FileNotFoundError:
            pass

        # Load cloud config
        try:
            with open(f"{self.data_dir}/prefect_cloud_config.json", "r", encoding="utf-8") as f:
                self.cloud_config = json.load(f)
        except FileNotFoundError:
            pass

    def _save_data(self) -> None:
        """Save data to persistence"""

        def convert_for_json(obj):
            if hasattr(obj, 'value'):
                return obj.value
            elif hasattr(obj, '__dict__') and not isinstance(obj, (dict, list, tuple, set, frozenset)):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple, set)):
                return [convert_for_json(x) for x in obj]
            return obj

        # Save flows
        flows_data = {flow_id: convert_for_json(asdict(flow)) for flow_id, flow in self.flows.items()}
        with open(f"{self.data_dir}/prefect_flows.json", "w", encoding="utf-8") as f:
            json.dump(flows_data, f, ensure_ascii=False, indent=2)

        # Save deployments
        deployments_data = {dep_id: convert_for_json(asdict(dep)) for dep_id, dep in self.deployments.items()}
        with open(f"{self.data_dir}/prefect_deployments.json", "w", encoding="utf-8") as f:
            json.dump(deployments_data, f, ensure_ascii=False, indent=2)

        # Save tasks
        tasks_data = {task_id: convert_for_json(asdict(task)) for task_id, task in self.tasks.items()}
        with open(f"{self.data_dir}/prefect_tasks.json", "w", encoding="utf-8") as f:
            json.dump(tasks_data, f, ensure_ascii=False, indent=2)

        # Save work queues
        queues_data = {queue_id: convert_for_json(asdict(queue)) for queue_id, queue in self.work_queues.items()}
        with open(f"{self.data_dir}/prefect_work_queues.json", "w", encoding="utf-8") as f:
            json.dump(queues_data, f, ensure_ascii=False, indent=2)

        # Save blocks
        blocks_data = {block_id: convert_for_json(asdict(block)) for block_id, block in self.blocks.items()}
        with open(f"{self.data_dir}/prefect_blocks.json", "w", encoding="utf-8") as f:
            json.dump(blocks_data, f, ensure_ascii=False, indent=2)

        # Save notification hooks
        hooks_data = {hook_id: convert_for_json(asdict(hook)) for hook_id, hook in self.notification_hooks.items()}
        with open(f"{self.data_dir}/prefect_notifications.json", "w", encoding="utf-8") as f:
            json.dump(hooks_data, f, ensure_ascii=False, indent=2)

        # Save result storage
        storage_data = {storage_id: convert_for_json(asdict(storage)) for storage_id, storage in self.result_storage.items()}
        with open(f"{self.data_dir}/prefect_result_storage.json", "w", encoding="utf-8") as f:
            json.dump(storage_data, f, ensure_ascii=False, indent=2)

        # Save cloud config
        with open(f"{self.data_dir}/prefect_cloud_config.json", "w", encoding="utf-8") as f:
            json.dump(self.cloud_config, f, ensure_ascii=False, indent=2)

    # ========== Flow Registration ==========

    def register_flow(self, flow_name: str, flow_data: Dict[str, Any],
                     parameters: Dict[str, Any] = None, description: str = "",
                     version: str = "1.0.0", tags: List[str] = None) -> str:
        """Register a Prefect flow"""
        flow_id = str(uuid.uuid4())[:12]

        flow = FlowDefinition(
            flow_id=flow_id,
            flow_name=flow_name,
            flow_data=flow_data,
            parameters=parameters or {},
            description=description,
            version=version,
            tags=tags or [],
            state=FlowState.REGISTERED
        )

        with self._lock:
            self.flows[flow_id] = flow

        self._save_data()
        self._trigger_notification("flow_registered", {"flow_id": flow_id, "flow_name": flow_name})
        return flow_id

    def get_flow(self, flow_id: str) -> Optional[FlowDefinition]:
        """Get flow by ID"""
        return self.flows.get(flow_id)

    def list_flows(self, tags: List[str] = None) -> List[FlowDefinition]:
        """List all flows, optionally filtered by tags"""
        flows = list(self.flows.values())
        if tags:
            flows = [f for f in flows if any(tag in f.tags for tag in tags)]
        return flows

    def update_flow(self, flow_id: str, **kwargs) -> bool:
        """Update flow attributes"""
        if flow_id not in self.flows:
            return False

        with self._lock:
            flow = self.flows[flow_id]
            for key, value in kwargs.items():
                if hasattr(flow, key):
                    setattr(flow, key, value)

        self._save_data()
        return True

    def delete_flow(self, flow_id: str) -> bool:
        """Delete a flow"""
        if flow_id not in self.flows:
            return False

        with self._lock:
            del self.flows[flow_id]

        self._save_data()
        return True

    # ========== Flow Execution ==========

    def run_flow(self, flow_id: str, parameters: Dict[str, Any] = None,
                 wait: bool = True, timeout: Optional[int] = None) -> str:
        """Run a flow with parameters"""
        flow = self.flows.get(flow_id)
        if not flow:
            raise ValueError(f"Flow {flow_id} not found")

        run_id = str(uuid.uuid4())[:12]
        run_params = {**flow.parameters, **(parameters or {})}

        run = FlowRun(
            run_id=run_id,
            flow_id=flow_id,
            flow_name=flow.name if hasattr(flow, 'name') else flow.flow_name,
            parameters=run_params,
            state=FlowState.RUNNING,
            run_start_time=time.time()
        )

        with self._lock:
            self.flow_runs[run_id] = run

        # Execute in thread
        def execute():
            try:
                result = self._execute_flow_steps(flow.flow_data, run_params, timeout)
                run.state = FlowState.COMPLETED
                run.result = result
            except Exception as e:
                run.state = FlowState.FAILED
                run.error = str(e)
                self._trigger_notification("flow_failed", {"flow_id": flow_id, "run_id": run_id, "error": str(e)})
            finally:
                run.run_end_time = time.time()
                flow.last_run_at = run.run_end_time
                self._save_data()

        thread = threading.Thread(target=execute)
        thread.start()

        if wait:
            thread.join(timeout=timeout)

        self._trigger_notification("flow_completed", {"flow_id": flow_id, "run_id": run_id})
        return run_id

    def _execute_flow_steps(self, flow_data: Dict[str, Any], parameters: Dict[str, Any],
                           timeout: Optional[int]) -> Any:
        """Execute flow steps"""
        steps = flow_data.get("steps", [])
        results = []

        for i, step in enumerate(steps):
            action = step.get("action", "")
            target = step.get("target", "")
            step_params = {**step.get("params", {}), **parameters}

            print(f"Executing step {i+1}: {action} -> {target}")
            results.append({"step": i + 1, "action": action, "target": target})

            time.sleep(0.1)

        return results

    def get_flow_run(self, run_id: str) -> Optional[FlowRun]:
        """Get flow run by ID"""
        return self.flow_runs.get(run_id)

    def list_flow_runs(self, flow_id: str = None, state: FlowState = None) -> List[FlowRun]:
        """List flow runs, optionally filtered"""
        runs = list(self.flow_runs.values())
        if flow_id:
            runs = [r for r in runs if r.flow_id == flow_id]
        if state:
            runs = [r for r in runs if r.state == state]
        return runs

    def cancel_flow_run(self, run_id: str) -> bool:
        """Cancel a running flow"""
        run = self.flow_runs.get(run_id)
        if not run or run.state != FlowState.RUNNING:
            return False

        run.state = FlowState.CANCELLED
        run.run_end_time = time.time()
        self._save_data()
        return True

    # ========== Deployment Management ==========

    def create_deployment(self, flow_id: str, deployment_name: str,
                         work_queue_name: str = "default", schedule: str = None,
                         tags: List[str] = None, parameters: Dict[str, Any] = None,
                         storage_block: str = None, infrastructure_block: str = None) -> str:
        """Create a deployment"""
        flow = self.flows.get(flow_id)
        if not flow:
            raise ValueError(f"Flow {flow_id} not found")

        deployment_id = str(uuid.uuid4())[:12]

        deployment = Deployment(
            deployment_id=deployment_id,
            flow_id=flow_id,
            flow_name=flow.flow_name,
            deployment_name=deployment_name,
            work_queue_name=work_queue_name,
            schedule=schedule,
            tags=tags or [],
            parameters=parameters or {},
            storage_block=storage_block,
            infrastructure_block=infrastructure_block
        )

        with self._lock:
            self.deployments[deployment_id] = deployment

        self._save_data()
        self._trigger_notification("deployment_created", {"deployment_id": deployment_id, "deployment_name": deployment_name})
        return deployment_id

    def get_deployment(self, deployment_id: str) -> Optional[Deployment]:
        """Get deployment by ID"""
        return self.deployments.get(deployment_id)

    def list_deployments(self, flow_id: str = None, tags: List[str] = None) -> List[Deployment]:
        """List deployments"""
        deps = list(self.deployments.values())
        if flow_id:
            deps = [d for d in deps if d.flow_id == flow_id]
        if tags:
            deps = [d for d in deps if any(tag in d.tags for tag in tags)]
        return deps

    def update_deployment(self, deployment_id: str, **kwargs) -> bool:
        """Update deployment attributes"""
        if deployment_id not in self.deployments:
            return False

        with self._lock:
            deployment = self.deployments[deployment_id]
            for key, value in kwargs.items():
                if hasattr(deployment, key):
                    setattr(deployment, key, value)

        self._save_data()
        return True

    def delete_deployment(self, deployment_id: str) -> bool:
        """Delete a deployment"""
        if deployment_id not in self.deployments:
            return False

        with self._lock:
            del self.deployments[deployment_id]

        self._save_data()
        return True

    # ========== Schedule Management ==========

    def set_deployment_schedule(self, deployment_id: str, schedule: str, enabled: bool = True) -> bool:
        """Set or update deployment schedule"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            return False

        deployment.schedule = schedule
        deployment.schedule_enabled = enabled
        self._save_data()
        return True

    def enable_schedule(self, deployment_id: str, enabled: bool = True) -> bool:
        """Enable or disable deployment schedule"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            return False

        deployment.schedule_enabled = enabled
        self._save_data()
        return True

    def get_next_scheduled_run(self, deployment_id: str) -> Optional[float]:
        """Get next scheduled run time for deployment"""
        deployment = self.deployments.get(deployment_id)
        if not deployment or not deployment.schedule:
            return None

        # Parse cron expression (simplified)
        schedule = deployment.schedule
        # In a real implementation, use croniter to calculate next run
        return time.time() + 3600  # Placeholder

    # ========== Task Management ==========

    def register_task(self, task_name: str, task_func: str,
                     parameters: Dict[str, Any] = None, description: str = "",
                     tags: List[str] = None, retry_policy: Dict[str, Any] = None,
                     timeout: int = None) -> str:
        """Register a task"""
        task_id = str(uuid.uuid4())[:12]

        task = TaskDefinition(
            task_id=task_id,
            task_name=task_name,
            task_func=task_func,
            parameters=parameters or {},
            description=description,
            tags=tags or [],
            retry_policy=retry_policy or {},
            timeout=timeout
        )

        with self._lock:
            self.tasks[task_id] = task

        self._save_data()
        return task_id

    def get_task(self, task_id: str) -> Optional[TaskDefinition]:
        """Get task by ID"""
        return self.tasks.get(task_id)

    def list_tasks(self, tags: List[str] = None) -> List[TaskDefinition]:
        """List all tasks"""
        tasks = list(self.tasks.values())
        if tags:
            tasks = [t for t in tasks if any(tag in t.tags for tag in tags)]
        return tasks

    def update_task(self, task_id: str, **kwargs) -> bool:
        """Update task attributes"""
        if task_id not in self.tasks:
            return False

        with self._lock:
            task = self.tasks[task_id]
            for key, value in kwargs.items():
                if hasattr(task, key):
                    setattr(task, key, value)

        self._save_data()
        return True

    def delete_task(self, task_id: str) -> bool:
        """Delete a task"""
        if task_id not in self.tasks:
            return False

        with self._lock:
            del self.tasks[task_id]

        self._save_data()
        return True

    # ========== Block Management ==========

    def create_block(self, block_name: str, block_type: str,
                     data: Dict[str, Any] = None, description: str = "") -> str:
        """Create a Prefect block"""
        block_id = str(uuid.uuid4())[:12]

        block = Block(
            block_id=block_id,
            block_name=block_name,
            block_type=block_type,
            data=data or {},
            description=description
        )

        with self._lock:
            self.blocks[block_id] = block

        self._save_data()
        return block_id

    def get_block(self, block_id: str) -> Optional[Block]:
        """Get block by ID"""
        return self.blocks.get(block_id)

    def get_block_by_name(self, block_name: str, block_type: str = None) -> Optional[Block]:
        """Get block by name and optional type"""
        for block in self.blocks.values():
            if block.block_name == block_name:
                if block_type is None or block.block_type == block_type:
                    return block
        return None

    def list_blocks(self, block_type: str = None) -> List[Block]:
        """List blocks, optionally filtered by type"""
        blocks = list(self.blocks.values())
        if block_type:
            blocks = [b for b in blocks if b.block_type == block_type]
        return blocks

    def update_block(self, block_id: str, **kwargs) -> bool:
        """Update block attributes"""
        if block_id not in self.blocks:
            return False

        with self._lock:
            block = self.blocks[block_id]
            for key, value in kwargs.items():
                if hasattr(block, key):
                    setattr(block, key, value)

        self._save_data()
        return True

    def delete_block(self, block_id: str) -> bool:
        """Delete a block"""
        if block_id not in self.blocks:
            return False

        with self._lock:
            del self.blocks[block_id]

        self._save_data()
        return True

    # ========== Work Queue Management ==========

    def create_work_queue(self, queue_name: str, concurrency: int = 10,
                         priority: int = 5, description: str = "") -> str:
        """Create a work queue"""
        queue_id = str(uuid.uuid4())[:12]

        queue = WorkQueue(
            queue_id=queue_id,
            queue_name=queue_name,
            concurrency=concurrency,
            priority=priority,
            description=description
        )

        with self._lock:
            self.work_queues[queue_id] = queue

        self._save_data()
        return queue_id

    def get_work_queue(self, queue_id: str) -> Optional[WorkQueue]:
        """Get work queue by ID"""
        return self.work_queues.get(queue_id)

    def get_work_queue_by_name(self, queue_name: str) -> Optional[WorkQueue]:
        """Get work queue by name"""
        for queue in self.work_queues.values():
            if queue.queue_name == queue_name:
                return queue
        return None

    def list_work_queues(self) -> List[WorkQueue]:
        """List all work queues"""
        return list(self.work_queues.values())

    def update_work_queue(self, queue_id: str, **kwargs) -> bool:
        """Update work queue attributes"""
        if queue_id not in self.work_queues:
            return False

        with self._lock:
            queue = self.work_queues[queue_id]
            for key, value in kwargs.items():
                if hasattr(queue, key):
                    setattr(queue, key, value)

        self._save_data()
        return True

    def delete_work_queue(self, queue_id: str) -> bool:
        """Delete a work queue"""
        if queue_id not in self.work_queues:
            return False

        with self._lock:
            del self.work_queues[queue_id]

        self._save_data()
        return True

    def pause_work_queue(self, queue_id: str, paused: bool = True) -> bool:
        """Pause or resume a work queue"""
        queue = self.work_queues.get(queue_id)
        if not queue:
            return False

        queue.state = WorkQueueState.PAUSED if paused else WorkQueueState.READY
        self._save_data()
        return True

    # ========== Cloud Integration ==========

    def configure_cloud(self, api_url: str, api_key: str,
                       workspace: str = None, tenant: str = None) -> None:
        """Configure Prefect Cloud connection"""
        self.cloud_config = {
            "api_url": api_url,
            "api_key": api_key,
            "workspace": workspace,
            "tenant": tenant,
            "configured": True,
            "configured_at": time.time()
        }
        self._save_data()

    def get_cloud_config(self) -> Dict[str, Any]:
        """Get cloud configuration"""
        return self.cloud_config.copy()

    def is_cloud_configured(self) -> bool:
        """Check if cloud is configured"""
        return self.cloud_config.get("configured", False)

    def sync_to_cloud(self) -> bool:
        """Sync local state to Prefect Cloud"""
        if not self.is_cloud_configured():
            return False

        # In a real implementation, this would sync to Prefect Cloud API
        print(f"Syncing to Prefect Cloud at {self.cloud_config.get('api_url')}")
        self._trigger_notification("cloud_sync", {"status": "synced"})
        return True

    def sync_from_cloud(self) -> bool:
        """Sync state from Prefect Cloud"""
        if not self.is_cloud_configured():
            return False

        # In a real implementation, this would fetch from Prefect Cloud API
        print(f"Fetching from Prefect Cloud at {self.cloud_config.get('api_url')}")
        return True

    # ========== Result Storage ==========

    def configure_result_storage(self, storage_type: str, location: str,
                                  config: Dict[str, Any] = None) -> str:
        """Configure result storage"""
        storage_id = str(uuid.uuid4())[:12]

        storage = ResultStorage(
            storage_id=storage_id,
            storage_type=storage_type,
            location=location,
            config=config or {}
        )

        with self._lock:
            self.result_storage[storage_id] = storage

        self._save_data()
        return storage_id

    def get_result_storage(self, storage_id: str) -> Optional[ResultStorage]:
        """Get result storage by ID"""
        return self.result_storage.get(storage_id)

    def list_result_storage(self) -> List[ResultStorage]:
        """List all result storage configurations"""
        return list(self.result_storage.values())

    def update_result_storage(self, storage_id: str, **kwargs) -> bool:
        """Update result storage attributes"""
        if storage_id not in self.result_storage:
            return False

        with self._lock:
            storage = self.result_storage[storage_id]
            for key, value in kwargs.items():
                if hasattr(storage, key):
                    setattr(storage, key, value)

        self._save_data()
        return True

    def delete_result_storage(self, storage_id: str) -> bool:
        """Delete result storage configuration"""
        if storage_id not in self.result_storage:
            return False

        with self._lock:
            del self.result_storage[storage_id]

        self._save_data()
        return True

    def store_flow_result(self, run_id: str, result: Any) -> bool:
        """Store flow run result"""
        run = self.flow_runs.get(run_id)
        if not run:
            return False

        run.result = result
        self._save_data()
        return True

    # ========== Notification Hooks ==========

    def create_notification_hook(self, name: str, event_type: str,
                                 channel_type: str, config: Dict[str, Any] = None,
                                 enabled: bool = True) -> str:
        """Create a notification hook"""
        hook_id = str(uuid.uuid4())[:12]

        hook = NotificationHook(
            hook_id=hook_id,
            name=name,
            event_type=event_type,
            channel_type=channel_type,
            config=config or {},
            enabled=enabled
        )

        with self._lock:
            self.notification_hooks[hook_id] = hook

        self._save_data()
        return hook_id

    def get_notification_hook(self, hook_id: str) -> Optional[NotificationHook]:
        """Get notification hook by ID"""
        return self.notification_hooks.get(hook_id)

    def list_notification_hooks(self, event_type: str = None) -> List[NotificationHook]:
        """List notification hooks"""
        hooks = list(self.notification_hooks.values())
        if event_type:
            hooks = [h for h in hooks if h.event_type == event_type]
        return hooks

    def update_notification_hook(self, hook_id: str, **kwargs) -> bool:
        """Update notification hook attributes"""
        if hook_id not in self.notification_hooks:
            return False

        with self._lock:
            hook = self.notification_hooks[hook_id]
            for key, value in kwargs.items():
                if hasattr(hook, key):
                    setattr(hook, key, value)

        self._save_data()
        return True

    def delete_notification_hook(self, hook_id: str) -> bool:
        """Delete a notification hook"""
        if hook_id not in self.notification_hooks:
            return False

        with self._lock:
            del self.notification_hooks[hook_id]

        self._save_data()
        return True

    def _trigger_notification(self, event_type: str, data: Dict[str, Any]) -> None:
        """Trigger notification for event"""
        hooks = self.list_notification_hooks(event_type=event_type)

        for hook in hooks:
            if not hook.enabled:
                continue

            try:
                self._send_notification(hook, event_type, data)
            except Exception as e:
                print(f"Notification failed for hook {hook.hook_id}: {e}")

    def _send_notification(self, hook: NotificationHook, event_type: str, data: Dict[str, Any]) -> None:
        """Send notification via hook"""
        if hook.channel_type == "webhook":
            # Send webhook notification
            print(f"Sending webhook notification: {hook.config.get('url')}")
        elif hook.channel_type == "email":
            # Send email notification
            print(f"Sending email notification: {hook.config.get('address')}")
        elif hook.channel_type == "slack":
            # Send Slack notification
            print(f"Sending Slack notification: {hook.config.get('channel')}")
        elif hook.channel_type == "callback" and self.notification_callback:
            # Call notification callback
            self.notification_callback(event_type, data)
        else:
            print(f"Unknown channel type: {hook.channel_type}")

    # ========== Utility Methods ==========

    def get_status(self) -> Dict[str, Any]:
        """Get overall status"""
        return {
            "flows": len(self.flows),
            "deployments": len(self.deployments),
            "tasks": len(self.tasks),
            "work_queues": len(self.work_queues),
            "blocks": len(self.blocks),
            "notification_hooks": len(self.notification_hooks),
            "result_storage": len(self.result_storage),
            "cloud_configured": self.is_cloud_configured(),
            "flow_runs": len(self.flow_runs)
        }

    def cleanup_old_runs(self, max_age_seconds: int = 604800) -> int:
        """Cleanup old flow runs (default 7 days)"""
        cutoff = time.time() - max_age_seconds
        removed = 0

        with self._lock:
            run_ids = list(self.flow_runs.keys())
            for run_id in run_ids:
                run = self.flow_runs[run_id]
                if run.run_end_time and run.run_end_time < cutoff:
                    del self.flow_runs[run_id]
                    removed += 1

        if removed > 0:
            self._save_data()

        return removed
