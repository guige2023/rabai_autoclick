"""
RabAI AutoClick REST API Server v22

FastAPI-based REST API server for remote workflow execution,
real-time progress via WebSocket, and OpenAPI documentation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Depends, Header, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import uvicorn


# =============================================================================
# Configuration
# =============================================================================

API_KEY = os.environ.get("RABAI_API_KEY", "rabai-secret-key-change-me")
RATE_LIMIT_REQUESTS = 100  # requests per window
RATE_LIMIT_WINDOW = 60  # seconds
CORS_ORIGINS = ["*"]  # Configure appropriately for production


# =============================================================================
# Data Models
# =============================================================================

class ExecutionStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class WorkflowStep(BaseModel):
    step_id: str
    name: str
    action: str
    params: Dict[str, Any] = {}
    enabled: bool = True
    timeout: int = 300


class WorkflowDefinition(BaseModel):
    workflow_id: str = Field(default_factory=lambda: f"wf_{uuid.uuid4().hex[:12]}")
    name: str
    description: str = ""
    steps: List[WorkflowStep] = []
    triggers: List[Dict[str, Any]] = []
    settings: Dict[str, Any] = {}
    version: str = "22.0.0"


class WorkflowExecution(BaseModel):
    execution_id: str
    workflow_id: str
    status: ExecutionStatus = ExecutionStatus.IDLE
    progress: float = 0.0
    current_step: Optional[int] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Any] = None


class ActionExecuteRequest(BaseModel):
    params: Dict[str, Any] = {}
    context: Dict[str, Any] = {}


class ActionExecuteResponse(BaseModel):
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration: float = 0.0


class HealthResponse(BaseModel):
    status: str
    timestamp: float
    version: str
    uptime: float


class MetricsResponse(BaseModel):
    total_workflows: int
    total_executions: int
    active_executions: int
    completed_executions: int
    failed_executions: int
    total_actions_executed: int
    average_execution_time: float
    success_rate: float


class WebSocketMessage(BaseModel):
    type: str
    execution_id: Optional[str] = None
    workflow_id: Optional[str] = None
    data: Any = None
    timestamp: float = Field(default_factory=time.time)


# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    """Simple in-memory rate limiter per client."""

    def __init__(self, max_requests: int = RATE_LIMIT_REQUESTS, window_seconds: int = RATE_LIMIT_WINDOW):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._clients: Dict[str, List[float]] = {}

    def _get_client_id(self, request: Request) -> str:
        """Get client identifier from request."""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def is_allowed(self, request: Request) -> bool:
        """Check if request is allowed under rate limit."""
        client_id = self._get_client_id(request)
        now = time.time()

        if client_id not in self._clients:
            self._clients[client_id] = []

        # Remove old timestamps outside the window
        self._clients[client_id] = [
            ts for ts in self._clients[client_id]
            if now - ts < self.window_seconds
        ]

        if len(self._clients[client_id]) >= self.max_requests:
            return False

        self._clients[client_id].append(now)
        return True

    def get_remaining(self, request: Request) -> int:
        """Get remaining requests for client."""
        client_id = self._get_client_id(request)
        now = time.time()

        if client_id not in self._clients:
            return self.max_requests

        recent = [ts for ts in self._clients[client_id] if now - ts < self.window_seconds]
        return max(0, self.max_requests - len(recent))


# =============================================================================
# Workflow Store (In-Memory for API Server)
# =============================================================================

class WorkflowStore:
    """In-memory store for workflows and executions."""

    def __init__(self):
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._executions: Dict[str, WorkflowExecution] = {}
        self._variables: Dict[str, Any] = {}
        self._execution_order: List[str] = []
        self._start_time = time.time()
        self._total_actions_executed = 0
        self._execution_times: List[float] = []

    # Workflow CRUD
    def create_workflow(self, workflow: WorkflowDefinition) -> WorkflowDefinition:
        self._workflows[workflow.workflow_id] = workflow
        return workflow

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        return self._workflows.get(workflow_id)

    def list_workflows(self) -> List[WorkflowDefinition]:
        return list(self._workflows.values())

    def update_workflow(self, workflow_id: str, workflow: WorkflowDefinition) -> Optional[WorkflowDefinition]:
        if workflow_id not in self._workflows:
            return None
        workflow.workflow_id = workflow_id
        self._workflows[workflow_id] = workflow
        return workflow

    def delete_workflow(self, workflow_id: str) -> bool:
        if workflow_id in self._workflows:
            del self._workflows[workflow_id]
            return True
        return False

    # Execution Management
    def create_execution(self, workflow_id: str) -> Optional[WorkflowExecution]:
        if workflow_id not in self._workflows:
            return None
        execution_id = f"exec_{uuid.uuid4().hex[:12]}"
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_id,
            status=ExecutionStatus.IDLE
        )
        self._executions[execution_id] = execution
        self._execution_order.append(execution_id)
        return execution

    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        return self._executions.get(execution_id)

    def get_workflow_executions(self, workflow_id: str) -> List[WorkflowExecution]:
        return [
            ex for ex in self._executions.values()
            if ex.workflow_id == workflow_id
        ]

    def update_execution(self, execution_id: str, **kwargs) -> Optional[WorkflowExecution]:
        if execution_id not in self._executions:
            return None
        for key, value in kwargs.items():
            if hasattr(self._executions[execution_id], key):
                setattr(self._executions[execution_id], key, value)
        return self._executions[execution_id]

    # Variables
    def get_variable(self, name: str, default: Any = None) -> Any:
        return self._variables.get(name, default)

    def set_variable(self, name: str, value: Any) -> None:
        self._variables[name] = value

    def get_all_variables(self) -> Dict[str, Any]:
        return self._variables.copy()

    # Metrics
    def get_metrics(self) -> MetricsResponse:
        executions = list(self._executions.values())
        completed = [ex for ex in executions if ex.status == ExecutionStatus.COMPLETED]
        failed = [ex for ex in executions if ex.status == ExecutionStatus.FAILED]
        active = [ex for ex in executions if ex.status == ExecutionStatus.RUNNING]

        avg_time = sum(self._execution_times) / len(self._execution_times) if self._execution_times else 0.0
        success_rate = len(completed) / len(executions) if executions else 0.0

        return MetricsResponse(
            total_workflows=len(self._workflows),
            total_executions=len(executions),
            active_executions=len(active),
            completed_executions=len(completed),
            failed_executions=len(failed),
            total_actions_executed=self._total_actions_executed,
            average_execution_time=avg_time,
            success_rate=success_rate
        )


# =============================================================================
# Action Executor (Mock/Stub for Remote Execution)
# =============================================================================

class ActionExecutor:
    """Executes actions locally (stub implementation)."""

    def __init__(self, store: WorkflowStore):
        self._store = store
        self._action_handlers: Dict[str, Callable] = self._register_handlers()

    def _register_handlers(self) -> Dict[str, Callable]:
        return {
            "delay": self._action_delay,
            "click": self._action_click,
            "type": self._action_type,
            "move": self._action_move,
            "scroll": self._action_scroll,
            "screenshot": self._action_screenshot,
            "wait_for": self._action_wait_for,
            "notify": self._action_notify,
        }

    async def execute(self, action_name: str, params: Dict[str, Any], context: Dict[str, Any]) -> ActionExecuteResponse:
        start_time = time.time()

        # Update context variables
        for key, value in context.items():
            self._store.set_variable(key, value)

        # Execute action
        handler = self._action_handlers.get(action_name)
        if handler:
            try:
                result = await handler(params)
                self._store._total_actions_executed += 1
                return ActionExecuteResponse(
                    success=True,
                    result=result,
                    duration=time.time() - start_time
                )
            except Exception as e:
                return ActionExecuteResponse(
                    success=False,
                    error=str(e),
                    duration=time.time() - start_time
                )
        else:
            # Generic execution for unknown actions
            self._store._total_actions_executed += 1
            return ActionExecuteResponse(
                success=True,
                result={"action": action_name, "params": params, "executed": True},
                duration=time.time() - start_time
            )

    async def _action_delay(self, params: Dict[str, Any]) -> Dict[str, Any]:
        duration = params.get("duration", 1.0)
        await asyncio.sleep(duration)
        return {"delayed": duration}

    async def _action_click(self, params: Dict[str, Any]) -> Dict[str, Any]:
        x = params.get("x", 0)
        y = params.get("y", 0)
        button = params.get("button", "left")
        clicks = params.get("clicks", 1)
        # In real implementation, would use pyautogui
        return {"clicked": True, "x": x, "y": y, "button": button, "clicks": clicks}

    async def _action_type(self, params: Dict[str, Any]) -> Dict[str, Any]:
        text = params.get("text", "")
        # In real implementation, would use pyautogui
        return {"typed": True, "text": text}

    async def _action_move(self, params: Dict[str, Any]) -> Dict[str, Any]:
        x = params.get("x", 0)
        y = params.get("y", 0)
        return {"moved": True, "x": x, "y": y}

    async def _action_scroll(self, params: Dict[str, Any]) -> Dict[str, Any]:
        x = params.get("x", 0)
        y = params.get("y", 0)
        direction = params.get("direction", "down")
        amount = params.get("amount", 3)
        return {"scrolled": True, "direction": direction, "amount": amount}

    async def _action_screenshot(self, params: Dict[str, Any]) -> Dict[str, Any]:
        path = params.get("path", "screenshot.png")
        region = params.get("region")
        return {"screenshot": True, "path": path, "region": region}

    async def _action_wait_for(self, params: Dict[str, Any]) -> Dict[str, Any]:
        timeout = params.get("timeout", 30)
        target = params.get("target", "")
        # In real implementation, would wait for image/text
        return {"waited": True, "target": target, "timeout": timeout}

    async def _action_notify(self, params: Dict[str, Any]) -> Dict[str, Any]:
        message = params.get("message", "")
        title = params.get("title", "RabAI")
        return {"notified": True, "title": title, "message": message}


# =============================================================================
# WebSocket Manager
# =============================================================================

class ConnectionManager:
    """Manages WebSocket connections for real-time updates."""

    def __init__(self):
        self._connections: Dict[str, Set[WebSocket]] = {}  # execution_id -> connections
        self._global_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket, execution_id: Optional[str] = None):
        await websocket.accept()
        if execution_id:
            if execution_id not in self._connections:
                self._connections[execution_id] = set()
            self._connections[execution_id].add(websocket)
        else:
            self._global_connections.add(websocket)

    def disconnect(self, websocket: WebSocket, execution_id: Optional[str] = None):
        if execution_id and execution_id in self._connections:
            self._connections[execution_id].discard(websocket)
            if not self._connections[execution_id]:
                del self._connections[execution_id]
        self._global_connections.discard(websocket)

    async def broadcast(self, message: WebSocketMessage, execution_id: Optional[str] = None):
        """Broadcast message to relevant connections."""
        import fastapi
        payload = json.dumps(asdict(message), default=str)

        if execution_id and execution_id in self._connections:
            dead = set()
            for ws in self._connections[execution_id]:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                self._connections[execution_id].discard(ws)

        # Also send to global connections
        dead = set()
        for ws in self._global_connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._global_connections.discard(ws)

    async def send_progress(self, execution_id: str, workflow_id: str,
                           progress: float, current_step: int, status: str):
        """Send progress update."""
        msg = WebSocketMessage(
            type="progress",
            execution_id=execution_id,
            workflow_id=workflow_id,
            data={
                "progress": progress,
                "current_step": current_step,
                "status": status
            }
        )
        await self.broadcast(msg, execution_id)


# =============================================================================
# Workflow Engine (Simplified Executor)
# =============================================================================

class WorkflowEngine:
    """Executes workflows with progress reporting."""

    def __init__(self, store: WorkflowStore, manager: ConnectionManager,
                 executor: ActionExecutor):
        self._store = store
        self._manager = manager
        self._executor = executor
        self._running_executions: Dict[str, asyncio.Task] = {}

    async def execute_workflow(self, execution_id: str, workflow_id: str):
        """Execute a workflow asynchronously."""
        execution = self._store.get_execution(execution_id)
        if not execution:
            return

        workflow = self._store.get_workflow(workflow_id)
        if not workflow:
            self._store.update_execution(execution_id,
                status=ExecutionStatus.FAILED,
                error="Workflow not found"
            )
            return

        # Update status to running
        self._store.update_execution(execution_id,
            status=ExecutionStatus.RUNNING,
            started_at=time.time()
        )

        await self._manager.send_progress(
            execution_id, workflow_id, 0.0, 0, ExecutionStatus.RUNNING.value
        )

        total_steps = len(workflow.steps)
        start_time = time.time()

        try:
            for idx, step in enumerate(workflow.steps):
                if not step.enabled:
                    continue

                # Check if execution was stopped
                current = self._store.get_execution(execution_id)
                if current and current.status == ExecutionStatus.STOPPED:
                    break

                # Update progress
                progress = (idx + 1) / total_steps * 100
                self._store.update_execution(execution_id,
                    progress=progress,
                    current_step=idx
                )

                await self._manager.send_progress(
                    execution_id, workflow_id, progress, idx, ExecutionStatus.RUNNING.value
                )

                # Execute step
                result = await self._executor.execute(
                    step.action, step.params, {}
                )

                if not result.success:
                    self._store.update_execution(execution_id,
                        status=ExecutionStatus.FAILED,
                        error=f"Step {step.name} failed: {result.error}",
                        result={"failed_step": idx, "error": result.error}
                    )
                    await self._manager.send_progress(
                        execution_id, workflow_id, progress, idx, ExecutionStatus.FAILED.value
                    )
                    return

            # Execution completed
            duration = time.time() - start_time
            self._store.update_execution(execution_id,
                status=ExecutionStatus.COMPLETED,
                progress=100.0,
                completed_at=time.time(),
                result={"completed": True, "steps": total_steps}
            )
            self._store._execution_times.append(duration)

            await self._manager.send_progress(
                execution_id, workflow_id, 100.0, total_steps - 1, ExecutionStatus.COMPLETED.value
            )

        except Exception as e:
            self._store.update_execution(execution_id,
                status=ExecutionStatus.FAILED,
                error=str(e)
            )
            await self._manager.send_progress(
                execution_id, workflow_id, 0, 0, ExecutionStatus.FAILED.value
            )

    def start_execution(self, execution_id: str, workflow_id: str) -> asyncio.Task:
        """Start workflow execution in background."""
        task = asyncio.create_task(self.execute_workflow(execution_id, workflow_id))
        self._running_executions[execution_id] = task
        return task

    async def pause_execution(self, execution_id: str) -> bool:
        """Pause execution (signal only - actual pause depends on implementation)."""
        execution = self._store.get_execution(execution_id)
        if not execution or execution.status != ExecutionStatus.RUNNING:
            return False
        # Signal pause - in real implementation would set a pause flag
        self._store.update_execution(execution_id, status=ExecutionStatus.PAUSED)
        return True

    async def resume_execution(self, execution_id: str) -> bool:
        """Resume execution."""
        execution = self._store.get_execution(execution_id)
        if not execution or execution.status != ExecutionStatus.PAUSED:
            return False
        self._store.update_execution(execution_id, status=ExecutionStatus.RUNNING)
        return True

    async def stop_execution(self, execution_id: str) -> bool:
        """Stop execution."""
        execution = self._store.get_execution(execution_id)
        if not execution:
            return False
        self._store.update_execution(execution_id,
            status=ExecutionStatus.STOPPED,
            completed_at=time.time()
        )
        # Cancel task if running
        if execution_id in self._running_executions:
            self._running_executions[execution_id].cancel()
        return True


# =============================================================================
# API Dependencies
# =============================================================================

store = WorkflowStore()
manager = ConnectionManager()
executor = ActionExecutor(store)
engine = WorkflowEngine(store, manager, executor)


async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """Verify API key from X-API-Key header."""
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


async def check_rate_limit(request: Request):
    """Check rate limit for request."""
    limiter = request.app.state.rate_limiter
    if not limiter.is_allowed(request):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"X-RateLimit-Remaining": "0"}
        )


# =============================================================================
# Request/Response Models
# =============================================================================

class ExecuteRequest(BaseModel):
    """Request to execute a workflow."""
    params: Dict[str, Any] = {}
    context: Dict[str, Any] = {}


class ExecuteResponse(BaseModel):
    """Response from workflow execution."""
    execution_id: str
    status: ExecutionStatus


class StatusResponse(BaseModel):
    """Execution status response."""
    execution_id: str
    workflow_id: str
    status: ExecutionStatus
    progress: float
    current_step: Optional[int]
    started_at: Optional[float]
    completed_at: Optional[float]
    error: Optional[str]
    result: Optional[Any]


class VariableResponse(BaseModel):
    """Variable response."""
    name: str
    value: Any


# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    app.state.rate_limiter = RateLimiter()
    app.state.start_time = time.time()
    yield
    # Cleanup


app = FastAPI(
    title="RabAI AutoClick API",
    description="REST API server for remote workflow execution with real-time progress",
    version="22.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Health & Metrics
# =============================================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Server health check."""
    return HealthResponse(
        status="healthy",
        timestamp=time.time(),
        version="22.0.0",
        uptime=time.time() - app.state.start_time
    )


@app.get("/metrics", response_model=MetricsResponse, tags=["Metrics"])
async def get_metrics():
    """Get execution statistics."""
    return store.get_metrics()


# =============================================================================
# Workflow CRUD Endpoints
# =============================================================================

@app.post("/workflows", response_model=WorkflowDefinition, tags=["Workflows"])
async def create_workflow(
    workflow: WorkflowDefinition,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Create a new workflow."""
    return store.create_workflow(workflow)


@app.get("/workflows", response_model=List[WorkflowDefinition], tags=["Workflows"])
async def list_workflows(
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List all workflows."""
    return store.list_workflows()


@app.get("/workflows/{workflow_id}", response_model=WorkflowDefinition, tags=["Workflows"])
async def get_workflow(
    workflow_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a specific workflow."""
    workflow = store.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


@app.put("/workflows/{workflow_id}", response_model=WorkflowDefinition, tags=["Workflows"])
async def update_workflow(
    workflow_id: str,
    workflow: WorkflowDefinition,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Update an existing workflow."""
    updated = store.update_workflow(workflow_id, workflow)
    if not updated:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return updated


@app.delete("/workflows/{workflow_id}", tags=["Workflows"])
async def delete_workflow(
    workflow_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Delete a workflow."""
    if not store.delete_workflow(workflow_id):
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"deleted": True, "workflow_id": workflow_id}


# =============================================================================
# Workflow Execution Endpoints
# =============================================================================

@app.post("/workflows/{workflow_id}/execute", response_model=ExecuteResponse, tags=["Execution"])
async def execute_workflow(
    workflow_id: str,
    request: ExecuteRequest = ExecuteRequest(),
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Start workflow execution."""
    # Verify workflow exists
    workflow = store.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    # Create execution
    execution = store.create_execution(workflow_id)
    if not execution:
        raise HTTPException(status_code=500, detail="Failed to create execution")

    # Set initial context variables
    for key, value in request.context.items():
        store.set_variable(key, value)

    # Start execution in background
    engine.start_execution(execution.execution_id, workflow_id)

    return ExecuteResponse(
        execution_id=execution.execution_id,
        status=ExecutionStatus.RUNNING
    )


@app.get("/workflows/{workflow_id}/status", response_model=List[StatusResponse], tags=["Execution"])
async def get_workflow_status(
    workflow_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get execution status for all executions of a workflow."""
    executions = store.get_workflow_executions(workflow_id)
    return [
        StatusResponse(
            execution_id=ex.execution_id,
            workflow_id=ex.workflow_id,
            status=ex.status,
            progress=ex.progress,
            current_step=ex.current_step,
            started_at=ex.started_at,
            completed_at=ex.completed_at,
            error=ex.error,
            result=ex.result
        )
        for ex in executions
    ]


@app.get("/executions/{execution_id}/status", response_model=StatusResponse, tags=["Execution"])
async def get_execution_status(
    execution_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get execution status for a specific execution."""
    execution = store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return StatusResponse(
        execution_id=execution.execution_id,
        workflow_id=execution.workflow_id,
        status=execution.status,
        progress=execution.progress,
        current_step=execution.current_step,
        started_at=execution.started_at,
        completed_at=execution.completed_at,
        error=execution.error,
        result=execution.result
    )


@app.post("/executions/{execution_id}/pause", tags=["Execution"])
async def pause_execution(
    execution_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Pause a running execution."""
    success = await engine.pause_execution(execution_id)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot pause execution")
    return {"paused": True, "execution_id": execution_id}


@app.post("/executions/{execution_id}/resume", tags=["Execution"])
async def resume_execution(
    execution_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Resume a paused execution."""
    success = await engine.resume_execution(execution_id)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot resume execution")
    return {"resumed": True, "execution_id": execution_id}


@app.post("/executions/{execution_id}/stop", tags=["Execution"])
async def stop_execution(
    execution_id: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Stop an execution."""
    success = await engine.stop_execution(execution_id)
    if not success:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"stopped": True, "execution_id": execution_id}


# =============================================================================
# Action Execution Endpoint
# =============================================================================

@app.post("/actions/{action_name}/execute", response_model=ActionExecuteResponse, tags=["Actions"])
async def execute_action(
    action_name: str,
    request: ActionExecuteRequest = ActionExecuteRequest(),
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Execute a single action."""
    return await executor.execute(action_name, request.params, request.context)


# =============================================================================
# Variable Management Endpoints
# =============================================================================

@app.get("/variables/{name}", response_model=VariableResponse, tags=["Variables"])
async def get_variable(
    name: str,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Get a context variable."""
    value = store.get_variable(name)
    return VariableResponse(name=name, value=value)


@app.put("/variables/{name}", response_model=VariableResponse, tags=["Variables"])
async def set_variable(
    name: str,
    value: Any,
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """Set a context variable."""
    store.set_variable(name, value)
    return VariableResponse(name=name, value=value)


@app.get("/variables", response_model=Dict[str, Any], tags=["Variables"])
async def list_variables(
    _api_key: str = Depends(verify_api_key),
    _rate_limit: None = Depends(check_rate_limit)
):
    """List all context variables."""
    return store.get_all_variables()


# =============================================================================
# WebSocket Endpoint
# =============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, execution_id: Optional[str] = None):
    """WebSocket endpoint for real-time execution progress."""
    await manager.connect(websocket, execution_id)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": time.time()
                    }))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket, execution_id)


@app.websocket("/ws/{execution_id}")
async def websocket_execution(websocket: WebSocket, execution_id: str):
    """WebSocket endpoint for specific execution progress."""
    await manager.connect(websocket, execution_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "timestamp": time.time()
                    }))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket, execution_id)


# =============================================================================
# Exception Handlers
# =============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=getattr(exc, "headers", None)
    )


# =============================================================================
# Main Entry Point
# =============================================================================

def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Run the API server."""
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RabAI AutoClick API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--api-key", default=None, help="Set API key (or use RABAI_API_KEY env)")
    args = parser.parse_args()

    if args.api_key:
        os.environ["RABAI_API_KEY"] = args.api_key

    run_server(host=args.host, port=args.port, reload=args.reload)
