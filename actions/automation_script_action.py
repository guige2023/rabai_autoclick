"""Automation script executor and manager module.

This module provides capabilities for executing automation scripts in various formats,
managing script lifecycle, and handling execution context and results.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import asyncio
import subprocess
import os
import sys
import json
import time
import signal
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
import tempfile
import hashlib


class ScriptLanguage(Enum):
    """Supported scripting languages."""
    PYTHON = "python"
    JAVASCRIPT = "javascript"
    BASH = "bash"
    POWERSHELL = "powershell"
    PYTHON3 = "python3"


class ExecutionStatus(Enum):
    """Status of script execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class ExecutionContext:
    """Context passed to script execution."""
    execution_id: str
    working_directory: Optional[str] = None
    environment: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 300
    max_memory_mb: Optional[int] = None
    user: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    """Result of script execution."""
    execution_id: str
    status: ExecutionStatus
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    output_files: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ScriptDefinition:
    """Definition of an automation script."""
    name: str
    language: ScriptLanguage
    source: str
    description: Optional[str] = None
    version: str = "1.0"
    requires: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)


class ScriptRegistry:
    """Registry for managing automation scripts.
    
    Example:
        >>> registry = ScriptRegistry()
        >>> registry.register(ScriptDefinition(
        ...     name="hello",
        ...     language=ScriptLanguage.PYTHON,
        ...     source="print('Hello, World!')"
        ... ))
        >>> result = registry.execute("hello", {"name": "Alice"})
        >>> print(result.stdout)
    """
    
    def __init__(self):
        self._scripts: Dict[str, ScriptDefinition] = {}
        self._execution_history: List[ExecutionResult] = []
    
    def register(self, script: ScriptDefinition) -> None:
        """Register a new script."""
        self._scripts[script.name] = script
    
    def unregister(self, name: str) -> bool:
        """Unregister a script."""
        return self._scripts.pop(name, None) is not None
    
    def get(self, name: str) -> Optional[ScriptDefinition]:
        """Get a script by name."""
        return self._scripts.get(name)
    
    def list_scripts(self) -> List[str]:
        """List all registered script names."""
        return list(self._scripts.keys())
    
    def execute(
        self,
        name: str,
        parameters: Optional[Dict[str, Any]] = None,
        context: Optional[ExecutionContext] = None
    ) -> ExecutionResult:
        """Execute a registered script."""
        script = self._scripts.get(name)
        if not script:
            return ExecutionResult(
                execution_id="unknown",
                status=ExecutionStatus.FAILED,
                error=f"Script not found: {name}"
            )
        
        executor = ScriptExecutor()
        return executor.execute_script(script, parameters or {}, context)
    
    def get_history(self, limit: int = 100) -> List[ExecutionResult]:
        """Get execution history."""
        return self._execution_history[-limit:]


class ScriptExecutor:
    """Executor for running automation scripts.
    
    Example:
        >>> executor = ScriptExecutor()
        >>> script = ScriptDefinition(
        ...     name="demo",
        ...     language=ScriptLanguage.PYTHON,
        ...     source="import sys; print(sys.version)"
        ... )
        >>> result = executor.execute_script(script, {})
        >>> print(result.status)
    """
    
    def __init__(self):
        self._active_processes: Dict[str, subprocess.Popen] = {}
    
    def execute_script(
        self,
        script: ScriptDefinition,
        parameters: Dict[str, Any],
        context: Optional[ExecutionContext] = None
    ) -> ExecutionResult:
        """Execute a script with given parameters."""
        execution_id = self._generate_execution_id()
        
        if context is None:
            context = ExecutionContext(execution_id=execution_id)
        else:
            context.execution_id = execution_id
        
        start_time = time.time()
        started_at = datetime.now(timezone.utc).isoformat()
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                script_path, output_files = self._prepare_script(script, parameters, tmpdir)
                
                process = self._run_process(
                    script_path,
                    script.language,
                    context,
                    tmpdir
                )
                
                self._active_processes[execution_id] = process
                
                try:
                    stdout, stderr, exit_code = process.communicate(
                        timeout=context.timeout_seconds
                    )
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
                    duration = time.time() - start_time
                    return ExecutionResult(
                        execution_id=execution_id,
                        status=ExecutionStatus.TIMEOUT,
                        stdout=stdout.decode("utf-8", errors="replace"),
                        stderr=stderr.decode("utf-8", errors="replace"),
                        duration_seconds=duration,
                        started_at=started_at,
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        error=f"Execution timed out after {context.timeout_seconds}s"
                    )
                finally:
                    self._active_processes.pop(execution_id, None)
                
                duration = time.time() - start_time
                status = ExecutionStatus.COMPLETED if exit_code == 0 else ExecutionStatus.FAILED
                
                return ExecutionResult(
                    execution_id=execution_id,
                    status=status,
                    exit_code=exit_code,
                    stdout=stdout.decode("utf-8", errors="replace"),
                    stderr=stderr.decode("utf-8", errors="replace"),
                    duration_seconds=duration,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    output_files=output_files
                )
                
        except Exception as e:
            duration = time.time() - start_time
            return ExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                duration_seconds=duration,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e)
            )
    
    def _prepare_script(
        self,
        script: ScriptDefinition,
        parameters: Dict[str, Any],
        tmpdir: str
    ) -> tuple:
        """Prepare script file for execution."""
        source = self._inject_parameters(script.source, parameters)
        
        ext_map = {
            ScriptLanguage.PYTHON: ".py",
            ScriptLanguage.PYTHON3: ".py",
            ScriptLanguage.JAVASCRIPT: ".js",
            ScriptLanguage.BASH: ".sh",
            ScriptLanguage.POWERSHELL: ".ps1",
        }
        
        ext = ext_map.get(script.language, ".txt")
        script_path = os.path.join(tmpdir, f"script{ext}")
        
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(source)
        
        os.chmod(script_path, 0o755)
        
        output_files = []
        
        return script_path, output_files
    
    def _inject_parameters(self, source: str, parameters: Dict[str, Any]) -> str:
        """Inject parameters into script source."""
        if not parameters:
            return source
        
        param_json = json.dumps(parameters)
        source = f"import json\n_parameters = json.loads('''{param_json}''')\n\n" + source
        
        return source
    
    def _run_process(
        self,
        script_path: str,
        language: ScriptLanguage,
        context: ExecutionContext,
        tmpdir: str
    ) -> subprocess.Popen:
        """Run the script as a process."""
        cmd_map = {
            ScriptLanguage.PYTHON: ["python", script_path],
            ScriptLanguage.PYTHON3: ["python3", script_path],
            ScriptLanguage.JAVASCRIPT: ["node", script_path],
            ScriptLanguage.BASH: ["bash", script_path],
            ScriptLanguage.POWERSHELL: ["pwsh", "-File", script_path],
        }
        
        cmd = cmd_map.get(language, ["python", script_path])
        
        env = os.environ.copy()
        env.update(context.environment)
        
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=context.working_directory or tmpdir,
            env=env,
            start_new_session=True
        )
    
    def cancel(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        process = self._active_processes.get(execution_id)
        if process:
            process.terminate()
            return True
        return False
    
    def _generate_execution_id(self) -> str:
        """Generate unique execution ID."""
        timestamp = str(time.time()).encode("utf-8")
        random_bytes = os.urandom(8)
        return hashlib.sha256(timestamp + random_bytes).hexdigest()[:16]


class AsyncScriptExecutor:
    """Async executor for running multiple scripts concurrently."""
    
    def __init__(self, max_concurrent: int = 5):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._executor = ScriptExecutor()
    
    async def execute_async(
        self,
        script: ScriptDefinition,
        parameters: Dict[str, Any],
        context: Optional[ExecutionContext] = None
    ) -> ExecutionResult:
        """Execute a script asynchronously."""
        async with self._semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self._executor.execute_script,
                script,
                parameters,
                context
            )
    
    async def execute_many(
        self,
        scripts: List[tuple[ScriptDefinition, Dict[str, Any]]]
    ) -> List[ExecutionResult]:
        """Execute multiple scripts concurrently."""
        tasks = [
            self.execute_async(script, params)
            for script, params in scripts
        ]
        return await asyncio.gather(*tasks)


__all__ = [
    "ScriptLanguage",
    "ExecutionStatus",
    "ExecutionContext",
    "ExecutionResult",
    "ScriptDefinition",
    "ScriptRegistry",
    "ScriptExecutor",
    "AsyncScriptExecutor",
]
