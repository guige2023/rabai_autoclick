"""
Process Monitoring and Management Module.

Monitors running processes, handles restarts, resource limits,
and provides process lifecycle management for automation.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class ProcessStatus(Enum):
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()
    UNKNOWN = auto()


@dataclass
class ProcessInfo:
    pid: int
    name: str
    command: str
    status: ProcessStatus = ProcessStatus.UNKNOWN
    started_at: Optional[datetime] = None
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    num_threads: int = 0
    exit_code: Optional[int] = None
    restart_count: int = 0


@dataclass
class ProcessConfig:
    name: str
    command: List[str]
    env: Optional[Dict[str, str]] = None
    cwd: Optional[str] = None
    uid: Optional[int] = None
    auto_restart: bool = False
    restart_delay: float = 5.0
    max_restarts: int = 3
    health_check_interval: float = 30.0
    shutdown_timeout: float = 10.0


class ProcessManager:
    """
    Manages process lifecycle including start, stop, restart, and monitoring.
    """

    def __init__(self):
        self._processes: Dict[str, Tuple[ProcessConfig, asyncio.subprocess.Process]] = {}
        self._monitors: Dict[str, asyncio.Task] = {}
        self._health_handlers: Dict[str, Callable] = {}
        self._running: bool = False
        self._event_handlers: Dict[str, List[Callable]] = defaultdict(list)

    def register_process(
        self,
        name: str,
        command: List[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        auto_restart: bool = False,
        restart_delay: float = 5.0,
        max_restarts: int = 3,
    ) -> ProcessConfig:
        config = ProcessConfig(
            name=name,
            command=command,
            env=env,
            cwd=cwd,
            auto_restart=auto_restart,
            restart_delay=restart_delay,
            max_restarts=max_restarts,
        )
        logger.info("Registered process: %s", name)
        return config

    async def start(self, name: str) -> bool:
        if name not in self._processes:
            logger.error("Process not registered: %s", name)
            return False

        config, proc = self._processes[name]
        if proc and proc.returncode is None:
            logger.warning("Process %s already running", name)
            return False

        try:
            process = await asyncio.create_subprocess_exec(
                *config.command,
                env={**os.environ, **(config.env or {})},
                cwd=config.cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            self._processes[name] = (config, process)
            self._emit_event("started", name, {"pid": process.pid})

            monitor_task = asyncio.create_task(self._monitor_process(name, process))
            self._monitors[name] = monitor_task

            logger.info("Started process %s (PID: %d)", name, process.pid)
            return True

        except Exception as exc:
            logger.error("Failed to start %s: %s", name, exc)
            return False

    async def stop(self, name: str, force: bool = False) -> bool:
        if name not in self._processes:
            return False

        config, process = self._processes[name]
        if process.returncode is not None:
            logger.info("Process %s already stopped", name)
            return True

        try:
            if force:
                process.kill()
            else:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=config.shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning("Process %s did not stop gracefully, killing", name)
                process.kill()
                await process.wait()

            self._emit_event("stopped", name, {"exit_code": process.returncode})

            if name in self._monitors:
                self._monitors[name].cancel()
                del self._monitors[name]

            logger.info("Stopped process %s", name)
            return True

        except Exception as exc:
            logger.error("Error stopping %s: %s", name, exc)
            return False

    async def restart(self, name: str) -> bool:
        await self.stop(name, force=True)
        await asyncio.sleep(1)
        return await self.start(name)

    async def _monitor_process(
        self, name: str, process: asyncio.subprocess.Process
    ) -> None:
        config, _ = self._processes.get(name, (None, None))
        if not config:
            return

        while process.returncode is None:
            await asyncio.sleep(config.health_check_interval)
            if process.returncode is not None:
                break

            try:
                poll_result = process.poll()
                if poll_result is not None:
                    break
            except Exception:
                break

        exit_code = process.returncode

        if config.auto_restart and config.max_restarts > 0:
            current_restarts = getattr(process, "_restart_count", 0)
            if current_restarts < config.max_restarts:
                logger.info("Auto-restarting %s (attempt %d)", name, current_restarts + 1)
                await asyncio.sleep(config.restart_delay)
                await self.start(name)

    async def get_process_info(self, name: str) -> Optional[ProcessInfo]:
        if name not in self._processes:
            return None
        config, process = self._processes[name]
        if process is None or process.returncode is not None:
            return None
        try:
            proc = psutil_process(process.pid)
        except Exception:
            return None
        return ProcessInfo(
            pid=process.pid,
            name=name,
            command=" ".join(config.command),
            status=ProcessStatus.RUNNING,
            cpu_percent=proc.cpu_percent(),
            memory_mb=proc.memory_info().rss / (1024 * 1024),
            num_threads=proc.num_threads(),
        )

    def _emit_event(self, event_type: str, process_name: str, data: Dict[str, Any]) -> None:
        key = f"{process_name}:{event_type}"
        handlers = self._event_handlers.get(key, [])
        for handler in handlers:
            try:
                handler(data)
            except Exception as exc:
                logger.error("Event handler error: %s", exc)

    def on_event(
        self, event_type: str, process_name: str, handler: Callable
    ) -> None:
        key = f"{process_name}:{event_type}"
        self._event_handlers[key].append(handler)

    def list_processes(self) -> List[str]:
        return list(self._processes.keys())

    async def cleanup(self) -> None:
        for name in list(self._processes.keys()):
            await self.stop(name, force=True)
        logger.info("Process manager cleaned up")


def psutil_process(pid: int):
    import psutil
    return psutil.Process(pid)
