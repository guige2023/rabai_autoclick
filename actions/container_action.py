"""Container management action module for RabAI AutoClick.

Provides container operations:
- ContainerListAction: List containers
- ContainerStartAction: Start container
- ContainerStopAction: Stop container
- ContainerRestartAction: Restart container
- ContainerRemoveAction: Remove container
- ContainerLogsAction: Get container logs
- ContainerStatsAction: Container statistics
- ContainerCreateAction: Create container
"""

import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContainerStore:
    """Mock container registry."""
    
    _containers: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def add(cls, container: Dict[str, Any]) -> None:
        cls._containers[container["id"]] = container
    
    @classmethod
    def get(cls, container_id: str) -> Optional[Dict[str, Any]]:
        return cls._containers.get(container_id)
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        return list(cls._containers.values())
    
    @classmethod
    def update_status(cls, container_id: str, status: str) -> None:
        if container_id in cls._containers:
            cls._containers[container_id]["status"] = status


class ContainerListAction(BaseAction):
    """List containers."""
    action_type = "container_list"
    display_name = "容器列表"
    description = "列出容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            all_containers = params.get("all", True)
            
            containers = ContainerStore.list_all()
            
            running = [c for c in containers if c.get("status") == "running"]
            stopped = [c for c in containers if c.get("status") == "stopped"]
            
            return ActionResult(
                success=True,
                message=f"Containers: {len(running)} running, {len(stopped)} stopped",
                data={
                    "containers": containers,
                    "running_count": len(running),
                    "stopped_count": len(stopped),
                    "total_count": len(containers)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container list failed: {str(e)}")


class ContainerStartAction(BaseAction):
    """Start container."""
    action_type = "container_start"
    display_name = "启动容器"
    description = "启动容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            ContainerStore.update_status(container_id, "running")
            
            return ActionResult(
                success=True,
                message=f"Started container: {container_id}",
                data={"container_id": container_id, "status": "running"}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container start failed: {str(e)}")


class ContainerStopAction(BaseAction):
    """Stop container."""
    action_type = "container_stop"
    display_name = "停止容器"
    description = "停止容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            timeout = params.get("timeout", 10)
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            ContainerStore.update_status(container_id, "stopped")
            
            return ActionResult(
                success=True,
                message=f"Stopped container: {container_id}",
                data={"container_id": container_id, "status": "stopped", "timeout": timeout}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container stop failed: {str(e)}")


class ContainerRestartAction(BaseAction):
    """Restart container."""
    action_type = "container_restart"
    display_name = "重启容器"
    description = "重启容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            ContainerStore.update_status(container_id, "running")
            
            return ActionResult(
                success=True,
                message=f"Restarted container: {container_id}",
                data={"container_id": container_id, "status": "running"}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container restart failed: {str(e)}")


class ContainerRemoveAction(BaseAction):
    """Remove container."""
    action_type = "container_remove"
    display_name = "删除容器"
    description = "删除容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            force = params.get("force", False)
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            if container.get("status") == "running" and not force:
                return ActionResult(success=False, message=f"Container {container_id} is running, use force=True")
            
            del ContainerStore._containers[container_id]
            
            return ActionResult(
                success=True,
                message=f"Removed container: {container_id}",
                data={"container_id": container_id, "removed": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container remove failed: {str(e)}")


class ContainerLogsAction(BaseAction):
    """Get container logs."""
    action_type = "container_logs"
    display_name = "容器日志"
    description = "获取容器日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            lines = params.get("lines", 100)
            tail = params.get("tail", True)
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            logs = [
                f"2026-04-08T10:00:00Z Container started",
                f"2026-04-08T10:00:01Z Initializing...",
                f"2026-04-08T10:00:02Z Ready to accept connections"
            ]
            
            return ActionResult(
                success=True,
                message=f"Got logs for container: {container_id}",
                data={"container_id": container_id, "logs": logs[-lines:], "count": len(logs)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container logs failed: {str(e)}")


class ContainerStatsAction(BaseAction):
    """Get container statistics."""
    action_type = "container_stats"
    display_name = "容器统计"
    description = "获取容器统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            container_id = params.get("container_id", "")
            stream = params.get("stream", False)
            
            if not container_id:
                return ActionResult(success=False, message="container_id required")
            
            container = ContainerStore.get(container_id)
            if not container:
                return ActionResult(success=False, message=f"Container not found: {container_id}")
            
            stats = {
                "container_id": container_id,
                "cpu_percent": 25.5,
                "memory_usage_mb": 512,
                "memory_limit_mb": 1024,
                "network_rx_bytes": 1024000,
                "network_tx_bytes": 512000,
                "disk_read_bytes": 10240000,
                "disk_write_bytes": 5120000
            }
            
            return ActionResult(
                success=True,
                message=f"Stats for container: {container_id}",
                data={"stats": stats}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container stats failed: {str(e)}")


class ContainerCreateAction(BaseAction):
    """Create container."""
    action_type = "container_create"
    display_name = "创建容器"
    description = "创建容器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            image = params.get("image", "")
            name = params.get("name", "")
            ports = params.get("ports", [])
            environment = params.get("environment", {})
            
            if not image:
                return ActionResult(success=False, message="image required")
            
            container_id = f"container-{int(time.time())}"
            
            container = {
                "id": container_id,
                "name": name or container_id,
                "image": image,
                "status": "created",
                "ports": ports,
                "environment": environment,
                "created_at": time.time()
            }
            
            ContainerStore.add(container)
            
            return ActionResult(
                success=True,
                message=f"Created container: {container_id}",
                data={"container": container}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Container create failed: {str(e)}")
