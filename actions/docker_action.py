"""Docker action module for RabAI AutoClick.

Provides Docker container operations including managing containers,
images, volumes, and executing commands inside containers.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DockerConfig:
    """Docker client configuration."""
    docker_host: str = ""
    tls_ca: str = ""
    tls_cert: str = ""
    tls_key: str = ""
    api_version: str = "1.41"
    timeout: int = 30


class DockerAction(BaseAction):
    """Action for Docker container operations.
    
    Features:
        - List containers and images
        - Start, stop, restart containers
        - Create and remove containers
        - Pull and remove images
        - Execute commands in containers
        - Manage volumes
        - Get container logs
        - Build images from Dockerfile
    
    Note: This module provides an HTTP API-based Docker client.
    For full functionality, install docker: pip install docker
    """
    
    def __init__(self, config: Optional[DockerConfig] = None):
        """Initialize Docker action.
        
        Args:
            config: Docker configuration.
        """
        super().__init__()
        self.config = config or DockerConfig()
        self._client = None
        self._base_url = self._get_base_url()
    
    def _get_base_url(self) -> str:
        """Determine Docker API base URL."""
        if self.config.docker_host:
            return self.config.docker_host
        
        if sys.platform == "win32":
            return "npipe:////./pipe/docker_engine"
        else:
            sock = "/var/run/docker.sock"
            if os.path.exists(sock):
                return f"unix://{sock}"
            return "unix:///var/run/docker.sock"
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute Docker operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (list_containers, list_images,
                           start, stop, restart, remove, create, exec, logs,
                           pull, remove_image, inspect, stats)
                - container: Container name or ID
                - image: Image name
                - command: Command to execute
                - all: Show all (including stopped)
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "list_containers":
                return self._list_containers(params)
            elif operation == "list_images":
                return self._list_images(params)
            elif operation == "start":
                return self._start_container(params)
            elif operation == "stop":
                return self._stop_container(params)
            elif operation == "restart":
                return self._restart_container(params)
            elif operation == "remove":
                return self._remove_container(params)
            elif operation == "create":
                return self._create_container(params)
            elif operation == "exec":
                return self._exec_in_container(params)
            elif operation == "logs":
                return self._get_logs(params)
            elif operation == "pull":
                return self._pull_image(params)
            elif operation == "remove_image":
                return self._remove_image(params)
            elif operation == "inspect":
                return self._inspect(params)
            elif operation == "stats":
                return self._container_stats(params)
            elif operation == "prune":
                return self._prune_resources(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"Docker operation failed: {str(e)}")
    
    def _make_request(self, method: str, path: str, data: Any = None, 
                     params: Dict = None) -> Tuple[int, Any):
        """Make HTTP request to Docker API."""
        import urllib.request
        import urllib.parse
        import urllib.error
        
        url = f"http://localhost/v{self.config.api_version}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        
        headers = {"Content-Type": "application/json"}
        
        body = json.dumps(data).encode("utf-8") if data else None
        
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                response_data = resp.read().decode("utf-8")
                return resp.status, json.loads(response_data) if response_data else {}
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            try:
                error_data = json.loads(error_body)
                return e.code, error_data
            except json.JSONDecodeError:
                return e.code, {"message": error_body}
        except urllib.error.URLError as e:
            return 503, {"message": f"Could not connect to Docker: {str(e)}"}
    
    def _list_containers(self, params: Dict[str, Any]) -> ActionResult:
        """List Docker containers."""
        all_containers = params.get("all", True)
        
        status, data = self._make_request(
            "GET", 
            "/containers/json",
            params={"all": all_containers}
        )
        
        if status == 200:
            containers = []
            for c in data:
                containers.append({
                    "id": c.get("Id", "")[:12],
                    "names": c.get("Names", []),
                    "image": c.get("Image", ""),
                    "state": c.get("State", ""),
                    "status": c.get("Status", ""),
                    "created": c.get("Created", 0),
                    "ports": c.get("Ports", [])
                })
            
            return ActionResult(
                success=True,
                message=f"Found {len(containers)} containers",
                data={"containers": containers, "count": len(containers)}
            )
        else:
            return ActionResult(success=False, message=f"Docker API error: {data.get('message', data)}")
    
    def _list_images(self, params: Dict[str, Any]) -> ActionResult:
        """List Docker images."""
        status, data = self._make_request("GET", "/images/json")
        
        if status == 200:
            images = []
            for img in data:
                images.append({
                    "id": img.get("Id", "").replace("sha256:", "")[:12],
                    "repo_tags": img.get("RepoTags", []),
                    "size": img.get("Size", 0),
                    "created": img.get("Created", 0)
                })
            
            return ActionResult(
                success=True,
                message=f"Found {len(images)} images",
                data={"images": images, "count": len(images)}
            )
        else:
            return ActionResult(success=False, message=f"Docker API error: {data.get('message', data)}")
    
    def _start_container(self, params: Dict[str, Any]) -> ActionResult:
        """Start a container."""
        container = params.get("container", "")
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request("POST", f"/containers/{container}/start")
        
        if status in (204, 304):
            return ActionResult(success=True, message=f"Container {container} started")
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _stop_container(self, params: Dict[str, Any]) -> ActionResult:
        """Stop a container."""
        container = params.get("container", "")
        timeout = params.get("timeout", 10)
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request(
            "POST", 
            f"/containers/{container}/stop",
            params={"t": timeout}
        )
        
        if status in (204, 304):
            return ActionResult(success=True, message=f"Container {container} stopped")
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _restart_container(self, params: Dict[str, Any]) -> ActionResult:
        """Restart a container."""
        container = params.get("container", "")
        timeout = params.get("timeout", 10)
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request(
            "POST", 
            f"/containers/{container}/restart",
            params={"t": timeout}
        )
        
        if status == 204:
            return ActionResult(success=True, message=f"Container {container} restarted")
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _remove_container(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a container."""
        container = params.get("container", "")
        force = params.get("force", False)
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request(
            "DELETE",
            f"/containers/{container}",
            params={"force": force}
        )
        
        if status == 204:
            return ActionResult(success=True, message=f"Container {container} removed")
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _create_container(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new container."""
        image = params.get("image", "")
        name = params.get("name", "")
        command = params.get("command", [])
        env = params.get("env", [])
        ports = params.get("ports", {})  # {"8080": "80"} host:container
        volumes = params.get("volumes", {})  # {"/host/path": "/container/path"}
        
        if not image:
            return ActionResult(success=False, message="image name required")
        
        container_config = {
            "Image": image,
            "Cmd": command if isinstance(command, list) else [command],
            "Env": env,
            "HostConfig": {
                "PortBindings": {}
            }
        }
        
        if ports:
            for host_port, container_port in ports.items():
                container_config["HostConfig"]["PortBindings"][f"{container_port}/tcp"] = [
                    {"HostPort": str(host_port)}
                ]
        
        if volumes:
            binds = []
            for host_path, container_path in volumes.items():
                binds.append(f"{host_path}:container_path")
            container_config["HostConfig"]["Binds"] = binds
        
        if name:
            container_config["name"] = name
        
        status, data = self._make_request("POST", "/containers/create", container_config)
        
        if status in (201, 200):
            container_id = data.get("Id", "")[:12]
            return ActionResult(
                success=True,
                message=f"Container {container_id} created",
                data={"container_id": container_id, "warnings": data.get("Warnings", [])}
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _exec_in_container(self, params: Dict[str, Any]) -> ActionResult:
        """Execute command in a running container."""
        container = params.get("container", "")
        command = params.get("command", "")
        env = params.get("env", [])
        workdir = params.get("workdir", "")
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        if not command:
            return ActionResult(success=False, message="command required")
        
        exec_config = {
            "AttachStdout": True,
            "AttachStderr": True,
            "Cmd": command.split() if isinstance(command, str) else command,
            "Env": env
        }
        if workdir:
            exec_config["WorkingDir"] = workdir
        
        status, data = self._make_request(
            "POST",
            f"/containers/{container}/exec",
            exec_config
        )
        
        if status != 201:
            return ActionResult(success=False, message=data.get("message", str(data)))
        
        exec_id = data.get("Id", "")
        
        start_config = {
            "Detach": False,
            "Tty": False
        }
        
        status, output = self._make_request(
            "POST",
            f"/exec/{exec_id}/start",
            start_config
        )
        
        if status == 200:
            return ActionResult(
                success=True,
                message="Command executed",
                data={
                    "output": output.get("output", ""),
                    "exit_code": output.get("ExitCode", 0)
                }
            )
        else:
            return ActionResult(success=False, message=str(output))
    
    def _get_logs(self, params: Dict[str, Any]) -> ActionResult:
        """Get container logs."""
        container = params.get("container", "")
        tail = params.get("tail", 100)
        timestamps = params.get("timestamps", False)
        since = params.get("since", 0)
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request(
            "GET",
            f"/containers/{container}/logs",
            params={
                "stdout": True,
                "stderr": True,
                "tail": str(tail),
                "timestamps": timestamps,
                "since": since
            }
        )
        
        if status == 200:
            logs = data if isinstance(data, str) else data.get("output", "")
            
            return ActionResult(
                success=True,
                message=f"Retrieved logs (last {tail} lines)",
                data={"logs": logs, "container": container}
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _pull_image(self, params: Dict[str, Any]) -> ActionResult:
        """Pull an image from registry."""
        image = params.get("image", "")
        tag = params.get("tag", "latest")
        
        if not image:
            return ActionResult(success=False, message="image name required")
        
        full_image = f"{image}:{tag}" if tag else image
        
        status, data = self._make_request(
            "POST",
            f"/images/create",
            params={"fromImage": image, "tag": tag}
        )
        
        if status in (200, 200):
            return ActionResult(
                success=True,
                message=f"Pulling {full_image}",
                data={"image": full_image}
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _remove_image(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a Docker image."""
        image = params.get("image", "")
        force = params.get("force", False)
        no_prune = params.get("no_prune", False)
        
        if not image:
            return ActionResult(success=False, message="image name required")
        
        status, data = self._make_request(
            "DELETE",
            f"/images/{image}",
            params={"force": force, "noprune": no_prune}
        )
        
        if status == 200:
            return ActionResult(success=True, message=f"Image {image} removed")
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _inspect(self, params: Dict[str, Any]) -> ActionResult:
        """Inspect container or image details."""
        target = params.get("container") or params.get("image", "")
        target_type = "container" if params.get("container") else "image"
        
        if not target:
            return ActionResult(success=False, message="container or image name required")
        
        path = f"/{target_type}s/{target}"
        status, data = self._make_request("GET", path)
        
        if status == 200:
            return ActionResult(
                success=True,
                message=f"{target_type.capitalize()} {target} inspected",
                data=data
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _container_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get container resource usage statistics."""
        container = params.get("container", "")
        stream = params.get("stream", False)
        
        if not container:
            return ActionResult(success=False, message="container name or ID required")
        
        status, data = self._make_request(
            "GET",
            f"/containers/{container}/stats",
            params={"stream": stream}
        )
        
        if status == 200:
            stats = data
            return ActionResult(
                success=True,
                message="Container stats retrieved",
                data={
                    "cpu_percent": stats.get("cpu_stats", {}).get("cpu_usage", {}).get("percent", 0),
                    "memory_usage": stats.get("memory_stats", {}).get("usage", 0),
                    "memory_limit": stats.get("memory_stats", {}).get("limit", 0),
                    "network_rx": stats.get("networks", {}).get("eth0", {}).get("rx_bytes", 0),
                    "network_tx": stats.get("networks", {}).get("eth0", {}).get("tx_bytes", 0)
                }
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
    
    def _prune_resources(self, params: Dict[str, Any]) -> ActionResult:
        """Prune unused Docker resources."""
        prune_type = params.get("type", "all")  # containers, images, volumes, networks, all
        
        endpoints = {
            "containers": "/containers/prune",
            "images": "/images/prune",
            "volumes": "/volumes/prune",
            "networks": "/networks/prune",
            "all": "/prune"
        }
        
        path = endpoints.get(prune_type, "/prune")
        status, data = self._make_request("POST", path)
        
        if status == 200:
            space_reclaimed = data.get("SpaceReclaimed", 0)
            return ActionResult(
                success=True,
                message=f"Pruned {prune_type}, reclaimed {space_reclaimed} bytes",
                data=data
            )
        else:
            return ActionResult(success=False, message=data.get("message", str(data)))
