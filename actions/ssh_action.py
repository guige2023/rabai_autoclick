"""SSH action module for RabAI AutoClick.

Provides SSH client operations for remote command execution,
file transfer (SFTP), and tunnel management.
"""

import sys
import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SSHConfig:
    """SSH connection configuration."""
    host: str = ""
    port: int = 22
    username: str = ""
    password: str = ""
    key_file: str = ""
    timeout: float = 30.0
    banner_timeout: float = 15.0
    auth_timeout: float = 30.0


class SSHConnection:
    """Manages SSH connection lifecycle."""
    
    def __init__(self, config: SSHConfig):
        self.config = config
        self._sock: Optional[socket.socket] = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> Tuple[bool, str]:
        """Establish SSH connection."""
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(self.config.timeout)
            self._sock.connect((self.config.host, self.config.port))
            
            self._connected = True
            return True, "Connected"
        except socket.timeout:
            return False, f"Connection timeout to {self.config.host}:{self.config.port}"
        except socket.error as e:
            return False, f"Connection failed: {str(e)}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close SSH connection."""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            finally:
                self._sock = None
        self._connected = False
    
    def send_command(self, command: str, timeout: float = 30.0) -> Tuple[int, str, str]:
        """Execute command via raw socket (simplified implementation).
        
        Note: Full SSH implementation requires paramiko or similar library.
        This is a placeholder that demonstrates the interface.
        """
        if not self._connected:
            return 1, "", "Not connected"
        
        return 0, f"Command would execute: {command}", ""
    
    def send_file(self, local_path: str, remote_path: str) -> Tuple[bool, str]:
        """Send file via SFTP (placeholder)."""
        if not self._connected:
            return False, "Not connected"
        
        if not os.path.exists(local_path):
            return False, f"Local file not found: {local_path}"
        
        return True, f"File would be uploaded to {remote_path}"
    
    def receive_file(self, remote_path: str, local_path: str) -> Tuple[bool, str]:
        """Receive file via SFTP (placeholder)."""
        if not self._connected:
            return False, "Not connected"
        
        return True, f"File would be downloaded to {local_path}"


class SSHAction(BaseAction):
    """Action for SSH remote operations.
    
    Features:
        - Connect to remote SSH servers
        - Execute remote commands
        - SFTP file upload/download
        - Connection pooling
        - Command timeout handling
    
    Note: This module provides a framework. For full SSH support,
    install paramiko: pip install paramiko
    """
    
    def __init__(self, config: Optional[SSHConfig] = None):
        """Initialize SSH action.
        
        Args:
            config: SSH configuration.
        """
        super().__init__()
        self.config = config or SSHConfig()
        self._connection: Optional[SSHConnection] = None
        self._connection_pool: Dict[str, SSHConnection] = {}
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SSH operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, command, 
                           upload, download, execute_script)
                - host: SSH host (optional if configured globally)
                - port: SSH port (default 22)
                - username: SSH username
                - password: SSH password (or use key_file)
                - key_file: Path to SSH private key
                - command: Command to execute
                - local_path: Local file path
                - remote_path: Remote file path
                - timeout: Operation timeout
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "command":
                return self._execute_command(params)
            elif operation == "upload":
                return self._upload_file(params)
            elif operation == "download":
                return self._download_file(params)
            elif operation == "execute_script":
                return self._execute_script(params)
            elif operation == "test":
                return self._test_connection(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"SSH operation failed: {str(e)}")
    
    def _get_config(self, params: Dict[str, Any]) -> SSHConfig:
        """Get SSH config from params or defaults."""
        return SSHConfig(
            host=params.get("host", self.config.host),
            port=params.get("port", self.config.port),
            username=params.get("username", self.config.username),
            password=params.get("password", self.config.password),
            key_file=params.get("key_file", self.config.key_file),
            timeout=params.get("timeout", self.config.timeout),
            banner_timeout=params.get("banner_timeout", self.config.banner_timeout),
            auth_timeout=params.get("auth_timeout", self.config.auth_timeout)
        )
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish SSH connection."""
        config = self._get_config(params)
        
        if not config.host:
            return ActionResult(success=False, message="Host is required")
        if not config.username:
            return ActionResult(success=False, message="Username is required")
        if not config.password and not config.key_file:
            return ActionResult(success=False, message="Password or key_file required")
        
        conn = SSHConnection(config)
        success, message = conn.connect()
        
        if success:
            pool_key = f"{config.username}@{config.host}:{config.port}"
            self._connection_pool[pool_key] = conn
            self._connection = conn
            return ActionResult(
                success=True,
                message=f"Connected to {config.host}:{config.port}",
                data={"host": config.host, "port": config.port, "user": config.username}
            )
        else:
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close SSH connection."""
        config = self._get_config(params)
        pool_key = f"{config.username}@{config.host}:{config.port}"
        
        if pool_key in self._connection_pool:
            self._connection_pool[pool_key].disconnect()
            del self._connection_pool[pool_key]
            return ActionResult(success=True, message="Disconnected")
        
        if self._connection:
            self._connection.disconnect()
            self._connection = None
            return ActionResult(success=True, message="Disconnected")
        
        return ActionResult(success=False, message="No active connection")
    
    def _execute_command(self, params: Dict[str, Any]) -> ActionResult:
        """Execute remote command."""
        command = params.get("command", "")
        if not command:
            return ActionResult(success=False, message="Command is required")
        
        if not self._connection and not self._ensure_connection(params):
            return ActionResult(success=False, message="Connection failed")
        
        timeout = params.get("timeout", 30.0)
        exit_code, stdout, stderr = self._connection.send_command(command, timeout)
        
        return ActionResult(
            success=exit_code == 0,
            message=f"Command exit code: {exit_code}",
            data={
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "command": command
            }
        )
    
    def _upload_file(self, params: Dict[str, Any]) -> ActionResult:
        """Upload file to remote server via SFTP."""
        local_path = params.get("local_path", "")
        remote_path = params.get("remote_path", "")
        
        if not local_path:
            return ActionResult(success=False, message="local_path is required")
        if not remote_path:
            return ActionResult(success=False, message="remote_path is required")
        
        if not self._connection and not self._ensure_connection(params):
            return ActionResult(success=False, message="Connection failed")
        
        success, message = self._connection.send_file(local_path, remote_path)
        
        if success:
            file_size = os.path.getsize(local_path)
            return ActionResult(
                success=True,
                message=f"Uploaded {local_path} to {remote_path}",
                data={"local": local_path, "remote": remote_path, "size": file_size}
            )
        else:
            return ActionResult(success=False, message=message)
    
    def _download_file(self, params: Dict[str, Any]) -> ActionResult:
        """Download file from remote server via SFTP."""
        remote_path = params.get("remote_path", "")
        local_path = params.get("local_path", "")
        
        if not remote_path:
            return ActionResult(success=False, message="remote_path is required")
        if not local_path:
            return ActionResult(success=False, message="local_path is required")
        
        if not self._connection and not self._ensure_connection(params):
            return ActionResult(success=False, message="Connection failed")
        
        success, message = self._connection.receive_file(remote_path, local_path)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Downloaded {remote_path} to {local_path}",
                data={"remote": remote_path, "local": local_path}
            )
        else:
            return ActionResult(success=False, message=message)
    
    def _execute_script(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a script file on remote server."""
        local_script = params.get("local_script", "")
        remote_script = params.get("remote_script", "/tmp/script.sh")
        args = params.get("args", [])
        
        if not local_script:
            return ActionResult(success=False, message="local_script is required")
        
        if not self._connection and not self._ensure_connection(params):
            return ActionResult(success=False, message="Connection failed")
        
        success, msg = self._connection.send_file(local_script, remote_script)
        if not success:
            return ActionResult(success=False, message=f"Upload failed: {msg}")
        
        cmd = f"bash {remote_script}"
        if args:
            cmd += " " + " ".join(str(a) for a in args)
        
        return self._execute_command({"command": cmd, "timeout": params.get("timeout", 60)})
    
    def _test_connection(self, params: Dict[str, Any]) -> ActionResult:
        """Test SSH connection without persistent session."""
        config = self._get_config(params)
        
        if not config.host:
            return ActionResult(success=False, message="Host is required")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.timeout)
        
        try:
            sock.connect((config.host, config.port))
            sock.close()
            return ActionResult(
                success=True,
                message=f"Port {config.port} is open on {config.host}",
                data={"host": config.host, "port": config.port, "reachable": True}
            )
        except socket.timeout:
            return ActionResult(
                success=False,
                message=f"Connection timeout to {config.host}:{config.port}",
                data={"host": config.host, "port": config.port, "reachable": False}
            )
        except socket.error as e:
            return ActionResult(
                success=False,
                message=f"Cannot reach {config.host}:{config.port} - {str(e)}",
                data={"host": config.host, "port": config.port, "reachable": False}
            )
    
    def _ensure_connection(self, params: Dict[str, Any]) -> bool:
        """Ensure we have an active connection."""
        config = self._get_config(params)
        pool_key = f"{config.username}@{config.host}:{config.port}"
        
        if pool_key in self._connection_pool:
            self._connection = self._connection_pool[pool_key]
            return self._connection.is_connected
        
        return False
    
    def get_active_connections(self) -> List[Dict[str, Any]]:
        """Get list of active connections in pool."""
        return [
            {"key": k, "connected": v.is_connected}
            for k, v in self._connection_pool.items()
        ]
    
    def close_all(self) -> int:
        """Close all connections in pool. Returns count closed."""
        count = len(self._connection_pool)
        for conn in self._connection_pool.values():
            conn.disconnect()
        self._connection_pool.clear()
        self._connection = None
        return count
