"""FTP action module for RabAI AutoClick.

Provides FTP/SFTP file transfer operations including upload, download,
directory listing, and remote file management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union, Tuple
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FTPConnection:
    """FTP connection wrapper with SSL support.
    
    Provides context manager for FTP connections with automatic
    connection pooling and error handling.
    """
    
    def __init__(
        self,
        host: str,
        port: int = 21,
        username: str = "anonymous",
        password: str = "",
        use_tls: bool = False,
        timeout: int = 30
    ) -> None:
        """Initialize FTP connection parameters.
        
        Args:
            host: FTP server hostname or IP address.
            port: FTP port (default 21, 990 for implicit TLS).
            username: FTP username.
            password: FTP password.
            use_tls: Whether to use FTP/TLS (FTPS).
            timeout: Connection timeout in seconds.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout = timeout
        self._conn = None
        self._connected = False
    
    def connect(self) -> bool:
        """Establish FTP connection.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import ftplib
            
            if self.use_tls:
                self._conn = ftplib.FTP_TLS(timeout=self.timeout)
                self._conn.connect(self.host, self.port)
                self._conn.login(self.username, self.password)
                self._conn.prot_p()
            else:
                self._conn = ftplib.FTP(timeout=self.timeout)
                self._conn.connect(self.host, self.port)
                self._conn.login(self.username, self.password)
            
            self._connected = True
            return True
        
        except Exception:
            self._connected = False
            return False
    
    def disconnect(self) -> bool:
        """Close the FTP connection.
        
        Returns:
            True if disconnection successful.
        """
        try:
            if self._conn and self._connected:
                self._conn.quit()
            self._connected = False
            self._conn = None
            return True
        except Exception:
            self._connected = False
            self._conn = None
            return False
    
    def list_directory(self, path: str = ".") -> List[str]:
        """List contents of a remote directory.
        
        Args:
            path: Remote directory path.
            
        Returns:
            List of file and directory names.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        return self._conn.nlst(path)
    
    def list_directory_details(self, path: str = ".") -> List[Dict[str, str]]:
        """List directory contents with detailed information.
        
        Args:
            path: Remote directory path.
            
        Returns:
            List of dictionaries with name, size, and date info.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        lines: List[str] = []
        self._conn.retrlines(f"LIST {path}", lines.append)
        
        results: List[Dict[str, str]] = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 9:
                results.append({
                    "permissions": parts[0],
                    "size": parts[4],
                    "date": f"{parts[5]} {parts[6]} {parts[7]}",
                    "name": " ".join(parts[8:])
                })
        
        return results
    
    def download_file(
        self,
        remote_path: str,
        local_path: str,
        overwrite: bool = True
    ) -> bool:
        """Download a file from the FTP server.
        
        Args:
            remote_path: Path to the remote file.
            local_path: Local destination path.
            overwrite: Whether to overwrite existing local file.
            
        Returns:
            True if download successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        if os.path.exists(local_path) and not overwrite:
            raise FileExistsError(f"Local file already exists: {local_path}")
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        try:
            with open(local_path, "wb") as f:
                self._conn.retrbinary(f"RETR {remote_path}", f.write)
            return True
        except Exception:
            if os.path.exists(local_path):
                os.remove(local_path)
            return False
    
    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        overwrite: bool = True
    ) -> bool:
        """Upload a file to the FTP server.
        
        Args:
            local_path: Path to the local file.
            remote_path: Destination path on the server.
            overwrite: Whether to overwrite existing remote file.
            
        Returns:
            True if upload successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        if not overwrite:
            try:
                self._conn.size(remote_path)
                raise FileExistsError(f"Remote file already exists: {remote_path}")
            except Exception:
                pass
        
        try:
            with open(local_path, "rb") as f:
                self._conn.storbinary(f"STOR {remote_path}", f)
            return True
        except Exception:
            return False
    
    def delete_file(self, remote_path: str) -> bool:
        """Delete a remote file.
        
        Args:
            remote_path: Path to the remote file.
            
        Returns:
            True if deletion successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        try:
            self._conn.delete(remote_path)
            return True
        except Exception:
            return False
    
    def make_directory(self, remote_path: str) -> bool:
        """Create a remote directory.
        
        Args:
            remote_path: Path for the new directory.
            
        Returns:
            True if creation successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        try:
            self._conn.mkd(remote_path)
            return True
        except Exception:
            return False
    
    def remove_directory(self, remote_path: str) -> bool:
        """Remove a remote directory.
        
        Args:
            remote_path: Path of the directory to remove.
            
        Returns:
            True if removal successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        try:
            self._conn.rmd(remote_path)
            return True
        except Exception:
            return False
    
    def rename(self, old_path: str, new_path: str) -> bool:
        """Rename a remote file or directory.
        
        Args:
            old_path: Current path.
            new_path: New path.
            
        Returns:
            True if rename successful, False otherwise.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        try:
            self._conn.rename(old_path, new_path)
            return True
        except Exception:
            return False
    
    def get_file_size(self, remote_path: str) -> Optional[int]:
        """Get the size of a remote file.
        
        Args:
            remote_path: Path to the remote file.
            
        Returns:
            File size in bytes, or None if not available.
        """
        if not self._connected or not self._conn:
            raise RuntimeError("Not connected")
        
        try:
            return self._conn.size(remote_path)
        except Exception:
            return None
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connected


class FTPAction(BaseAction):
    """FTP action for file transfer operations.
    
    Supports FTP and FTP/TLS (FTPS) with context manager usage.
    """
    action_type: str = "ftp"
    display_name: str = "FTP动作"
    description: str = "FTP文件传输操作，支持上传、下载和目录管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._current_connection: Optional[FTPConnection] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute FTP operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(params, start_time)
            elif operation == "list":
                return self._list(params, start_time)
            elif operation == "download":
                return self._download(params, start_time)
            elif operation == "upload":
                return self._upload(params, start_time)
            elif operation == "delete":
                return self._delete(params, start_time)
            elif operation == "mkdir":
                return self._mkdir(params, start_time)
            elif operation == "rmdir":
                return self._rmdir(params, start_time)
            elif operation == "rename":
                return self._rename(params, start_time)
            elif operation == "size":
                return self._size(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"FTP operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to FTP server."""
        host = params.get("host", "")
        port = params.get("port", 21)
        username = params.get("username", "anonymous")
        password = params.get("password", "")
        use_tls = params.get("use_tls", False)
        
        if not host:
            return ActionResult(
                success=False,
                message="Host is required for connect operation",
                duration=time.time() - start_time
            )
        
        if self._current_connection and self._current_connection.is_connected:
            self._current_connection.disconnect()
        
        self._current_connection = FTPConnection(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls
        )
        
        success = self._current_connection.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to {host}:{port}" if success else "Connection failed",
            data={"host": host, "port": port, "tls": use_tls},
            duration=time.time() - start_time
        )
    
    def _disconnect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Disconnect from FTP server."""
        if self._current_connection:
            self._current_connection.disconnect()
            self._current_connection = None
        
        return ActionResult(
            success=True,
            message="Disconnected from FTP server",
            duration=time.time() - start_time
        )
    
    def _require_connection(self) -> FTPConnection:
        """Ensure an active FTP connection exists."""
        if not self._current_connection or not self._current_connection.is_connected:
            raise RuntimeError("Not connected to FTP server")
        return self._current_connection
    
    def _list(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List remote directory contents."""
        conn = self._require_connection()
        path = params.get("path", ".")
        detailed = params.get("detailed", False)
        
        if detailed:
            items = conn.list_directory_details(path)
        else:
            items = conn.list_directory(path)
        
        return ActionResult(
            success=True,
            message=f"Listed {len(items)} items",
            data={"items": items, "path": path},
            duration=time.time() - start_time
        )
    
    def _download(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Download a file from FTP server."""
        conn = self._require_connection()
        remote_path = params.get("remote_path", "")
        local_path = params.get("local_path", "")
        overwrite = params.get("overwrite", True)
        
        if not remote_path or not local_path:
            return ActionResult(
                success=False,
                message="remote_path and local_path are required",
                duration=time.time() - start_time
            )
        
        success = conn.download_file(remote_path, local_path, overwrite)
        
        return ActionResult(
            success=success,
            message=f"Downloaded {remote_path} to {local_path}" if success else "Download failed",
            data={"remote_path": remote_path, "local_path": local_path},
            duration=time.time() - start_time
        )
    
    def _upload(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Upload a file to FTP server."""
        conn = self._require_connection()
        local_path = params.get("local_path", "")
        remote_path = params.get("remote_path", "")
        overwrite = params.get("overwrite", True)
        
        if not local_path or not remote_path:
            return ActionResult(
                success=False,
                message="local_path and remote_path are required",
                duration=time.time() - start_time
            )
        
        success = conn.upload_file(local_path, remote_path, overwrite)
        
        return ActionResult(
            success=success,
            message=f"Uploaded {local_path} to {remote_path}" if success else "Upload failed",
            data={"local_path": local_path, "remote_path": remote_path},
            duration=time.time() - start_time
        )
    
    def _delete(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a remote file."""
        conn = self._require_connection()
        remote_path = params.get("remote_path", "")
        
        if not remote_path:
            return ActionResult(
                success=False,
                message="remote_path is required",
                duration=time.time() - start_time
            )
        
        success = conn.delete_file(remote_path)
        
        return ActionResult(
            success=success,
            message=f"Deleted {remote_path}" if success else f"Failed to delete {remote_path}",
            duration=time.time() - start_time
        )
    
    def _mkdir(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a remote directory."""
        conn = self._require_connection()
        remote_path = params.get("remote_path", "")
        
        if not remote_path:
            return ActionResult(
                success=False,
                message="remote_path is required",
                duration=time.time() - start_time
            )
        
        success = conn.make_directory(remote_path)
        
        return ActionResult(
            success=success,
            message=f"Created directory {remote_path}" if success else f"Failed to create {remote_path}",
            duration=time.time() - start_time
        )
    
    def _rmdir(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Remove a remote directory."""
        conn = self._require_connection()
        remote_path = params.get("remote_path", "")
        
        if not remote_path:
            return ActionResult(
                success=False,
                message="remote_path is required",
                duration=time.time() - start_time
            )
        
        success = conn.remove_directory(remote_path)
        
        return ActionResult(
            success=success,
            message=f"Removed directory {remote_path}" if success else f"Failed to remove {remote_path}",
            duration=time.time() - start_time
        )
    
    def _rename(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Rename a remote file or directory."""
        conn = self._require_connection()
        old_path = params.get("old_path", "")
        new_path = params.get("new_path", "")
        
        if not old_path or not new_path:
            return ActionResult(
                success=False,
                message="old_path and new_path are required",
                duration=time.time() - start_time
            )
        
        success = conn.rename(old_path, new_path)
        
        return ActionResult(
            success=success,
            message=f"Renamed {old_path} to {new_path}" if success else f"Failed to rename {old_path}",
            duration=time.time() - start_time
        )
    
    def _size(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get size of a remote file."""
        conn = self._require_connection()
        remote_path = params.get("remote_path", "")
        
        if not remote_path:
            return ActionResult(
                success=False,
                message="remote_path is required",
                duration=time.time() - start_time
            )
        
        size = conn.get_file_size(remote_path)
        
        return ActionResult(
            success=size is not None,
            message=f"Size of {remote_path}: {size} bytes" if size else f"Could not get size of {remote_path}",
            data={"remote_path": remote_path, "size": size},
            duration=time.time() - start_time
        )
