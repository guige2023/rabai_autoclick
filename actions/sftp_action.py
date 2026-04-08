"""SFTP Action Module.

Provides secure file transfer capabilities over SSH including
upload, download, directory listing, and file permission management.
"""
from __future__ import annotations

import hashlib
import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os as _os

_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class FileType(Enum):
    """SFTP file type."""
    REGULAR = "regular"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    DEVICE = "device"
    FIFO = "fifo"
    SOCKET = "socket"


@dataclass
class SFTPFileInfo:
    """File information from SFTP listing."""
    filename: str
    path: str
    file_type: FileType
    size: int
    permissions: str
    uid: int
    gid: int
    atime: float
    mtime: float


@dataclass
class TransferProgress:
    """File transfer progress."""
    total_bytes: int
    transferred_bytes: int
    speed_bps: float
    eta_seconds: float
    progress_percent: float


@dataclass
class OperationResult:
    """SFTP operation result."""
    success: bool
    operation: str
    path: str
    message: str
    data: Any = None
    duration_ms: float = 0.0


class SFTPSimulator:
    """Simulated SFTP file system for testing.

    In production, replace with paramiko or similar library.
    """

    def __init__(self, root_dir: str = "/tmp/sftp_sim"):
        self._root = root_dir
        _os.makedirs(root_dir, exist_ok=True)

    def _resolve_path(self, path: str) -> str:
        """Resolve relative path to absolute."""
        if not path.startswith("/"):
            path = _os.path.join(self._root, path)
        return _os.path.normpath(path)

    def list_dir(self, path: str) -> List[SFTPFileInfo]:
        """List directory contents."""
        abs_path = self._resolve_path(path)
        if not _os.path.exists(abs_path):
            return []

        files = []
        for name in _os.listdir(abs_path):
            full_path = _os.path.join(abs_path, name)
            stat = _os.stat(full_path)
            mode = stat.st_mode

            if _os.path.isdir(full_path):
                ftype = FileType.DIRECTORY
            elif _os.path.islink(full_path):
                ftype = FileType.SYMLINK
            elif _os.path.isfile(full_path):
                ftype = FileType.REGULAR
            else:
                ftype = FileType.REGULAR

            files.append(SFTPFileInfo(
                filename=name,
                path=full_path,
                file_type=ftype,
                size=stat.st_size,
                permissions=self._format_permissions(mode),
                uid=stat.st_uid,
                gid=stat.st_gid,
                atime=stat.st_atime,
                mtime=stat.st_mtime
            ))

        return sorted(files, key=lambda f: f.filename)

    def _format_permissions(self, mode: int) -> str:
        """Format file permissions as string."""
        perms = []
        for who in "USR", "GRP", "OTH":
            for what in "R", "W", "X":
                attr = f"{who[0]}{what[0]}"
                perms.append(what if mode & getattr(_os, f"S_I{what}{attr}") else "-")
        return "".join(perms)

    def stat(self, path: str) -> Optional[SFTPFileInfo]:
        """Get file info."""
        abs_path = self._resolve_path(path)
        if not _os.path.exists(abs_path):
            return None

        stat = _os.stat(abs_path)
        mode = stat.st_mode

        if _os.path.isdir(abs_path):
            ftype = FileType.DIRECTORY
        elif _os.path.islink(abs_path):
            ftype = FileType.SYMLINK
        else:
            ftype = FileType.REGULAR

        return SFTPFileInfo(
            filename=_os.path.basename(abs_path),
            path=abs_path,
            file_type=ftype,
            size=stat.st_size,
            permissions=self._format_permissions(mode),
            uid=stat.st_uid,
            gid=stat.st_gid,
            atime=stat.st_atime,
            mtime=stat.st_mtime
        )

    def download_file(self, remote_path: str, local_path: str,
                       progress_callback: Optional[Callable[[TransferProgress], None]] = None) -> OperationResult:
        """Download file from SFTP."""
        start = time.time()
        try:
            abs_remote = self._resolve_path(remote_path)
            if not _os.path.exists(abs_remote):
                return OperationResult(
                    success=False,
                    operation="download",
                    path=remote_path,
                    message=f"Remote file not found: {remote_path}",
                    duration_ms=(time.time() - start) * 1000
                )

            stat = _os.stat(abs_remote)
            total = stat.st_size

            with open(abs_remote, "rb") as remote_f:
                with open(local_path, "wb") as local_f:
                    transferred = 0
                    while True:
                        chunk = remote_f.read(8192)
                        if not chunk:
                            break
                        local_f.write(chunk)
                        transferred += len(chunk)

                        if progress_callback:
                            progress = TransferProgress(
                                total_bytes=total,
                                transferred_bytes=transferred,
                                speed_bps=transferred / max((time.time() - start), 0.001),
                                eta_seconds=(total - transferred) / max(transferred / max((time.time() - start), 0.001), 0.001),
                                progress_percent=(transferred / total * 100) if total > 0 else 100
                            )
                            progress_callback(progress)

            return OperationResult(
                success=True,
                operation="download",
                path=remote_path,
                message=f"Downloaded {remote_path} to {local_path}",
                data={"size": total},
                duration_ms=(time.time() - start) * 1000
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation="download",
                path=remote_path,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )

    def upload_file(self, local_path: str, remote_path: str,
                    progress_callback: Optional[Callable[[TransferProgress], None]] = None) -> OperationResult:
        """Upload file to SFTP."""
        start = time.time()
        try:
            if not _os.path.exists(local_path):
                return OperationResult(
                    success=False,
                    operation="upload",
                    path=remote_path,
                    message=f"Local file not found: {local_path}",
                    duration_ms=(time.time() - start) * 1000
                )

            abs_remote = self._resolve_path(remote_path)
            _os.makedirs(_os.path.dirname(abs_remote), exist_ok=True)

            stat = _os.stat(local_path)
            total = stat.st_size

            with open(local_path, "rb") as local_f:
                with open(abs_remote, "wb") as remote_f:
                    transferred = 0
                    while True:
                        chunk = local_f.read(8192)
                        if not chunk:
                            break
                        remote_f.write(chunk)
                        transferred += len(chunk)

                        if progress_callback:
                            elapsed = max(time.time() - start, 0.001)
                            progress = TransferProgress(
                                total_bytes=total,
                                transferred_bytes=transferred,
                                speed_bps=transferred / elapsed,
                                eta_seconds=(total - transferred) / max(transferred / elapsed, 0.001),
                                progress_percent=(transferred / total * 100) if total > 0 else 100
                            )
                            progress_callback(progress)

            return OperationResult(
                success=True,
                operation="upload",
                path=remote_path,
                message=f"Uploaded {local_path} to {remote_path}",
                data={"size": total},
                duration_ms=(time.time() - start) * 1000
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation="upload",
                path=remote_path,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )

    def delete(self, path: str, recursive: bool = False) -> OperationResult:
        """Delete file or directory."""
        start = time.time()
        try:
            abs_path = self._resolve_path(path)
            if not _os.path.exists(abs_path):
                return OperationResult(
                    success=False,
                    operation="delete",
                    path=path,
                    message=f"Path not found: {path}",
                    duration_ms=(time.time() - start) * 1000
                )

            if _os.path.isdir(abs_path):
                if recursive:
                    import shutil
                    shutil.rmtree(abs_path)
                else:
                    _os.rmdir(abs_path)
            else:
                _os.remove(abs_path)

            return OperationResult(
                success=True,
                operation="delete",
                path=path,
                message=f"Deleted {path}",
                duration_ms=(time.time() - start) * 1000
            )

        except Exception as e:
            return OperationResult(
                success=False,
                operation="delete",
                path=path,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )

    def mkdir(self, path: str, parents: bool = True) -> OperationResult:
        """Create directory."""
        start = time.time()
        try:
            abs_path = self._resolve_path(path)
            _os.makedirs(abs_path, exist_ok=parents)
            return OperationResult(
                success=True,
                operation="mkdir",
                path=path,
                message=f"Created directory {path}",
                duration_ms=(time.time() - start) * 1000
            )
        except Exception as e:
            return OperationResult(
                success=False,
                operation="mkdir",
                path=path,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )

    def rename(self, old_path: str, new_path: str) -> OperationResult:
        """Rename file or directory."""
        start = time.time()
        try:
            abs_old = self._resolve_path(old_path)
            abs_new = self._resolve_path(new_path)
            _os.rename(abs_old, abs_new)
            return OperationResult(
                success=True,
                operation="rename",
                path=old_path,
                message=f"Renamed {old_path} to {new_path}",
                data={"new_path": new_path},
                duration_ms=(time.time() - start) * 1000
            )
        except Exception as e:
            return OperationResult(
                success=False,
                operation="rename",
                path=old_path,
                message=str(e),
                duration_ms=(time.time() - start) * 1000
            )


_global_simulator = SFTPSimulator()


class SFTPAction:
    """SFTP file transfer action.

    Example:
        action = SFTPAction()

        files = action.list("/remote/path")
        action.download("/remote/file.txt", "/local/file.txt")
        action.upload("/local/file.txt", "/remote/file.txt")
    """

    def __init__(self, simulator: Optional[SFTPSimulator] = None):
        self._sftp = simulator or _global_simulator

    def connect(self, host: str, port: int, username: str,
                password: Optional[str] = None,
                key_file: Optional[str] = None) -> Dict[str, Any]:
        """Connect to SFTP server (simulated)."""
        return {
            "success": True,
            "host": host,
            "port": port,
            "username": username,
            "message": f"Connected to {host}:{port}"
        }

    def list(self, remote_path: str = "/") -> Dict[str, Any]:
        """List remote directory.

        Args:
            remote_path: Remote directory path

        Returns:
            Dict with list of files
        """
        try:
            files = self._sftp.list_dir(remote_path)
            return {
                "success": True,
                "path": remote_path,
                "files": [
                    {
                        "filename": f.filename,
                        "type": f.file_type.value,
                        "size": f.size,
                        "permissions": f.permissions,
                        "mtime": f.mtime
                    }
                    for f in files
                ],
                "count": len(files)
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def stat(self, remote_path: str) -> Dict[str, Any]:
        """Get remote file info.

        Args:
            remote_path: Remote file path

        Returns:
            Dict with file info
        """
        try:
            info = self._sftp.stat(remote_path)
            if info:
                return {
                    "success": True,
                    "file": {
                        "filename": info.filename,
                        "type": info.file_type.value,
                        "size": info.size,
                        "permissions": info.permissions,
                        "uid": info.uid,
                        "gid": info.gid,
                        "atime": info.atime,
                        "mtime": info.mtime
                    }
                }
            return {"success": False, "message": "File not found"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def download(self, remote_path: str, local_path: str) -> Dict[str, Any]:
        """Download file from remote.

        Args:
            remote_path: Remote file path
            local_path: Local destination path

        Returns:
            Dict with operation result
        """
        result = self._sftp.download_file(remote_path, local_path)
        return {
            "success": result.success,
            "operation": result.operation,
            "path": result.path,
            "message": result.message,
            "data": result.data,
            "duration_ms": result.duration_ms
        }

    def upload(self, local_path: str, remote_path: str) -> Dict[str, Any]:
        """Upload file to remote.

        Args:
            local_path: Local file path
            remote_path: Remote destination path

        Returns:
            Dict with operation result
        """
        result = self._sftp.upload_file(local_path, remote_path)
        return {
            "success": result.success,
            "operation": result.operation,
            "path": result.path,
            "message": result.message,
            "data": result.data,
            "duration_ms": result.duration_ms
        }

    def delete(self, remote_path: str, recursive: bool = False) -> Dict[str, Any]:
        """Delete remote file or directory.

        Args:
            remote_path: Remote path to delete
            recursive: Delete directories recursively

        Returns:
            Dict with operation result
        """
        result = self._sftp.delete(remote_path, recursive)
        return {
            "success": result.success,
            "operation": result.operation,
            "path": result.path,
            "message": result.message,
            "duration_ms": result.duration_ms
        }

    def mkdir(self, remote_path: str, parents: bool = True) -> Dict[str, Any]:
        """Create remote directory.

        Args:
            remote_path: Remote directory path
            parents: Create parent directories

        Returns:
            Dict with operation result
        """
        result = self._sftp.mkdir(remote_path, parents)
        return {
            "success": result.success,
            "operation": result.operation,
            "path": result.path,
            "message": result.message,
            "duration_ms": result.duration_ms
        }

    def rename(self, old_path: str, new_path: str) -> Dict[str, Any]:
        """Rename remote file or directory.

        Args:
            old_path: Current path
            new_path: New path

        Returns:
            Dict with operation result
        """
        result = self._sftp.rename(old_path, new_path)
        return {
            "success": result.success,
            "operation": result.operation,
            "path": result.path,
            "message": result.message,
            "data": result.data,
            "duration_ms": result.duration_ms
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SFTP action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "connect", "list", "stat", "download", "upload",
                         "delete", "mkdir", "rename"
            - host: SFTP host (for connect)
            - port: SFTP port (for connect)
            - username: Username (for connect)
            - password: Password (for connect)
            - key_file: SSH key file (for connect)
            - remote_path: Remote path
            - local_path: Local path (for download/upload)
            - old_path: Current path (for rename)
            - new_path: New path (for rename)
            - recursive: Recursive delete flag

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "list")
    action = SFTPAction()

    try:
        if operation == "connect":
            host = params.get("host", "localhost")
            port = params.get("port", 22)
            username = params.get("username", "")
            return action.connect(
                host=host,
                port=port,
                username=username,
                password=params.get("password"),
                key_file=params.get("key_file")
            )

        elif operation == "list":
            remote_path = params.get("remote_path", "/")
            return action.list(remote_path)

        elif operation == "stat":
            remote_path = params.get("remote_path", "")
            if not remote_path:
                return {"success": False, "message": "remote_path required"}
            return action.stat(remote_path)

        elif operation == "download":
            remote_path = params.get("remote_path", "")
            local_path = params.get("local_path", "")
            if not remote_path or not local_path:
                return {"success": False, "message": "remote_path and local_path required"}
            return action.download(remote_path, local_path)

        elif operation == "upload":
            local_path = params.get("local_path", "")
            remote_path = params.get("remote_path", "")
            if not local_path or not remote_path:
                return {"success": False, "message": "local_path and remote_path required"}
            return action.upload(local_path, remote_path)

        elif operation == "delete":
            remote_path = params.get("remote_path", "")
            recursive = params.get("recursive", False)
            if not remote_path:
                return {"success": False, "message": "remote_path required"}
            return action.delete(remote_path, recursive)

        elif operation == "mkdir":
            remote_path = params.get("remote_path", "")
            parents = params.get("parents", True)
            if not remote_path:
                return {"success": False, "message": "remote_path required"}
            return action.mkdir(remote_path, parents)

        elif operation == "rename":
            old_path = params.get("old_path", "")
            new_path = params.get("new_path", "")
            if not old_path or not new_path:
                return {"success": False, "message": "old_path and new_path required"}
            return action.rename(old_path, new_path)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"SFTP error: {str(e)}"}
