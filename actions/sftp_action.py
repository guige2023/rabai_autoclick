"""SFTP action module for RabAI AutoClick.

Provides secure file transfer operations via SFTP protocol
for remote file management and automated uploads/downloads.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SFTPAction(BaseAction):
    """SFTP integration for secure file transfer operations.

    Supports upload, download, directory listing, file permissions,
    and remote command execution.

    Args:
        config: SFTP configuration containing host, port, username,
                password or private_key_path
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "")
        self.port = self.config.get("port", 22)
        self.username = self.config.get("username", "")
        self.password = self.config.get("password", "")
        self.private_key_path = self.config.get("private_key_path", "")
        self._client = None

    def _get_client(self):
        """Get or create SFTP client."""
        if self._client is None:
            try:
                import paramiko
            except ImportError:
                raise ImportError(
                    "paramiko not installed. Run: pip install paramiko"
                )

            transport = paramiko.Transport((self.host, self.port))
            if self.password:
                transport.connect(username=self.username, password=self.password)
            else:
                key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                transport.connect(username=self.username, pkey=key)

            self._client = paramiko.SFTPClient.from_transport(transport)
        return self._client

    def list_directory(self, remote_path: str = ".") -> ActionResult:
        """List remote directory contents.

        Args:
            remote_path: Remote directory path

        Returns:
            ActionResult with files list
        """
        try:
            client = self._get_client()
            files = client.listdir(remote_path)
            return ActionResult(success=True, data={"files": files})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def download_file(
        self,
        remote_path: str,
        local_path: str,
    ) -> ActionResult:
        """Download a file from remote.

        Args:
            remote_path: Remote file path
            local_path: Local destination path

        Returns:
            ActionResult with download status
        """
        try:
            client = self._get_client()
            client.get(remote_path, local_path)
            return ActionResult(
                success=True,
                data={"downloaded": local_path, "source": remote_path},
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def upload_file(
        self,
        local_path: str,
        remote_path: str,
    ) -> ActionResult:
        """Upload a file to remote.

        Args:
            local_path: Local file path
            remote_path: Remote destination path

        Returns:
            ActionResult with upload status
        """
        try:
            client = self._get_client()
            client.put(local_path, remote_path)
            return ActionResult(
                success=True,
                data={"uploaded": remote_path, "source": local_path},
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def create_directory(self, remote_path: str) -> ActionResult:
        """Create a remote directory.

        Args:
            remote_path: Directory path to create

        Returns:
            ActionResult with creation status
        """
        try:
            client = self._get_client()
            client.mkdir(remote_path)
            return ActionResult(success=True, data={"created": remote_path})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def remove_file(self, remote_path: str) -> ActionResult:
        """Remove a remote file.

        Args:
            remote_path: File path to remove

        Returns:
            ActionResult with removal status
        """
        try:
            client = self._get_client()
            client.remove(remote_path)
            return ActionResult(success=True, data={"removed": remote_path})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def remove_directory(self, remote_path: str) -> ActionResult:
        """Remove a remote directory.

        Args:
            remote_path: Directory path to remove

        Returns:
            ActionResult with removal status
        """
        try:
            client = self._get_client()
            client.rmdir(remote_path)
            return ActionResult(success=True, data={"removed": remote_path})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_file_info(self, remote_path: str) -> ActionResult:
        """Get file/directory information.

        Args:
            remote_path: Path to file or directory

        Returns:
            ActionResult with file info
        """
        try:
            client = self._get_client()
            stat = client.stat(remote_path)
            return ActionResult(
                success=True,
                data={
                    "path": remote_path,
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "mode": stat.st_mode,
                    "is_file": stat.isfile(),
                    "is_dir": stat.isdir(),
                },
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def rename(
        self,
        old_path: str,
        new_path: str,
    ) -> ActionResult:
        """Rename or move a remote file/directory.

        Args:
            old_path: Current path
            new_path: New path

        Returns:
            ActionResult with rename status
        """
        try:
            client = self._get_client()
            client.rename(old_path, new_path)
            return ActionResult(
                success=True, data={"renamed": f"{old_path} -> {new_path}"}
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def chmod(self, remote_path: str, mode: int) -> ActionResult:
        """Change file/directory permissions.

        Args:
            remote_path: Path to file or directory
            mode: Permission mode (octal)

        Returns:
            ActionResult with chmod status
        """
        try:
            client = self._get_client()
            client.chmod(remote_path, mode)
            return ActionResult(success=True, data={"chmod": mode})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def close(self) -> None:
        """Close SFTP connection."""
        if self._client:
            self._client.close()
            self._client = None

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute SFTP operation."""
        operations = {
            "list_directory": self.list_directory,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "create_directory": self.create_directory,
            "remove_file": self.remove_file,
            "remove_directory": self.remove_directory,
            "get_file_info": self.get_file_info,
            "rename": self.rename,
            "chmod": self.chmod,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
