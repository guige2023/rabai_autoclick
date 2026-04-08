"""
SFTP (Secure File Transfer Protocol) client utilities.

Provides high-level SFTP operations for secure file transfers.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Callable


try:
    import paramiko
except ImportError:
    paramiko = None  # type: ignore


class SFTPClient:
    """Context manager for SFTP operations."""

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "root",
        password: str | None = None,
        key_filename: str | None = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self._client: "paramiko.SSHClient | None" = None
        self._sftp: "paramiko.SFTPClient | None" = None

    def __enter__(self) -> "SFTPClient":
        if paramiko is None:
            raise ImportError("paramiko required: pip install paramiko")
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(
            self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            key_filename=self.key_filename,
        )
        self._sftp = self._client.open_sftp()
        return self

    def __exit__(self, *args: object) -> None:
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """
        Upload a file to remote server.

        Args:
            local_path: Local file path
            remote_path: Remote destination path
            callback: Optional progress callback
        """
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.put(str(local_path), remote_path, callback=callback)

    def download_file(
        self,
        remote_path: str,
        local_path: str | Path,
        callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """
        Download a file from remote server.

        Args:
            remote_path: Remote file path
            local_path: Local destination path
            callback: Optional progress callback
        """
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.get(remote_path, str(local_path), callback=callback)

    def list_dir(self, remote_path: str = ".") -> list[str]:
        """
        List remote directory contents.

        Args:
            remote_path: Remote directory path

        Returns:
            List of filenames
        """
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        return self._sftp.listdir(remote_path)

    def list_dir_attr(
        self,
        remote_path: str = ".",
    ) -> list["paramiko.SFTPAttributes"]:
        """
        List directory with full attributes.

        Args:
            remote_path: Remote directory path

        Returns:
            List of SFTPAttributes
        """
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        return self._sftp.listdir_attr(remote_path)

    def mkdir(self, remote_path: str, mode: int = 0o755) -> None:
        """Create remote directory."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.mkdir(remote_path, mode)

    def rmdir(self, remote_path: str) -> None:
        """Remove remote directory."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.rmdir(remote_path)

    def remove(self, remote_path: str) -> None:
        """Remove remote file."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.remove(remote_path)

    def rename(self, old_path: str, new_path: str) -> None:
        """Rename/move remote file."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.rename(old_path, new_path)

    def chmod(self, remote_path: str, mode: int) -> None:
        """Change remote file permissions."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        self._sftp.chmod(remote_path, mode)

    def stat(self, remote_path: str) -> "paramiko.SFTPAttributes":
        """Get remote file attributes."""
        if self._sftp is None:
            raise RuntimeError("SFTP not connected")
        return self._sftp.stat(remote_path)

    def is_file(self, remote_path: str) -> bool:
        """Check if path is a regular file."""
        try:
            return stat.S_ISREG(self.stat(remote_path).st_mode)
        except OSError:
            return False

    def is_dir(self, remote_path: str) -> bool:
        """Check if path is a directory."""
        try:
            return stat.S_ISDIR(self.stat(remote_path).st_mode)
        except OSError:
            return False

    def get_file_size(self, remote_path: str) -> int:
        """Get remote file size in bytes."""
        return self.stat(remote_path).st_size

    def upload_directory(
        self,
        local_path: str | Path,
        remote_base: str,
    ) -> None:
        """
        Recursively upload directory.

        Args:
            local_path: Local directory path
            remote_base: Remote base directory
        """
        local_path = Path(local_path)
        for root, dirs, files in os.walk(local_path):
            rel = Path(root).relative_to(local_path)
            remote_dir = f"{remote_base}/{rel}" if str(rel) != "." else remote_base
            try:
                self.mkdir(remote_dir)
            except OSError:
                pass
            for fname in files:
                self.upload_file(Path(root) / fname, f"{remote_dir}/{fname}")

    def download_directory(
        self,
        remote_path: str,
        local_base: str | Path,
    ) -> None:
        """
        Recursively download directory.

        Args:
            remote_path: Remote directory path
            local_base: Local base directory
        """
        local_base = Path(local_base)
        local_base.mkdir(parents=True, exist_ok=True)
        for entry in self.list_dir_attr(remote_path):
            remote_full = f"{remote_path}/{entry.filename}"
            local_full = local_base / entry.filename
            if stat.S_ISDIR(entry.st_mode):
                self.download_directory(remote_full, local_full)
            else:
                self.download_file(remote_full, local_full)
