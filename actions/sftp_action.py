"""SFTP action module.

Provides SFTP client functionality for secure file transfers
with support for directory operations, file manipulation, and streaming.
"""

from __future__ import annotations

import os
import io
import time
import logging
from typing import Any, Optional, Callable, BinaryIO
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import paramiko
import threading

logger = logging.getLogger(__name__)


class FileTransferMode(Enum):
    """SFTP file transfer modes."""
    BINARY = "b"
    TEXT = "t"


@dataclass
class SftpConfig:
    """SFTP connection configuration."""
    host: str
    port: int = 22
    username: str
    password: Optional[str] = None
    key_filename: Optional[str] = None
    pkey: Optional[paramiko.PKey] = None
    timeout: float = 30.0
    banner_timeout: float = 15.0
    auth_timeout: float = 15.0


@dataclass
class SftpFileInfo:
    """SFTP file information."""
    filename: str
    path: str
    size: int
    mode: int
    uid: int
    gid: int
    atime: int
    mtime: int
    is_dir: bool
    is_file: bool
    is_symlink: bool


class SftpClient:
    """SFTP client for file operations."""

    def __init__(self, config: SftpConfig):
        """Initialize SFTP client.

        Args:
            config: SFTP connection configuration
        """
        self.config = config
        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._connected = False
        self._lock = threading.Lock()

    def connect(self) -> bool:
        """Establish SFTP connection.

        Returns:
            True if connection successful
        """
        try:
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: dict[str, Any] = {
                "hostname": self.config.host,
                "port": self.config.port,
                "username": self.config.username,
                "timeout": self.config.timeout,
                "banner_timeout": self.config.banner_timeout,
                "auth_timeout": self.config.auth_timeout,
            }

            if self.config.password:
                connect_kwargs["password"] = self.config.password
            if self.config.key_filename:
                connect_kwargs["key_filename"] = self.config.key_filename
            if self.config.pkey:
                connect_kwargs["pkey"] = self.config.pkey

            self._ssh_client.connect(**connect_kwargs)
            self._sftp = self._ssh_client.open_sftp()
            self._connected = True
            logger.info(f"Connected to SFTP server: {self.config.host}:{self.config.port}")
            return True

        except Exception as e:
            logger.error(f"SFTP connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        """Close SFTP connection."""
        with self._lock:
            if self._sftp:
                try:
                    self._sftp.close()
                except Exception:
                    pass
                self._sftp = None

            if self._ssh_client:
                try:
                    self._ssh_client.close()
                except Exception:
                    pass
                self._ssh_client = None

            self._connected = False
            logger.info("Disconnected from SFTP server")

    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    def list_directory(self, path: str = ".") -> list[SftpFileInfo]:
        """List directory contents.

        Args:
            path: Directory path

        Returns:
            List of file info objects
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        files: list[SftpFileInfo] = []
        try:
            for item in self._sftp.listdir_attr(path):
                files.append(SftpFileInfo(
                    filename=item.filename,
                    path=os.path.join(path, item.filename),
                    size=item.st_size,
                    mode=item.st_mode,
                    uid=item.st_uid,
                    gid=item.st_gid,
                    atime=int(item.st_atime),
                    mtime=int(item.st_mtime),
                    is_dir=paramiko.SFTPAttributes().is_dir(),
                    is_file=paramiko.SFTPAttributes().is_file(),
                    is_symlink=paramiko.SFTPAttributes().is_link(),
                ))
        except Exception as e:
            logger.error(f"Failed to list directory {path}: {e}")
            raise

        return files

    def get_file(self, remote_path: str, local_path: Optional[str] = None) -> bytes:
        """Download file from remote server.

        Args:
            remote_path: Remote file path
            local_path: Local destination path (optional)

        Returns:
            File content as bytes
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            if local_path:
                self._sftp.get(remote_path, local_path)
                logger.info(f"Downloaded {remote_path} to {local_path}")
                return b""

            with io.BytesIO() as buffer:
                self._sftp.getfo(remote_path, buffer)
                return buffer.getvalue()

        except Exception as e:
            logger.error(f"Failed to download {remote_path}: {e}")
            raise

    def put_file(
        self,
        local_path: Optional[str],
        remote_path: str,
        content: Optional[bytes] = None,
    ) -> None:
        """Upload file to remote server.

        Args:
            local_path: Local source path
            remote_path: Remote destination path
            content: File content as bytes (alternative to local_path)
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            if content is not None:
                with io.BytesIO(content) as buffer:
                    self._sftp.putfo(buffer, remote_path)
            elif local_path:
                self._sftp.put(local_path, remote_path)
            else:
                raise ValueError("Either local_path or content must be provided")

            logger.info(f"Uploaded file to {remote_path}")

        except Exception as e:
            logger.error(f"Failed to upload {remote_path}: {e}")
            raise

    def create_directory(self, path: str, mode: int = 0o755) -> None:
        """Create remote directory.

        Args:
            path: Directory path to create
            mode: Directory permissions
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            self._sftp.mkdir(path, mode)
            logger.info(f"Created directory: {path}")
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {e}")
            raise

    def remove_directory(self, path: str) -> None:
        """Remove remote directory.

        Args:
            path: Directory path to remove
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            self._sftp.rmdir(path)
            logger.info(f"Removed directory: {path}")
        except Exception as e:
            logger.error(f"Failed to remove directory {path}: {e}")
            raise

    def remove_file(self, path: str) -> None:
        """Remove remote file.

        Args:
            path: File path to remove
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            self._sftp.remove(path)
            logger.info(f"Removed file: {path}")
        except Exception as e:
            logger.error(f"Failed to remove file {path}: {e}")
            raise

    def rename(self, old_path: str, new_path: str) -> None:
        """Rename/move remote file or directory.

        Args:
            old_path: Current path
            new_path: New path
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            self._sftp.rename(old_path, new_path)
            logger.info(f"Renamed {old_path} to {new_path}")
        except Exception as e:
            logger.error(f"Failed to rename {old_path}: {e}")
            raise

    def get_file_info(self, path: str) -> SftpFileInfo:
        """Get file information.

        Args:
            path: File path

        Returns:
            SftpFileInfo object
        """
        if not self._sftp:
            raise ConnectionError("Not connected to SFTP server")

        try:
            stat = self._sftp.stat(path)
            return SftpFileInfo(
                filename=os.path.basename(path),
                path=path,
                size=stat.st_size,
                mode=stat.st_mode,
                uid=stat.st_uid,
                gid=stat.st_gid,
                atime=int(stat.st_atime),
                mtime=int(stat.st_mtime),
                is_dir=False,
                is_file=True,
                is_symlink=False,
            )
        except Exception as e:
            logger.error(f"Failed to get file info {path}: {e}")
            raise


def create_sftp_client(
    host: str,
    username: str,
    password: Optional[str] = None,
    port: int = 22,
    key_filename: Optional[str] = None,
) -> SftpClient:
    """Create SFTP client instance.

    Args:
        host: SFTP server hostname
        username: Username
        password: Password (optional)
        port: Port number
        key_filename: Path to private key file

    Returns:
        SftpClient instance
    """
    config = SftpConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        key_filename=key_filename,
    )
    return SftpClient(config)
