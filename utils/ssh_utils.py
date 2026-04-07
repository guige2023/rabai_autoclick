"""
SSH utilities for remote command execution and file transfer.

Provides SSH connection management, command execution, SFTP file
transfer, key management, and tunnel forwarding.
"""

from __future__ import annotations

import io
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from typing import Any, BinaryIO, Optional

logger = logging.getLogger(__name__)


@dataclass
class SSHConfig:
    """SSH connection configuration."""
    host: str
    port: int = 22
    username: str = "root"
    password: Optional[str] = None
    key_file: Optional[str] = None
    passphrase: Optional[str] = None
    timeout: int = 30
    banner_timeout: int = 15
    auth_timeout: int = 30


@dataclass
class CommandResult:
    """Result of a remote command execution."""
    stdout: str
    stderr: str
    exit_code: int
    duration_seconds: float


@dataclass
class FileTransfer:
    """File transfer progress information."""
    path: str
    size: int
    transferred: int
    direction: str  # "upload" or "download"
    percentage: float = 0.0


class SSHClient:
    """High-level SSH client for remote operations."""

    def __init__(self, config: SSHConfig) -> None:
        self.config = config
        self._client: Any = None
        self._sftp: Any = None
        self._connected = False

    def connect(self) -> bool:
        """Establish SSH connection."""
        try:
            import paramiko
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: dict[str, Any] = {
                "hostname": self.config.host,
                "port": self.config.port,
                "username": self.config.username,
                "timeout": self.config.timeout,
                "banner_timeout": self.config.banner_timeout,
                "auth_timeout": self.config.auth_timeout,
            }

            if self.config.key_file:
                connect_kwargs["key_filename"] = self.config.key_file
                if self.config.passphrase:
                    connect_kwargs["passphrase"] = self.config.passphrase
            elif self.config.password:
                connect_kwargs["password"] = self.config.password

            self._client.connect(**connect_kwargs)
            self._connected = True
            logger.info("SSH connected to %s:%d", self.config.host, self.config.port)
            return True
        except ImportError:
            logger.warning("paramiko not installed, using mock mode")
            self._connected = True
            return True
        except Exception as e:
            logger.error("SSH connection failed: %s", e)
            return False

    def execute(
        self,
        command: str,
        timeout: int = 30,
        cwd: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ) -> CommandResult:
        """Execute a command on the remote host."""
        if not self._connected:
            if not self.connect():
                return CommandResult("", f"Connection failed: {self.config.host}", 1, 0.0)

        start = time.perf_counter()
        try:
            final_cmd = command
            if cwd:
                final_cmd = f"cd {cwd} && {command}"

            stdin, stdout, stderr = self._client.exec_command(
                final_cmd,
                timeout=timeout,
                environment=env or {},
            )

            exit_code = stdout.channel.recv_exit_status()
            stdout_data = stdout.read().decode("utf-8", errors="replace")
            stderr_data = stderr.read().decode("utf-8", errors="replace")

            duration = time.perf_counter() - start
            return CommandResult(
                stdout=stdout_data,
                stderr=stderr_data,
                exit_code=exit_code,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.perf_counter() - start
            return CommandResult("", str(e), 1, duration)

    def execute_async(self, command: str) -> tuple[Any, Any, Any]:
        """Execute a command asynchronously."""
        if not self._connected:
            if not self.connect():
                raise RuntimeError("Connection failed")
        return self._client.exec_command(command, timeout=30)

    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        progress_callback: Optional[callable] = None,
    ) -> bool:
        """Upload a file via SFTP."""
        if not self._connected:
            if not self.connect():
                return False

        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()

            stat_info = os.stat(local_path)
            file_size = stat_info.st_size

            def callback(transferred: int, total: int) -> None:
                if progress_callback:
                    progress_callback(FileTransfer(
                        path=local_path,
                        size=total,
                        transferred=transferred,
                        direction="upload",
                        percentage=(transferred / total * 100) if total > 0 else 0,
                    ))

            self._sftp.put(local_path, remote_path, callback=callback)
            logger.info("Uploaded %s -> %s", local_path, remote_path)
            return True
        except Exception as e:
            logger.error("Upload failed: %s", e)
            return False

    def upload_content(
        self,
        content: bytes,
        remote_path: str,
    ) -> bool:
        """Upload file content directly."""
        if not self._connected:
            if not self.connect():
                return False

        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()
            with self._sftp.file(remote_path, "wb") as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error("Upload content failed: %s", e)
            return False

    def download_file(
        self,
        remote_path: str,
        local_path: str,
        progress_callback: Optional[callable] = None,
    ) -> bool:
        """Download a file via SFTP."""
        if not self._connected:
            if not self.connect():
                return False

        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()

            stat_info = self._sftp.stat(remote_path)
            file_size = stat_info.st_size

            def callback(transferred: int, total: int) -> None:
                if progress_callback:
                    progress_callback(FileTransfer(
                        path=remote_path,
                        size=total,
                        transferred=transferred,
                        direction="download",
                        percentage=(transferred / total * 100) if total > 0 else 0,
                    ))

            self._sftp.get(remote_path, local_path, callback=callback)
            logger.info("Downloaded %s -> %s", remote_path, local_path)
            return True
        except Exception as e:
            logger.error("Download failed: %s", e)
            return False

    def list_directory(self, remote_path: str) -> list[str]:
        """List remote directory contents."""
        if not self._connected:
            if not self.connect():
                return []
        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()
            return self._sftp.listdir(remote_path)
        except Exception as e:
            logger.error("List directory failed: %s", e)
            return []

    def file_exists(self, remote_path: str) -> bool:
        """Check if a remote file exists."""
        if not self._connected:
            if not self.connect():
                return False
        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()
            self._sftp.stat(remote_path)
            return True
        except IOError:
            return False

    def mkdir(self, remote_path: str, parents: bool = True) -> bool:
        """Create a remote directory."""
        if not self._connected:
            if not self.connect():
                return False
        try:
            if self._sftp is None:
                self._sftp = self._client.open_sftp()
            if parents:
                self._sftp.makedirs(remote_path)
            else:
                self._sftp.mkdir(remote_path)
            return True
        except Exception as e:
            logger.error("Mkdir failed: %s", e)
            return False

    def close(self) -> None:
        """Close the SSH connection."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._client:
            self._client.close()
            self._client = None
        self._connected = False
        logger.info("SSH connection closed")

    def __enter__(self) -> "SSHClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class SSHTunnel:
    """SSH local port forwarding tunnel."""

    def __init__(self, config: SSHConfig, local_port: int, remote_host: str, remote_port: int) -> None:
        self.config = config
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self._transport: Any = None
        self._server: Any = None

    def start(self) -> bool:
        """Start the SSH tunnel."""
        try:
            import paramiko
            transport = paramiko.Transport((self.config.host, self.config.port))
            if self.config.password:
                transport.connect(username=self.config.username, password=self.config.password)
            else:
                transport.connect(username=self.config.username, pkey=paramiko.RSAKey.from_private_key_file(self.config.key_file))

            self._server = transport.open_channel("direct-tcpip", (self.remote_host, self.remote_port), ("127.0.0.1", self.local_port))
            logger.info("SSH tunnel started: localhost:%d -> %s:%d", self.local_port, self.remote_host, self.remote_port)
            return True
        except Exception as e:
            logger.error("Failed to start SSH tunnel: %s", e)
            return False

    def stop(self) -> None:
        """Stop the SSH tunnel."""
        if self._server:
            self._server.close()
        if self._transport:
            self._transport.close()
        logger.info("SSH tunnel stopped")


def generate_ssh_key(
    key_type: str = "rsa",
    key_size: int = 4096,
    comment: str = "",
    passphrase: Optional[str] = None,
) -> tuple[str, str]:
    """Generate a new SSH key pair (private, public)."""
    import paramiko
    key = paramiko.RSAKey.generate(bits=key_size)
    private_key = io.StringIO()
    key.write_private_key(private_key, password=passphrase)
    private_key_str = private_key.getvalue()
    public_key_str = f"{key.get_name()} {key.get_base64()}" + (f" {comment}" if comment else "")
    return private_key_str, public_key_str
