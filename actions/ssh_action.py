"""SSH action module for RabAI AutoClick.

Provides SSH operations:
- SshConnectAction: Connect via SSH and execute command
- SshUploadAction: Upload file via SCP
- SshDownloadAction: Download file via SCP
- SshTunnelAction: Create SSH tunnel
- SshKeygenAction: Generate SSH key pair
"""

import subprocess
import os
import tempfile
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SshConnectAction(BaseAction):
    """Connect via SSH and execute command."""
    action_type = "ssh_connect"
    display_name = "SSH执行命令"
    description = "通过SSH连接执行远程命令"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SSH.

        Args:
            context: Execution context.
            params: Dict with host, user, port, command, key_path, password, output_var.

        Returns:
            ActionResult with command output.
        """
        host = params.get('host', '')
        user = params.get('user', 'root')
        port = params.get('port', 22)
        command = params.get('command', '')
        key_path = params.get('key_path', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'ssh_output')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_user = context.resolve_value(user)
            resolved_port = context.resolve_value(port)
            resolved_cmd = context.resolve_value(command)
            resolved_timeout = context.resolve_value(timeout)

            cmd = ['ssh']

            if resolved_port != 22:
                cmd.extend(['-p', str(resolved_port)])

            if key_path:
                resolved_key = context.resolve_value(key_path)
                if os.path.exists(resolved_key):
                    cmd.extend(['-i', resolved_key])

            cmd.extend([f'{resolved_user}@{resolved_host}', resolved_cmd])

            env = os.environ.copy()
            if password:
                resolved_pwd = context.resolve_value(password)
                # Use SSH_ASKPASS for password
                import uuid
                script = f'#!/bin/bash\necho "{resolved_pwd}"\n'
                askpass = f'/tmp/askpass_{uuid.uuid4().hex[:8]}'
                with open(askpass, 'w') as f:
                    f.write(script)
                os.chmod(askpass, 0o700)
                env['SSH_ASKPASS'] = askpass
                env['DISPLAY'] = ':0'
                env['SSH_ASKPASS_REQUIRE'] = 'force'

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(resolved_timeout),
                env=env
            )

            output = result.stdout
            error = result.stderr

            context.set(output_var, output)

            return ActionResult(
                success=result.returncode == 0,
                message=f"SSH命令 {'成功' if result.returncode == 0 else '失败'} (退出码 {result.returncode})",
                data={
                    'returncode': result.returncode,
                    'stdout': output,
                    'stderr': error,
                    'output_var': output_var
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"SSH命令超时 ({resolved_timeout}s)"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ssh命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SSH执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'user': 'root', 'port': 22, 'key_path': '', 'password': '',
            'output_var': 'ssh_output', 'timeout': 30
        }


class SshUploadAction(BaseAction):
    """Upload file via SCP."""
    action_type = "ssh_upload"
    display_name = "SCP上传文件"
    description = "通过SCP上传文件到远程服务器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upload.

        Args:
            context: Execution context.
            params: Dict with local_path, remote_path, host, user, port, key_path, password.

        Returns:
            ActionResult indicating success.
        """
        local_path = params.get('local_path', '')
        remote_path = params.get('remote_path', '')
        host = params.get('host', '')
        user = params.get('user', 'root')
        port = params.get('port', 22)
        key_path = params.get('key_path', '')
        password = params.get('password', '')

        valid, msg = self.validate_type(local_path, str, 'local_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(remote_path, str, 'remote_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_local = context.resolve_value(local_path)
            resolved_remote = context.resolve_value(remote_path)
            resolved_host = context.resolve_value(host)
            resolved_user = context.resolve_value(user)
            resolved_port = context.resolve_value(port)

            if not os.path.exists(resolved_local):
                return ActionResult(
                    success=False,
                    message=f"本地文件不存在: {resolved_local}"
                )

            cmd = ['scp', '-o', 'StrictHostKeyChecking=no']

            if resolved_port != 22:
                cmd.extend(['-P', str(resolved_port)])

            if key_path:
                resolved_key = context.resolve_value(key_path)
                if os.path.exists(resolved_key):
                    cmd.extend(['-i', resolved_key])

            cmd.extend([resolved_local, f'{resolved_user}@{resolved_host}:{resolved_remote}'])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"SCP上传失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"已上传: {resolved_local} -> {resolved_user}@{resolved_host}:{resolved_remote}",
                data={'local': resolved_local, 'remote': resolved_remote}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="SCP上传超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="scp命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SCP上传失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['local_path', 'remote_path', 'host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'user': 'root', 'port': 22, 'key_path': '', 'password': ''}


class SshDownloadAction(BaseAction):
    """Download file via SCP."""
    action_type = "ssh_download"
    display_name = "SCP下载文件"
    description = "通过SCP从远程服务器下载文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute download.

        Args:
            context: Execution context.
            params: Dict with remote_path, local_path, host, user, port, key_path.

        Returns:
            ActionResult indicating success.
        """
        remote_path = params.get('remote_path', '')
        local_path = params.get('local_path', '')
        host = params.get('host', '')
        user = params.get('user', 'root')
        port = params.get('port', 22)
        key_path = params.get('key_path', '')

        valid, msg = self.validate_type(remote_path, str, 'remote_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(local_path, str, 'local_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_remote = context.resolve_value(remote_path)
            resolved_local = context.resolve_value(local_path)
            resolved_host = context.resolve_value(host)
            resolved_user = context.resolve_value(user)
            resolved_port = context.resolve_value(port)

            cmd = ['scp', '-o', 'StrictHostKeyChecking=no']

            if resolved_port != 22:
                cmd.extend(['-P', str(resolved_port)])

            if key_path:
                resolved_key = context.resolve_value(key_path)
                if os.path.exists(resolved_key):
                    cmd.extend(['-i', resolved_key])

            cmd.extend([f'{resolved_user}@{resolved_host}:{resolved_remote}', resolved_local])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"SCP下载失败: {result.stderr}"
                )

            size = os.path.getsize(resolved_local)

            return ActionResult(
                success=True,
                message=f"已下载: {resolved_user}@{resolved_host}:{resolved_remote} -> {resolved_local} ({size} bytes)",
                data={'local': resolved_local, 'remote': resolved_remote, 'size': size}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="SCP下载超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="scp命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SCP下载失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['remote_path', 'local_path', 'host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'user': 'root', 'port': 22, 'key_path': ''}


class SshKeygenAction(BaseAction):
    """Generate SSH key pair."""
    action_type = "ssh_keygen"
    display_name = "生成SSH密钥"
    description = "生成SSH密钥对"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keygen.

        Args:
            context: Execution context.
            params: Dict with output_path, comment, key_type, bits, passphrase.

        Returns:
            ActionResult with key paths.
        """
        output_path = params.get('output_path', '~/.ssh/id_rsa')
        comment = params.get('comment', '')
        key_type = params.get('key_type', 'rsa')
        bits = params.get('bits', 4096)
        passphrase = params.get('passphrase', '')

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_output = os.path.expanduser(context.resolve_value(output_path))
            resolved_comment = context.resolve_value(comment) if comment else ''
            resolved_type = context.resolve_value(key_type)
            resolved_bits = context.resolve_value(bits)
            resolved_pass = context.resolve_value(passphrase) if passphrase else ''

            if os.path.exists(resolved_output):
                return ActionResult(
                    success=False,
                    message=f"密钥已存在: {resolved_output}"
                )

            cmd = ['ssh-keygen', '-t', resolved_type, '-b', str(resolved_bits), '-f', resolved_output, '-N', resolved_pass]

            if resolved_comment:
                cmd.extend(['-C', resolved_comment])
            else:
                cmd.extend(['-C', f'autogenerated-{int(time.time())}'])

            import time
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                input='\n'  # Confirm overwrite if prompted
            )

            if result.returncode != 0 and 'already exists' not in result.stderr:
                return ActionResult(
                    success=False,
                    message=f"SSH密钥生成失败: {result.stderr}"
                )

            # Read public key
            pub_key_path = f'{resolved_output}.pub'
            with open(pub_key_path, 'r') as f:
                public_key = f.read().strip()

            return ActionResult(
                success=True,
                message=f"SSH密钥已生成: {resolved_output}",
                data={
                    'private_key': resolved_output,
                    'public_key': pub_key_path,
                    'public_key_content': public_key
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="SSH密钥生成超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ssh-keygen命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SSH密钥生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_path': '~/.ssh/id_rsa', 'comment': '', 'key_type': 'rsa',
            'bits': 4096, 'passphrase': ''
        }


class SshTunnelAction(BaseAction):
    """Create SSH tunnel."""
    action_type = "ssh_tunnel"
    display_name = "创建SSH隧道"
    description = "创建SSH端口转发隧道"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tunnel.

        Args:
            context: Execution context.
            params: Dict with host, user, port, local_port, remote_host, remote_port, key_path, background.

        Returns:
            ActionResult indicating tunnel started.
        """
        host = params.get('host', '')
        user = params.get('user', 'root')
        port = params.get('port', 22)
        local_port = params.get('local_port', 8080)
        remote_host = params.get('remote_host', 'localhost')
        remote_port = params.get('remote_port', 80)
        key_path = params.get('key_path', '')
        background = params.get('background', True)

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_user = context.resolve_value(user)
            resolved_port = context.resolve_value(port)
            resolved_local = context.resolve_value(local_port)
            resolved_remote_host = context.resolve_value(remote_host)
            resolved_remote_port = context.resolve_value(remote_port)
            resolved_bg = context.resolve_value(background)

            cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-L', f'{resolved_local}:{resolved_remote_host}:{resolved_remote_port}']

            if resolved_port != 22:
                cmd.extend(['-p', str(resolved_port)])

            if key_path:
                resolved_key = context.resolve_value(key_path)
                if os.path.exists(resolved_key):
                    cmd.extend(['-i', resolved_key])

            if resolved_bg:
                cmd.append('-f')
                cmd.append('-N')

            cmd.append(f'{resolved_user}@{resolved_host}')

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode != 0 and 'already allocated' not in result.stderr:
                return ActionResult(
                    success=False,
                    message=f"SSH隧道创建失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"SSH隧道已创建: localhost:{resolved_local} -> {resolved_host}:{resolved_remote_port}",
                data={
                    'local_port': resolved_local,
                    'remote_host': resolved_remote_host,
                    'remote_port': resolved_remote_port,
                    'host': resolved_host
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="SSH隧道创建超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ssh命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SSH隧道创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['host', 'local_port', 'remote_port']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'user': 'root', 'port': 22, 'remote_host': 'localhost',
            'key_path': '', 'background': True
        }
