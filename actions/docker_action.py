"""Docker action module for RabAI AutoClick.

Provides Docker container operations:
- DockerPsAction: List running containers
- DockerImagesAction: List images
- DockerRunAction: Run a container
- DockerStopAction: Stop container
- DockerStartAction: Start container
- DockerRestartAction: Restart container
- DockerRmAction: Remove container
- DockerExecAction: Execute command in container
- DockerLogsAction: Get container logs
- DockerStatsAction: Get container stats
- DockerBuildAction: Build image from Dockerfile
- DockerPushAction: Push image to registry
- DockerPullAction: Pull image from registry
"""

import subprocess
import json
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def docker_run(args: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run docker command."""
    cmd = ['docker'] + args
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


class DockerPsAction(BaseAction):
    """List running containers."""
    action_type = "docker_ps"
    display_name = "Docker容器列表"
    description = "列出所有运行的容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ps.

        Args:
            context: Execution context.
            params: Dict with all, output_var.

        Returns:
            ActionResult with container list.
        """
        all_containers = params.get('all', False)
        output_var = params.get('output_var', 'docker_containers')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_all = context.resolve_value(all_containers)

            args = ['ps', '--format', '{{json .}}']
            if resolved_all:
                args.append('-a')

            result = docker_run(args)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker ps失败: {result.stderr}"
                )

            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    try:
                        containers.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

            context.set(output_var, containers)

            return ActionResult(
                success=True,
                message=f"Docker容器: {len(containers)} 个",
                data={'count': len(containers), 'containers': containers, 'output_var': output_var}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Docker命令超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="docker命令未找到"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker ps失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'all': False, 'output_var': 'docker_containers'}


class DockerImagesAction(BaseAction):
    """List images."""
    action_type = "docker_images"
    display_name = "Docker镜像列表"
    description = "列出所有Docker镜像"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute images.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with image list.
        """
        output_var = params.get('output_var', 'docker_images')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result = docker_run(['images', '--format', '{{json .}}'])
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker images失败: {result.stderr}"
                )

            images = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    try:
                        images.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

            context.set(output_var, images)

            return ActionResult(
                success=True,
                message=f"Docker镜像: {len(images)} 个",
                data={'count': len(images), 'images': images, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker images失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'docker_images'}


class DockerRunAction(BaseAction):
    """Run a container."""
    action_type = "docker_run"
    display_name = "Docker运行容器"
    description = "运行Docker容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute run.

        Args:
            context: Execution context.
            params: Dict with image, name, ports, env, volumes, detach, command, output_var.

        Returns:
            ActionResult with container ID.
        """
        image = params.get('image', '')
        name = params.get('name', '')
        ports = params.get('ports', [])
        env = params.get('env', {})
        volumes = params.get('volumes', [])
        detach = params.get('detach', True)
        command = params.get('command', '')
        output_var = params.get('output_var', 'container_id')

        valid, msg = self.validate_type(image, str, 'image')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_image = context.resolve_value(image)
            resolved_name = context.resolve_value(name) if name else ''
            resolved_detach = context.resolve_value(detach)
            resolved_command = context.resolve_value(command) if command else ''

            args = ['run']

            if resolved_name:
                args.extend(['--name', resolved_name])

            if resolved_detach:
                args.append('-d')

            if ports:
                resolved_ports = context.resolve_value(ports)
                for port in resolved_ports:
                    args.extend(['-p', str(port)])

            if env:
                resolved_env = context.resolve_value(env)
                for k, v in resolved_env.items():
                    args.extend(['-e', f'{k}={v}'])

            if volumes:
                resolved_vols = context.resolve_value(volumes)
                for vol in resolved_vols:
                    args.extend(['-v', str(vol)])

            args.append(resolved_image)

            if resolved_command:
                args.extend(resolved_command.split())

            result = docker_run(args, timeout=300)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker run失败: {result.stderr}"
                )

            container_id = result.stdout.strip()[:12]
            context.set(output_var, container_id)

            return ActionResult(
                success=True,
                message=f"容器已启动: {container_id}",
                data={'container_id': container_id, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker run失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'name': '', 'ports': [], 'env': {}, 'volumes': [],
            'detach': True, 'command': '', 'output_var': 'container_id'
        }


class DockerStopAction(BaseAction):
    """Stop container."""
    action_type = "docker_stop"
    display_name = "Docker停止容器"
    description = "停止Docker容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop.

        Args:
            context: Execution context.
            params: Dict with container, timeout.

        Returns:
            ActionResult indicating success.
        """
        container = params.get('container', '')
        timeout = params.get('timeout', 10)

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)
            resolved_timeout = context.resolve_value(timeout)

            result = docker_run(['stop', '-t', str(resolved_timeout), resolved_container], timeout=int(resolved_timeout) + 10)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker stop失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"容器已停止: {resolved_container}",
                data={'container': resolved_container}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker stop失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 10}


class DockerStartAction(BaseAction):
    """Start container."""
    action_type = "docker_start"
    display_name = "Docker启动容器"
    description = "启动Docker容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start.

        Args:
            context: Execution context.
            params: Dict with container.

        Returns:
            ActionResult indicating success.
        """
        container = params.get('container', '')

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)

            result = docker_run(['start', resolved_container])
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker start失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"容器已启动: {resolved_container}",
                data={'container': resolved_container}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker start失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DockerRestartAction(BaseAction):
    """Restart container."""
    action_type = "docker_restart"
    display_name = "Docker重启容器"
    description = "重启Docker容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute restart.

        Args:
            context: Execution context.
            params: Dict with container, timeout.

        Returns:
            ActionResult indicating success.
        """
        container = params.get('container', '')
        timeout = params.get('timeout', 10)

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)
            resolved_timeout = context.resolve_value(timeout)

            result = docker_run(['restart', '-t', str(resolved_timeout), resolved_container], timeout=int(resolved_timeout) + 20)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker restart失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"容器已重启: {resolved_container}",
                data={'container': resolved_container}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker restart失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 10}


class DockerRmAction(BaseAction):
    """Remove container."""
    action_type = "docker_rm"
    display_name = "Docker删除容器"
    description = "删除Docker容器"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rm.

        Args:
            context: Execution context.
            params: Dict with container, force.

        Returns:
            ActionResult indicating success.
        """
        container = params.get('container', '')
        force = params.get('force', False)

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)
            resolved_force = context.resolve_value(force)

            args = ['rm']
            if resolved_force:
                args.append('-f')

            args.append(resolved_container)

            result = docker_run(args)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker rm失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"容器已删除: {resolved_container}",
                data={'container': resolved_container}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker rm失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'force': False}


class DockerExecAction(BaseAction):
    """Execute command in container."""
    action_type = "docker_exec"
    display_name = "Docker执行命令"
    description = "在容器内执行命令"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exec.

        Args:
            context: Execution context.
            params: Dict with container, command, output_var.

        Returns:
            ActionResult with command output.
        """
        container = params.get('container', '')
        command = params.get('command', '')
        output_var = params.get('output_var', 'docker_exec_output')

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)
            resolved_command = context.resolve_value(command)

            args = ['exec', resolved_container, 'sh', '-c', resolved_command]

            result = docker_run(args, timeout=60)
            output = result.stdout
            error = result.stderr

            context.set(output_var, output)

            return ActionResult(
                success=result.returncode == 0,
                message=f"Docker exec {'成功' if result.returncode == 0 else '失败'}",
                data={
                    'returncode': result.returncode,
                    'stdout': output,
                    'stderr': error,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker exec失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container', 'command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'docker_exec_output'}


class DockerLogsAction(BaseAction):
    """Get container logs."""
    action_type = "docker_logs"
    display_name = "Docker查看日志"
    description = "获取Docker容器日志"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logs.

        Args:
            context: Execution context.
            params: Dict with container, tail, output_var.

        Returns:
            ActionResult with logs.
        """
        container = params.get('container', '')
        tail = params.get('tail', 100)
        output_var = params.get('output_var', 'docker_logs')

        valid, msg = self.validate_type(container, str, 'container')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_container = context.resolve_value(container)
            resolved_tail = context.resolve_value(tail)

            result = docker_run(['logs', '--tail', str(resolved_tail), resolved_container])
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker logs失败: {result.stderr}"
                )

            logs = result.stdout + result.stderr
            context.set(output_var, logs)

            return ActionResult(
                success=True,
                message=f"获取日志: {len(logs)} 字符",
                data={'logs': logs, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker logs失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['container']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'tail': 100, 'output_var': 'docker_logs'}


class DockerBuildAction(BaseAction):
    """Build image from Dockerfile."""
    action_type = "docker_build"
    display_name = "Docker构建镜像"
    description = "从Dockerfile构建镜像"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute build.

        Args:
            context: Execution context.
            params: Dict with path, tag, output_var.

        Returns:
            ActionResult with image tag.
        """
        path = params.get('path', '.')
        tag = params.get('tag', '')
        output_var = params.get('output_var', 'docker_image_tag')

        try:
            resolved_path = context.resolve_value(path)
            resolved_tag = context.resolve_value(tag)

            if not resolved_tag:
                return ActionResult(
                    success=False,
                    message="tag参数不能为空"
                )

            result = docker_run(['build', '-t', resolved_tag, resolved_path], timeout=600)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker build失败: {result.stderr}"
                )

            context.set(output_var, resolved_tag)

            return ActionResult(
                success=True,
                message=f"镜像已构建: {resolved_tag}",
                data={'tag': resolved_tag, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker build失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'tag']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'docker_image_tag'}


class DockerPullAction(BaseAction):
    """Pull image from registry."""
    action_type = "docker_pull"
    display_name = "Docker拉取镜像"
    description = "从镜像仓库拉取镜像"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pull.

        Args:
            context: Execution context.
            params: Dict with image.

        Returns:
            ActionResult indicating success.
        """
        image = params.get('image', '')

        valid, msg = self.validate_type(image, str, 'image')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_image = context.resolve_value(image)

            result = docker_run(['pull', resolved_image], timeout=300)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker pull失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"镜像已拉取: {resolved_image}",
                data={'image': resolved_image}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker pull失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DockerPushAction(BaseAction):
    """Push image to registry."""
    action_type = "docker_push"
    display_name = "Docker推送镜像"
    description = "推送镜像到仓库"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with image.

        Returns:
            ActionResult indicating success.
        """
        image = params.get('image', '')

        valid, msg = self.validate_type(image, str, 'image')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_image = context.resolve_value(image)

            result = docker_run(['push', resolved_image], timeout=300)
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Docker push失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"镜像已推送: {resolved_image}",
                data={'image': resolved_image}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Docker push失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
