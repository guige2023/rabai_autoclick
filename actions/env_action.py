"""Environment variable and system info action module for RabAI AutoClick.

Provides environment operations:
- EnvGetAction: Get environment variable
- EnvSetAction: Set environment variable
- EnvListAction: List all environment variables
- EnvDeleteAction: Delete environment variable
- EnvExistsAction: Check if env var exists
- EnvExpandAction: Expand environment variables in path
- EnvHomeAction: Get home directory
- EnvTempAction: Get temp directory
- EnvCwdAction: Get current working directory
- EnvPlatformAction: Get platform info
- EnvArchAction: Get architecture
- EnvPythonVersionAction: Get Python version
- EnvHostnameAction: Get hostname
- EnvUserAction: Get current user
- EnvShellAction: Get shell
- EnvLanguageAction: Get language
- EnvLocaleAction: Get locale
- EnvTimezoneAction: Get timezone
- EnvCpuCountAction: Get CPU count
- EnvMemoryAction: Get memory info
- EnvDiskAction: Get disk info
- EnvLoadAvgAction: Get load average
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


ENV_AVAILABLE = True


class EnvGetAction(BaseAction):
    """Get environment variable."""
    action_type = "env_get"
    display_name = "获取环境变量"
    description = "获取环境变量值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get environment variable."""
        name = params.get('name', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'env_value')

        if not name:
            return ActionResult(success=False, message="变量名不能为空")

        value = os.environ.get(name, default)

        if value is None:
            return ActionResult(
                success=True,
                message=f"环境变量不存在: {name}",
                data={'name': name, 'value': None, 'exists': False}
            )

        context.set(output_var, value)

        return ActionResult(
            success=True,
            message=f"获取成功: {name}={value[:50] if len(str(value)) > 50 else value}",
            data={'name': name, 'value': value, 'exists': True}
        )


class EnvSetAction(BaseAction):
    """Set environment variable."""
    action_type = "env_set"
    display_name = "设置环境变量"
    description = "设置环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute set environment variable."""
        name = params.get('name', '')
        value = params.get('value', '')
        scope = params.get('scope', 'process')

        if not name:
            return ActionResult(success=False, message="变量名不能为空")

        if scope == 'process':
            os.environ[name] = value
        elif scope == 'permanent':
            return ActionResult(
                success=False,
                message="永久环境变量需要使用shell配置（如.bashrc/.zshrc）"
            )

        context.set('_last_env_set', name)

        return ActionResult(
            success=True,
            message=f"设置成功: {name}={value[:50]}",
            data={'name': name, 'value': value, 'scope': scope}
        )


class EnvListAction(BaseAction):
    """List all environment variables."""
    action_type = "env_list"
    display_name = "列出环境变量"
    description = "列出所有环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute list environment variables."""
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'env_list_result')

        if prefix:
            env_vars = {k: v for k, v in os.environ.items() if k.startswith(prefix)}
        else:
            env_vars = dict(os.environ)

        context.set(output_var, env_vars)

        return ActionResult(
            success=True,
            message=f"环境变量数量: {len(env_vars)}",
            data={'count': len(env_vars), 'variables': list(env_vars.keys())[:50]}
        )


class EnvDeleteAction(BaseAction):
    """Delete environment variable."""
    action_type = "env_delete"
    display_name = "删除环境变量"
    description = "删除环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute delete environment variable."""
        name = params.get('name', '')

        if not name:
            return ActionResult(success=False, message="变量名不能为空")

        if name in os.environ:
            del os.environ[name]
            message = f"删除成功: {name}"
        else:
            message = f"变量不存在: {name}"

        context.set('_last_env_deleted', name)

        return ActionResult(
            success=True,
            message=message,
            data={'name': name, 'deleted': name in os.environ}
        )


class EnvExistsAction(BaseAction):
    """Check if env var exists."""
    action_type = "env_exists"
    display_name = "检查环境变量"
    description = "检查环境变量是否存在"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute check env var exists."""
        name = params.get('name', '')
        output_var = params.get('output_var', 'env_exists_result')

        if not name:
            return ActionResult(success=False, message="变量名不能为空")

        exists = name in os.environ
        context.set(output_var, exists)

        return ActionResult(
            success=True,
            message=f"存在: {exists}",
            data={'name': name, 'exists': exists}
        )


class EnvExpandAction(BaseAction):
    """Expand environment variables in path."""
    action_type = "env_expand"
    display_name = "展开路径"
    description = "展开路径中的环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute expand path."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'expanded_path')

        if not path:
            return ActionResult(success=False, message="路径不能为空")

        expanded = os.path.expanduser(os.path.expandvars(path))
        context.set(output_var, expanded)

        return ActionResult(
            success=True,
            message=f"展开成功: {expanded}",
            data={'original': path, 'expanded': expanded}
        )


class EnvHomeAction(BaseAction):
    """Get home directory."""
    action_type = "env_home"
    display_name = "获取用户目录"
    description = "获取用户主目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get home directory."""
        output_var = params.get('output_var', 'home_dir')

        home = os.path.expanduser('~')
        context.set(output_var, home)

        return ActionResult(
            success=True,
            message=f"用户目录: {home}",
            data={'home': home}
        )


class EnvTempAction(BaseAction):
    """Get temp directory."""
    action_type = "env_temp"
    display_name = "获取临时目录"
    description = "获取系统临时目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get temp directory."""
        output_var = params.get('output_var', 'temp_dir')

        import tempfile
        temp = tempfile.gettempdir()
        context.set(output_var, temp)

        return ActionResult(
            success=True,
            message=f"临时目录: {temp}",
            data={'temp': temp}
        )


class EnvCwdAction(BaseAction):
    """Get current working directory."""
    action_type = "env_cwd"
    display_name = "获取当前目录"
    description = "获取当前工作目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get current working directory."""
        output_var = params.get('output_var', 'cwd')

        cwd = os.getcwd()
        context.set(output_var, cwd)

        return ActionResult(
            success=True,
            message=f"当前目录: {cwd}",
            data={'cwd': cwd}
        )


class EnvPlatformAction(BaseAction):
    """Get platform info."""
    action_type = "env_platform"
    display_name = "获取平台信息"
    description = "获取操作系统平台信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get platform info."""
        output_var = params.get('output_var', 'platform_info')

        import platform
        info = {
            'system': platform.system(),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'node': platform.node(),
            'processor': platform.processor(),
        }

        context.set(output_var, info)

        return ActionResult(
            success=True,
            message=f"平台: {info['system']} {info['release']}",
            data=info
        )


class EnvArchAction(BaseAction):
    """Get architecture."""
    action_type = "env_arch"
    display_name = "获取架构"
    description = "获取系统架构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get architecture."""
        output_var = params.get('output_var', 'arch')

        arch = platform.machine()
        context.set(output_var, arch)

        return ActionResult(
            success=True,
            message=f"架构: {arch}",
            data={'arch': arch}
        )


class EnvPythonVersionAction(BaseAction):
    """Get Python version."""
    action_type = "env_python_version"
    display_name = "获取Python版本"
    description = "获取Python版本信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get Python version."""
        output_var = params.get('output_var', 'python_version')

        version = sys.version
        version_info = sys.version_info

        info = {
            'version': version,
            'major': version_info.major,
            'minor': version_info.minor,
            'micro': version_info.micro,
            'platform': sys.platform,
        }

        context.set(output_var, info)

        return ActionResult(
            success=True,
            message=f"Python: {version_info.major}.{version_info.minor}.{version_info.micro}",
            data=info
        )


class EnvHostnameAction(BaseAction):
    """Get hostname."""
    action_type = "env_hostname"
    display_name = "获取主机名"
    description = "获取主机名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get hostname."""
        output_var = params.get('output_var', 'hostname')

        hostname = platform.node()
        context.set(output_var, hostname)

        return ActionResult(
            success=True,
            message=f"主机名: {hostname}",
            data={'hostname': hostname}
        )


class EnvUserAction(BaseAction):
    """Get current user."""
    action_type = "env_user"
    display_name = "获取当前用户"
    description = "获取当前用户名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get current user."""
        output_var = params.get('output_var', 'current_user')

        user = os.environ.get('USER', os.environ.get('USERNAME', 'unknown'))
        context.set(output_var, user)

        return ActionResult(
            success=True,
            message=f"当前用户: {user}",
            data={'user': user}
        )


class EnvShellAction(BaseAction):
    """Get shell."""
    action_type = "env_shell"
    display_name = "获取Shell"
    description = "获取当前shell"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get shell."""
        output_var = params.get('output_var', 'shell')

        shell = os.environ.get('SHELL', '/bin/sh')
        context.set(output_var, shell)

        return ActionResult(
            success=True,
            message=f"Shell: {shell}",
            data={'shell': shell}
        )


class EnvLanguageAction(BaseAction):
    """Get language."""
    action_type = "env_language"
    display_name = "获取语言"
    description = "获取系统语言"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get language."""
        output_var = params.get('output_var', 'language')

        lang = os.environ.get('LANG', os.environ.get('LC_ALL', 'unknown'))
        context.set(output_var, lang)

        return ActionResult(
            success=True,
            message=f"语言: {lang}",
            data={'language': lang}
        )


class EnvLocaleAction(BaseAction):
    """Get locale."""
    action_type = "env_locale"
    display_name = "获取区域设置"
    description = "获取区域设置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get locale."""
        output_var = params.get('output_var', 'locale')

        locale = os.environ.get('LC_ALL', os.environ.get('LANG', 'unknown'))
        context.set(output_var, locale)

        return ActionResult(
            success=True,
            message=f"区域: {locale}",
            data={'locale': locale}
        )


class EnvTimezoneAction(BaseAction):
    """Get timezone."""
    action_type = "env_timezone"
    display_name = "获取时区"
    description = "获取系统时区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get timezone."""
        output_var = params.get('output_var', 'timezone')

        try:
            import time
            timezone = time.tzname
            offset = time.timezone
            offset_hours = offset / 3600 * -1 if offset < 0 else offset / 3600

            tzinfo = {
                'name': timezone,
                'offset_seconds': offset,
                'offset_hours': offset_hours,
            }

            context.set(output_var, tzinfo)

            return ActionResult(
                success=True,
                message=f"时区: {timezone[0]} (UTC{'+' if offset_hours >= 0 else ''}{offset_hours:.1f})",
                data=tzinfo
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时区失败: {str(e)}"
            )


class EnvCpuCountAction(BaseAction):
    """Get CPU count."""
    action_type = "env_cpu_count"
    display_name = "获取CPU数量"
    description = "获取CPU核心数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get CPU count."""
        output_var = params.get('output_var', 'cpu_count')

        import multiprocessing
        cpu_count = multiprocessing.cpu_count()

        context.set(output_var, cpu_count)

        return ActionResult(
            success=True,
            message=f"CPU核心数: {cpu_count}",
            data={'cpu_count': cpu_count}
        )


class EnvMemoryAction(BaseAction):
    """Get memory info."""
    action_type = "env_memory"
    display_name = "获取内存信息"
    description = "获取系统内存信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get memory info."""
        output_var = params.get('output_var', 'memory_info')

        try:
            import psutil
            mem = psutil.virtual_memory()

            info = {
                'total': mem.total,
                'available': mem.available,
                'used': mem.used,
                'percent': mem.percent,
                'total_gb': round(mem.total / (1024**3), 2),
                'available_gb': round(mem.available / (1024**3), 2),
                'used_gb': round(mem.used / (1024**3), 2),
            }

            context.set(output_var, info)

            return ActionResult(
                success=True,
                message=f"内存: {info['used_gb']}GB / {info['total_gb']}GB ({mem.percent}%)",
                data=info
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="psutil库不可用，请安装: pip install psutil"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取内存信息失败: {str(e)}"
            )


class EnvDiskAction(BaseAction):
    """Get disk info."""
    action_type = "env_disk"
    display_name = "获取磁盘信息"
    description = "获取磁盘使用信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get disk info."""
        path = params.get('path', '/')
        output_var = params.get('output_var', 'disk_info')

        try:
            import psutil
            disk = psutil.disk_usage(path)

            info = {
                'total': disk.total,
                'used': disk.used,
                'free': disk.free,
                'percent': disk.percent,
                'total_gb': round(disk.total / (1024**3), 2),
                'used_gb': round(disk.used / (1024**3), 2),
                'free_gb': round(disk.free / (1024**3), 2),
            }

            context.set(output_var, info)

            return ActionResult(
                success=True,
                message=f"磁盘: {info['used_gb']}GB / {info['total_gb']}GB ({disk.percent}%)",
                data=info
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="psutil库不可用，请安装: pip install psutil"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取磁盘信息失败: {str(e)}"
            )


class EnvLoadAvgAction(BaseAction):
    """Get load average."""
    action_type = "env_loadavg"
    display_name = "获取负载"
    description = "获取系统负载平均值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get load average."""
        output_var = params.get('output_var', 'load_avg')

        try:
            import os
            load1, load5, load15 = os.getloadavg()

            info = {
                '1min': load1,
                '5min': load5,
                '15min': load15,
            }

            context.set(output_var, info)

            return ActionResult(
                success=True,
                message=f"负载: {load1:.2f} (1m) {load5:.2f} (5m) {load15:.2f} (15m)",
                data=info
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取负载失败: {str(e)}"
            )
