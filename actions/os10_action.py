"""OS10 action module for RabAI AutoClick.

Provides additional OS operations:
- OSDirAction: List directory
- OSMkdirAction: Create directory
- OSRmdirAction: Remove directory
- OSWalkAction: Walk directory tree
- OSPathJoinAction: Join path
- OSPathSplitAction: Split path
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OSDirAction(BaseAction):
    """List directory."""
    action_type = "os10_dir"
    display_name = "列出目录"
    description = "列出目录内容"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dir.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with directory contents.
        """
        path = params.get('path', '.')
        output_var = params.get('output_var', 'dir_contents')

        try:
            resolved_path = context.resolve_value(path)

            contents = os.listdir(resolved_path)
            context.set(output_var, contents)

            return ActionResult(
                success=True,
                message=f"列出目录: {len(contents)}项",
                data={
                    'path': resolved_path,
                    'contents': contents,
                    'count': len(contents),
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"目录不存在: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '.', 'output_var': 'dir_contents'}


class OSMkdirAction(BaseAction):
    """Create directory."""
    action_type = "os10_mkdir"
    display_name = "创建目录"
    description = "创建目录"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mkdir.

        Args:
            context: Execution context.
            params: Dict with path, parents, output_var.

        Returns:
            ActionResult with mkdir status.
        """
        path = params.get('path', '')
        parents = params.get('parents', False)
        output_var = params.get('output_var', 'mkdir_status')

        try:
            resolved_path = context.resolve_value(path)
            resolved_parents = context.resolve_value(parents) if parents else False

            if resolved_parents:
                os.makedirs(resolved_path, exist_ok=True)
            else:
                os.mkdir(resolved_path)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"创建目录: {resolved_path}",
                data={
                    'path': resolved_path,
                    'parents': resolved_parents,
                    'output_var': output_var
                }
            )
        except FileExistsError:
            return ActionResult(
                success=False,
                message=f"目录已存在: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'parents': False, 'output_var': 'mkdir_status'}


class OSRmdirAction(BaseAction):
    """Remove directory."""
    action_type = "os10_rmdir"
    display_name = "删除目录"
    description = "删除空目录"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rmdir.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with rmdir status.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'rmdir_status')

        try:
            resolved_path = context.resolve_value(path)

            os.rmdir(resolved_path)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"删除目录: {resolved_path}",
                data={
                    'path': resolved_path,
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"目录不存在: {resolved_path}"
            )
        except OSError:
            return ActionResult(
                success=False,
                message=f"目录非空: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rmdir_status'}


class OSWalkAction(BaseAction):
    """Walk directory tree."""
    action_type = "os10_walk"
    display_name = "遍历目录"
    description = "遍历目录树"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute walk.

        Args:
            context: Execution context.
            params: Dict with path, topdown, output_var.

        Returns:
            ActionResult with directory tree.
        """
        path = params.get('path', '.')
        topdown = params.get('topdown', True)
        output_var = params.get('output_var', 'dir_tree')

        try:
            resolved_path = context.resolve_value(path)
            resolved_topdown = context.resolve_value(topdown) if topdown else True

            tree = []
            for root, dirs, files in os.walk(resolved_path, topdown=resolved_topdown):
                tree.append({
                    'root': root,
                    'dirs': dirs,
                    'files': files
                })

            context.set(output_var, tree)

            return ActionResult(
                success=True,
                message=f"遍历目录: {len(tree)}个目录",
                data={
                    'path': resolved_path,
                    'tree': tree,
                    'count': len(tree),
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"目录不存在: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"遍历目录失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '.', 'topdown': True, 'output_var': 'dir_tree'}


class OSPathJoinAction(BaseAction):
    """Join path."""
    action_type = "os10_path_join"
    display_name = "连接路径"
    description = "连接路径"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path join.

        Args:
            context: Execution context.
            params: Dict with parts, output_var.

        Returns:
            ActionResult with joined path.
        """
        parts = params.get('parts', [])
        output_var = params.get('output_var', 'joined_path')

        try:
            resolved_parts = context.resolve_value(parts)

            if not isinstance(resolved_parts, (list, tuple)):
                resolved_parts = [resolved_parts]

            result = os.path.join(*resolved_parts)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接路径: {result}",
                data={
                    'parts': resolved_parts,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['parts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'joined_path'}


class OSPathSplitAction(BaseAction):
    """Split path."""
    action_type = "os10_path_split"
    display_name = "分割路径"
    description = "分割路径"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute path split.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with split path.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'split_path')

        try:
            resolved_path = context.resolve_value(path)

            dir_name, base_name = os.path.split(resolved_path)
            root, ext = os.path.splitext(base_name)

            result = {
                'dir': dir_name,
                'base': base_name,
                'root': root,
                'ext': ext
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分割路径: {base_name}",
                data={
                    'path': resolved_path,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'split_path'}