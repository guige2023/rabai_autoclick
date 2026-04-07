"""Thread12 action module for RabAI AutoClick.

Provides additional threading operations:
- ThreadCreateAction: Create thread
- ThreadJoinAction: Join thread
- ThreadActiveCountAction: Get active thread count
- ThreadCurrentAction: Get current thread
- ThreadLockAction: Create lock
- ThreadEventAction: Create event
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ThreadCreateAction(BaseAction):
    """Create thread."""
    action_type = "thread12_create"
    display_name = "创建线程"
    description = "创建新线程"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create thread.

        Args:
            context: Execution context.
            params: Dict with name, target, args, output_var.

        Returns:
            ActionResult with thread info.
        """
        name = params.get('name', 'Thread')
        target = params.get('target', None)
        args = params.get('args', [])
        output_var = params.get('output_var', 'thread_info')

        try:
            import threading

            resolved_name = context.resolve_value(name) if name else 'Thread'
            resolved_args = context.resolve_value(args) if args else ()

            thread = threading.Thread(
                name=resolved_name,
                target=self._dummy_target,
                args=resolved_args
            )
            thread.start()

            result = {
                'name': thread.name,
                'ident': thread.ident,
                'alive': thread.is_alive()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建线程: {thread.name}",
                data={
                    'thread': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建线程失败: {str(e)}"
            )

    def _dummy_target(self, *args):
        """Dummy target function."""
        pass

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'target': None, 'args': [], 'output_var': 'thread_info'}


class ThreadJoinAction(BaseAction):
    """Join thread."""
    action_type = "thread12_join"
    display_name = "等待线程"
    description = "等待线程结束"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join thread.

        Args:
            context: Execution context.
            params: Dict with thread_name, timeout, output_var.

        Returns:
            ActionResult with join status.
        """
        thread_name = params.get('thread_name', '')
        timeout = params.get('timeout', None)
        output_var = params.get('output_var', 'join_status')

        try:
            import threading

            resolved_name = context.resolve_value(thread_name) if thread_name else ''
            resolved_timeout = float(context.resolve_value(timeout)) if timeout else None

            thread = None
            for t in threading.enumerate():
                if t.name == resolved_name:
                    thread = t
                    break

            if thread is None:
                return ActionResult(
                    success=False,
                    message=f"线程不存在: {resolved_name}"
                )

            thread.join(timeout=resolved_timeout)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"等待线程结束: {resolved_name}",
                data={
                    'thread_name': resolved_name,
                    'finished': not thread.is_alive(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待线程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['thread_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': None, 'output_var': 'join_status'}


class ThreadActiveCountAction(BaseAction):
    """Get active thread count."""
    action_type = "thread12_active_count"
    display_name = "活跃线程数"
    description = "获取活跃线程数"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute active thread count.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with thread count.
        """
        output_var = params.get('output_var', 'active_count')

        try:
            import threading

            result = threading.active_count()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"活跃线程数: {result}",
                data={
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取活跃线程数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'active_count'}


class ThreadCurrentAction(BaseAction):
    """Get current thread."""
    action_type = "thread12_current"
    display_name = "当前线程"
    description = "获取当前线程"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute current thread.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current thread info.
        """
        output_var = params.get('output_var', 'current_thread')

        try:
            import threading

            current = threading.current_thread()
            result = {
                'name': current.name,
                'ident': current.ident,
                'alive': current.is_alive()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前线程: {current.name}",
                data={
                    'thread': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前线程失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'current_thread'}


class ThreadLockAction(BaseAction):
    """Create lock."""
    action_type = "thread12_lock"
    display_name = "创建锁"
    description = "创建线程锁"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create lock.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with lock info.
        """
        name = params.get('name', 'Lock')
        output_var = params.get('output_var', 'lock_info')

        try:
            import threading

            resolved_name = context.resolve_value(name) if name else 'Lock'
            lock = threading.Lock()

            result = {
                'name': resolved_name,
                'acquired': False
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建锁: {resolved_name}",
                data={
                    'lock': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建锁失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lock_info'}


class ThreadEventAction(BaseAction):
    """Create event."""
    action_type = "thread12_event"
    display_name = "创建事件"
    description = "创建线程事件"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create event.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with event info.
        """
        name = params.get('name', 'Event')
        output_var = params.get('output_var', 'event_info')

        try:
            import threading

            resolved_name = context.resolve_value(name) if name else 'Event'
            event = threading.Event()

            result = {
                'name': resolved_name,
                'is_set': event.is_set()
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建事件: {resolved_name}",
                data={
                    'event': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建事件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'event_info'}