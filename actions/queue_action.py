"""Queue action module for RabAI AutoClick.

Provides message queue operations:
- QueuePushAction: Push message to queue
- QueuePopAction: Pop message from queue
- QueuePeekAction: Peek at queue message
- QueueSizeAction: Get queue size
- QueueClearAction: Clear queue
- QueueListAction: List all queues
"""

import json
import os
import pickle
import uuid
from typing import Any, Dict, List, Optional
from pathlib import Path

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueueManager:
    """Simple file-based queue manager."""

    def __init__(self, queue_dir: str = '/tmp/rabai_queues'):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self._locks = {}

    def _get_queue_path(self, name: str) -> Path:
        return self.queue_dir / f"{name}.queue"

    def _get_lock_path(self, name: str) -> Path:
        return self.queue_dir / f"{name}.lock"

    def push(self, queue_name: str, message: Any, priority: int = 0) -> bool:
        """Push message to queue."""
        queue_path = self._get_queue_path(queue_name)
        lock_path = self._get_lock_path(queue_name)

        with open(lock_path, 'w') as f:
            f.write(str(uuid.uuid4()))

        try:
            messages = []
            if queue_path.exists():
                with open(queue_path, 'rb') as f:
                    try:
                        messages = pickle.load(f)
                    except:
                        messages = []

            entry = {
                'id': str(uuid.uuid4()),
                'message': message,
                'priority': priority,
                'timestamp': str(Path(queue_path).stat().st_mtime) if queue_path.exists() else '0'
            }
            messages.append(entry)
            messages.sort(key=lambda x: x['priority'], reverse=True)

            with open(queue_path, 'wb') as f:
                pickle.dump(messages, f)

            return True
        finally:
            if lock_path.exists():
                lock_path.unlink()

    def pop(self, queue_name: str) -> Optional[Any]:
        """Pop message from queue."""
        queue_path = self._get_queue_path(queue_name)
        lock_path = self._get_lock_path(queue_name)

        with open(lock_path, 'w') as f:
            f.write(str(uuid.uuid4()))

        try:
            if not queue_path.exists():
                return None

            with open(queue_path, 'rb') as f:
                messages = pickle.load(f)

            if not messages:
                return None

            message = messages.pop(0)

            with open(queue_path, 'wb') as f:
                pickle.dump(messages, f)

            return message.get('message')
        finally:
            if lock_path.exists():
                lock_path.unlink()

    def peek(self, queue_name: str) -> Optional[Any]:
        """Peek at first message."""
        queue_path = self._get_queue_path(queue_name)

        if not queue_path.exists():
            return None

        with open(queue_path, 'rb') as f:
            messages = pickle.load(f)

        if not messages:
            return None

        return messages[0].get('message')

    def size(self, queue_name: str) -> int:
        """Get queue size."""
        queue_path = self._get_queue_path(queue_name)

        if not queue_path.exists():
            return 0

        with open(queue_path, 'rb') as f:
            messages = pickle.load(f)

        return len(messages)

    def clear(self, queue_name: str) -> bool:
        """Clear queue."""
        queue_path = self._get_queue_path(queue_name)
        lock_path = self._get_lock_path(queue_name)

        with open(lock_path, 'w') as f:
            f.write(str(uuid.uuid4()))

        try:
            if queue_path.exists():
                queue_path.unlink()
            return True
        finally:
            if lock_path.exists():
                lock_path.unlink()

    def list_queues(self) -> List[str]:
        """List all queues."""
        queues = []
        for f in self.queue_dir.glob('*.queue'):
            queues.append(f.stem)
        return queues


class QueuePushAction(BaseAction):
    """Push message to queue."""
    action_type = "queue_push"
    display_name = "队列推送"
    description = "推送消息到队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with queue_name, message, priority, queue_dir.

        Returns:
            ActionResult indicating success.
        """
        queue_name = params.get('queue_name', '')
        message = params.get('message', '')
        priority = params.get('priority', 0)
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')

        valid, msg = self.validate_type(queue_name, str, 'queue_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(queue_name)
            resolved_msg = context.resolve_value(message)
            resolved_priority = context.resolve_value(priority)
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            success = manager.push(resolved_name, resolved_msg, int(resolved_priority))

            if success:
                return ActionResult(
                    success=True,
                    message=f"消息已推入队列: {resolved_name}",
                    data={'queue': resolved_name, 'priority': resolved_priority}
                )
            else:
                return ActionResult(success=False, message="队列推送失败")
        except Exception as e:
            return ActionResult(success=False, message=f"队列推送失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['queue_name', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'priority': 0, 'queue_dir': '/tmp/rabai_queues'}


class QueuePopAction(BaseAction):
    """Pop message from queue."""
    action_type = "queue_pop"
    display_name = "队列弹出"
    description = "从队列弹出消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var, queue_dir.

        Returns:
            ActionResult with message.
        """
        queue_name = params.get('queue_name', '')
        output_var = params.get('output_var', 'queue_message')
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')

        valid, msg = self.validate_type(queue_name, str, 'queue_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(queue_name)
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            message = manager.pop(resolved_name)

            if message is not None:
                context.set(output_var, message)
                return ActionResult(
                    success=True,
                    message=f"消息已弹出: {resolved_name}",
                    data={'message': message, 'output_var': output_var}
                )
            else:
                context.set(output_var, None)
                return ActionResult(
                    success=True,
                    message=f"队列为空: {resolved_name}",
                    data={'message': None, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"队列弹出失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'queue_message', 'queue_dir': '/tmp/rabai_queues'}


class QueuePeekAction(BaseAction):
    """Peek at queue message."""
    action_type = "queue_peek"
    display_name = "队列查看"
    description = "查看队列头部消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute peek.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var, queue_dir.

        Returns:
            ActionResult with message.
        """
        queue_name = params.get('queue_name', '')
        output_var = params.get('output_var', 'queue_message')
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')

        valid, msg = self.validate_type(queue_name, str, 'queue_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(queue_name)
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            message = manager.peek(resolved_name)

            context.set(output_var, message)

            return ActionResult(
                success=True,
                message=f"队列头部消息" if message else f"队列为空: {resolved_name}",
                data={'message': message, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"队列查看失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'queue_message', 'queue_dir': '/tmp/rabai_queues'}


class QueueSizeAction(BaseAction):
    """Get queue size."""
    action_type = "queue_size"
    display_name = "队列大小"
    description = "获取队列大小"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with queue_name, output_var, queue_dir.

        Returns:
            ActionResult with size.
        """
        queue_name = params.get('queue_name', '')
        output_var = params.get('output_var', 'queue_size')
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')

        valid, msg = self.validate_type(queue_name, str, 'queue_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(queue_name)
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            size = manager.size(resolved_name)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"队列大小: {size}",
                data={'size': size, 'queue': resolved_name, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"队列大小查询失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'queue_size', 'queue_dir': '/tmp/rabai_queues'}


class QueueClearAction(BaseAction):
    """Clear queue."""
    action_type = "queue_clear"
    display_name = "清空队列"
    description = "清空队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with queue_name, queue_dir.

        Returns:
            ActionResult indicating success.
        """
        queue_name = params.get('queue_name', '')
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')

        valid, msg = self.validate_type(queue_name, str, 'queue_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(queue_name)
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            manager.clear(resolved_name)

            return ActionResult(
                success=True,
                message=f"队列已清空: {resolved_name}",
                data={'queue': resolved_name}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"清空队列失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_dir': '/tmp/rabai_queues'}


class QueueListAction(BaseAction):
    """List all queues."""
    action_type = "queue_list"
    display_name = "列出队列"
    description = "列出所有队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with queue_dir, output_var.

        Returns:
            ActionResult with queue list.
        """
        queue_dir = params.get('queue_dir', '/tmp/rabai_queues')
        output_var = params.get('output_var', 'queues')

        try:
            resolved_dir = context.resolve_value(queue_dir)

            manager = QueueManager(resolved_dir)
            queues = manager.list_queues()

            context.set(output_var, queues)

            return ActionResult(
                success=True,
                message=f"队列列表: {len(queues)} 个",
                data={'count': len(queues), 'queues': queues, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列出队列失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'queue_dir': '/tmp/rabai_queues', 'output_var': 'queues'}
