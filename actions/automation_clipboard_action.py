"""Automation Clipboard Action Module for RabAI AutoClick.

Manages system clipboard operations for copy/paste automation,
including text, images, and custom data formats.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationClipboardAction(BaseAction):
    """Clipboard operations for automation workflows.

    Read and write text, images, and files to/from the system
    clipboard. Supports clipboard history, format detection, and
    clipboard locking for multi-step copy operations.
    """
    action_type = "automation_clipboard"
    display_name = "剪贴板自动化"
    description = "系统剪贴板读写操作，支持历史记录"

    _history: List[Dict[str, Any]] = []
    _max_history = 50
    _locked = False
    _lock_holder: Optional[str] = None

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'copy', 'paste', 'cut', 'clear',
                               'history', 'get_format', 'lock', 'unlock'
                - text: str (optional) - text to copy
                - image_path: str (optional) - image file to copy
                - file_paths: list (optional) - files to copy
                - format: str (optional) - 'text', 'image', 'file', 'auto'
                - index: int (optional) - history index to retrieve
                - lock_id: str (optional) - identifier for clipboard lock

        Returns:
            ActionResult with clipboard operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'copy')

            if operation == 'copy':
                return self._copy(params, start_time)
            elif operation == 'paste':
                return self._paste(params, start_time)
            elif operation == 'cut':
                return self._cut(params, start_time)
            elif operation == 'clear':
                return self._clear_clipboard(start_time)
            elif operation == 'history':
                return self._get_history(params, start_time)
            elif operation == 'get_format':
                return self._get_format(start_time)
            elif operation == 'lock':
                return self._lock_clipboard(params, start_time)
            elif operation == 'unlock':
                return self._unlock_clipboard(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clipboard action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _copy(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Copy content to clipboard."""
        if self._locked and self._lock_holder != params.get('lock_id'):
            return ActionResult(
                success=False,
                message="Clipboard is locked by another process",
                data={'locked_by': self._lock_holder},
                duration=time.time() - start_time
            )

        text = params.get('text', '')
        image_path = params.get('image_path', '')
        file_paths = params.get('file_paths', [])
        format_type = params.get('format', 'auto')

        content_type = 'text'
        content = text

        if file_paths:
            content_type = 'file'
            content = file_paths
        elif image_path:
            content_type = 'image'
            content = image_path
        elif text:
            content_type = 'text'
            content = text
        elif format_type != 'auto':
            content_type = format_type

        try:
            if content_type == 'text' and content:
                self._write_text(content)
            elif content_type == 'image' and content:
                self._write_image(content)
            elif content_type == 'file' and content:
                self._write_files(content)

            entry = {
                'type': content_type,
                'content': content,
                'timestamp': time.time(),
                'size': len(str(content))
            }
            self._history.insert(0, entry)
            if len(self._history) > self._max_history:
                self._history.pop()

            return ActionResult(
                success=True,
                message=f"Copied to clipboard: {content_type}",
                data={
                    'type': content_type,
                    'history_index': 0,
                    'total_history': len(self._history)
                },
                duration=time.time() - start_time
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Copy failed: {str(e)}",
                duration=time.time() - start_time
            )

    def _paste(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Read content from clipboard."""
        index = params.get('index', 0)
        format_type = params.get('format', 'auto')

        if index == 0:
            content = self._read_clipboard(format_type)
            if content is None:
                if self._history:
                    entry = self._history[0]
                    return ActionResult(
                        success=True,
                        message=f"Pasted from history: {entry['type']}",
                        data={
                            'type': entry['type'],
                            'content': entry['content'],
                            'from_history': True
                        },
                        duration=time.time() - start_time
                    )
                return ActionResult(
                    success=False,
                    message="Clipboard is empty",
                    duration=time.time() - start_time
                )
        else:
            if 0 < index < len(self._history):
                entry = self._history[index]
                content = entry['content']
            else:
                return ActionResult(
                    success=False,
                    message=f"History index out of range: {index}",
                    duration=time.time() - start_time
                )

        return ActionResult(
            success=True,
            message=f"Content retrieved from clipboard",
            data={
                'type': entry.get('type', 'text') if index > 0 else format_type,
                'content': content
            },
            duration=time.time() - start_time
        )

    def _cut(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Cut operation (copy + clear)."""
        copy_result = self._copy(params, start_time)
        if copy_result.success:
            self._clear_clipboard(start_time)
        return copy_result

    def _clear_clipboard(self, start_time: float) -> ActionResult:
        """Clear the clipboard."""
        try:
            self._write_text('')
            return ActionResult(
                success=True,
                message="Clipboard cleared",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clear failed: {str(e)}",
                duration=time.time() - start_time
            )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get clipboard history."""
        limit = params.get('limit', 10)
        history = self._history[:limit]
        return ActionResult(
            success=True,
            message=f"History: {len(history)} entries",
            data={
                'history': [
                    {
                        'index': i,
                        'type': h['type'],
                        'size': h['size'],
                        'timestamp': h['timestamp']
                    }
                    for i, h in enumerate(history)
                ],
                'total': len(self._history)
            },
            duration=time.time() - start_time
        )

    def _get_format(self, start_time: float) -> ActionResult:
        """Detect clipboard content format."""
        try:
            import subprocess
            result = subprocess.run(
                ['pbpaste', '-Prefer', 'txt'],
                capture_output=True, timeout=2
            )
            if result.returncode == 0 and result.stdout:
                return ActionResult(
                    success=True,
                    message="Format detected: text",
                    data={'format': 'text'},
                    duration=time.time() - start_time
                )
            return ActionResult(
                success=True,
                message="Format: unknown",
                data={'format': 'unknown'},
                duration=time.time() - start_time
            )
        except Exception:
            return ActionResult(
                success=True,
                message="Format detection failed",
                data={'format': 'unknown'},
                duration=time.time() - start_time
            )

    def _lock_clipboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Lock clipboard for exclusive access."""
        lock_id = params.get('lock_id', 'default')
        if self._locked:
            return ActionResult(
                success=False,
                message="Clipboard already locked",
                data={'locked_by': self._lock_holder},
                duration=time.time() - start_time
            )
        self._locked = True
        self._lock_holder = lock_id
        return ActionResult(
            success=True,
            message=f"Clipboard locked: {lock_id}",
            duration=time.time() - start_time
        )

    def _unlock_clipboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Unlock clipboard."""
        lock_id = params.get('lock_id', 'default')
        if self._lock_holder != lock_id:
            return ActionResult(
                success=False,
                message="Not the lock holder",
                data={'locked_by': self._lock_holder},
                duration=time.time() - start_time
            )
        self._locked = False
        self._lock_holder = None
        return ActionResult(
            success=True,
            message="Clipboard unlocked",
            duration=time.time() - start_time
        )

    def _write_text(self, text: str) -> None:
        """Write text to clipboard using pbcopy."""
        import subprocess
        process = subprocess.Popen(
            ['pbcopy'],
            stdin=subprocess.PIPE
        )
        process.communicate(input=text.encode('utf-8'))

    def _write_image(self, image_path: str) -> None:
        """Write image file to clipboard."""
        import subprocess
        subprocess.run(
            ['osascript', '-e',
             f'set the clipboard to (read (POSIX file "{image_path}") as JPEG picture)'],
            timeout=5
        )

    def _write_files(self, file_paths: List[str]) -> None:
        """Write file references to clipboard."""
        import subprocess
        file_list = ' '.join([f'POSIX file "{p}"' for p in file_paths])
        subprocess.run(
            ['osascript', '-e',
             f'set the clipboard to {{{file_list}}}'],
            timeout=5
        )

    def _read_clipboard(self, format_type: str) -> Optional[str]:
        """Read text from clipboard using pbpaste."""
        import subprocess
        try:
            if format_type == 'text' or format_type == 'auto':
                result = subprocess.run(
                    ['pbpaste'],
                    capture_output=True, timeout=2
                )
                if result.returncode == 0:
                    return result.stdout.decode('utf-8')
        except Exception:
            pass
        return None
