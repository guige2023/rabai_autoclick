"""Clipboard action module for RabAI AutoClick.

Provides clipboard operations for reading, writing,
and transforming clipboard content.
"""

import sys
import os
import subprocess
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClipboardReadAction(BaseAction):
    """Read content from system clipboard.
    
    Supports plain text reading with encoding handling.
    """
    action_type = "clipboard_read"
    display_name = "读取剪贴板"
    description = "读取系统剪贴板内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Read clipboard.
        
        Args:
            context: Execution context.
            params: Dict with keys: encoding, strip, save_to_var.
        
        Returns:
            ActionResult with clipboard content.
        """
        encoding = params.get('encoding', 'utf-8')
        strip = params.get('strip', False)
        save_to_var = params.get('save_to_var', None)

        try:
            # Use pbpaste on macOS
            result = subprocess.run(
                ['pbpaste'],
                capture_output=True,
                text=True,
                encoding=encoding,
                errors='replace'
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Failed to read clipboard: {result.stderr}"
                )

            content = result.stdout
            if strip:
                content = content.strip()

            result_data = {
                'content': content,
                'length': len(content),
                'lines': content.count('\n') + 1 if content else 0
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"剪贴板读取成功: {len(content)} 字符",
                data=result_data
            )

        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="pbpaste not found (requires macOS)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板读取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'utf-8',
            'strip': False,
            'save_to_var': None
        }


class ClipboardWriteAction(BaseAction):
    """Write content to system clipboard.
    
    Supports plain text with encoding handling.
    """
    action_type = "clipboard_write"
    display_name = "写入剪贴板"
    description = "写入内容到系统剪贴板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Write to clipboard.
        
        Args:
            context: Execution context.
            params: Dict with keys: content, append, save_to_var.
        
        Returns:
            ActionResult with write result.
        """
        content = params.get('content', '')
        append = params.get('append', False)
        save_to_var = params.get('save_to_var', None)

        if not content and not append:
            return ActionResult(success=False, message="Content is empty")

        try:
            # Read existing if appending
            if append:
                try:
                    read_result = subprocess.run(
                        ['pbpaste'],
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace'
                    )
                    if read_result.returncode == 0:
                        content = read_result.stdout + content
                except Exception:
                    pass

            # Write using pbcopy on macOS
            result = subprocess.run(
                ['pbcopy'],
                input=content,
                capture_output=True,
                text=True,
                encoding='utf-8'
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Failed to write clipboard: {result.stderr}"
                )

            result_data = {
                'written': True,
                'length': len(content),
                'appended': append
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"剪贴板写入成功: {len(content)} 字符",
                data=result_data
            )

        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="pbcopy not found (requires macOS)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'append': False,
            'save_to_var': None
        }


class ClipboardClearAction(BaseAction):
    """Clear system clipboard.
    """
    action_type = "clipboard_clear"
    display_name = "清空剪贴板"
    description = "清空系统剪贴板内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Clear clipboard."""
        try:
            # Write empty string
            subprocess.run(
                ['pbcopy'],
                input='',
                capture_output=True,
                text=True
            )
            return ActionResult(
                success=True,
                message="剪贴板已清空"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="pbcopy not found (requires macOS)"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
