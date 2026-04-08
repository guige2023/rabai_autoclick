"""Clipboard action module for RabAI AutoClick.

Provides clipboard read, write, and manipulation actions.
"""

import sys
import os
import subprocess
from typing import Any, Dict, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClipboardReadAction(BaseAction):
    """Read text content from the system clipboard.
    
    Retrieves current clipboard contents as plain text with
    optional encoding handling and empty content detection.
    """
    action_type = "clipboard_read"
    display_name = "读取剪贴板"
    description = "读取系统剪贴板内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read clipboard contents.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: encoding, strip, max_length.
        
        Returns:
            ActionResult with clipboard content.
        """
        encoding = params.get('encoding', 'utf-8')
        strip = params.get('strip', False)
        max_length = params.get('max_length', 10000)
        
        try:
            # Use pbpaste for macOS clipboard
            result = subprocess.run(
                ['pbpaste'],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"pbpaste failed: {result.stderr.decode()}"
                )
            
            content = result.stdout.decode(encoding, errors='replace')
            
            if strip:
                content = content.strip()
            
            if max_length and len(content) > max_length:
                content = content[:max_length]
                truncated = True
            else:
                truncated = False
            
            return ActionResult(
                success=True,
                message=f"Read {len(content)} chars" + (" (truncated)" if truncated else ""),
                data=content
            )
            
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Clipboard read timed out"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clipboard read error: {e}",
                data={'error': str(e)}
            )


class ClipboardWriteAction(BaseAction):
    """Write text content to the system clipboard.
    
    Copies specified text to clipboard with optional newline handling.
    """
    action_type = "clipboard_write"
    display_name = "写入剪贴板"
    description = "将文本内容写入系统剪贴板"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Write content to clipboard.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: text, append_newline, encoding.
        
        Returns:
            ActionResult with write status.
        """
        text = params.get('text', '')
        append_newline = params.get('append_newline', False)
        encoding = params.get('encoding', 'utf-8')
        
        if text is None:
            return ActionResult(success=False, message="text is required")
        
        if append_newline and not text.endswith('\n'):
            text += '\n'
        
        try:
            # Use pbcopy for macOS clipboard
            result = subprocess.run(
                ['pbcopy'],
                input=text.encode(encoding),
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Wrote {len(text)} chars to clipboard",
                    data={'length': len(text)}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"pbcopy failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Clipboard write timed out"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clipboard write error: {e}",
                data={'error': str(e)}
            )


class ClipboardClearAction(BaseAction):
    """Clear the system clipboard contents.
    
    Removes all content from clipboard, useful before sensitive operations.
    """
    action_type = "clipboard_clear"
    display_name = "清空剪贴板"
    description = "清空系统剪贴板内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear clipboard contents.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict (unused).
        
        Returns:
            ActionResult with clear status.
        """
        try:
            # Write empty string to clipboard
            result = subprocess.run(
                ['pbcopy'],
                input=b'',
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="Clipboard cleared"
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"pbcopy failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="Clipboard clear timed out"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clipboard clear error: {e}",
                data={'error': str(e)}
            )


class ClipboardHistoryAction(BaseAction):
    """Manage clipboard history (if available).
    
    Provides access to clipboard history when using clipboard managers.
    """
    action_type = "clipboard_history"
    display_name = "剪贴板历史"
    description = "访问剪贴板历史记录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get clipboard history.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: max_items, search.
        
        Returns:
            ActionResult with history items.
        """
        max_items = params.get('max_items', 10)
        search = params.get('search', '')
        
        # Try to use third-party clipboard manager if available
        # This is a placeholder that returns empty history
        # Real implementation would check for Alfred, Paste, etc.
        
        try:
            # Check for common clipboard managers
            clipboard_apps = [
                '/Applications/Alfred 4.app',
                '/Applications/Alfred 5.app',
                '/Applications/Paste.app',
                '/Applications/Pastebot.app'
            ]
            
            available = [a for a in clipboard_apps if os.path.exists(a)]
            
            if available:
                return ActionResult(
                    success=True,
                    message=f"Clipboard manager(s) found: {len(available)}",
                    data={'managers': available, 'count': len(available)}
                )
            else:
                return ActionResult(
                    success=True,
                    message="No clipboard manager installed",
                    data={'history': [], 'count': 0}
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Clipboard history error: {e}",
                data={'error': str(e)}
            )
