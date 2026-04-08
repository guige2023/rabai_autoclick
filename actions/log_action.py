"""Log action module for RabAI AutoClick.

Provides logging actions for workflow execution tracking.
"""

import time
import json
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LogMessageAction(BaseAction):
    """Log a message to file or console.
    
    Supports different log levels and formatted output.
    """
    action_type = "log_message"
    display_name = "记录日志"
    description = "记录日志消息到文件或控制台"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Log message.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: message, level, file_path, format.
        
        Returns:
            ActionResult with log status.
        """
        message = params.get('message', '')
        level = params.get('level', 'INFO')
        file_path = params.get('file_path', None)
        format_type = params.get('format', 'text')
        
        if not message:
            return ActionResult(success=False, message="message required")
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if level not in valid_levels:
            level = 'INFO'
        
        timestamp = datetime.now().isoformat()
        
        if format_type == 'json':
            log_entry = json.dumps({
                'timestamp': timestamp,
                'level': level,
                'message': message
            })
        else:
            log_entry = f"[{timestamp}] [{level}] {message}"
        
        try:
            if file_path:
                with open(file_path, 'a') as f:
                    f.write(log_entry + '\n')
            
            # Also print to console
            print(log_entry)
            
            return ActionResult(
                success=True,
                message=f"Logged: {level}",
                data={'level': level, 'entry': log_entry}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Log error: {e}",
                data={'error': str(e)}
            )


class LogReadAction(BaseAction):
    """Read log file contents.
    
    Returns recent log entries with optional filtering.
    """
    action_type = "log_read"
    display_name = "读取日志"
    description = "读取日志文件内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read log file.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: file_path, lines, level_filter.
        
        Returns:
            ActionResult with log entries.
        """
        file_path = params.get('file_path', '')
        lines = params.get('lines', 100)
        level_filter = params.get('level_filter', None)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"Log file not found: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                all_lines = f.readlines()
            
            # Get last N lines
            recent = all_lines[-lines:] if lines > 0 else all_lines
            
            # Filter by level if specified
            if level_filter:
                recent = [l for l in recent if f"[{level_filter}]" in l]
            
            return ActionResult(
                success=True,
                message=f"Read {len(recent)} log entries",
                data={'entries': recent, 'count': len(recent)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Log read error: {e}",
                data={'error': str(e)}
            )


class LogClearAction(BaseAction):
    """Clear log file contents.
    
    Truncates or deletes log file.
    """
    action_type = "log_clear"
    display_name = "清空日志"
    description = "清空日志文件内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear log file.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: file_path, delete_file.
        
        Returns:
            ActionResult with clear status.
        """
        file_path = params.get('file_path', '')
        delete_file = params.get('delete_file', False)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        try:
            if delete_file:
                os.remove(file_path)
                return ActionResult(
                    success=True,
                    message=f"Deleted: {file_path}",
                    data={'deleted': True}
                )
            else:
                with open(file_path, 'w') as f:
                    f.write('')
                return ActionResult(
                    success=True,
                    message=f"Cleared: {file_path}",
                    data={'cleared': True}
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Log clear error: {e}",
                data={'error': str(e)}
            )
