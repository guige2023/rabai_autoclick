"""Logger action module for RabAI AutoClick.

Provides logging actions for structured message logging,
log level control, and file output with rotation.
"""

import os
import sys
import time
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LogMessageAction(BaseAction):
    """Log a structured message to file or console.
    
    Supports multiple log levels, timestamps, context variables,
    and JSON-formatted output.
    """
    action_type = "log_message"
    display_name = "日志记录"
    description = "记录日志消息，支持多级别和时间戳"

    LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    LOG_FILE = "/tmp/rabai_autoclick.log"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Log a message.
        
        Args:
            context: Execution context.
            params: Dict with keys: message, level, tag, 
                   include_context, output_file, console.
        
        Returns:
            ActionResult with logging result.
        """
        message = params.get('message', '')
        level = params.get('level', 'INFO').upper()
        tag = params.get('tag', 'AUTOCLICK')
        include_context = params.get('include_context', False)
        output_file = params.get('output_file', None)
        console = params.get('console', True)

        # Validate log level
        if level not in self.LOG_LEVELS:
            return ActionResult(
                success=False,
                message=f"Invalid log level: {level}. Valid: {self.LOG_LEVELS}"
            )

        if not message:
            return ActionResult(
                success=False,
                message="Log message cannot be empty"
            )

        # Build log entry
        timestamp = datetime.now().isoformat()
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'tag': tag,
            'message': message
        }

        # Include context variables if requested
        if include_context and hasattr(context, 'variables'):
            log_entry['context'] = {
                k: str(v)[:200] for k, v in context.variables.items()
            }

        # Format output
        if output_file and output_file.endswith('.jsonl'):
            line = json.dumps(log_entry, ensure_ascii=False)
        else:
            line = f"[{timestamp}] [{level}] [{tag}] {message}"
            if include_context and 'context' in log_entry:
                line += f" | context: {log_entry['context']}"

        # Write to file
        file_written = False
        if output_file:
            try:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(line + '\n')
                file_written = True
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to write log file: {str(e)}"
                )

        # Console output (default)
        if console:
            print(line)

        result_data = {
            'logged': True,
            'level': level,
            'tag': tag,
            'file_written': file_written,
            'output_file': output_file
        }

        return ActionResult(
            success=True,
            message=f"Logged [{level}]: {message[:50]}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'level': 'INFO',
            'tag': 'AUTOCLICK',
            'include_context': False,
            'output_file': None,
            'console': True
        }


class LogQueryAction(BaseAction):
    """Query and filter log entries from log file.
    
    Supports level filtering, time range queries,
    pattern matching, and tail functionality.
    """
    action_type = "log_query"
    display_name = "查询日志"
    description = "查询过滤日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Query log entries.
        
        Args:
            context: Execution context.
            params: Dict with keys: log_file, level, pattern,
                   since, limit, tail, save_to_var.
        
        Returns:
            ActionResult with matching log entries.
        """
        log_file = params.get('log_file', LogMessageAction.LOG_FILE)
        level = params.get('level', None)
        pattern = params.get('pattern', None)
        since = params.get('since', None)
        limit = params.get('limit', 100)
        tail = params.get('tail', False)
        save_to_var = params.get('save_to_var', None)

        if not os.path.exists(log_file):
            return ActionResult(
                success=False,
                message=f"Log file not found: {log_file}"
            )

        entries = []
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Apply tail
            if tail:
                lines = lines[-limit:] if limit else lines[-100:]

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Try to parse as JSON
                parsed = None
                if line.startswith('{'):
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError:
                        pass

                # Level filter
                if level:
                    if parsed:
                        if parsed.get('level', '').upper() != level.upper():
                            continue
                    elif f'[{level}]' not in line:
                        continue

                # Pattern filter
                if pattern:
                    if parsed:
                        msg = parsed.get('message', '')
                        tag = parsed.get('tag', '')
                        if pattern not in msg and pattern not in tag:
                            continue
                    elif pattern not in line:
                        continue

                # Time filter
                if since and parsed:
                    entry_time = parsed.get('timestamp', '')
                    if entry_time < since:
                        continue

                entries.append(parsed if parsed else line)

                if limit and len(entries) >= limit:
                    break

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to read log file: {str(e)}"
            )

        result_data = {
            'count': len(entries),
            'entries': entries[-limit:] if limit else entries,
            'log_file': log_file
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Found {len(entries)} log entries",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'log_file': LogMessageAction.LOG_FILE,
            'level': None,
            'pattern': None,
            'since': None,
            'limit': 100,
            'tail': False,
            'save_to_var': None
        }
