"""Audit logging utilities for RabAI AutoClick.

Logs all workflow executions with timestamps, user info, duration,
and success/failure status to a JSON file.
"""

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional


class AuditLogger:
    """Logs workflow executions for audit trail.
    
    Thread-safe audit logger that stores execution records in JSON format.
    """
    
    def __init__(
        self,
        log_file: Optional[str] = None,
        max_entries: int = 10000
    ) -> None:
        """Initialize the audit logger.
        
        Args:
            log_file: Path to the audit log file. Defaults to
                     logs/audit.json in the project root.
            max_entries: Maximum number of entries to keep (FIFO overflow).
        """
        if log_file is None:
            project_root = os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
            log_file = os.path.join(project_root, "logs", "audit.json")
        
        self._log_file = log_file
        self._max_entries = max_entries
        self._lock = threading.Lock()
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(self._log_file), exist_ok=True)
        
        # Initialize file if it doesn't exist
        if not os.path.exists(self._log_file):
            self._write_entries([])
    
    def _read_entries(self) -> List[Dict[str, Any]]:
        """Read all entries from the audit log file.
        
        Returns:
            List of audit log entries.
        """
        try:
            with open(self._log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (json.JSONDecodeError, FileNotFoundError):
            return []
    
    def _write_entries(self, entries: List[Dict[str, Any]]) -> None:
        """Write entries to the audit log file.
        
        Args:
            entries: List of audit log entries to write.
        """
        with open(self._log_file, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, default=str)
    
    def log_execution(
        self,
        workflow_name: str,
        user: Optional[str] = None,
        duration: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Log a workflow execution.
        
        Args:
            workflow_name: Name of the workflow executed.
            user: User who executed the workflow.
            duration: Execution duration in seconds.
            success: Whether execution succeeded.
            error: Error message if execution failed.
            metadata: Additional metadata to log.
            
        Returns:
            The created audit entry.
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "workflow_name": workflow_name,
            "user": user or "unknown",
            "duration_seconds": duration,
            "success": success,
            "error": error,
            "metadata": metadata or {},
        }
        
        with self._lock:
            entries = self._read_entries()
            entries.append(entry)
            
            # Enforce max entries (FIFO)
            if len(entries) > self._max_entries:
                entries = entries[-self._max_entries:]
            
            self._write_entries(entries)
        
        return entry
    
    def query_logs(
        self,
        workflow_name: Optional[str] = None,
        user: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        success: Optional[bool] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query audit logs with filters.
        
        Args:
            workflow_name: Filter by workflow name (partial match).
            user: Filter by user (partial match).
            start_time: Filter by start timestamp (ISO format).
            end_time: Filter by end timestamp (ISO format).
            success: Filter by success status.
            limit: Maximum number of entries to return.
            
        Returns:
            List of matching audit entries.
        """
        entries = self._read_entries()
        results = []
        
        for entry in reversed(entries):
            # Filter by workflow name
            if workflow_name and workflow_name.lower() not in entry.get(
                "workflow_name", ""
            ).lower():
                continue
            
            # Filter by user
            if user and user.lower() not in entry.get("user", "").lower():
                continue
            
            # Filter by time range
            timestamp = entry.get("timestamp", "")
            if start_time and timestamp < start_time:
                continue
            if end_time and timestamp > end_time:
                continue
            
            # Filter by success
            if success is not None and entry.get("success") != success:
                continue
            
            results.append(entry)
            
            if len(results) >= limit:
                break
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get audit log statistics.
        
        Returns:
            Dictionary with execution statistics.
        """
        entries = self._read_entries()
        
        if not entries:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "success_rate": 0.0,
                "average_duration": 0.0,
                "total_duration": 0.0,
            }
        
        successful = sum(1 for e in entries if e.get("success", False))
        failed = len(entries) - successful
        
        durations = [
            e.get("duration_seconds", 0)
            for e in entries
            if e.get("duration_seconds") is not None
        ]
        total_duration = sum(durations)
        avg_duration = total_duration / len(durations) if durations else 0.0
        
        return {
            "total_executions": len(entries),
            "successful_executions": successful,
            "failed_executions": failed,
            "success_rate": successful / len(entries) if entries else 0.0,
            "average_duration": avg_duration,
            "total_duration": total_duration,
            "unique_workflows": len(set(e.get("workflow_name") for e in entries)),
            "unique_users": len(set(e.get("user") for e in entries)),
        }
