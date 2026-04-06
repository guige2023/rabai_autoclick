"""Execution statistics tracking for RabAI AutoClick.

Provides ExecutionStats class for tracking workflow execution metrics,
including session history, step performance, and success rates.
"""

import os
import json
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional


# Maximum number of history entries to retain
MAX_HISTORY_SIZE: int = 1000


class ExecutionStats:
    """Track and persist workflow execution statistics.
    
    Records session data, step performance, loop durations, and errors.
    Provides aggregated statistics and historical query methods.
    """
    
    def __init__(self) -> None:
        """Initialize the execution stats tracker."""
        self.stats_file: str = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'execution_stats.json'
        )
        self.current_session: Optional[Dict[str, Any]] = None
        self.history: List[Dict[str, Any]] = []
        self._load_stats()
    
    def _load_stats(self) -> None:
        """Load historical stats from JSON file."""
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    self.history = json.load(f)
            except Exception:
                self.history = []
        else:
            self.history = []
    
    def _save_stats(self) -> None:
        """Save stats to JSON file (last MAX_HISTORY_SIZE entries)."""
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.history[-MAX_HISTORY_SIZE:], f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    
    def start_session(
        self, 
        workflow_name: str = "未命名", 
        loop_count: int = 1
    ) -> Dict[str, Any]:
        """Start a new execution session.
        
        Args:
            workflow_name: Name of the workflow being executed.
            loop_count: Number of loops configured for this session.
            
        Returns:
            The session dictionary.
        """
        self.current_session = {
            'start_time': time.time(),
            'workflow_name': workflow_name,
            'loop_count': loop_count,
            'loops': [],
            'steps': [],
            'errors': []
        }
        return self.current_session
    
    def record_loop(
        self,
        loop_index: int,
        duration: float,
        success: bool,
        step_count: int
    ) -> None:
        """Record a loop iteration result.
        
        Args:
            loop_index: Index of this loop iteration.
            duration: Time taken for this loop in seconds.
            success: Whether the loop completed successfully.
            step_count: Number of steps executed in this loop.
        """
        if self.current_session:
            self.current_session['loops'].append({
                'index': loop_index,
                'duration': duration,
                'success': success,
                'step_count': step_count,
                'timestamp': time.time()
            })
    
    def record_step(
        self,
        step_type: str,
        duration: float,
        success: bool,
        message: str = ""
    ) -> None:
        """Record a step execution result.
        
        Args:
            step_type: Type/name of the step action.
            duration: Time taken for this step in seconds.
            success: Whether the step completed successfully.
            message: Optional message or error description.
        """
        if self.current_session:
            self.current_session['steps'].append({
                'type': step_type,
                'duration': duration,
                'success': success,
                'message': message[:100] if message else "",
                'timestamp': time.time()
            })
    
    def record_error(
        self,
        step_type: str,
        error_msg: str
    ) -> None:
        """Record an error that occurred during execution.
        
        Args:
            step_type: Type/name of the step where error occurred.
            error_msg: Error message description.
        """
        if self.current_session:
            self.current_session['errors'].append({
                'type': step_type,
                'message': error_msg[:200],
                'timestamp': time.time()
            })
    
    def end_session(self, success: bool = True) -> Optional[Dict[str, Any]]:
        """End the current session and compute statistics.
        
        Args:
            success: Overall session success status.
            
        Returns:
            The completed session dictionary, or None if no session active.
        """
        if not self.current_session:
            return None
        
        self.current_session['end_time'] = time.time()
        self.current_session['total_duration'] = (
            self.current_session['end_time'] - self.current_session['start_time']
        )
        self.current_session['success'] = success
        
        loops = self.current_session['loops']
        if loops:
            self.current_session['avg_loop_duration'] = (
                sum(l['duration'] for l in loops) / len(loops)
            )
            self.current_session['success_rate'] = (
                sum(1 for l in loops if l['success']) / len(loops) * 100
            )
        else:
            self.current_session['avg_loop_duration'] = 0
            self.current_session['success_rate'] = 100 if success else 0
        
        # Compute per-step statistics
        steps = self.current_session['steps']
        if steps:
            step_durations: Dict[str, List[float]] = defaultdict(list)
            for s in steps:
                step_durations[s['type']].append(s['duration'])
            
            self.current_session['step_stats'] = {
                stype: {
                    'count': len(durs),
                    'avg_duration': sum(durs) / len(durs),
                    'total_duration': sum(durs),
                    'success_rate': (
                        sum(1 for s in steps if s['type'] == stype and s['success']) 
                        / len(durs) * 100
                    )
                }
                for stype, durs in step_durations.items()
            }
        
        self.current_session['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.history.append(self.current_session)
        self._save_stats()
        
        result = self.current_session.copy()
        self.current_session = None
        return result
    
    def get_summary(self) -> Dict[str, Any]:
        """Get aggregated statistics across all sessions.
        
        Returns:
            Dictionary with total_sessions, avg_duration, success_rate, etc.
        """
        if not self.history:
            return {
                'total_sessions': 0,
                'total_duration': 0,
                'avg_duration': 0,
                'success_rate': 0,
                'total_loops': 0,
                'step_stats': {}
            }
        
        total_sessions = len(self.history)
        total_duration = sum(h.get('total_duration', 0) for h in self.history)
        successful = sum(1 for h in self.history if h.get('success', False))
        total_loops = sum(h.get('loop_count', 1) for h in self.history)
        
        # Aggregate step statistics
        all_step_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'count': 0, 'total_duration': 0, 'success_count': 0}
        )
        
        for h in self.history:
            step_stats = h.get('step_stats', {})
            for stype, stats in step_stats.items():
                all_step_stats[stype]['count'] += stats['count']
                all_step_stats[stype]['total_duration'] += stats['total_duration']
                all_step_stats[stype]['success_count'] += int(
                    stats['count'] * stats['success_rate'] / 100
                )
        
        step_summary: Dict[str, Dict[str, Any]] = {}
        for stype, data in all_step_stats.items():
            count = data['count']
            step_summary[stype] = {
                'count': count,
                'avg_duration': data['total_duration'] / count if count > 0 else 0,
                'total_duration': data['total_duration'],
                'success_rate': (
                    data['success_count'] / count * 100 if count > 0 else 0
                )
            }
        
        return {
            'total_sessions': total_sessions,
            'total_duration': total_duration,
            'avg_duration': total_duration / total_sessions if total_sessions > 0 else 0,
            'success_rate': successful / total_sessions * 100 if total_sessions > 0 else 0,
            'total_loops': total_loops,
            'step_stats': step_summary
        }
    
    def get_recent_sessions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get the most recent sessions.
        
        Args:
            limit: Maximum number of sessions to return.
            
        Returns:
            List of session dictionaries, most recent first.
        """
        return self.history[-limit:][::-1]
    
    def get_step_performance(self) -> Dict[str, Dict[str, Any]]:
        """Get performance metrics for each step type.
        
        Returns:
            Dictionary mapping step_type to performance metrics.
        """
        step_data: Dict[str, Dict[str, List[Any]]] = defaultdict(
            lambda: {'durations': [], 'successes': [], 'errors': []}
        )
        
        for session in self.history[-100:]:
            for step in session.get('steps', []):
                step_data[step['type']]['durations'].append(step['duration'])
                step_data[step['type']]['successes'].append(step['success'])
            
            for error in session.get('errors', []):
                step_data[error['type']]['errors'].append(error['message'])
        
        result: Dict[str, Dict[str, Any]] = {}
        for stype, data in step_data.items():
            if data['durations']:
                durations = data['durations']
                successes = data['successes']
                result[stype] = {
                    'count': len(durations),
                    'avg_duration': sum(durations) / len(durations),
                    'min_duration': min(durations),
                    'max_duration': max(durations),
                    'success_rate': sum(successes) / len(successes) * 100,
                    'error_count': len(data['errors']),
                    'common_errors': list(set(data['errors'][:5]))
                }
        
        return result
    
    def clear_history(self) -> None:
        """Clear all historical statistics."""
        self.history = []
        self._save_stats()


# Global singleton instance
execution_stats: ExecutionStats = ExecutionStats()
