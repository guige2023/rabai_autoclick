import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict

class ExecutionStats:
    def __init__(self):
        self.stats_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'execution_stats.json')
        self.current_session = None
        self._load_stats()
    
    def _load_stats(self):
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    self.history = json.load(f)
            except:
                self.history = []
        else:
            self.history = []
    
    def _save_stats(self):
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.history[-1000:], f, ensure_ascii=False, indent=2)
        except:
            pass
    
    def start_session(self, workflow_name: str = "未命名", loop_count: int = 1):
        self.current_session = {
            'start_time': time.time(),
            'workflow_name': workflow_name,
            'loop_count': loop_count,
            'loops': [],
            'steps': [],
            'errors': []
        }
        return self.current_session
    
    def record_loop(self, loop_index: int, duration: float, success: bool, step_count: int):
        if self.current_session:
            self.current_session['loops'].append({
                'index': loop_index,
                'duration': duration,
                'success': success,
                'step_count': step_count,
                'timestamp': time.time()
            })
    
    def record_step(self, step_type: str, duration: float, success: bool, message: str = ""):
        if self.current_session:
            self.current_session['steps'].append({
                'type': step_type,
                'duration': duration,
                'success': success,
                'message': message[:100] if message else "",
                'timestamp': time.time()
            })
    
    def record_error(self, step_type: str, error_msg: str):
        if self.current_session:
            self.current_session['errors'].append({
                'type': step_type,
                'message': error_msg[:200],
                'timestamp': time.time()
            })
    
    def end_session(self, success: bool = True):
        if not self.current_session:
            return None
        
        self.current_session['end_time'] = time.time()
        self.current_session['total_duration'] = self.current_session['end_time'] - self.current_session['start_time']
        self.current_session['success'] = success
        
        loops = self.current_session['loops']
        if loops:
            self.current_session['avg_loop_duration'] = sum(l['duration'] for l in loops) / len(loops)
            self.current_session['success_rate'] = sum(1 for l in loops if l['success']) / len(loops) * 100
        else:
            self.current_session['avg_loop_duration'] = 0
            self.current_session['success_rate'] = 100 if success else 0
        
        steps = self.current_session['steps']
        if steps:
            step_durations = defaultdict(list)
            for s in steps:
                step_durations[s['type']].append(s['duration'])
            
            self.current_session['step_stats'] = {
                stype: {
                    'count': len(durs),
                    'avg_duration': sum(durs) / len(durs),
                    'total_duration': sum(durs),
                    'success_rate': sum(1 for s in steps if s['type'] == stype and s['success']) / len(durs) * 100
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
        
        all_step_stats = defaultdict(lambda: {'count': 0, 'total_duration': 0, 'success_count': 0})
        
        for h in self.history:
            step_stats = h.get('step_stats', {})
            for stype, stats in step_stats.items():
                all_step_stats[stype]['count'] += stats['count']
                all_step_stats[stype]['total_duration'] += stats['total_duration']
                all_step_stats[stype]['success_count'] += int(stats['count'] * stats['success_rate'] / 100)
        
        step_summary = {}
        for stype, data in all_step_stats.items():
            step_summary[stype] = {
                'count': data['count'],
                'avg_duration': data['total_duration'] / data['count'] if data['count'] > 0 else 0,
                'total_duration': data['total_duration'],
                'success_rate': data['success_count'] / data['count'] * 100 if data['count'] > 0 else 0
            }
        
        return {
            'total_sessions': total_sessions,
            'total_duration': total_duration,
            'avg_duration': total_duration / total_sessions if total_sessions > 0 else 0,
            'success_rate': successful / total_sessions * 100 if total_sessions > 0 else 0,
            'total_loops': total_loops,
            'step_stats': step_summary
        }
    
    def get_recent_sessions(self, limit: int = 20) -> List[Dict]:
        return self.history[-limit:][::-1]
    
    def get_step_performance(self) -> Dict[str, Dict]:
        step_data = defaultdict(lambda: {'durations': [], 'successes': [], 'errors': []})
        
        for session in self.history[-100:]:
            for step in session.get('steps', []):
                step_data[step['type']]['durations'].append(step['duration'])
                step_data[step['type']]['successes'].append(step['success'])
            
            for error in session.get('errors', []):
                step_data[error['type']]['errors'].append(error['message'])
        
        result = {}
        for stype, data in step_data.items():
            if data['durations']:
                result[stype] = {
                    'count': len(data['durations']),
                    'avg_duration': sum(data['durations']) / len(data['durations']),
                    'min_duration': min(data['durations']),
                    'max_duration': max(data['durations']),
                    'success_rate': sum(data['successes']) / len(data['successes']) * 100,
                    'error_count': len(data['errors']),
                    'common_errors': list(set(data['errors'][:5]))
                }
        
        return result
    
    def clear_history(self):
        self.history = []
        self._save_stats()


execution_stats = ExecutionStats()
