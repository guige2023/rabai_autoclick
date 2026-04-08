"""Data fanout action module for RabAI AutoClick.

Provides fanout operations for distributing data to multiple targets:
- DataFanout: Distribute data to multiple handlers
- FanoutRouter: Route data to specific targets based on rules
- DataBroadcaster: Broadcast data to all registered handlers
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type
import time
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FanoutMode(Enum):
    """Fanout distribution modes."""
    BROADCAST = "broadcast"
    SELECTIVE = "selective"
    ROUND_ROBIN = "round_robin"
    HASH_BASED = "hash_based"
    WEIGHTED = "weighted"
    PRIORITY = "priority"


@dataclass
class FanoutTarget:
    """Individual fanout target."""
    name: str
    handler: Callable
    enabled: bool = True
    priority: int = 0
    weight: float = 1.0
    filter_fn: Optional[Callable] = None
    max_batch_size: int = 100
    timeout: Optional[float] = None


@dataclass
class DataFanoutConfig:
    """Configuration for data fanout."""
    mode: FanoutMode = FanoutMode.BROADCAST
    max_concurrent: int = 10
    batch_mode: bool = True
    batch_size: int = 50
    batch_timeout: float = 1.0
    stop_on_target_error: bool = False
    collect_results: bool = True
    result_timeout: float = 30.0


class FanoutRouter:
    """Route data to specific targets based on rules."""
    
    def __init__(self):
        self._rules: List[Tuple[Callable, List[str]]] = []
        self._default_targets: List[str] = []
        self._lock = threading.RLock()
    
    def add_rule(self, condition: Callable, target_names: List[str]):
        """Add routing rule."""
        with self._lock:
            self._rules.append((condition, target_names))
    
    def set_default(self, target_names: List[str]):
        """Set default target(s)."""
        with self._lock:
            self._default_targets = target_names
    
    def route(self, data: Any) -> List[str]:
        """Route data and return target names."""
        with self._lock:
            for condition, targets in self._rules:
                try:
                    if condition(data):
                        return targets
                except Exception:
                    continue
            return list(self._default_targets)


class DataFanout:
    """Fanout data to multiple targets."""
    
    def __init__(self, name: str, config: Optional[DataFanoutConfig] = None):
        self.name = name
        self.config = config or DataFanoutConfig()
        self._targets: Dict[str, FanoutTarget] = {}
        self._router = FanoutRouter()
        self._batch_buffers: Dict[str, List[Any]] = defaultdict(list)
        self._batch_timers: Dict[str, Any] = {}
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        self._stats = {"total_items": 0, "total_deliveries": 0, "failed_deliveries": 0, "skipped_deliveries": 0}
    
    def register_target(self, target: FanoutTarget):
        """Register a fanout target."""
        with self._lock:
            self._targets[target.name] = target
    
    def unregister_target(self, name: str):
        """Unregister a target."""
        with self._lock:
            self._targets.pop(name, None)
    
    def add_rule(self, condition: Callable, target_names: List[str]):
        """Add routing rule."""
        self._router.add_rule(condition, target_names)
    
    def set_default_targets(self, target_names: List[str]):
        """Set default targets."""
        self._router.set_default(target_names)
    
    def _get_targets_for_data(self, data: Any) -> List[str]:
        """Get target names for given data."""
        if self.config.mode == FanoutMode.BROADCAST:
            return [name for name, t in self._targets.items() if t.enabled]
        
        if self.config.mode == FanoutMode.SELECTIVE:
            return self._router.route(data)
        
        if self.config.mode == FanoutMode.ROUND_ROBIN:
            enabled = [name for name, t in self._targets.items() if t.enabled]
            if not enabled:
                return []
            idx = self._round_robin_index[self.name] % len(enabled)
            self._round_robin_index[self.name] += 1
            return [enabled[idx]]
        
        if self.config.mode == FanoutMode.HASH_BASED:
            enabled = [name for name, t in self._targets.items() if t.enabled]
            if not enabled:
                return []
            data_hash = hash(data) % len(enabled)
            return [enabled[data_hash]]
        
        if self.config.mode == FanoutMode.PRIORITY:
            enabled = [(t.priority, name) for name, t in self._targets.items() if t.enabled]
            enabled.sort(key=lambda x: -x[0])
            return [name for _, name in enabled]
        
        return [name for name, t in self._targets.items() if t.enabled]
    
    def _should_deliver(self, target: FanoutTarget, data: Any) -> bool:
        """Check if data should be delivered to target."""
        if not target.enabled:
            return False
        if target.filter_fn:
            try:
                return target.filter_fn(data)
            except Exception:
                return False
        return True
    
    def _deliver_to_target(self, target: FanoutTarget, data: Any) -> Tuple[bool, Any]:
        """Deliver data to single target."""
        try:
            if target.timeout:
                result = [None]
                error = [None]
                
                def worker():
                    try:
                        result[0] = target.handler(data)
                    except Exception as e:
                        error[0] = e
                
                t = threading.Thread(target=worker)
                t.daemon = True
                t.start()
                t.join(timeout=target.timeout)
                
                if t.is_alive():
                    return False, TimeoutError(f"Target {target.name} timed out")
                if error[0]:
                    return False, error[0]
                return True, result[0]
            else:
                result = target.handler(data)
                return True, result
        except Exception as e:
            return False, e
    
    def send(self, data: Any) -> Dict[str, Tuple[bool, Any]]:
        """Send data to appropriate targets."""
        with self._lock:
            self._stats["total_items"] += 1
            target_names = self._get_targets_for_data(data)
        
        if not target_names:
            return {}
        
        results = {}
        
        if self.config.batch_mode:
            for target_name in target_names:
                with self._lock:
                    self._batch_buffers[target_name].append(data)
                    buffer = list(self._batch_buffers[target_name])
                
                if len(buffer) >= self.config.batch_size:
                    self._flush_target(target_name)
            
            return results
        
        for target_name in target_names:
            with self._lock:
                target = self._targets.get(target_name)
            
            if not target or not self._should_deliver(target, data):
                with self._lock:
                    self._stats["skipped_deliveries"] += 1
                continue
            
            success, result = self._deliver_to_target(target, data)
            
            with self._lock:
                if success:
                    self._stats["total_deliveries"] += 1
                else:
                    self._stats["failed_deliveries"] += 1
            
            results[target_name] = (success, result)
        
        return results
    
    def _flush_target(self, target_name: str):
        """Flush batch buffer for target."""
        with self._lock:
            buffer = self._batch_buffers.pop(target_name, [])
            timer = self._batch_timers.pop(target_name, None)
            if timer:
                timer.cancel()
        
        if not buffer:
            return
        
        target = self._targets.get(target_name)
        if not target:
            return
        
        try:
            target.handler(buffer)
            with self._lock:
                self._stats["total_deliveries"] += len(buffer)
        except Exception as e:
            with self._lock:
                self._stats["failed_deliveries"] += len(buffer)
    
    def flush_all(self):
        """Flush all batch buffers."""
        with self._lock:
            target_names = list(self._batch_buffers.keys())
        
        for target_name in target_names:
            self._flush_target(target_name)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fanout statistics."""
        with self._lock:
            return {
                "name": self.name,
                "mode": self.config.mode.value,
                "target_count": len(self._targets),
                **{k: v for k, v in self._stats.items()},
            }


class DataFanoutAction(BaseAction):
    """Data fanout action."""
    action_type = "data_fanout"
    display_name = "数据分发"
    description = "数据向多目标分发"
    
    def __init__(self):
        super().__init__()
        self._fanouts: Dict[str, DataFanout] = {}
        self._lock = threading.Lock()
    
    def _get_fanout(self, name: str, config: Optional[DataFanoutConfig] = None) -> DataFanout:
        """Get or create fanout."""
        with self._lock:
            if name not in self._fanouts:
                self._fanouts[name] = DataFanout(name, config)
            return self._fanouts[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute fanout operation."""
        try:
            fanout_name = params.get("fanout", "default")
            command = params.get("command", "send")
            data = params.get("data")
            
            config = DataFanoutConfig(
                mode=FanoutMode[params.get("mode", "broadcast").upper()],
                max_concurrent=params.get("max_concurrent", 10),
                batch_mode=params.get("batch_mode", True),
                batch_size=params.get("batch_size", 50),
            )
            
            fanout = self._get_fanout(fanout_name, config)
            
            if command == "send" and data is not None:
                results = fanout.send(data)
                return ActionResult(success=True, data={"results": results, "count": len(results)})
            
            elif command == "register":
                target_name = params.get("target_name")
                handler = params.get("handler")
                
                if target_name and handler:
                    target = FanoutTarget(name=target_name, handler=handler)
                    fanout.register_target(target)
                    return ActionResult(success=True, message=f"Target {target_name} registered")
                return ActionResult(success=False, message="target_name and handler required")
            
            elif command == "flush":
                fanout.flush_all()
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = fanout.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataFanoutAction error: {str(e)}")
