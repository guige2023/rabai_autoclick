"""API Load Balancer action module for RabAI AutoClick.

Provides load balancing operations:
- LBRoundRobinAction: Round-robin balancing
- LBWeightedAction: Weighted balancing
- LBConsistentHashAction: Consistent hash balancing
- LBHealthAction: Health-based balancing
"""

from __future__ import annotations

import sys
import os
import hashlib
from typing import Any, Dict, List, Optional
from collections import defaultdict

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LBRoundRobinAction(BaseAction):
    """Round-robin balancing."""
    action_type = "lb_round_robin"
    display_name = "轮询负载"
    description = "轮询负载均衡"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._counters = defaultdict(int)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute round-robin selection."""
        servers = params.get('servers', [])
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'selected_server')

        if not servers:
            return ActionResult(success=False, message="servers list is required")

        try:
            resolved_servers = context.resolve_value(servers) if context else servers

            counter = self._counters[key]
            selected = resolved_servers[counter % len(resolved_servers)]
            self._counters[key] = counter + 1

            result = {
                'selected': selected,
                'index': counter % len(resolved_servers),
                'total_servers': len(resolved_servers),
                'strategy': 'round_robin',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Selected server {counter % len(resolved_servers) + 1}/{len(resolved_servers)}: {selected}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Round-robin error: {e}")


class LBWeightedAction(BaseAction):
    """Weighted balancing."""
    action_type = "lb_weighted"
    display_name = "加权负载"
    description = "加权负载均衡"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._weights = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute weighted selection."""
        servers = params.get('servers', [])
        weights = params.get('weights', [])
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'selected_server')

        if not servers or not weights:
            return ActionResult(success=False, message="servers and weights are required")

        try:
            resolved_servers = context.resolve_value(servers) if context else servers
            resolved_weights = context.resolve_value(weights) if context else weights

            expanded = []
            for server, weight in zip(resolved_servers, resolved_weights):
                expanded.extend([server] * weight)

            import random
            selected = random.choice(expanded)

            result = {
                'selected': selected,
                'weights': dict(zip(resolved_servers, resolved_weights)),
                'strategy': 'weighted',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Selected (weighted): {selected}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Weighted balancing error: {e}")


class LBConsistentHashAction(BaseAction):
    """Consistent hash balancing."""
    action_type = "lb_consistent_hash"
    display_name = "一致性哈希负载"
    description = "一致性哈希负载均衡"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._ring = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute consistent hash selection."""
        servers = params.get('servers', [])
        request_key = params.get('request_key', '')
        output_var = params.get('output_var', 'selected_server')

        if not servers or not request_key:
            return ActionResult(success=False, message="servers and request_key are required")

        try:
            resolved_servers = context.resolve_value(servers) if context else servers
            resolved_key = context.resolve_value(request_key) if context else request_key

            hash_value = int(hashlib.md5(resolved_key.encode()).hexdigest(), 16)

            positions = sorted([int(hashlib.md5(s.encode()).hexdigest(), 16) for s in resolved_servers])

            selected_idx = 0
            for pos in positions:
                if hash_value <= pos:
                    selected_idx = positions.index(pos)
                    break

            selected = resolved_servers[selected_idx]

            result = {
                'selected': selected,
                'request_key': resolved_key,
                'hash': hash_value,
                'strategy': 'consistent_hash',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Consistent hash selected: {selected}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Consistent hash error: {e}")


class LBHealthAction(BaseAction):
    """Health-based balancing."""
    action_type = "lb_health"
    display_name = "健康负载"
    description = "健康检测负载均衡"
    version = "1.0"

    def __init__(self):
        super().__init__()
        self._health = defaultdict(lambda: {'healthy': True, 'latency': 0, 'failures': 0})

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute health-based selection."""
        servers = params.get('servers', [])
        key = params.get('key', 'default')
        output_var = params.get('output_var', 'selected_server')

        if not servers:
            return ActionResult(success=False, message="servers list is required")

        try:
            resolved_servers = context.resolve_value(servers) if context else servers

            healthy = [s for s in resolved_servers if self._health[s]['healthy']]

            if not healthy:
                healthy = resolved_servers

            import random
            selected = random.choice(healthy)

            result = {
                'selected': selected,
                'healthy_count': len(healthy),
                'total_count': len(resolved_servers),
                'strategy': 'health_based',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Health-based selected: {selected} ({len(healthy)}/{len(resolved_servers)} healthy)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health balancing error: {e}")
