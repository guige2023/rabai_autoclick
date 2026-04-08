"""Data Broadcaster action module for RabAI AutoClick.

Broadcasts data to multiple sinks with fan-out patterns,
weighted routing, and delivery guarantees.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataBroadcasterAction(BaseAction):
    """Broadcast data to multiple destinations.

    Fan-out pattern with configurable routing rules,
    retry, and delivery tracking.
    """
    action_type = "data_broadcaster"
    display_name = "数据广播器"
    description = "向多个目标广播数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Broadcast data.

        Args:
            context: Execution context.
            params: Dict with keys: data, destinations, broadcast_mode,
                   delivery_guarantee, timeout.

        Returns:
            ActionResult with broadcast results.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            destinations = params.get('destinations', [])
            broadcast_mode = params.get('broadcast_mode', 'parallel')
            delivery_guarantee = params.get('delivery_guarantee', 'at_least_once')
            timeout = params.get('timeout', 30)
            retry_count = params.get('retry_count', 2)

            if not data:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            if not destinations:
                return ActionResult(
                    success=False,
                    message="At least one destination is required",
                    duration=time.time() - start_time,
                )

            results = []
            if broadcast_mode == 'parallel':
                results = self._broadcast_parallel(data, destinations, timeout, retry_count)
            else:
                results = self._broadcast_sequential(data, destinations, timeout, retry_count)

            success_count = sum(1 for r in results if r.get('success', False))
            all_success = success_count == len(destinations)

            duration = time.time() - start_time
            return ActionResult(
                success=all_success,
                message=f"Broadcast to {success_count}/{len(destinations)} destinations",
                data={
                    'total': len(destinations),
                    'successful': success_count,
                    'failed': len(destinations) - success_count,
                    'results': results,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Broadcast error: {str(e)}",
                duration=duration,
            )

    def _broadcast_parallel(
        self,
        data: Any,
        destinations: List[Dict],
        timeout: int,
        retry_count: int
    ) -> List[Dict]:
        """Broadcast to destinations in parallel."""
        results = []
        with ThreadPoolExecutor(max_workers=min(len(destinations), 10)) as executor:
            futures = {
                executor.submit(self._send_to_destination, data, dest, timeout, retry_count): dest
                for dest in destinations
            }
            for future in as_completed(futures):
                results.append(future.result())
        return results

    def _broadcast_sequential(
        self,
        data: Any,
        destinations: List[Dict],
        timeout: int,
        retry_count: int
    ) -> List[Dict]:
        """Broadcast to destinations sequentially."""
        results = []
        for dest in destinations:
            results.append(self._send_to_destination(data, dest, timeout, retry_count))
        return results

    def _send_to_destination(
        self,
        data: Any,
        destination: Dict,
        timeout: int,
        retry_count: int
    ) -> Dict:
        """Send data to a single destination."""
        dest_type = destination.get('type', 'api')
        dest_url = destination.get('url', '')
        dest_headers = destination.get('headers', {})
        dest_name = destination.get('name', dest_url)

        for attempt in range(retry_count + 1):
            try:
                if dest_type == 'api':
                    from urllib.request import Request, urlopen
                    body = json.dumps(data).encode('utf-8') if isinstance(data, (dict, list)) else str(data).encode('utf-8')
                    headers = {**dest_headers}
                    if isinstance(data, (dict, list)):
                        headers.setdefault('Content-Type', 'application/json')
                    req = Request(dest_url, data=body, headers=headers, method='POST')
                    with urlopen(req, timeout=timeout) as resp:
                        return {
                            'destination': dest_name,
                            'success': True,
                            'status': resp.status,
                            'attempt': attempt + 1,
                        }
                elif dest_type == 'file':
                    path = destination.get('path', '/tmp/broadcast.json')
                    mode = destination.get('mode', 'a')
                    with open(path, mode) as f:
                        if isinstance(data, (dict, list)):
                            json.dump(data, f)
                        else:
                            f.write(str(data))
                    return {'destination': dest_name, 'success': True, 'attempt': attempt + 1}
                else:
                    return {'destination': dest_name, 'success': False, 'error': f'Unknown type: {dest_type}', 'attempt': attempt + 1}
            except Exception as e:
                if attempt == retry_count:
                    return {'destination': dest_name, 'success': False, 'error': str(e), 'attempt': attempt + 1}
        return {'destination': dest_name, 'success': False, 'error': 'Max retries exceeded'}


class DataRouterAction(BaseAction):
    """Route data to different sinks based on rules.

    Content-based routing with multiple conditions
    and fallback destinations.
    """
    action_type = "data_router"
    display_name = "数据路由器"
    description = "基于规则路由数据到不同目标"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route data.

        Args:
            context: Execution context.
            params: Dict with keys: data, rules, default_destination.

        Returns:
            ActionResult with routing result.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            rules = params.get('rules', [])
            default_destination = params.get('default_destination')

            if not data:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            matched_rule = None
            for rule in rules:
                condition = rule.get('condition')
                if self._evaluate_condition(condition, data):
                    matched_rule = rule
                    break

            destination = matched_rule.get('destination') if matched_rule else default_destination

            if not destination:
                return ActionResult(
                    success=False,
                    message="No matching rule and no default destination",
                    duration=time.time() - start_time,
                )

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Routed to {destination.get('name', 'destination')}",
                data={
                    'destination': destination,
                    'matched_rule': matched_rule.get('name') if matched_rule else None,
                    'routed_data': data,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Router error: {str(e)}",
                duration=duration,
            )

    def _evaluate_condition(self, condition: Any, data: Any) -> bool:
        """Evaluate routing condition."""
        if condition is None:
            return True
        if callable(condition):
            return condition(data)
        if isinstance(condition, dict) and isinstance(data, dict):
            for key, expected in condition.items():
                actual = data.get(key)
                if isinstance(expected, dict):
                    op = expected.get('op', 'eq')
                    value = expected.get('value')
                    if op == 'eq' and actual != value:
                        return False
                    elif op == 'ne' and actual == value:
                        return False
                    elif op == 'gt' and not (actual > value):
                        return False
                    elif op == 'lt' and not (actual < value):
                        return False
                    elif op == 'in' and actual not in value:
                        return False
                    elif op == 'contains' and value not in str(actual):
                        return False
                elif actual != expected:
                    return False
            return True
        return False
