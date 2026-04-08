"""Data aggregator action module for RabAI AutoClick.

Provides data aggregation capabilities including grouping,
summing, averaging, and statistical operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AggregationType(Enum):
    """Aggregation types."""
    pass


from enum import Enum


@dataclass
class Aggregation:
    """Single aggregation definition."""
    field: str
    function: str
    alias: Optional[str] = None


class DataAggregatorAction(BaseAction):
    """Data aggregator action for grouping and aggregating data.
    
    Supports GROUP BY aggregation with sum, count, avg, min, max,
    and custom aggregation functions.
    """
    action_type = "data_aggregator"
    display_name = "数据聚合"
    description = "数据分组聚合与统计"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data aggregation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: List of items to aggregate
                group_by: Field(s) to group by
                aggregations: List of aggregation definitions
                having: Having clause for filtering groups
                order_by: Field to order results by
                limit: Maximum number of results.
        
        Returns:
            ActionResult with aggregated results.
        """
        data = params.get('data', [])
        group_by = params.get('group_by')
        aggregations = params.get('aggregations', [])
        having = params.get('having')
        order_by = params.get('order_by')
        limit = params.get('limit')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="Data must be a list")
        
        if not group_by:
            return self._aggregate_all(data, aggregations)
        
        return self._aggregate_grouped(
            data, group_by, aggregations, having, order_by, limit
        )
    
    def _aggregate_all(
        self,
        data: List[Any],
        aggregations: List[Dict[str, str]]
    ) -> ActionResult:
        """Aggregate entire dataset without grouping."""
        if not aggregations:
            return ActionResult(
                success=True,
                message="No aggregations specified",
                data={'items': data, 'count': len(data)}
            )
        
        result = {}
        
        for agg in aggregations:
            field = agg['field']
            func = agg.get('function', 'count')
            alias = agg.get('alias', f"{func}_{field}")
            
            values = [item.get(field) for item in data if item.get(field) is not None]
            
            if func == 'count':
                result[alias] = len(values) if values else 0
            elif func == 'sum':
                result[alias] = sum(values) if values else 0
            elif func == 'avg':
                result[alias] = sum(values) / len(values) if values else None
            elif func == 'min':
                result[alias] = min(values) if values else None
            elif func == 'max':
                result[alias] = max(values) if values else None
            elif func == 'first':
                result[alias] = values[0] if values else None
            elif func == 'last':
                result[alias] = values[-1] if values else None
            elif func == 'array':
                result[alias] = values
        
        return ActionResult(
            success=True,
            message="Aggregated all data",
            data={'result': result}
        )
    
    def _aggregate_grouped(
        self,
        data: List[Any],
        group_by: str,
        aggregations: List[Dict[str, str]],
        having: Optional[Dict],
        order_by: Optional[str],
        limit: Optional[int]
    ) -> ActionResult:
        """Aggregate data with grouping."""
        groups: Dict[Any, List[Any]] = defaultdict(list)
        
        group_fields = group_by.split(',') if isinstance(group_by, str) else group_by
        
        for item in data:
            if isinstance(item, dict):
                key = tuple(item.get(f.strip()) for f in group_fields)
                groups[key].append(item)
        
        results = []
        
        for key, group_items in groups.items():
            result = {}
            
            if len(group_fields) == 1:
                result[group_fields[0].strip()] = key[0]
            else:
                for i, field in enumerate(group_fields):
                    result[field.strip()] = key[i]
            
            for agg in aggregations:
                field = agg['field']
                func = agg.get('function', 'count')
                alias = agg.get('alias', f"{func}_{field}")
                
                values = [item.get(field) for item in group_items if item.get(field) is not None]
                
                if func == 'count':
                    result[alias] = len(group_items)
                elif func == 'count_distinct':
                    result[alias] = len(set(values))
                elif func == 'sum':
                    result[alias] = sum(values) if values else 0
                elif func == 'avg':
                    result[alias] = round(sum(values) / len(values), 2) if values else None
                elif func == 'min':
                    result[alias] = min(values) if values else None
                elif func == 'max':
                    result[alias] = max(values) if values else None
                elif func == 'first':
                    result[alias] = values[0] if values else None
                elif func == 'last':
                    result[alias] = values[-1] if values else None
                elif func == 'array':
                    result[alias] = values
                elif func == 'stddev':
                    if len(values) > 1:
                        import statistics
                        result[alias] = statistics.stdev(values)
                    else:
                        result[alias] = 0
                elif func == 'median':
                    if values:
                        import statistics
                        result[alias] = statistics.median(values)
                    else:
                        result[alias] = None
            
            results.append(result)
        
        if having:
            results = self._apply_having(results, having)
        
        if order_by:
            ascending = params.get('order_ascending', True)
            results = sorted(results, key=lambda x: x.get(order_by, 0), reverse=not ascending)
        
        if limit:
            results = results[:limit]
        
        return ActionResult(
            success=True,
            message=f"Aggregated into {len(results)} groups",
            data={
                'items': results,
                'group_count': len(results),
                'total_records': len(data)
            }
        )
    
    def _apply_having(self, results: List[Dict], having: Dict) -> List[Dict]:
        """Apply having clause to filter groups."""
        filtered = []
        
        for result in results:
            passes = True
            
            for field, condition in having.items():
                if isinstance(condition, dict):
                    op = condition.get('op', '==')
                    value = condition.get('value')
                    
                    if op == '>' and not result.get(field, 0) > value:
                        passes = False
                    elif op == '>=' and not result.get(field, 0) >= value:
                        passes = False
                    elif op == '<' and not result.get(field, 0) < value:
                        passes = False
                    elif op == '<=' and not result.get(field, 0) <= value:
                        passes = False
                    elif op == '!=' and not result.get(field) != value:
                        passes = False
                    elif op == '==' and not result.get(field) == value:
                        passes = False
                else:
                    if result.get(field) != condition:
                        passes = False
                
                if not passes:
                    break
            
            if passes:
                filtered.append(result)
        
        return filtered
