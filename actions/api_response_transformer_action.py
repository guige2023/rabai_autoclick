"""API Response Transformer action module for RabAI AutoClick.

Transforms, filters, and enriches API responses with
JMESPath expressions and custom transformations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiResponseTransformerAction(BaseAction):
    """Transform API responses with JMESPath or custom functions.

    Filters, maps, enriches, and reshapes API response
    data before passing to next step.
    """
    action_type = "api_response_transformer"
    display_name = "API响应转换器"
    description = "使用JMESPath转换和过滤API响应"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform API response.

        Args:
            context: Execution context.
            params: Dict with keys: response, transform_type,
                   jmespath_expr, map_fields, enrich_with,
                   filter_fn.

        Returns:
            ActionResult with transformed data.
        """
        start_time = time.time()
        try:
            response = params.get('response')
            transform_type = params.get('transform_type', 'jmespath')
            jmespath_expr = params.get('jmespath_expr', '')
            map_fields = params.get('map_fields', {})
            enrich_with = params.get('enrich_with', {})
            filter_fn = params.get('filter_fn')
            default_value = params.get('default_value', None)

            if response is None:
                return ActionResult(
                    success=False,
                    message="Response is required",
                    duration=time.time() - start_time,
                )

            data = response
            if isinstance(response, dict):
                if 'data' in response:
                    data = response['data']
                elif 'body' in response:
                    data = response['body']
                elif 'result' in response:
                    data = response['result']

            # Apply transform
            if transform_type == 'jmespath' and jmespath_expr:
                try:
                    import jmespath
                    data = jmespath.search(jmespath_expr, data)
                except Exception as e:
                    return ActionResult(
                        success=False,
                        message=f"JMESPath error: {str(e)}",
                        duration=time.time() - start_time,
                    )

            elif transform_type == 'map' and map_fields:
                data = self._map_fields(data, map_fields)

            elif transform_type == 'filter' and filter_fn:
                if callable(filter_fn):
                    if isinstance(data, list):
                        data = [item for item in data if filter_fn(item, context)]
                    elif isinstance(data, dict):
                        data = {k: v for k, v in data.items() if filter_fn({k: v}, context)}
                else:
                    return ActionResult(
                        success=False,
                        message="filter_fn must be callable",
                        duration=time.time() - start_time,
                    )

            # Enrich data
            if enrich_with:
                data = self._enrich(data, enrich_with, context)

            # Handle None result
            if data is None and default_value is not None:
                data = default_value

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Transformed response ({transform_type})",
                data=data,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Transform error: {str(e)}",
                duration=duration,
            )

    def _map_fields(self, data: Any, field_map: Dict[str, str]) -> Any:
        """Map/rename fields in data."""
        if isinstance(data, list):
            return [self._map_fields(item, field_map) for item in data]
        elif isinstance(data, dict):
            result = {}
            for old_key, new_key in field_map.items():
                if old_key in data:
                    result[new_key] = data[old_key]
            for key, value in data.items():
                if key not in field_map:
                    result[key] = value
            return result
        return data

    def _enrich(self, data: Any, enrich_config: Dict, context: Any) -> Any:
        """Enrich data with computed or context values."""
        if isinstance(data, list):
            return [self._enrich(item, enrich_config, context) for item in data]
        elif isinstance(data, dict):
            for key, value in enrich_config.items():
                if callable(value):
                    data[key] = value(data, context)
                else:
                    data[key] = value
            return data
        return data


class ApiResponseFilterAction(BaseAction):
    """Filter API response data based on conditions.

    Supports field selection, value filtering, and
    pagination trimming.
    """
    action_type = "api_response_filter"
    display_name = "API响应过滤器"
    description = "根据条件过滤API响应数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter response data.

        Args:
            context: Execution context.
            params: Dict with keys: response, fields (select list),
                   where (filter conditions), limit, offset.

        Returns:
            ActionResult with filtered data.
        """
        start_time = time.time()
        try:
            response = params.get('response')
            fields = params.get('fields', None)
            where = params.get('where', {})
            limit = params.get('limit', 0)
            offset = params.get('offset', 0)

            if response is None:
                return ActionResult(
                    success=False,
                    message="Response is required",
                    duration=time.time() - start_time,
                )

            # Extract data
            data = response
            if isinstance(response, dict):
                if 'data' in response:
                    data = response['data']
                elif 'items' in response:
                    data = response['items']
                elif 'results' in response:
                    data = response['results']

            if not isinstance(data, list):
                data = [data] if isinstance(data, dict) else data

            # Filter by fields
            if fields and isinstance(data, list) and data and isinstance(data[0], dict):
                data = [{k: item.get(k) for k in fields if k in item} for item in data]

            # Filter by conditions
            if where:
                data = self._apply_where(data, where)

            # Offset
            if offset > 0 and isinstance(data, list):
                data = data[offset:]

            # Limit
            if limit > 0 and isinstance(data, list):
                data = data[:limit]

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Filtered to {len(data) if isinstance(data, list) else 1} items",
                data=data,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Filter error: {str(e)}",
                duration=duration,
            )

    def _apply_where(self, data: Any, conditions: Dict) -> Any:
        """Apply WHERE conditions to filter data."""
        if not isinstance(data, list):
            return data

        def matches(item: Dict) -> bool:
            for key, condition in conditions.items():
                value = item.get(key)
                if isinstance(condition, dict):
                    op = condition.get('op', 'eq')
                    comp = condition.get('value')
                    if op == 'eq' and value != comp:
                        return False
                    elif op == 'ne' and value == comp:
                        return False
                    elif op == 'gt' and not (value > comp):
                        return False
                    elif op == 'gte' and not (value >= comp):
                        return False
                    elif op == 'lt' and not (value < comp):
                        return False
                    elif op == 'lte' and not (value <= comp):
                        return False
                    elif op == 'in' and value not in comp:
                        return False
                    elif op == 'contains' and comp not in str(value):
                        return False
                elif value != condition:
                    return False
            return True

        return [item for item in data if isinstance(item, dict) and matches(item)]


class ApiPaginationAction(BaseAction):
    """Handle API pagination (cursor, offset, page-based).

    Automatically fetches all pages and combines results.
    """
    action_type = "api_pagination"
    display_name = "API分页处理"
    description = "自动处理API分页并合并结果"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle paginated API requests.

        Args:
            context: Execution context.
            params: Dict with keys: base_url, page_param_type,
                   page_size, max_pages, headers, path_template.

        Returns:
            ActionResult with all pages combined.
        """
        start_time = time.time()
        try:
            base_url = params.get('base_url', '')
            page_param_type = params.get('page_param_type', 'offset')
            page_size = params.get('page_size', 100)
            max_pages = params.get('max_pages', 100)
            headers = params.get('headers', {})
            path_template = params.get('path_template', '/items')

            if not base_url:
                return ActionResult(
                    success=False,
                    message="base_url is required",
                    duration=time.time() - start_time,
                )

            all_items = []
            page = 0

            while page < max_pages:
                page += 1
                if page_param_type == 'offset':
                    url = f"{base_url}?offset={page * page_size}&limit={page_size}"
                elif page_param_type == 'page':
                    url = f"{base_url}?page={page}&page_size={page_size}"
                elif page_param_type == 'cursor':
                    url = base_url if page == 1 else cursor
                else:
                    url = f"{base_url}?page={page}"

                req = Request(url, headers=headers)
                with urlopen(req, timeout=30) as resp:
                    page_data = json.loads(resp.read())

                # Extract items from page
                items = []
                if isinstance(page_data, dict):
                    items = page_data.get('data', page_data.get('items', page_data.get('results', [])))
                    cursor = page_data.get('next_cursor', page_data.get('cursor'))
                    total = page_data.get('total', 0)
                else:
                    items = page_data if isinstance(page_data, list) else []

                all_items.extend(items)

                # Check if more pages
                if page_param_type == 'cursor' and not cursor:
                    break
                if isinstance(page_data, dict) and page_data.get('has_more') is False:
                    break
                if len(items) < page_size:
                    break

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Fetched {len(all_items)} items across {page} pages",
                data={
                    'items': all_items,
                    'total_pages': page,
                    'total_items': len(all_items),
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Pagination error: {str(e)}",
                duration=duration,
            )
