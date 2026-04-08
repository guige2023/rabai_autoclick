"""Find utilities action module for RabAI AutoClick.

Provides search and find operations for lists,
strings, and files with pattern matching.
"""

import sys
import os
import fnmatch
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FindInListAction(BaseAction):
    """Find items in list matching criteria.
    
    Supports value match, regex, and lambda conditions.
    """
    action_type = "find_in_list"
    display_name = "列表查找"
    description = "在列表中查找匹配项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Find in list.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, value, field,
                   mode, first_only, save_to_var.
        
        Returns:
            ActionResult with found items.
        """
        items = params.get('items', [])
        value = params.get('value', None)
        field = params.get('field', None)
        mode = params.get('mode', 'exact')  # exact, contains, startswith, endswith, regex
        first_only = params.get('first_only', False)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        found = []
        for i, item in enumerate(items):
            if field:
                if isinstance(item, dict):
                    item_value = item.get(field)
                else:
                    item_value = getattr(item, field, None)
            else:
                item_value = item

            match = False
            if mode == 'exact':
                match = (item_value == value)
            elif mode == 'contains':
                match = (str(value) in str(item_value))
            elif mode == 'startswith':
                match = str(item_value).startswith(str(value))
            elif mode == 'endswith':
                match = str(item_value).endswith(str(value))

            if match:
                if first_only:
                    found = [{'index': i, 'item': item}]
                    break
                found.append({'index': i, 'item': item})

        result_data = {
            'found': found,
            'count': len(found),
            'mode': mode
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if found:
            return ActionResult(
                success=True,
                message=f"找到 {len(found)} 个匹配项",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message="未找到匹配项",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'mode': 'exact',
            'first_only': False,
            'save_to_var': None
        }


class FindInStringAction(BaseAction):
    """Find substrings in string.
    
    Supports regex patterns and position extraction.
    """
    action_type = "find_in_string"
    display_name = "字符串查找"
    description = "在字符串中查找子串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Find in string.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, pattern, regex,
                   all_matches, save_to_var.
        
        Returns:
            ActionResult with found positions.
        """
        text = params.get('text', '')
        pattern = params.get('pattern', '')
        regex = params.get('regex', False)
        all_matches = params.get('all_matches', True)
        save_to_var = params.get('save_to_var', None)

        if not text or not pattern:
            return ActionResult(success=False, message="Text and pattern required")

        if regex:
            import re
            try:
                if all_matches:
                    matches = list(re.finditer(pattern, text))
                    found = [{'match': m.group(), 'start': m.start(), 'end': m.end()} for m in matches]
                else:
                    m = re.search(pattern, text)
                    if m:
                        found = [{'match': m.group(), 'start': m.start(), 'end': m.end()}]
                    else:
                        found = []
            except Exception as e:
                return ActionResult(success=False, message=f"Regex error: {e}")
        else:
            if all_matches:
                found = []
                start = 0
                while True:
                    idx = text.find(pattern, start)
                    if idx == -1:
                        break
                    found.append({'match': pattern, 'start': idx, 'end': idx + len(pattern)})
                    start = idx + 1
            else:
                idx = text.find(pattern)
                if idx != -1:
                    found = [{'match': pattern, 'start': idx, 'end': idx + len(pattern)}]
                else:
                    found = []

        result_data = {
            'found': found,
            'count': len(found)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if found:
            return ActionResult(
                success=True,
                message=f"找到 {len(found)} 个匹配",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message="未找到匹配",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'regex': False,
            'all_matches': True,
            'save_to_var': None
        }
