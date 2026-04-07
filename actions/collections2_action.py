"""Extended collections operations module for RabAI AutoClick.

Provides additional collections operations:
- DequeCreateAction: Create a deque
- DequeAppendAction: Append to deque
- DequeExtendAction: Extend deque
- DequeRotateAction: Rotate deque
- NamedtupleCreateAction: Create namedtuple type
- ChainmapCreateAction: Create ChainMap
- ChainmapGetAction: Get from ChainMap
- DequeMaxlenAction: Create bounded deque
"""

from typing import Any, Dict, List

import collections
from collections import deque, namedtuple, ChainMap

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DequeCreateAction(BaseAction):
    """Create a new deque."""
    action_type = "deque_create"
    display_name = "创建双端队列"
    description = "创建新的双端队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deque creation.

        Args:
            context: Execution context.
            params: Dict with items, maxlen, output_var.

        Returns:
            ActionResult with created deque.
        """
        items = params.get('items', [])
        maxlen = params.get('maxlen', None)
        output_var = params.get('output_var', 'created_deque')

        try:
            resolved_items = context.resolve_value(items) if items else []
            resolved_maxlen = context.resolve_value(maxlen) if maxlen is not None else None

            if resolved_maxlen is not None and resolved_maxlen <= 0:
                return ActionResult(success=False, message="maxlen必须为正整数")

            result = deque(resolved_items, maxlen=resolved_maxlen)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"双端队列创建完成: {len(result)}个元素, maxlen={resolved_maxlen}",
                data={
                    'length': len(result),
                    'maxlen': resolved_maxlen,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双端队列创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': [], 'maxlen': None, 'output_var': 'created_deque'}


class DequeAppendAction(BaseAction):
    """Append to deque."""
    action_type = "deque_append"
    display_name = "双端队列追加"
    description = "向双端队列末尾追加元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deque append.

        Args:
            context: Execution context.
            params: Dict with deque_name, item, side, output_var.

        Returns:
            ActionResult with append result.
        """
        deque_name = params.get('deque_name', '')
        item = params.get('item', None)
        side = params.get('side', 'right')
        output_var = params.get('output_var', 'append_result')

        valid, msg = self.validate_type(deque_name, str, 'deque_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(deque_name)
            d = context.get(resolved_name)

            if not isinstance(d, deque):
                return ActionResult(success=False, message="指定的变量不是双端队列")

            resolved_item = context.resolve_value(item) if item is not None else None

            if side == 'left':
                d.appendleft(resolved_item)
            else:
                d.append(resolved_item)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追加成功: {side}侧, 共{len(d)}个元素",
                data={
                    'side': side,
                    'length': len(d),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双端队列追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['deque_name', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'side': 'right', 'output_var': 'append_result'}


class DequeExtendAction(BaseAction):
    """Extend deque."""
    action_type = "deque_extend"
    display_name = "双端队列扩展"
    description = "向双端队列扩展多个元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deque extend.

        Args:
            context: Execution context.
            params: Dict with deque_name, items, side, output_var.

        Returns:
            ActionResult with extend result.
        """
        deque_name = params.get('deque_name', '')
        items = params.get('items', [])
        side = params.get('side', 'right')
        output_var = params.get('output_var', 'extend_result')

        valid, msg = self.validate_type(deque_name, str, 'deque_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(deque_name)
            d = context.get(resolved_name)

            if not isinstance(d, deque):
                return ActionResult(success=False, message="指定的变量不是双端队列")

            resolved_items = context.resolve_value(items)

            if not isinstance(resolved_items, (list, tuple)):
                return ActionResult(success=False, message="items必须是列表或元组")

            if side == 'left':
                d.extendleft(resolved_items)
            else:
                d.extend(resolved_items)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"扩展成功: {side}侧, 新增{len(resolved_items)}个元素, 共{len(d)}个",
                data={
                    'side': side,
                    'added_count': len(resolved_items),
                    'total_length': len(d),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双端队列扩展失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['deque_name', 'items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'side': 'right', 'output_var': 'extend_result'}


class DequeRotateAction(BaseAction):
    """Rotate deque."""
    action_type = "deque_rotate"
    display_name = "双端队列旋转"
    description = "旋转双端队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deque rotate.

        Args:
            context: Execution context.
            params: Dict with deque_name, n, output_var.

        Returns:
            ActionResult with rotate result.
        """
        deque_name = params.get('deque_name', '')
        n = params.get('n', 1)
        output_var = params.get('output_var', 'rotate_result')

        valid, msg = self.validate_type(deque_name, str, 'deque_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(deque_name)
            d = context.get(resolved_name)

            if not isinstance(d, deque):
                return ActionResult(success=False, message="指定的变量不是双端队列")

            resolved_n = context.resolve_value(n)
            d.rotate(resolved_n)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"旋转完成: 旋转{resolved_n}步, 首元素为{list(d)[0]}",
                data={
                    'n': resolved_n,
                    'first_element': list(d)[0] if d else None,
                    'length': len(d),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双端队列旋转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['deque_name', 'n']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rotate_result'}


class NamedtupleCreateAction(BaseAction):
    """Create a namedtuple type."""
    action_type = "namedtuple_create"
    display_name = "创建命名元组"
    description = "创建命名元组类型"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute namedtuple creation.

        Args:
            context: Execution context.
            params: Dict with typename, field_names, output_var.

        Returns:
            ActionResult with created namedtuple type.
        """
        typename = params.get('typename', 'Point')
        field_names = params.get('field_names', ['x', 'y'])
        output_var = params.get('output_var', 'namedtuple_type')

        valid, msg = self.validate_type(typename, str, 'typename')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_typename = context.resolve_value(typename)
            resolved_fields = context.resolve_value(field_names)

            if isinstance(resolved_fields, str):
                resolved_fields = resolved_fields.replace(',', ' ').split()

            result = namedtuple(resolved_typename, resolved_fields)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"命名元组类型{resolved_typename}创建成功: {resolved_fields}",
                data={
                    'typename': resolved_typename,
                    'field_names': resolved_fields,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"命名元组创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['typename', 'field_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'namedtuple_type'}


class ChainmapCreateAction(BaseAction):
    """Create a ChainMap."""
    action_type = "chainmap_create"
    display_name = "创建链式映射"
    description = "创建多个字典的ChainMap"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ChainMap creation.

        Args:
            context: Execution context.
            params: Dict with maps, output_var.

        Returns:
            ActionResult with created ChainMap.
        """
        maps = params.get('maps', [])
        output_var = params.get('output_var', 'chainmap')

        valid, msg = self.validate_type(maps, list, 'maps')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_maps = context.resolve_value(maps)

            if not resolved_maps:
                return ActionResult(success=False, message="maps不能为空")

            for m in resolved_maps:
                if not isinstance(m, dict):
                    return ActionResult(success=False, message="所有maps必须是字典")

            result = ChainMap(*resolved_maps)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"ChainMap创建成功: {len(resolved_maps)}个映射",
                data={
                    'maps_count': len(resolved_maps),
                    'keys': list(result.keys()),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ChainMap创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['maps']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chainmap'}


class ChainmapGetAction(BaseAction):
    """Get value from ChainMap."""
    action_type = "chainmap_get"
    display_name = "链式映射取值"
    description = "从ChainMap获取值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ChainMap get.

        Args:
            context: Execution context.
            params: Dict with chainmap_name, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        chainmap_name = params.get('chainmap_name', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'chainmap_get_result')

        valid, msg = self.validate_type(chainmap_name, str, 'chainmap_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(chainmap_name)
            cm = context.get(resolved_name)

            if not isinstance(cm, ChainMap):
                return ActionResult(success=False, message="指定的变量不是ChainMap")

            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None

            if resolved_key in cm:
                result = cm[resolved_key]
                found = True
            else:
                result = resolved_default
                found = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取{'成功' if found else '失败(使用默认值)'}: {result}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'found': found,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ChainMap取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['chainmap_name', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'chainmap_get_result'}


class DequeMaxlenAction(BaseAction):
    """Create bounded deque with maxlen."""
    action_type = "deque_maxlen"
    display_name = "创建有界双端队列"
    description = "创建有最大长度限制的双端队列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bounded deque creation.

        Args:
            context: Execution context.
            params: Dict with items, maxlen, output_var.

        Returns:
            ActionResult with bounded deque.
        """
        items = params.get('items', [])
        maxlen = params.get('maxlen', 10)
        output_var = params.get('output_var', 'bounded_deque')

        try:
            resolved_items = context.resolve_value(items) if items else []
            resolved_maxlen = context.resolve_value(maxlen)

            if resolved_maxlen is None or resolved_maxlen <= 0:
                return ActionResult(success=False, message="maxlen必须是正整数")

            result = deque(resolved_items, maxlen=resolved_maxlen)
            context.set(output_var, result)

            dropped = max(0, len(resolved_items) - resolved_maxlen) if items else 0

            return ActionResult(
                success=True,
                message=f"有界双端队列创建: maxlen={resolved_maxlen}, 保留{len(result)}个元素, 丢弃{dropped}个",
                data={
                    'maxlen': resolved_maxlen,
                    'length': len(result),
                    'dropped': dropped,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"有界双端队列创建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['maxlen']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': [], 'output_var': 'bounded_deque'}
