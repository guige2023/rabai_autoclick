"""Extended itertools operations module for RabAI AutoClick.

Provides additional itertools operations:
- ItertoolsCombinationsAction: Generate combinations
- ItertoolsCombinationsWithReplacementAction: Combinations with replacement
- ItertoolsPermutationsAction: Generate permutations
- ItertoolsProductAction: Cartesian product
- ItertoolsAccumulateAction: Cumulative accumulation
- ItertoolsCompressAction: Filter by mask
- ItertoolsFilterfalseAction: Filterfalse iterator
- ItertoolsGroupbyAction: Group consecutive items
"""

from typing import Any, Callable, Dict, Iterator, List

import itertools as it

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ItertoolsCombinationsAction(BaseAction):
    """Generate combinations of items."""
    action_type = "itertools_combinations"
    display_name = "组合生成"
    description = "生成元素的所有r元组合"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute combinations.

        Args:
            context: Execution context.
            params: Dict with iterable, r, output_var.

        Returns:
            ActionResult with list of combinations.
        """
        iterable = params.get('iterable', [])
        r = params.get('r', 2)
        output_var = params.get('output_var', 'combinations')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)
            resolved_r = context.resolve_value(r)

            if resolved_r < 0:
                return ActionResult(success=False, message="r必须非负")

            if resolved_r > len(resolved_iter):
                return ActionResult(success=False, message="r不能大于iterable长度")

            result = [tuple(comb) for comb in it.combinations(resolved_iter, resolved_r)]
            context.set(output_var, result)

            count = len(result)
            return ActionResult(
                success=True,
                message=f"组合生成完成: {count}个组合",
                data={
                    'combinations': result,
                    'count': count,
                    'r': resolved_r,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"组合生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable', 'r']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'combinations'}


class ItertoolsCombinationsWithReplacementAction(BaseAction):
    """Generate combinations with replacement."""
    action_type = "itertools_combinations_with_replacement"
    display_name = "带替换组合"
    description = "生成带替换的r元组合"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute combinations with replacement.

        Args:
            context: Execution context.
            params: Dict with iterable, r, output_var.

        Returns:
            ActionResult with list of combinations.
        """
        iterable = params.get('iterable', [])
        r = params.get('r', 2)
        output_var = params.get('output_var', 'combinations_wr')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)
            resolved_r = context.resolve_value(r)

            if resolved_r < 0:
                return ActionResult(success=False, message="r必须非负")

            result = [tuple(comb) for comb in it.combinations_with_replacement(resolved_iter, resolved_r)]
            context.set(output_var, result)

            count = len(result)
            return ActionResult(
                success=True,
                message=f"带替换组合生成完成: {count}个组合",
                data={
                    'combinations': result,
                    'count': count,
                    'r': resolved_r,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"带替换组合生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable', 'r']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'combinations_wr'}


class ItertoolsPermutationsAction(BaseAction):
    """Generate permutations of items."""
    action_type = "itertools_permutations"
    display_name = "排列生成"
    description = "生成元素的全排列"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute permutations.

        Args:
            context: Execution context.
            params: Dict with iterable, r, output_var.

        Returns:
            ActionResult with list of permutations.
        """
        iterable = params.get('iterable', [])
        r = params.get('r', None)
        output_var = params.get('output_var', 'permutations')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)
            resolved_r = context.resolve_value(r) if r is not None else None

            if resolved_r is not None and resolved_r < 0:
                return ActionResult(success=False, message="r必须非负")

            if resolved_r is not None and resolved_r > len(resolved_iter):
                return ActionResult(success=False, message="r不能大于iterable长度")

            result = [tuple(perm) for perm in it.permutations(resolved_iter, resolved_r)]
            context.set(output_var, result)

            count = len(result)
            return ActionResult(
                success=True,
                message=f"排列生成完成: {count}个排列",
                data={
                    'permutations': result,
                    'count': count,
                    'r': resolved_r,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"排列生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'r': None, 'output_var': 'permutations'}


class ItertoolsProductAction(BaseAction):
    """Generate Cartesian product."""
    action_type = "itertools_product"
    display_name = "笛卡尔积"
    description = "生成多个迭代器的笛卡尔积"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute product.

        Args:
            context: Execution context.
            params: Dict with iterables, repeat, output_var.

        Returns:
            ActionResult with list of product tuples.
        """
        iterables = params.get('iterables', [])
        repeat = params.get('repeat', 1)
        output_var = params.get('output_var', 'product')

        valid, msg = self.validate_type(iterables, list, 'iterables')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iters = context.resolve_value(iterables)
            resolved_repeat = context.resolve_value(repeat)

            if not resolved_iters:
                return ActionResult(success=False, message="iterables不能为空")

            if resolved_repeat < 0:
                return ActionResult(success=False, message="repeat必须非负")

            result = [tuple(p) for p in it.product(*resolved_iters, repeat=resolved_repeat)]
            context.set(output_var, result)

            count = len(result)
            return ActionResult(
                success=True,
                message=f"笛卡尔积生成完成: {count}个元组",
                data={
                    'product': result,
                    'count': count,
                    'repeat': resolved_repeat,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"笛卡尔积生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterables']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'repeat': 1, 'output_var': 'product'}


class ItertoolsAccumulateAction(BaseAction):
    """Cumulative accumulation."""
    action_type = "itertools_accumulate"
    display_name = "累积累加"
    description = "累积计算迭代器元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute accumulate.

        Args:
            context: Execution context.
            params: Dict with iterable, func, output_var.

        Returns:
            ActionResult with accumulated values.
        """
        iterable = params.get('iterable', [])
        func_name = params.get('func', 'add')
        output_var = params.get('output_var', 'accumulated')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)

            if func_name == 'add':
                func = lambda x, y: x + y
            elif func_name == 'mul':
                func = lambda x, y: x * y
            elif func_name == 'max':
                func = lambda x, y: max(x, y)
            elif func_name == 'min':
                func = lambda x, y: min(x, y)
            elif func_name == 'sub':
                func = lambda x, y: x - y
            else:
                return ActionResult(success=False, message=f"不支持的函数: {func_name}")

            result = list(it.accumulate(resolved_iter, func))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"累积完成: {result}",
                data={
                    'accumulated': result,
                    'func': func_name,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"累积计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'func': 'add', 'output_var': 'accumulated'}


class ItertoolsCompressAction(BaseAction):
    """Filter iterable by mask."""
    action_type = "itertools_compress"
    display_name = "压缩过滤"
    description = "根据掩码过滤元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compress.

        Args:
            context: Execution context.
            params: Dict with iterable, selectors, output_var.

        Returns:
            ActionResult with filtered values.
        """
        iterable = params.get('iterable', [])
        selectors = params.get('selectors', [])
        output_var = params.get('output_var', 'compressed')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(selectors, (list, tuple), 'selectors')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)
            resolved_selectors = context.resolve_value(selectors)

            if len(resolved_iter) != len(resolved_selectors):
                return ActionResult(
                    success=False,
                    message=f"iterable和selectors长度必须相同: {len(resolved_iter)} vs {len(resolved_selectors)}"
                )

            result = list(it.compress(resolved_iter, resolved_selectors))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"压缩过滤完成: {len(result)}个元素",
                data={
                    'compressed': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压缩过滤失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable', 'selectors']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compressed'}


class ItertoolsFilterfalseAction(BaseAction):
    """Filter items where predicate is False."""
    action_type = "itertools_filterfalse"
    display_name = "反向过滤"
    description = "过滤出函数返回False的元素"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filterfalse.

        Args:
            context: Execution context.
            params: Dict with iterable, predicate_str, output_var.

        Returns:
            ActionResult with filtered values.
        """
        iterable = params.get('iterable', [])
        predicate_str = params.get('predicate', 'x > 5')
        output_var = params.get('output_var', 'filtered_false')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)

            # Build simple predicate
            if predicate_str == 'None' or predicate_str is None:
                pred = None
            else:
                # Simple lambda builder
                pred = eval(f"lambda x: {predicate_str}")

            if pred is None:
                # Filter out None values
                result = list(it.filterfalse(lambda x: x is None, resolved_iter))
            else:
                result = list(it.filterfalse(pred, resolved_iter))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反向过滤完成: {len(result)}个元素",
                data={
                    'filtered': result,
                    'predicate': predicate_str,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反向过滤失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'predicate': 'x > 5', 'output_var': 'filtered_false'}


class ItertoolsGroupbyAction(BaseAction):
    """Group consecutive items by key."""
    action_type = "itertools_groupby"
    display_name = "分组"
    description = "对连续相同键的元素分组"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute groupby.

        Args:
            context: Execution context.
            params: Dict with iterable, key_str, output_var.

        Returns:
            ActionResult with grouped results.
        """
        iterable = params.get('iterable', [])
        key_str = params.get('key', 'None')
        output_var = params.get('output_var', 'grouped')

        valid, msg = self.validate_type(iterable, (list, tuple), 'iterable')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_iter = context.resolve_value(iterable)

            # Build key function
            if key_str == 'None' or key_str is None:
                key_func = None
            else:
                key_func = eval(f"lambda x: {key_str}")

            result = []
            for key, group in it.groupby(resolved_iter, key_func):
                result.append({
                    'key': key,
                    'items': list(group)
                })

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分组完成: {len(result)}组",
                data={
                    'groups': result,
                    'group_count': len(result),
                    'key_func': key_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iterable']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': 'None', 'output_var': 'grouped'}
