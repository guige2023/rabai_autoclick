"""Itertools action module for RabAI AutoClick.

Provides itertools extensions and utilities:
- ChunkFunction: Split iterable into chunks of fixed size
- WindowFunction: Sliding window over iterable
- FlattenFunction: Flatten nested iterables
- UniqueFunction: Get unique elements preserving order
- UniqueByFunction: Unique by key function
- GrouperFunction: Group elements in chunks
- RoundrobinFunction: Round-robin from iterables
- PartitionFunction: Partition by predicate
- TakeFunction: Take n elements
- DropFunction: Drop n elements
- PowersetFunction: All subsets of iterable
- PairwiseFunction: Consecutive pairs
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Union, Generic
import sys
import itertools as it

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

T = TypeVar('T')
K = TypeVar('K')


def chunk(iterable: Iterator, size: int) -> Iterator[List]:
    """Split iterable into chunks of fixed size.
    
    Args:
        iterable: Source iterable.
        size: Chunk size (must be positive).
    
    Yields:
        Lists of up to size elements.
    """
    if size < 1:
        raise ValueError(f"chunk size must be positive, got {size}")
    it_iter = iter(iterable)
    while True:
        batch = list(it.islice(it_iter, size))
        if not batch:
            break
        yield batch


def window(iterable: Iterator, n: int = 2) -> Iterator[tuple]:
    """Sliding window over iterable.
    
    Args:
        iterable: Source iterable.
        n: Window size (default 2).
    
    Yields:
        Tuples of n consecutive elements.
    """
    if n < 1:
        raise ValueError(f"window size must be positive, got {n}")
    it_iter = iter(iterable)
    win = list(it.islice(it_iter, n))
    if len(win) < n:
        return
    yield tuple(win)
    for item in it_iter:
        win = win[1:] + [item]
        yield tuple(win)


def flatten(nested: Any, depth: Optional[int] = None) -> Iterator:
    """Flatten nested iterables.
    
    Args:
        nested: Nested iterable to flatten.
        depth: Max depth to flatten (None = unlimited).
    
    Yields:
        Individual elements.
    """
    for item in nested:
        if depth is None or depth > 0:
            if isinstance(item, (list, tuple, set)):
                yield from flatten(item, None if depth is None else depth - 1)
            else:
                yield item
        else:
            yield item


def unique(iterable: Iterator) -> Iterator:
    """Get unique elements preserving order.
    
    Args:
        iterable: Source iterable.
    
    Yields:
        Elements in order of first appearance.
    """
    seen = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item


def unique_by(iterable: Iterator, key: Callable[[Any], K]) -> Iterator:
    """Get unique elements by key function.
    
    Args:
        iterable: Source iterable.
        key: Function to extract comparison key.
    
    Yields:
        First element for each unique key.
    """
    seen = set()
    for item in iterable:
        k = key(item)
        if k not in seen:
            seen.add(k)
            yield item


def grouper(iterable: Iterator, n: int, fillvalue: Any = None) -> Iterator[tuple]:
    """Group elements in chunks with fill value.
    
    Args:
        iterable: Source iterable.
        n: Chunk size.
        fillvalue: Value to fill incomplete last chunk.
    
    Yields:
        Tuples of n elements.
    """
    it_iter = iter(iterable)
    while True:
        batch = list(it.islice(it_iter, n))
        if not batch:
            break
        if len(batch) < n:
            batch.extend([fillvalue] * (n - len(batch)))
        yield tuple(batch)


def roundrobin(*iterables: Iterator) -> Iterator:
    """Round-robin from multiple iterables.
    
    Args:
        *iterables: Multiple source iterables.
    
    Yields:
        Elements in round-robin fashion.
    """
    pending = len(iterables)
    nexts = iter(iterables)
    nexts_list = []
    for i in range(pending):
        try:
            nexts_list.append(next(nexts).__iter__())
        except StopIteration:
            pass
    pending = len(nexts_list)
    while pending:
        to_remove = []
        for i, nxt in enumerate(nexts_list):
            try:
                yield next(nxt)
            except StopIteration:
                to_remove.append(i)
        for i in reversed(to_remove):
            del nexts_list[i]
            pending -= 1


def partition(pred: Callable[[Any], bool], iterable: Iterator) -> tuple:
    """Partition iterable by predicate.
    
    Args:
        pred: Predicate function (True/False).
        iterable: Source iterable.
    
    Returns:
        Tuple of (elements passing, elements failing).
    """
    true_part = []
    false_part = []
    for item in iterable:
        if pred(item):
            true_part.append(item)
        else:
            false_part.append(item)
    return true_part, false_part


def take(n: int, iterable: Iterator) -> List:
    """Take first n elements.
    
    Args:
        n: Number of elements.
        iterable: Source iterable.
    
    Returns:
        List of first n elements.
    """
    return list(it.islice(iterable, n))


def drop(n: int, iterable: Iterator) -> Iterator:
    """Drop first n elements.
    
    Args:
        n: Number of elements to skip.
        iterable: Source iterable.
    
    Yields:
        Elements after dropping first n.
    """
    return it.islice(iterable, n, None)


def powerset(iterable: Iterator) -> Iterator[tuple]:
    """All subsets of iterable.
    
    Args:
        iterable: Source iterable.
    
    Yields:
        Tuples representing all subsets.
    """
    items = list(iterable)
    for r in range(len(items) + 1):
        yield from it.combinations(items, r)


def pairwise(iterable: Iterator) -> Iterator[tuple]:
    """Consecutive pairs.
    
    Args:
        iterable: Source iterable.
    
    Yields:
        Tuples of consecutive elements.
    """
    a, b = it.tee(iterable)
    next(b, None)
    return zip(a, b)


class ItertoolsChunkAction(BaseAction):
    """Split iterable into chunks of fixed size."""
    action_type = "itertools_chunk"
    display_name = "拆分块"
    description = "将可迭代对象拆分为固定大小的块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute chunk operation."""
        iterable = params.get('iterable', [])
        size = params.get('size', 10)
        output_var = params.get('output_var', 'chunk_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            resolved_size = context.resolve_value(size)
            result = list(chunk(iter(resolved_iterable), resolved_size))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"chunked into {len(result)} chunks")
        except Exception as e:
            return ActionResult(success=False, message=f"chunk failed: {e}")


class ItertoolsWindowAction(BaseAction):
    """Sliding window over iterable."""
    action_type = "itertools_window"
    display_name = "滑动窗口"
    description = "对可迭代对象执行滑动窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute window operation."""
        iterable = params.get('iterable', [])
        n = params.get('n', 2)
        output_var = params.get('output_var', 'window_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            resolved_n = context.resolve_value(n)
            result = list(window(iter(resolved_iterable), resolved_n))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"window size {n}: {len(result)} windows")
        except Exception as e:
            return ActionResult(success=False, message=f"window failed: {e}")


class ItertoolsFlattenAction(BaseAction):
    """Flatten nested iterables."""
    action_type = "itertools_flatten"
    display_name = "扁平化"
    description = "将嵌套的可迭代对象扁平化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute flatten operation."""
        nested = params.get('nested', [])
        depth = params.get('depth', None)
        output_var = params.get('output_var', 'flatten_result')

        try:
            resolved_nested = context.resolve_value(nested)
            resolved_depth = context.resolve_value(depth) if depth is not None else None
            result = list(flatten(resolved_nested, resolved_depth))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"flattened to {len(result)} elements")
        except Exception as e:
            return ActionResult(success=False, message=f"flatten failed: {e}")


class ItertoolsUniqueAction(BaseAction):
    """Get unique elements preserving order."""
    action_type = "itertools_unique"
    display_name = "去重"
    description = "获取不重复元素（保持顺序）"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute unique operation."""
        iterable = params.get('iterable', [])
        output_var = params.get('output_var', 'unique_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            result = list(unique(iter(resolved_iterable)))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"got {len(result)} unique elements")
        except Exception as e:
            return ActionResult(success=False, message=f"unique failed: {e}")


class ItertoolsUniqueByAction(BaseAction):
    """Unique elements by key function."""
    action_type = "itertools_unique_by"
    display_name = "按Key去重"
    description = "通过Key函数获取不重复元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute unique_by operation."""
        iterable = params.get('iterable', [])
        key_str = params.get('key', 'lambda x: x')
        output_var = params.get('output_var', 'unique_by_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            key_func = eval(key_str, {"__builtins__": {}}, {})
            result = list(unique_by(iter(resolved_iterable), key_func))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"got {len(result)} unique elements by key")
        except Exception as e:
            return ActionResult(success=False, message=f"unique_by failed: {e}")


class ItertoolsGrouperAction(BaseAction):
    """Group elements in chunks."""
    action_type = "itertools_grouper"
    display_name = "分组"
    description = "将元素按固定大小分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute grouper operation."""
        iterable = params.get('iterable', [])
        n = params.get('n', 3)
        fillvalue = params.get('fillvalue', None)
        output_var = params.get('output_var', 'grouper_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            resolved_n = context.resolve_value(n)
            result = list(grouper(iter(resolved_iterable), resolved_n, fillvalue))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"grouped into {len(result)} groups")
        except Exception as e:
            return ActionResult(success=False, message=f"grouper failed: {e}")


class ItertoolsPartitionAction(BaseAction):
    """Partition iterable by predicate."""
    action_type = "itertools_partition"
    display_name = "分区"
    description = "按条件将元素分为两部分"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute partition operation."""
        iterable = params.get('iterable', [])
        pred_str = params.get('predicate', 'lambda x: bool(x)')
        output_var = params.get('output_var', 'partition_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            pred_func = eval(pred_str, {"__builtins__": {}}, {})
            true_part, false_part = partition(pred_func, iter(resolved_iterable))
            result = {"true": true_part, "false": false_part}
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"partitioned: {len(true_part)} true, {len(false_part)} false")
        except Exception as e:
            return ActionResult(success=False, message=f"partition failed: {e}")


class ItertoolsTakeAction(BaseAction):
    """Take first n elements."""
    action_type = "itertools_take"
    display_name = "取前N个"
    description = "获取可迭代对象的前N个元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute take operation."""
        iterable = params.get('iterable', [])
        n = params.get('n', 5)
        output_var = params.get('output_var', 'take_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            resolved_n = context.resolve_value(n)
            result = take(resolved_n, iter(resolved_iterable))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"took {len(result)} elements")
        except Exception as e:
            return ActionResult(success=False, message=f"take failed: {e}")


class ItertoolsDropAction(BaseAction):
    """Drop first n elements."""
    action_type = "itertools_drop"
    display_name = "丢弃前N个"
    description = "丢弃可迭代对象的前N个元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute drop operation."""
        iterable = params.get('iterable', [])
        n = params.get('n', 5)
        output_var = params.get('output_var', 'drop_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            resolved_n = context.resolve_value(n)
            result = list(drop(resolved_n, iter(resolved_iterable)))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"dropped, {len(result)} remaining")
        except Exception as e:
            return ActionResult(success=False, message=f"drop failed: {e}")


class ItertoolsPowersetAction(BaseAction):
    """All subsets of iterable."""
    action_type = "itertools_powerset"
    display_name = "幂集"
    description = "生成可迭代对象的所有子集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute powerset operation."""
        iterable = params.get('iterable', [])
        output_var = params.get('output_var', 'powerset_result')

        try:
            resolved_iterable = context.resolve_value(iterable)
            result = list(powerset(iter(resolved_iterable)))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"generated {len(result)} subsets")
        except Exception as e:
            return ActionResult(success=False, message=f"powerset failed: {e}")
