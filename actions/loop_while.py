"""Loop-while action module for RabAI AutoClick.

Provides loop actions that iterate based on condition expressions.
"""

from typing import Any, Dict, List

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class LoopWhileAction(BaseAction):
    """Loop while a condition expression evaluates to true."""
    
    action_type = "loop_while"
    display_name = "条件循环"
    description = "当条件表达式为真时重复执行循环体中的步骤"
    
    def __init__(self) -> None:
        super().__init__()
        self._iteration_store: Dict[str, int] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        condition = params.get("condition")
        loop_start = params.get("loop_start")
        loop_end = params.get("loop_end")
        max_iterations = params.get("max_iterations", 1000)
        counter_var = params.get("counter_var", "_loop_counter")
        
        if not condition:
            return ActionResult(success=False, message="condition 参数是必需的")
        if loop_start is None or loop_end is None:
            return ActionResult(success=False, message="loop_start 和 loop_end 参数是必需的")
        
        # Get or initialize iteration counter for this loop context
        loop_id = f"{loop_start}_{loop_end}"
        if loop_id not in self._iteration_store:
            self._iteration_store[loop_id] = 0
        
        current_iter = self._iteration_store[loop_id]
        
        # Check max iterations
        if current_iter >= max_iterations:
            self._iteration_store[loop_id] = 0
            return ActionResult(
                success=True,
                message=f"循环达到最大迭代次数 {max_iterations}",
                next_step_id=loop_end
            )
        
        # Evaluate condition
        try:
            result = context._evaluate_expression(condition)
            should_continue = bool(result)
        except Exception as e:
            return ActionResult(success=False, message=f"条件表达式求值失败: {str(e)}")
        
        # Update counter in context
        context.set(counter_var, current_iter)
        
        if should_continue:
            self._iteration_store[loop_id] = current_iter + 1
            return ActionResult(
                success=True,
                message=f"条件满足，继续循环 (迭代 {current_iter})",
                next_step_id=loop_start,
                data={"iteration": current_iter, "condition": condition}
            )
        else:
            self._iteration_store[loop_id] = 0
            return ActionResult(
                success=True,
                message=f"条件不满足，退出循环 (迭代 {current_iter})",
                next_step_id=loop_end,
                data={"iteration": current_iter, "condition": condition}
            )
    
    def get_required_params(self) -> List[str]:
        return ["condition", "loop_start", "loop_end"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"max_iterations": 1000, "counter_var": "_loop_counter"}


class LoopWhileBreakAction(BaseAction):
    """Break out of a loop_while loop."""
    
    action_type = "loop_while_break"
    display_name = "跳出循环"
    description = "立即退出当前条件循环"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        loop_end = params.get("loop_end")
        
        if loop_end is None:
            return ActionResult(success=False, message="loop_end 参数是必需的")
        
        return ActionResult(
            success=True,
            message="跳出循环",
            next_step_id=loop_end
        )
    
    def get_required_params(self) -> List[str]:
        return ["loop_end"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class LoopWhileContinueAction(BaseAction):
    """Continue to next iteration of loop_while."""
    
    action_type = "loop_while_continue"
    display_name = "继续循环"
    description = "跳过当前迭代的剩余步骤，进入下一次循环"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        loop_start = params.get("loop_start")
        
        if loop_start is None:
            return ActionResult(success=False, message="loop_start 参数是必需的")
        
        return ActionResult(
            success=True,
            message="继续下一次循环",
            next_step_id=loop_start
        )
    
    def get_required_params(self) -> List[str]:
        return ["loop_start"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ForEachAction(BaseAction):
    """Iterate over a list or range of values."""
    
    action_type = "for_each"
    display_name = "遍历循环"
    description = "遍历列表、字典或范围内的每个元素"
    
    def __init__(self) -> None:
        super().__init__()
        self._for_each_store: Dict[str, int] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        items_expr = params.get("items")
        loop_start = params.get("loop_start")
        loop_end = params.get("loop_end")
        item_var = params.get("item_var", "_for_item")
        index_var = params.get("index_var", "_for_index")
        max_iterations = params.get("max_iterations", 10000)
        
        if not items_expr:
            return ActionResult(success=False, message="items 参数是必需的")
        if loop_start is None or loop_end is None:
            return ActionResult(success=False, message="loop_start 和 loop_end 参数是必需的")
        
        # Evaluate items expression
        try:
            items = context.resolve_value(items_expr)
        except Exception as e:
            return ActionResult(success=False, message=f"items 表达式求值失败: {str(e)}")
        
        # Normalize to list
        if isinstance(items, range):
            items = list(items)
        elif not isinstance(items, (list, tuple, dict, str)):
            return ActionResult(success=False, message=f"items 必须是列表、字典或字符串，得到 {type(items).__name__}")
        
        # Get or initialize for this loop
        store_key = f"{loop_start}_{loop_end}"
        if store_key not in self._for_each_store:
            self._for_each_store[store_key] = 0
        
        current_index = self._for_each_store[store_key]
        
        # Get item
        if isinstance(items, dict):
            keys = list(items.keys())
            if current_index >= len(keys):
                self._for_each_store[store_key] = 0
                return ActionResult(success=True, message="遍历完成", next_step_id=loop_end)
            key = keys[current_index]
            context.set(item_var, items[key])
            context.set(index_var, current_index)
            context.set(f"{item_var}_key", key)
        else:
            if current_index >= len(items):
                self._for_each_store[store_key] = 0
                return ActionResult(success=True, message="遍历完成", next_step_id=loop_end)
            context.set(item_var, items[current_index])
            context.set(index_var, current_index)
        
        if current_index >= max_iterations:
            self._for_each_store[store_key] = 0
            return ActionResult(success=True, message=f"达到最大迭代次数 {max_iterations}", next_step_id=loop_end)
        
        self._for_each_store[store_key] = current_index + 1
        return ActionResult(
            success=True,
            message=f"遍历索引 {current_index}",
            next_step_id=loop_start,
            data={"index": current_index, "total": len(items) if isinstance(items, (list, tuple)) else len(items) if isinstance(items, dict) else len(items)}
        )
    
    def get_required_params(self) -> List[str]:
        return ["items", "loop_start", "loop_end"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"item_var": "_for_item", "index_var": "_for_index", "max_iterations": 10000}
