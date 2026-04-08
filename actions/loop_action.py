"""Loop control action module for RabAI AutoClick.

Provides loop iteration actions including for-each, while, and break/continue controls.
"""

import time
import copy
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ForEachAction(BaseAction):
    """Iterate over a collection of items.
    
    Provides for-each loop functionality for lists, dicts, ranges,
    and other iterable objects with index tracking.
    """
    action_type = "for_each"
    display_name = "遍历循环"
    description = "遍历集合中的每个元素"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute for-each iteration.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, item_var, index_var, max_iterations.
        
        Returns:
            ActionResult with iteration info and current item.
        """
        items = params.get('items', [])
        item_var = params.get('item_var', 'item')
        index_var = params.get('index_var', 'index')
        max_iterations = params.get('max_iterations', None)
        
        if not isinstance(items, (list, dict, str, tuple, range)):
            return ActionResult(
                success=False,
                message=f"Items must be iterable, got {type(items).__name__}"
            )
        
        # Get current iteration from context
        if not hasattr(context, '_loop_state'):
            context._loop_state = {}
        
        loop_id = params.get('loop_id', 'default')
        loop_key = f"for_each_{loop_id}"
        
        if loop_key not in context._loop_state:
            # Start new loop
            if isinstance(items, dict):
                context._loop_state[loop_key] = {
                    'iterator': iter(items.items()),
                    'index': -1,
                    'items': items,
                    'total': len(items)
                }
            else:
                context._loop_state[loop_key] = {
                    'iterator': iter(items),
                    'index': -1,
                    'items': items,
                    'total': len(items) if hasattr(items, '__len__') else None
                }
        
        state = context._loop_state[loop_key]
        
        # Check max iterations
        if max_iterations and state['index'] >= max_iterations - 1:
            del context._loop_state[loop_key]
            return ActionResult(
                success=True,
                message=f"Reached max iterations ({max_iterations})",
                data={'completed': True, 'iterations': max_iterations}
            )
        
        # Get next item
        try:
            state['index'] += 1
            if isinstance(items, dict):
                key, value = next(state['iterator'])
                result = {'key': key, 'value': value}
            else:
                result = next(state['iterator'])
            
            return ActionResult(
                success=True,
                message=f"Iteration {state['index']}" + 
                        (f" of {state['total']}" if state['total'] else ""),
                data={
                    'index': state['index'],
                    'total': state['total'],
                    'item': result,
                    'is_last': state['total'] and state['index'] >= state['total'] - 1
                }
            )
            
        except StopIteration:
            del context._loop_state[loop_key]
            return ActionResult(
                success=True,
                message=f"Loop completed ({state['index'] + 1} iterations)",
                data={'completed': True, 'iterations': state['index'] + 1}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Loop error: {e}",
                data={'error': str(e)}
            )


class WhileLoopAction(BaseAction):
    """Execute loop while a condition is true.
    
    Provides while loop with max iteration protection and condition evaluation.
    """
    action_type = "while_loop"
    display_name = "条件循环"
    description = "当条件满足时循环执行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute while loop iteration.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: condition_var, max_iterations, loop_id.
        
        Returns:
            ActionResult with loop status.
        """
        condition_var = params.get('condition_var', 'condition')
        max_iterations = params.get('max_iterations', 1000)
        loop_id = params.get('loop_id', 'default')
        
        if not hasattr(context, '_loop_state'):
            context._loop_state = {}
        
        loop_key = f"while_{loop_id}"
        
        if loop_key not in context._loop_state:
            context._loop_state[loop_key] = {
                'iterations': 0,
                'started': False
            }
        
        state = context._loop_state[loop_key]
        
        # Check condition from context
        condition = getattr(context, condition_var, False)
        
        # Evaluate condition if it's a callable
        if callable(condition):
            try:
                condition = condition()
            except Exception:
                condition = False
        
        if not condition:
            if state['started']:
                del context._loop_state[loop_key]
                return ActionResult(
                    success=True,
                    message=f"While loop ended (condition false after {state['iterations']} iterations)",
                    data={'completed': True, 'iterations': state['iterations']}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Condition not met: {condition_var}"
                )
        
        state['iterations'] += 1
        state['started'] = True
        
        if state['iterations'] >= max_iterations:
            del context._loop_state[loop_key]
            return ActionResult(
                success=False,
                message=f"Max iterations reached ({max_iterations})",
                data={'max_iterations': max_iterations, 'iterations': state['iterations']}
            )
        
        return ActionResult(
            success=True,
            message=f"While iteration {state['iterations']}/{max_iterations}",
            data={
                'iteration': state['iterations'],
                'max_iterations': max_iterations,
                'condition_met': True
            }
        )


class BreakLoopAction(BaseAction):
    """Break out of the current loop.
    
    Marks the current loop for early exit on next iteration check.
    """
    action_type = "break_loop"
    display_name = "跳出循环"
    description = "提前退出当前循环"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Break current loop.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: loop_id, levels.
        
        Returns:
            ActionResult with break status.
        """
        loop_id = params.get('loop_id', 'default')
        levels = params.get('levels', 1)
        
        if not hasattr(context, '_loop_state'):
            context._loop_state = {}
        
        # Clear loop state for the specified loop(s)
        cleared = 0
        for i in range(levels):
            key_suffix = '' if i == 0 else f"_{i}"
            key = f"while_{loop_id}{key_suffix}"
            if key in context._loop_state:
                del context._loop_state[key]
                cleared += 1
            key = f"for_each_{loop_id}{key_suffix}"
            if key in context._loop_state:
                del context._loop_state[key]
                cleared += 1
        
        return ActionResult(
            success=True,
            message=f"Loop break executed",
            data={'cleared': cleared, 'levels': levels}
        )


class ContinueLoopAction(BaseAction):
    """Skip to next iteration of the loop.
    
    Sets a flag that signals the next iteration check to skip execution.
    """
    action_type = "continue_loop"
    display_name = "跳过本次循环"
    description = "跳到下一次循环迭代"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Continue to next iteration.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: loop_id.
        
        Returns:
            ActionResult with continue status.
        """
        loop_id = params.get('loop_id', 'default')
        
        if not hasattr(context, '_loop_state'):
            context._loop_state = {}
        
        # Set skip flag for the next iteration
        skip_key = f"_skip_{loop_id}"
        context._loop_state[skip_key] = True
        
        return ActionResult(
            success=True,
            message=f"Continue to next iteration",
            data={'loop_id': loop_id}
        )


class LoopCounterAction(BaseAction):
    """Track and increment loop iteration counters.
    
    Maintains named counters for loop tracking and conditional logic.
    """
    action_type = "loop_counter"
    display_name = "循环计数"
    description = "维护循环计数器"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage loop counter.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: name, increment, reset, threshold.
        
        Returns:
            ActionResult with counter value and threshold status.
        """
        name = params.get('name', 'default')
        increment = params.get('increment', 1)
        reset = params.get('reset', False)
        threshold = params.get('threshold', None)
        
        if not hasattr(context, '_counters'):
            context._counters = {}
        
        if reset or name not in context._counters:
            context._counters[name] = 0
            return ActionResult(
                success=True,
                message=f"Counter '{name}' reset to 0",
                data={'name': name, 'value': 0}
            )
        
        context._counters[name] += increment
        
        result_data = {
            'name': name,
            'value': context._counters[name],
            'increment': increment
        }
        
        if threshold is not None:
            result_data['threshold_reached'] = context._counters[name] >= threshold
        
        return ActionResult(
            success=True,
            message=f"Counter '{name}': {context._counters[name]}",
            data=result_data
        )
