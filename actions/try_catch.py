"""Try-catch action module for RabAI AutoClick.

Provides exception handling actions.
"""

from typing import Any, Dict, List

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class TryCatchAction(BaseAction):
    """Execute steps with exception handling.
    
    The TryCatchAction works with the FlowEngine to handle exceptions:
    - try_steps: executed in a try block
    - catch_steps: executed if an exception occurs
    - exception_var: context variable to store the exception message
    
    Note: This action relies on the FlowEngine to track and report exceptions.
    The flow engine should call this action before risky steps and handle
    the exception_var after execution.
    """
    
    action_type = "try_catch"
    display_name = "异常捕获"
    description = "捕获执行过程中的异常，转移到 catch 分支处理"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try_steps = params.get("try_steps", [])
        catch_steps = params.get("catch_steps", [])
        exception_var = params.get("exception_var", "_exception")
        finally_steps = params.get("finally_steps", [])
        
        # Check if an exception was caught by a previous step
        exception_msg = context.get(exception_var)
        
        if exception_msg:
            # Exception occurred, execute catch steps
            context.delete(exception_var)
            return ActionResult(
                success=True,
                message=f"执行 catch 分支 (异常: {exception_msg})",
                next_step_id=catch_steps[0] if catch_steps else None,
                data={"branch": "catch", "exception": exception_msg}
            )
        else:
            # Normal execution, execute try steps
            return ActionResult(
                success=True,
                message=f"执行 try 分支 ({len(try_steps)} 步)",
                next_step_id=try_steps[0] if try_steps else None,
                data={"branch": "try", "finally_steps": finally_steps}
            )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "try_steps": [],
            "catch_steps": [],
            "finally_steps": [],
            "exception_var": "_exception"
        }


class ThrowAction(BaseAction):
    """Manually throw an exception to trigger try-catch."""
    
    action_type = "throw"
    display_name = "抛出异常"
    description = "手动抛出异常，用于测试或触发异常处理流程"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        message = params.get("message", "手动抛出异常")
        exception_type = params.get("type", "RuntimeError")
        exception_var = params.get("exception_var", "_exception")
        
        context.set(exception_var, f"{exception_type}: {message}")
        
        return ActionResult(
            success=False,
            message=f"抛出异常: {message}",
            data={"type": exception_type, "message": message}
        )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"message": "手动抛出异常", "type": "RuntimeError", "exception_var": "_exception"}


class RethrowAction(BaseAction):
    """Re-throw the currently caught exception."""
    
    action_type = "rethrow"
    display_name = "重新抛出"
    description = "重新抛出当前捕获的异常"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        exception_var = params.get("exception_var", "_exception")
        exception_msg = context.get(exception_var)
        
        if not exception_msg:
            return ActionResult(success=False, message="没有可重新抛出的异常")
        
        context.set(exception_var, exception_msg)
        
        return ActionResult(
            success=False,
            message=f"重新抛出异常: {exception_msg}",
            data={"exception": exception_msg}
        )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"exception_var": "_exception"}


class AssertAction(BaseAction):
    """Assert a condition, throw if false."""
    
    action_type = "assert"
    display_name = "断言"
    description = "断言条件为真，否则抛出异常"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        condition_expr = params.get("condition")
        message = params.get("message", "断言失败")
        exception_var = params.get("exception_var", "_exception")
        
        if not condition_expr:
            return ActionResult(success=False, message="condition 参数是必需的")
        
        try:
            result = context._evaluate_expression(condition_expr)
            if not bool(result):
                context.set(exception_var, f"AssertionError: {message}")
                return ActionResult(
                    success=False,
                    message=f"断言失败: {condition_expr} = {result}",
                    data={"condition": condition_expr, "result": result}
                )
        except Exception as e:
            context.set(exception_var, f"AssertionError: {str(e)}")
            return ActionResult(
                success=False,
                message=f"断言表达式错误: {str(e)}",
                data={"condition": condition_expr, "error": str(e)}
            )
        
        return ActionResult(
            success=True,
            message=f"断言通过: {condition_expr}",
            data={"condition": condition_expr}
        )
    
    def get_required_params(self) -> List[str]:
        return ["condition"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"message": "断言失败", "exception_var": "_exception"}
