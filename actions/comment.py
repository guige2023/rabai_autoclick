"""Comment and documentation action module for RabAI AutoClick.

Provides no-op actions for documentation and organization.
"""

from typing import Any, Dict, List

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class CommentAction(BaseAction):
    """No-op action for documentation in workflows.
    
    Useful for adding comments and documentation within a workflow.
    The comment text is stored in the context and can be accessed
    via the output_var parameter.
    """
    
    action_type = "comment"
    display_name = "注释"
    description = "工作流注释，不执行任何操作，仅供文档用途"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        text = params.get("text", "")
        output_var = params.get("output_var")
        
        if output_var:
            context.set(output_var, text)
        
        return ActionResult(
            success=True,
            message=f"注释: {text}" if text else "空注释",
            data={"comment": text} if text else None
        )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"text": "", "output_var": None}


class LabelAction(BaseAction):
    """Define a named jump target in the workflow.
    
    Labels serve as anchor points that can be referenced by GotoAction.
    Unlike step IDs, labels are semantic names like "cleanup" or "retry_point".
    """
    
    action_type = "label"
    display_name = "标签"
    description = "定义工作流中的跳转目标点"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name")
        description = params.get("description", "")
        output_var = params.get("output_var")
        
        if not name:
            return ActionResult(success=False, message="name 参数是必需的")
        
        # Store label in context for GotoAction to reference
        if output_var:
            context.set(output_var, name)
        
        # Also maintain a labels registry
        if not hasattr(context, '_label_registry'):
            context._label_registry = {}
        context._label_registry[name] = params.get("_step_id")
        
        return ActionResult(
            success=True,
            message=f"标签: {name}",
            data={"label": name, "description": description}
        )
    
    def get_required_params(self) -> List[str]:
        return ["name"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"description": "", "output_var": None}


class GotoAction(BaseAction):
    """Jump to a labeled point in the workflow.
    
    Supports conditional jumps based on expressions.
    """
    
    action_type = "goto"
    display_name = "跳转"
    description = "跳转到指定标签的步骤"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        label = params.get("label")
        condition = params.get("condition")  # optional: only jump if condition is true
        
        if not label:
            return ActionResult(success=False, message="label 参数是必需的")
        
        # Check condition if provided
        if condition:
            try:
                result = context._evaluate_expression(condition)
                if not bool(result):
                    return ActionResult(
                        success=True,
                        message=f"条件 {condition} 为假，跳过跳转",
                        data={"condition": condition, "result": result}
                    )
            except Exception as e:
                return ActionResult(success=False, message=f"条件表达式求值失败: {str(e)}")
        
        # Look up label target
        label_registry = getattr(context, '_label_registry', {})
        target_step = label_registry.get(label)
        
        if target_step is None:
            # Label not found in registry, try treating as step ID directly
            return ActionResult(
                success=True,
                message=f"跳转到: {label}",
                next_step_id=label,  # Try as step ID
                data={"label": label, "target": label}
            )
        
        return ActionResult(
            success=True,
            message=f"跳转到标签: {label}",
            next_step_id=target_step,
            data={"label": label, "target": target_step}
        )
    
    def get_required_params(self) -> List[str]:
        return ["label"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"condition": None}


class LogAction(BaseAction):
    """Log a message to the workflow execution log.
    
    Useful for debugging and tracing workflow execution.
    """
    
    action_type = "log"
    display_name = "日志"
    description = "向执行日志写入消息"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        message = params.get("message", "")
        level = params.get("level", "info")  # debug, info, warning, error
        output_var = params.get("output_var")
        
        if not message:
            # Try to resolve from expression
            message_expr = params.get("message_expr")
            if message_expr:
                try:
                    message = context.resolve_value(message_expr)
                except Exception as e:
                    message = f"{{解析失败: {str(e)}}}"
        
        # Store log entry
        log_entry = {"level": level, "message": str(message)}
        if not hasattr(context, '_log_entries'):
            context._log_entries = []
        context._log_entries.append(log_entry)
        
        # Also set output_var if provided
        if output_var:
            context.set(output_var, str(message))
        
        return ActionResult(
            success=True,
            message=f"[{level.upper()}] {message}",
            data=log_entry
        )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {"message": "", "message_expr": None, "level": "info", "output_var": None}
