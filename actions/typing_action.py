"""Typing action module for RabAI AutoClick.

Provides typing utilities:
- GetTypeHintsAction: Get type hints
- GetAnnotationsAction: Get annotations
- IsTypeAction: Check if value is of type
- CastAction: Type cast
- NewTypeAction: Create new type
- GenericAliasAction: Generic alias operations
- UnionCheckAction: Check union types
- OptionalCheckAction: Check optional types
- LiteralCheckAction: Literal type checking
- FinalCheckAction: Final type checking
"""

from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, get_type_hints, get_origin, get_args
import sys

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TypingGetTypeHintsAction(BaseAction):
    """Get type hints."""
    action_type = "typing_get_type_hints"
    display_name = "获取类型提示"
    description = "获取函数或类的类型提示"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get type hints."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'type_hints_result')

        try:
            if obj is None:
                return ActionResult(success=False, message="object is required")
            
            resolved_obj = context.resolve_value(obj) if isinstance(obj, str) else obj
            hints = get_type_hints(resolved_obj)
            
            hints_str = {k: str(v) for k, v in hints.items()}
            context.set_variable(output_var, hints_str)
            return ActionResult(success=True, message=f"got {len(hints)} type hints")
        except Exception as e:
            return ActionResult(success=False, message=f"get_type_hints failed: {e}")


class TypingGetAnnotationsAction(BaseAction):
    """Get annotations."""
    action_type = "typing_get_annotations"
    display_name = "获取注解"
    description = "获取对象的注解"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get annotations."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'annotations_result')

        try:
            if obj is None:
                return ActionResult(success=False, message="object is required")
            
            resolved_obj = context.resolve_value(obj) if isinstance(obj, str) else obj
            annotations = getattr(resolved_obj, '__annotations__', {})
            
            annotations_str = {k: str(v) for k, v in annotations.items()}
            context.set_variable(output_var, annotations_str)
            return ActionResult(success=True, message=f"got {len(annotations)} annotations")
        except Exception as e:
            return ActionResult(success=False, message=f"get_annotations failed: {e}")


class TypingIsTypeAction(BaseAction):
    """Check if value is of type."""
    action_type = "typing_is_type"
    display_name = "类型检查"
    description = "检查值是否是指定类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is type check."""
        value = params.get('value', None)
        type_str = params.get('type', 'Any')
        output_var = params.get('output_var', 'is_type_result')

        try:
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            resolved_type_str = context.resolve_value(type_str) if isinstance(type_str, str) else type_str
            
            target_type = eval(resolved_type_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            result = isinstance(resolved_value, target_type)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"is {resolved_type_str}: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_type failed: {e}")


class TypingCastAction(BaseAction):
    """Type cast."""
    action_type = "typing_cast"
    display_name = "类型转换"
    description = "类型转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cast."""
        type_str = params.get('type', 'Any')
        value = params.get('value', None)
        output_var = params.get('output_var', 'cast_result')

        try:
            resolved_type_str = context.resolve_value(type_str) if isinstance(type_str, str) else type_str
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            
            from typing import cast
            target_type = eval(resolved_type_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            result = cast(target_type, resolved_value)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"cast to {resolved_type_str}")
        except Exception as e:
            return ActionResult(success=False, message=f"cast failed: {e}")


class TypingNewTypeAction(BaseAction):
    """Create new type."""
    action_type = "typing_new_type"
    display_name = "创建新类型"
    description = "创建新的类型别名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute new type."""
        name = params.get('name', 'NewType')
        type_str = params.get('type', 'Any')
        output_var = params.get('output_var', 'new_type_result')

        try:
            resolved_name = context.resolve_value(name) if isinstance(name, str) else name
            resolved_type_str = context.resolve_value(type_str) if isinstance(type_str, str) else type_str
            
            from typing import NewType
            base_type = eval(resolved_type_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            new_type = NewType(resolved_name, base_type)
            
            context.set_variable(output_var, new_type)
            return ActionResult(success=True, message=f"created NewType: {resolved_name}")
        except Exception as e:
            return ActionResult(success=False, message=f"new_type failed: {e}")


class TypingGenericAliasAction(BaseAction):
    """Generic alias operations."""
    action_type = "typing_generic_alias"
    display_name = "泛型别名"
    description = "处理泛型别名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute generic alias."""
        type_str = params.get('type', 'List[int]')
        output_var = params.get('output_var', 'generic_alias_result')

        try:
            resolved_type_str = context.resolve_value(type_str) if isinstance(type_str, str) else type_str
            
            generic_type = eval(resolved_type_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            origin = get_origin(generic_type)
            args = get_args(generic_type)
            
            context.set_variable(output_var, {
                "origin": str(origin) if origin else None,
                "args": [str(a) for a in args] if args else [],
                "str": resolved_type_str
            })
            return ActionResult(success=True, message=f"generic alias: {resolved_type_str}")
        except Exception as e:
            return ActionResult(success=False, message=f"generic_alias failed: {e}")


class TypingUnionCheckAction(BaseAction):
    """Check union types."""
    action_type = "typing_union_check"
    display_name = "联合类型检查"
    description = "检查联合类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute union check."""
        value = params.get('value', None)
        union_str = params.get('union', 'Union[str, int]')
        output_var = params.get('output_var', 'union_check_result')

        try:
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            resolved_union_str = context.resolve_value(union_str) if isinstance(union_str, str) else union_str
            
            from typing import Union, get_args, get_origin
            union_type = eval(resolved_union_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            args = get_args(union_type)
            
            result = isinstance(resolved_value, args) if args else False
            context.set_variable(output_var, {
                "is_union_member": result,
                "union_types": [str(a) for a in args] if args else []
            })
            return ActionResult(success=True, message=f"union check: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"union_check failed: {e}")


class TypingOptionalCheckAction(BaseAction):
    """Check optional types."""
    action_type = "typing_optional_check"
    display_name = "可选类型检查"
    description = "检查可选类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute optional check."""
        value = params.get('value', None)
        optional_str = params.get('optional', 'Optional[str]')
        output_var = params.get('output_var', 'optional_check_result')

        try:
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            resolved_optional_str = context.resolve_value(optional_str) if isinstance(optional_str, str) else optional_str
            
            from typing import Optional, get_args
            optional_type = eval(resolved_optional_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            args = get_args(optional_type)
            
            is_none = resolved_value is None
            is_valid = is_none or isinstance(resolved_value, args)
            
            context.set_variable(output_var, {
                "is_optional_valid": is_valid,
                "is_none": is_none,
                "inner_type": str(args[0]) if args else None
            })
            return ActionResult(success=True, message=f"optional check: {is_valid}")
        except Exception as e:
            return ActionResult(success=False, message=f"optional_check failed: {e}")


class TypingLiteralCheckAction(BaseAction):
    """Literal type checking."""
    action_type = "typing_literal_check"
    display_name = "字面量检查"
    description = "检查字面量类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute literal check."""
        value = params.get('value', None)
        literal_str = params.get('literal', 'Literal["a", "b", "c"]')
        output_var = params.get('output_var', 'literal_check_result')

        try:
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            resolved_literal_str = context.resolve_value(literal_str) if isinstance(literal_str, str) else literal_str
            
            from typing import Literal, get_args
            literal_type = eval(resolved_literal_str, {"__builtins__": __builtins__, "typing": __import__('typing')}, {})
            args = get_args(literal_type)
            
            result = resolved_value in args if args else False
            context.set_variable(output_var, {
                "is_literal_match": result,
                "literal_values": list(args) if args else []
            })
            return ActionResult(success=True, message=f"literal check: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"literal_check failed: {e}")


class TypingFinalCheckAction(BaseAction):
    """Final type checking."""
    action_type = "typing_final_check"
    display_name = "Final类型检查"
    description = "检查Final类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute final check."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'final_check_result')

        try:
            resolved_obj = context.resolve_value(obj) if isinstance(obj, str) else obj
            
            from typing import Final, get_type_hints
            hints = get_type_hints(resolved_obj) if hasattr(resolved_obj, '__annotations__') else {}
            final_attrs = {k: v for k, v in hints.items() if hasattr(v, '__origin__') and str(v).__contains__('Final')}
            
            context.set_variable(output_var, {
                "has_final": len(final_attrs) > 0,
                "final_attrs": list(final_attrs.keys())
            })
            return ActionResult(success=True, message=f"final check: {len(final_attrs)} final attrs")
        except Exception as e:
            return ActionResult(success=False, message=f"final_check failed: {e}")
