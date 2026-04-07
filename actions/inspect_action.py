"""Inspect action module for RabAI AutoClick.

Provides module and function introspection utilities:
- GetMembersAction: Get object members
- GetSourceAction: Get function source code
- GetDocAction: Get object documentation
- GetCallArgsAction: Get function call arguments
- GetTypeAction: Get object type
- GetModuleAction: Get object module
- GetFileAction: Get object file location
- IsFunctionAction: Check if object is function
- IsClassAction: Check if object is class
- IsMethodAction: Check if object is method
- SignatureAction: Get function signature
"""

from typing import Any, Callable, Dict, List, Optional, Union
import sys
import inspect
import importlib

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class InspectGetMembersAction(BaseAction):
    """Get object members."""
    action_type = "inspect_get_members"
    display_name = "获取成员"
    description = "获取对象的成员列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get members operation."""
        obj = params.get('object', None)
        predicate_str = params.get('predicate', None)
        output_var = params.get('output_var', 'members_result')

        try:
            resolved_obj = context.resolve_value(obj)
            predicate = None
            if predicate_str:
                predicate = eval(predicate_str, {"__builtins__": {}}, {"inspect": inspect})
            
            members = inspect.getmembers(resolved_obj, predicate=predicate)
            member_names = [name for name, _ in members]
            context.set_variable(output_var, member_names)
            return ActionResult(success=True, message=f"got {len(member_names)} members")
        except Exception as e:
            return ActionResult(success=False, message=f"get_members failed: {e}")


class InspectGetSourceAction(BaseAction):
    """Get function source code."""
    action_type = "inspect_get_source"
    display_name = "获取源码"
    description = "获取函数的源代码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get source operation."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'source_result')

        try:
            resolved_obj = context.resolve_value(obj)
            source = inspect.getsource(resolved_obj)
            context.set_variable(output_var, source)
            return ActionResult(success=True, message=f"got source ({len(source)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"get_source failed: {e}")


class InspectGetDocAction(BaseAction):
    """Get object documentation."""
    action_type = "inspect_get_doc"
    display_name = "获取文档"
    description = "获取对象的文档字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get doc operation."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'doc_result')

        try:
            resolved_obj = context.resolve_value(obj)
            doc = inspect.getdoc(resolved_obj) or ""
            context.set_variable(output_var, doc)
            return ActionResult(success=True, message=f"got doc ({len(doc)} chars)")
        except Exception as e:
            return ActionResult(success=False, message=f"get_doc failed: {e}")


class InspectGetCallArgsAction(BaseAction):
    """Get function call arguments."""
    action_type = "inspect_get_call_args"
    display_name = "获取参数"
    description = "获取函数的调用参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get call args operation."""
        func = params.get('function', None)
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        output_var = params.get('output_var', 'call_args_result')

        try:
            resolved_func = context.resolve_value(func)
            resolved_args = context.resolve_value(args)
            resolved_kwargs = context.resolve_value(kwargs)
            
            sig = inspect.signature(resolved_func)
            bound = sig.bind(*resolved_args, **resolved_kwargs)
            bound.apply_defaults()
            context.set_variable(output_var, dict(bound.arguments))
            return ActionResult(success=True, message=f"got {len(bound.arguments)} arguments")
        except Exception as e:
            return ActionResult(success=False, message=f"get_call_args failed: {e}")


class InspectGetTypeAction(BaseAction):
    """Get object type."""
    action_type = "inspect_get_type"
    display_name = "获取类型"
    description = "获取对象的类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get type operation."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'type_result')

        try:
            resolved_obj = context.resolve_value(obj)
            type_obj = type(resolved_obj)
            context.set_variable(output_var, {"type": type_obj.__name__, "module": type_obj.__module__})
            return ActionResult(success=True, message=f"type is {type_obj.__name__}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_type failed: {e}")


class InspectGetModuleAction(BaseAction):
    """Get object module."""
    action_type = "inspect_get_module"
    display_name = "获取模块"
    description = "获取对象所属的模块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get module operation."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'module_result')

        try:
            resolved_obj = context.resolve_value(obj)
            module = inspect.getmodule(resolved_obj)
            module_name = module.__name__ if module else None
            context.set_variable(output_var, module_name)
            return ActionResult(success=True, message=f"module is {module_name}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_module failed: {e}")


class InspectGetFileAction(BaseAction):
    """Get object file location."""
    action_type = "inspect_get_file"
    display_name = "获取文件"
    description = "获取对象定义所在文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get file operation."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'file_result')

        try:
            resolved_obj = context.resolve_value(obj)
            file_path = inspect.getfile(resolved_obj)
            context.set_variable(output_var, file_path)
            return ActionResult(success=True, message=f"file: {file_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_file failed: {e}")


class InspectIsFunctionAction(BaseAction):
    """Check if object is function."""
    action_type = "inspect_is_function"
    display_name = "是函数"
    description = "检查对象是否是函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is function check."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'is_function_result')

        try:
            resolved_obj = context.resolve_value(obj)
            is_func = inspect.isfunction(resolved_obj)
            context.set_variable(output_var, is_func)
            return ActionResult(success=True, message=f"is_function: {is_func}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_function failed: {e}")


class InspectIsClassAction(BaseAction):
    """Check if object is class."""
    action_type = "inspect_is_class"
    display_name = "是类"
    description = "检查对象是否是类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is class check."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'is_class_result')

        try:
            resolved_obj = context.resolve_value(obj)
            is_cls = inspect.isclass(resolved_obj)
            context.set_variable(output_var, is_cls)
            return ActionResult(success=True, message=f"is_class: {is_cls}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_class failed: {e}")


class InspectIsMethodAction(BaseAction):
    """Check if object is method."""
    action_type = "inspect_is_method"
    display_name = "是方法"
    description = "检查对象是否是不可绑定方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is method check."""
        obj = params.get('object', None)
        output_var = params.get('output_var', 'is_method_result')

        try:
            resolved_obj = context.resolve_value(obj)
            is_meth = inspect.ismethod(resolved_obj)
            context.set_variable(output_var, is_meth)
            return ActionResult(success=True, message=f"ismethod: {is_meth}")
        except Exception as e:
            return ActionResult(success=False, message=f"ismethod failed: {e}")


class InspectSignatureAction(BaseAction):
    """Get function signature."""
    action_type = "inspect_signature"
    display_name = "获取签名"
    description = "获取函数的签名信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute signature operation."""
        func = params.get('function', None)
        output_var = params.get('output_var', 'signature_result')

        try:
            resolved_func = context.resolve_value(func)
            sig = inspect.signature(resolved_func)
            params_info = []
            for name, param in sig.parameters.items():
                params_info.append({
                    "name": name,
                    "kind": str(param.kind),
                    "default": str(param.default) if param.default != inspect.Parameter.empty else None
                })
            context.set_variable(output_var, {"signature": str(sig), "parameters": params_info})
            return ActionResult(success=True, message=f"signature: {sig}")
        except Exception as e:
            return ActionResult(success=False, message=f"signature failed: {e}")


class InspectGetMembersRecursiveAction(BaseAction):
    """Get members recursively."""
    action_type = "inspect_get_members_recursive"
    display_name = "递归获取成员"
    description = "递归获取对象的所有成员"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute recursive get members."""
        obj = params.get('object', None)
        max_depth = params.get('max_depth', 2)
        output_var = params.get('output_var', 'members_recursive_result')

        try:
            resolved_obj = context.resolve_value(obj)
            resolved_depth = context.resolve_value(max_depth)
            
            def get_members_recursive(o, depth=0):
                if depth >= resolved_depth:
                    return {}
                members = {}
                for name, value in inspect.getmembers(o):
                    if name.startswith('_'):
                        continue
                    try:
                        members[name] = {"type": type(value).__name__, "value": str(value)[:50]}
                    except Exception:
                        pass
                return members
            
            result = get_members_recursive(resolved_obj)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"got members recursively")
        except Exception as e:
            return ActionResult(success=False, message=f"get_members_recursive failed: {e}")
