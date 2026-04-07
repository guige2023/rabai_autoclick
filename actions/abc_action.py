"""ABC action module for RabAI AutoClick.

Provides abstract base class utilities:
- IsAbstractAction: Check if class is abstract
- GetAbstractMethodsAction: Get abstract methods
- RegisterABCAction: Register virtual subclass
- VerifyABCImplementationAction: Verify ABC implementation
- AbstractMethodCheckAction: Check abstract method implementation
- ABCPropertiesAction: Get ABC properties
- GetMROAction: Get method resolution order
- IsSubclassAction: Check subclass relationship
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union
import sys
import abc

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ABCIsAbstractAction(BaseAction):
    """Check if class is abstract."""
    action_type = "abc_is_abstract"
    display_name = "是抽象类"
    description = "检查类是否是抽象类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is abstract check."""
        class_path = params.get('class', None)
        output_var = params.get('output_var', 'is_abstract_result')

        try:
            if class_path is None:
                return ActionResult(success=False, message="class is required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            result = abc.ABC in resolved_class.__mro__ or any(getattr(resolved_class, '__abstractmethods__', []))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"is_abstract: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_abstract failed: {e}")


class ABCGetAbstractMethodsAction(BaseAction):
    """Get abstract methods."""
    action_type = "abc_get_abstract_methods"
    display_name = "获取抽象方法"
    description = "获取类的抽象方法列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get abstract methods."""
        class_path = params.get('class', None)
        output_var = params.get('output_var', 'abstract_methods_result')

        try:
            if class_path is None:
                return ActionResult(success=False, message="class is required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            abstract_methods = list(getattr(resolved_class, '__abstractmethods__', []))
            context.set_variable(output_var, abstract_methods)
            return ActionResult(success=True, message=f"found {len(abstract_methods)} abstract methods")
        except Exception as e:
            return ActionResult(success=False, message=f"get_abstract_methods failed: {e}")


class ABCRegisterAction(BaseAction):
    """Register virtual subclass."""
    action_type = "abc_register"
    display_name = "注册子类"
    description = "为抽象基类注册虚子类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute register."""
        abc_path = params.get('abc_class', None)
        subclass = params.get('subclass', None)
        output_var = params.get('output_var', 'register_result')

        try:
            if abc_path is None or subclass is None:
                return ActionResult(success=False, message="abc_class and subclass are required")
            
            resolved_abc = context.resolve_value(abc_path) if isinstance(abc_path, str) else abc_path
            resolved_subclass = context.resolve_value(subclass) if isinstance(subclass, str) else subclass
            
            if isinstance(resolved_abc, str):
                resolved_abc = eval(resolved_abc, {"__builtins__": __builtins__, "abc": abc}, {})
            if isinstance(resolved_subclass, str):
                resolved_subclass = eval(resolved_subclass, {"__builtins__": __builtins__}, {})
            
            resolved_abc.register(resolved_subclass)
            context.set_variable(output_var, {"registered": True, "subclass": str(resolved_subclass)})
            return ActionResult(success=True, message=f"registered {resolved_subclass.__name__}")
        except Exception as e:
            return ActionResult(success=False, message=f"register failed: {e}")


class ABCVerifyImplementationAction(BaseAction):
    """Verify ABC implementation."""
    action_type = "abc_verify_implementation"
    display_name = "验证实现"
    description = "验证ABC实现完整性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute verify implementation."""
        class_path = params.get('class', None)
        output_var = params.get('output_var', 'verify_result')

        try:
            if class_path is None:
                return ActionResult(success=False, message="class is required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            abstract_methods = set(getattr(resolved_class, '__abstractmethods__', []))
            concrete_methods = set()
            
            for name in dir(resolved_class):
                if name.startswith('_'):
                    continue
                attr = getattr(resolved_class, name, None)
                if callable(attr) and not getattr(attr, '__isabstractmethod__', False):
                    concrete_methods.add(name)
            
            unimplemented = abstract_methods - concrete_methods
            is_fully_implemented = len(unimplemented) == 0
            
            context.set_variable(output_var, {
                "fully_implemented": is_fully_implemented,
                "abstract_methods": list(abstract_methods),
                "unimplemented": list(unimplemented)
            })
            return ActionResult(success=True, message=f"verified: {'complete' if is_fully_implemented else 'incomplete'}")
        except Exception as e:
            return ActionResult(success=False, message=f"verify failed: {e}")


class ABCAbstractMethodCheckAction(BaseAction):
    """Check abstract method implementation."""
    action_type = "abc_method_check"
    display_name = "检查抽象方法"
    description = "检查具体类是否实现了抽象方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute abstract method check."""
        class_path = params.get('class', None)
        method_name = params.get('method', None)
        output_var = params.get('output_var', 'method_check_result')

        try:
            if class_path is None or method_name is None:
                return ActionResult(success=False, message="class and method are required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            resolved_method = context.resolve_value(method_name) if isinstance(method_name, str) else method_name
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            abstract_methods = set(getattr(resolved_class, '__abstractmethods__', []))
            has_method = hasattr(resolved_class, resolved_method)
            is_abstract = resolved_method in abstract_methods
            
            context.set_variable(output_var, {
                "is_abstract": is_abstract,
                "has_implementation": has_method and not is_abstract,
                "is_required": is_abstract
            })
            return ActionResult(success=True, message=f"method {resolved_method}: abstract={is_abstract}, implemented={has_method and not is_abstract}")
        except Exception as e:
            return ActionResult(success=False, message=f"method_check failed: {e}")


class ABCPropertiesAction(BaseAction):
    """Get ABC properties."""
    action_type = "abc_properties"
    display_name = "ABC属性"
    description = "获取ABC的属性列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get properties."""
        class_path = params.get('class', None)
        output_var = params.get('output_var', 'abc_properties_result')

        try:
            if class_path is None:
                return ActionResult(success=False, message="class is required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            abstract_properties = []
            for name in dir(resolved_class):
                if name.startswith('_'):
                    continue
                attr = getattr(resolved_class, name, None)
                if isinstance(attr, (property, abc.abstractproperty)) or getattr(attr, '__isabstractmethod__', False):
                    abstract_properties.append(name)
            
            context.set_variable(output_var, abstract_properties)
            return ActionResult(success=True, message=f"found {len(abstract_properties)} abstract properties")
        except Exception as e:
            return ActionResult(success=False, message=f"properties failed: {e}")


class ABCGetMROAction(BaseAction):
    """Get method resolution order."""
    action_type = "abc_get_mro"
    display_name = "获取MRO"
    description = "获取类的方法解析顺序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute get MRO."""
        class_path = params.get('class', None)
        output_var = params.get('output_var', 'mro_result')

        try:
            if class_path is None:
                return ActionResult(success=False, message="class is required")
            
            resolved_class = context.resolve_value(class_path) if isinstance(class_path, str) else class_path
            
            if isinstance(resolved_class, str):
                resolved_class = eval(resolved_class, {"__builtins__": __builtins__, "abc": abc}, {})
            
            mro = [cls.__name__ for cls in resolved_class.__mro__]
            context.set_variable(output_var, mro)
            return ActionResult(success=True, message=f"MRO: {' -> '.join(mro)}")
        except Exception as e:
            return ActionResult(success=False, message=f"get_mro failed: {e}")


class ABCIsSubclassAction(BaseAction):
    """Check subclass relationship."""
    action_type = "abc_is_subclass"
    display_name = "检查子类"
    description = "检查类之间的继承关系"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is subclass check."""
        class1 = params.get('class1', None)
        class2 = params.get('class2', None)
        output_var = params.get('output_var', 'is_subclass_result')

        try:
            if class1 is None or class2 is None:
                return ActionResult(success=False, message="class1 and class2 are required")
            
            resolved_class1 = context.resolve_value(class1) if isinstance(class1, str) else class1
            resolved_class2 = context.resolve_value(class2) if isinstance(class2, str) else class2
            
            if isinstance(resolved_class1, str):
                resolved_class1 = eval(resolved_class1, {"__builtins__": __builtins__, "abc": abc}, {})
            if isinstance(resolved_class2, str):
                resolved_class2 = eval(resolved_class2, {"__builtins__": __builtins__, "abc": abc}, {})
            
            result = issubclass(resolved_class1, resolved_class2)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"issubclass({resolved_class1.__name__}, {resolved_class2.__name__}): {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_subclass failed: {e}")


class ABCCreateAbstractAction(BaseAction):
    """Create abstract base class."""
    action_type = "abc_create"
    display_name = "创建抽象类"
    description = "创建新的抽象基类"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute create abstract."""
        name = params.get('name', 'MyAbstractBase')
        methods_str = params.get('methods', '[]')
        output_var = params.get('output_var', 'create_abc_result')

        try:
            resolved_name = context.resolve_value(name) if isinstance(name, str) else name
            resolved_methods = context.resolve_value(methods_str) if isinstance(methods_str, str) else methods_str
            
            if isinstance(resolved_methods, str):
                resolved_methods = eval(resolved_methods, {"__builtins__": __builtins__, "abc": abc}, {})
            
            attrs = {'__abstractmethods__': set()}
            for method in resolved_methods:
                attrs[method] = abc.abstractmethod(lambda self: None)
                attrs['__abstractmethods__'].add(method)
            
            AbstractClass = type(resolved_name, (abc.ABC,), attrs)
            context.set_variable(output_var, AbstractClass)
            return ActionResult(success=True, message=f"created abstract class: {resolved_name}")
        except Exception as e:
            return ActionResult(success=False, message=f"create_abstract failed: {e}")
