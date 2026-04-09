"""Action loader module for RabAI AutoClick.

Dynamically loads action classes from Python files in the actions directory.
"""

import os
import sys
import ast
import re
import time
import random
import importlib.util
import importlib
import inspect
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Type

from .base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ActionLoader:
    """Dynamically loads and registers action classes from Python files.
    
    Scans the actions directory for Python files and imports action classes
    that inherit from BaseAction and define action_type.
    """
    
    def __init__(self, actions_dir: Optional[str] = None) -> None:
        """Initialize the action loader.
        
        Args:
            actions_dir: Optional custom directory for action modules.
                        Defaults to the project 'actions' directory.
        """
        if actions_dir is None:
            actions_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                "actions"
            )
        self._actions_dir: str = actions_dir
        self._actions: Dict[str, Type[BaseAction]] = {}
    
    def load_all(self) -> Dict[str, Type[BaseAction]]:
        """Load all action classes from the actions directory.
        
        Returns:
            Dictionary mapping action_type to action class.
        """
        actions_path = Path(self._actions_dir)
        
        if not actions_path.exists():
            logger.warning(f"Actions directory does not exist: {self._actions_dir}")
            return self._actions
        
        for file_path in sorted(actions_path.glob("*.py")):
            if file_path.name.startswith("_"):
                continue
            self._load_action_from_file(file_path)
        
        logger.info(f"Loaded {len(self._actions)} actions: {list(self._actions.keys())}")
        return self._actions
    
    def _load_action_from_file(self, file_path: Path) -> Optional[Type[BaseAction]]:
        """Load action classes from a single Python file.
        
        Args:
            file_path: Path to the Python file.
            
        Returns:
            The first loaded action class, or None if none found.
        """
        try:
            # Check if we're loading from the default actions directory
            # (within the rabai_autoclick package)
            default_actions_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                "actions"
            )
            is_default_actions = self._actions_dir == default_actions_dir
            
            if is_default_actions:
                # For default actions directory, we need to handle the fact that
                # 'actions' is a package within 'rabai_autoclick' but we may be 
                # running from the source directory without the package being installed.
                # 
                # Strategy: Pre-process the code to remove relative import statements
                # since we will provide the imported names directly in the exec namespace.
                import sys
                
                project_root = os.path.dirname(os.path.dirname(__file__))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                
                # Import base classes from the same module that action_loader uses
                # to ensure we get the SAME class objects (not duplicate class defs)
                from rabai_autoclick.core.base_action import BaseAction, ActionResult
                
                # Read the action file
                with open(file_path, 'r', encoding='utf-8') as f:
                    source = f.read()
                
                # Parse and transform the AST to replace relative imports
                tree = ast.parse(source)
                
                # Transformer to replace relative imports with pass statements
                class RelativeImportReplacer(ast.NodeTransformer):
                    def visit_ImportFrom(self, node):
                        if node.module and node.module.startswith('..'):
                            # Replace relative import with assignment of None for each alias
                            # This allows the code to execute without the actual import
                            return ast.Pass()
                        return node
                
                transformer = RelativeImportReplacer()
                new_tree = transformer.visit(tree)
                ast.fix_missing_locations(new_tree)
                
                # Compile the modified AST
                code = compile(new_tree, str(file_path), 'exec')
                
                # Create a module-like namespace with all needed imports
                module_globals = {
                    '__name__': f'actions.{file_path.stem}',
                    '__file__': str(file_path),
                    '__package__': 'actions',
                    '__builtins__': __builtins__,
                    'BaseAction': BaseAction,
                    'ActionResult': ActionResult,
                    'ast': ast,
                    're': re,
                    'time': time,
                    'random': random,
                    'sys': sys,
                    'os': os,
                }
                # Add typing module
                import typing
                module_globals['typing'] = typing
                
                exec(code, module_globals)
                
                # Get all classes from the module_globals that inherit from BaseAction
                first_action: Optional[Type[BaseAction]] = None
                for name, obj in module_globals.items():
                    if (isinstance(obj, type) and 
                        issubclass(obj, BaseAction) and 
                        obj is not BaseAction and 
                        hasattr(obj, 'action_type')):
                        self._actions[obj.action_type] = obj
                        if first_action is None:
                            first_action = obj
                        logger.debug(f"Loaded action: {obj.action_type} from {file_path.name}")
                
                return first_action
            else:
                # For custom action directories, use spec_from_file_location
                spec = importlib.util.spec_from_file_location(
                    f"{file_path.stem}", 
                    str(file_path)
                )
                if spec is None or spec.loader is None:
                    return None
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                first_action: Optional[Type[BaseAction]] = None
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (issubclass(obj, BaseAction) and 
                        obj is not BaseAction and 
                        hasattr(obj, 'action_type')):
                        self._actions[obj.action_type] = obj
                        if first_action is None:
                            first_action = obj
                        logger.debug(f"Loaded action: {obj.action_type} from {file_path.name}")
                
                return first_action
        except Exception as e:
            logger.error(f"加载动作文件失败 {file_path}: {e}")
            return None
    
    def get_action(self, action_type: str) -> Optional[Type[BaseAction]]:
        """Get an action class by its type name.
        
        Args:
            action_type: The action_type string (e.g., 'click', 'delay').
            
        Returns:
            The action class, or None if not found.
        """
        return self._actions.get(action_type)
    
    def get_all_actions(self) -> Dict[str, Type[BaseAction]]:
        """Get a copy of all registered actions.
        
        Returns:
            Dictionary mapping action_type to action class.
        """
        return self._actions.copy()
    
    def get_action_info(self) -> Dict[str, Dict[str, Any]]:
        """Get metadata about all registered actions.
        
        Returns:
            Dictionary mapping action_type to dict with display_name,
            description, required_params, and optional_params.
        """
        info: Dict[str, Dict[str, Any]] = {}
        
        for action_type, action_class in self._actions.items():
            try:
                instance = action_class()
                info[action_type] = {
                    "display_name": action_class.display_name,
                    "description": action_class.description,
                    "required_params": action_class.get_required_params(),
                    "optional_params": action_class.get_optional_params(),
                }
            except Exception as e:
                logger.warning(
                    f"Failed to get info for action {action_type}: {e}"
                )
                info[action_type] = {
                    "display_name": action_class.display_name,
                    "description": action_class.description,
                    "required_params": [],
                    "optional_params": {},
                }
        
        return info
    
    def register_action(self, action_class: Type[BaseAction]) -> bool:
        """Manually register an action class.
        
        Args:
            action_class: The action class to register.
            
        Returns:
            True if registered successfully, False otherwise.
        """
        if issubclass(action_class, BaseAction) and hasattr(action_class, 'action_type'):
            self._actions[action_class.action_type] = action_class
            return True
        return False
    
    def unregister_action(self, action_type: str) -> bool:
        """Unregister an action by type.
        
        Args:
            action_type: The action_type string to remove.
            
        Returns:
            True if removed, False if not found.
        """
        if action_type in self._actions:
            del self._actions[action_type]
            return True
        return False
