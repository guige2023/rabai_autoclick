"""Action loader module for RabAI AutoClick.

Dynamically loads action classes from Python files in the actions directory.
"""

import os
import importlib.util
import inspect
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Type

from .base_action import BaseAction


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
            spec = importlib.util.spec_from_file_location(
                f"actions.{file_path.stem}", 
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
