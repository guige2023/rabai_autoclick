import os
import importlib.util
import inspect
import logging
from typing import Dict, Type, Optional
from pathlib import Path
from .base_action import BaseAction

logger = logging.getLogger(__name__)


class ActionLoader:
    def __init__(self, actions_dir: str = None):
        self._actions: Dict[str, Type[BaseAction]] = {}
        self._actions_dir = actions_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "actions"
        )
    
    def load_all(self) -> Dict[str, Type[BaseAction]]:
        actions_path = Path(self._actions_dir)
        
        if not actions_path.exists():
            return self._actions
        
        for file_path in actions_path.glob("*.py"):
            if file_path.name.startswith("_"):
                continue
            
            self._load_action_from_file(file_path)
        
        return self._actions
    
    def _load_action_from_file(self, file_path: Path) -> Optional[Type[BaseAction]]:
        try:
            spec = importlib.util.spec_from_file_location(
                f"actions.{file_path.stem}", 
                str(file_path)
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            first_action = None
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (issubclass(obj, BaseAction) and 
                    obj is not BaseAction and 
                    hasattr(obj, 'action_type')):
                    self._actions[obj.action_type] = obj
                    if first_action is None:
                        first_action = obj
            return first_action
        except Exception as e:
            logger.error(f"加载动作文件失败 {file_path}: {e}")
        
        return None
    
    def get_action(self, action_type: str) -> Optional[Type[BaseAction]]:
        return self._actions.get(action_type)
    
    def get_all_actions(self) -> Dict[str, Type[BaseAction]]:
        return self._actions.copy()
    
    def get_action_info(self) -> Dict[str, dict]:
        info = {}
        for action_type, action_class in self._actions.items():
            info[action_type] = {
                "display_name": action_class.display_name,
                "description": action_class.description,
                "required_params": action_class().get_required_params(),
                "optional_params": action_class().get_optional_params(),
            }
        return info
    
    def register_action(self, action_class: Type[BaseAction]) -> None:
        if issubclass(action_class, BaseAction) and hasattr(action_class, 'action_type'):
            self._actions[action_class.action_type] = action_class
