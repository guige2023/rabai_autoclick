from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass


@dataclass
class ActionResult:
    success: bool
    message: str = ""
    data: Any = None
    next_step_id: Optional[int] = None


class BaseAction(ABC):
    action_type: str = "base"
    display_name: str = "基础动作"
    description: str = "动作基类"
    
    def __init__(self):
        self.params: Dict[str, Any] = {}
    
    def set_params(self, params: Dict[str, Any]) -> None:
        self.params = params
    
    @abstractmethod
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        pass
    
    def validate_params(self, params: Dict[str, Any]) -> tuple:
        return True, ""
    
    def get_param(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {}
