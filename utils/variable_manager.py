
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from PyQt5.QtCore import QObject, pyqtSignal


class VariableType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    COORDINATE = "coordinate"
    REGION = "region"
    LIST = "list"
    DICT = "dict"


@dataclass
class Variable:
    name: str
    var_type: VariableType
    default_value: Any
    description: str = ""
    current_value: Any = None
    
    def __post_init__(self):
        if self.current_value is None:
            self.current_value = self.default_value
    
    def reset(self):
        self.current_value = self.default_value
    
    def set_value(self, value: Any):
        self.current_value = value
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'var_type': self.var_type.value,
            'default_value': self.default_value,
            'description': self.description,
            'current_value': self.current_value
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Variable':
        return cls(
            name=data['name'],
            var_type=VariableType(data.get('var_type', 'string')),
            default_value=data.get('default_value', ''),
            description=data.get('description', ''),
            current_value=data.get('current_value')
        )


class VariableManager(QObject):
    variables_changed = pyqtSignal()
    
    def __init__(self):
        super().__init__()
        self._variables: Dict[str, Variable] = {}
        self._output_mappings: Dict[str, str] = {}
    
    def add_variable(self, name: str, var_type: VariableType = VariableType.STRING,
                     default_value: Any = None, description: str = "") -> bool:
        if name in self._variables:
            return False
        
        if default_value is None:
            default_value = self._get_default_for_type(var_type)
        
        self._variables[name] = Variable(
            name=name,
            var_type=var_type,
            default_value=default_value,
            description=description
        )
        self.variables_changed.emit()
        return True
    
    def remove_variable(self, name: str) -> bool:
        if name not in self._variables:
            return False
        
        del self._variables[name]
        self.variables_changed.emit()
        return True
    
    def get_variable(self, name: str) -> Optional[Variable]:
        return self._variables.get(name)
    
    def get_value(self, name: str) -> Any:
        var = self._variables.get(name)
        return var.current_value if var else None
    
    def set_value(self, name: str, value: Any) -> bool:
        if name not in self._variables:
            return False
        
        self._variables[name].set_value(value)
        return True
    
    def get_all_variables(self) -> Dict[str, Variable]:
        return self._variables.copy()
    
    def get_variable_names(self) -> List[str]:
        return list(self._variables.keys())
    
    def get_variables_by_type(self, var_type: VariableType) -> List[str]:
        return [name for name, var in self._variables.items() if var.var_type == var_type]
    
    def reset_all(self):
        for var in self._variables.values():
            var.reset()
        self.variables_changed.emit()
    
    def clear(self):
        self._variables.clear()
        self._output_mappings.clear()
        self.variables_changed.emit()
    
    def load_from_dict(self, data: Dict[str, Any]):
        self._variables.clear()
        for name, var_data in data.items():
            if isinstance(var_data, dict):
                self._variables[name] = Variable.from_dict(var_data)
            else:
                self._variables[name] = Variable(
                    name=name,
                    var_type=VariableType.STRING,
                    default_value=var_data
                )
        self.variables_changed.emit()
    
    def to_dict(self) -> Dict[str, Any]:
        return {name: var.to_dict() for name, var in self._variables.items()}
    
    def resolve_value(self, value: Any) -> Any:
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            var_name = value[2:-1]
            return self.get_value(var_name)
        return value
    
    def _get_default_for_type(self, var_type: VariableType) -> Any:
        defaults = {
            VariableType.STRING: "",
            VariableType.INTEGER: 0,
            VariableType.FLOAT: 0.0,
            VariableType.BOOLEAN: False,
            VariableType.COORDINATE: (0, 0),
            VariableType.REGION: (0, 0, 100, 100),
            VariableType.LIST: [],
            VariableType.DICT: {}
        }
        return defaults.get(var_type, "")
    
    def validate_type(self, name: str, value: Any) -> bool:
        var = self._variables.get(name)
        if not var:
            return False
        
        type_validators = {
            VariableType.STRING: lambda v: isinstance(v, str),
            VariableType.INTEGER: lambda v: isinstance(v, int) and not isinstance(v, bool),
            VariableType.FLOAT: lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            VariableType.BOOLEAN: lambda v: isinstance(v, bool),
            VariableType.COORDINATE: lambda v: isinstance(v, (tuple, list)) and len(v) == 2,
            VariableType.REGION: lambda v: isinstance(v, (tuple, list)) and len(v) == 4,
            VariableType.LIST: lambda v: isinstance(v, list),
            VariableType.DICT: lambda v: isinstance(v, dict)
        }
        
        validator = type_validators.get(var.var_type)
        return validator(value) if validator else True


variable_manager = VariableManager()
