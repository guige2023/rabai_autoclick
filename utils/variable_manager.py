"""Variable manager for RabAI AutoClick.

Provides Variable and VariableManager classes for managing workflow variables
with type safety, validation, and PyQt signal support.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from PyQt5.QtCore import QObject, pyqtSignal


class VariableType(Enum):
    """Enumeration of supported variable types."""
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
    """Represents a typed variable with default and current values."""
    
    name: str
    var_type: VariableType
    default_value: Any
    description: str = ""
    current_value: Any = None
    
    def __post_init__(self) -> None:
        """Initialize current_value from default_value if not set."""
        if self.current_value is None:
            self.current_value = self.default_value
    
    def reset(self) -> None:
        """Reset current_value to default_value."""
        self.current_value = self.default_value
    
    def set_value(self, value: Any) -> None:
        """Set the current value.
        
        Args:
            value: New value to set.
        """
        self.current_value = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert variable to dictionary representation.
        
        Returns:
            Dictionary with name, var_type, default_value, description, current_value.
        """
        return {
            'name': self.name,
            'var_type': self.var_type.value,
            'default_value': self.default_value,
            'description': self.description,
            'current_value': self.current_value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Variable':
        """Create a Variable from a dictionary.
        
        Args:
            data: Dictionary with variable data.
            
        Returns:
            New Variable instance.
        """
        return cls(
            name=data['name'],
            var_type=VariableType(data.get('var_type', 'string')),
            default_value=data.get('default_value', ''),
            description=data.get('description', ''),
            current_value=data.get('current_value')
        )


class VariableManager(QObject):
    """Manage workflow variables with type safety and PyQt signals."""
    
    # PyQt signal emitted when variables change
    variables_changed = pyqtSignal()
    
    def __init__(self) -> None:
        """Initialize the variable manager."""
        super().__init__()
        self._variables: Dict[str, Variable] = {}
        self._output_mappings: Dict[str, str] = {}
    
    def add_variable(
        self, 
        name: str, 
        var_type: VariableType = VariableType.STRING,
        default_value: Any = None, 
        description: str = ""
    ) -> bool:
        """Add a new variable.
        
        Args:
            name: Variable name.
            var_type: Type of the variable.
            default_value: Default value (uses type default if None).
            description: Optional description.
            
        Returns:
            True if added, False if name already exists.
        """
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
        """Remove a variable by name.
        
        Args:
            name: Name of variable to remove.
            
        Returns:
            True if removed, False if not found.
        """
        if name not in self._variables:
            return False
        
        del self._variables[name]
        self.variables_changed.emit()
        return True
    
    def get_variable(self, name: str) -> Optional[Variable]:
        """Get a Variable object by name.
        
        Args:
            name: Variable name.
            
        Returns:
            Variable object, or None if not found.
        """
        return self._variables.get(name)
    
    def get_value(self, name: str) -> Any:
        """Get the current value of a variable.
        
        Args:
            name: Variable name.
            
        Returns:
            Current value, or None if not found.
        """
        var = self._variables.get(name)
        return var.current_value if var else None
    
    def set_value(self, name: str, value: Any) -> bool:
        """Set the current value of a variable.
        
        Args:
            name: Variable name.
            value: New value.
            
        Returns:
            True if set, False if variable not found.
        """
        if name not in self._variables:
            return False
        
        self._variables[name].set_value(value)
        return True
    
    def get_all_variables(self) -> Dict[str, Variable]:
        """Get all variables.
        
        Returns:
            Dictionary of name -> Variable.
        """
        return self._variables.copy()
    
    def get_variable_names(self) -> List[str]:
        """Get all variable names.
        
        Returns:
            List of variable names.
        """
        return list(self._variables.keys())
    
    def get_variables_by_type(self, var_type: VariableType) -> List[str]:
        """Get variable names filtered by type.
        
        Args:
            var_type: VariableType to filter by.
            
        Returns:
            List of matching variable names.
        """
        return [
            name for name, var in self._variables.items() 
            if var.var_type == var_type
        ]
    
    def reset_all(self) -> None:
        """Reset all variables to their default values."""
        for var in self._variables.values():
            var.reset()
        self.variables_changed.emit()
    
    def clear(self) -> None:
        """Clear all variables."""
        self._variables.clear()
        self._output_mappings.clear()
        self.variables_changed.emit()
    
    def load_from_dict(self, data: Dict[str, Any]) -> None:
        """Load variables from a dictionary.
        
        Args:
            data: Dictionary of name -> variable data.
        """
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
        """Export all variables to a dictionary.
        
        Returns:
            Dictionary of name -> variable dict.
        """
        return {name: var.to_dict() for name, var in self._variables.items()}
    
    def resolve_value(self, value: Any) -> Any:
        """Resolve a variable reference in ${name} format.
        
        Args:
            value: Value that may contain ${name} reference.
            
        Returns:
            Resolved value, or original if not a reference.
        """
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            var_name = value[2:-1]
            return self.get_value(var_name)
        return value
    
    def _get_default_for_type(self, var_type: VariableType) -> Any:
        """Get default value for a variable type.
        
        Args:
            var_type: VariableType enum value.
            
        Returns:
            Default value for the type.
        """
        defaults: Dict[VariableType, Any] = {
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
        """Validate that a value matches the variable's type.
        
        Args:
            name: Variable name.
            value: Value to validate.
            
        Returns:
            True if value matches type, False otherwise.
        """
        var = self._variables.get(name)
        if not var:
            return False
        
        type_validators: Dict[
            VariableType, Callable[[Any], bool]
        ] = {
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


# Global singleton instance
variable_manager: VariableManager = VariableManager()
