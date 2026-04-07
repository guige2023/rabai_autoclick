"""
Enum Action Module

Provides comprehensive enum operations including creation, iteration,
value mapping, and custom enum implementations for the automation framework.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

# Type variables for generic enum support
E = TypeVar("E", bound=enum.Enum)
T = TypeVar("T")


class EnumAction:
    """
    Main enum action handler providing utility methods for enum operations.
    
    This class wraps Python's built-in enum module functionality with
    additional features for automation workflows.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    @staticmethod
    def create_enum(
        name: str,
        values: Union[List[str], List[Tuple[str, Any]], Dict[str, Any]],
        *,
        module: Optional[str] = None,
        qualname: Optional[str] = None,
        type_: Optional[Type] = None,
        start: int = 1,
        boundary: Optional[enum.EnumBoundary] = None,
    ) -> Type[enum.Enum]:
        """
        Create a new enum class dynamically.
        
        Args:
            name: Name of the enum class
            values: List of values, tuples (name, value), or dict {name: value}
            module: Module name for the enum
            qualname: Qualified name for the enum
            type_: Mixin type for the enum values
            start: Starting number for auto-numbered enums
            boundary: Boundary mode (STRICT, CONFORM, EJECT, WARN)
        
        Returns:
            The created enum class
        
        Raises:
            ValueError: If values list is empty or format is invalid
            TypeError: If type_ is not a valid mixin type
        
        Example:
            >>> Color = EnumAction.create_enum("Color", ["RED", "GREEN", "BLUE"])
            >>> Color.RED
            <Color.RED: 1>
        """
        if not name:
            raise ValueError("Enum name cannot be empty")
        
        if not values:
            raise ValueError("Values list cannot be empty")
        
        # Determine the enum base class to use
        if type_ is int:
            enum_class = enum.IntEnum
        elif type_ is str:
            enum_class = enum.StrEnum
        else:
            enum_class = enum.Enum
        
        # Build kwargs for enum creation
        kwargs: Dict[str, Any] = {}
        if module is not None:
            kwargs["module"] = module
        if qualname is not None:
            kwargs["qualname"] = qualname
        if boundary is not None:
            kwargs["boundary"] = boundary
        
        # Process values based on input type
        if isinstance(values, list):
            if not values:
                raise ValueError("Values list cannot be empty")
            
            # Check if list of tuples or dict
            if isinstance(values[0], tuple) and len(values[0]) == 2:
                # List of (name, value) tuples
                enum_dict = {v[0]: v[1] for v in values}
            elif isinstance(values[0], str):
                # Simple list of names (auto-numbered)
                enum_dict = {v: i + start for i, v in enumerate(values)}
            else:
                raise ValueError(f"Invalid value format in list: {type(values[0])}")
        elif isinstance(values, dict):
            enum_dict = values
        else:
            raise TypeError(f"Values must be list or dict, got {type(values)}")
        
        return enum_class(name, enum_dict, **kwargs)
    
    @staticmethod
    def get_enum_value(enum_member: enum.Enum) -> Any:
        """
        Get the raw value of an enum member.
        
        Args:
            enum_member: An enum member instance
        
        Returns:
            The raw value of the enum member
        
        Raises:
            TypeError: If argument is not an enum member
        
        Example:
            >>> Color = enum.Enum("Color", "RED GREEN BLUE")
            >>> EnumAction.get_enum_value(Color.RED)
            'RED'
        """
        if not isinstance(enum_member, enum.Enum):
            raise TypeError(f"Expected enum member, got {type(enum_member)}")
        return enum_member.value
    
    @staticmethod
    def get_enum_name(enum_member: enum.Enum) -> str:
        """
        Get the name of an enum member.
        
        Args:
            enum_member: An enum member instance
        
        Returns:
            The name string of the enum member
        
        Raises:
            TypeError: If argument is not an enum member
        
        Example:
            >>> Color = enum.Enum("Color", "RED GREEN BLUE")
            >>> EnumAction.get_enum_name(Color.RED)
            'RED'
        """
        if not isinstance(enum_member, enum.Enum):
            raise TypeError(f"Expected enum member, got {type(enum_member)}")
        return enum_member.name
    
    @staticmethod
    def get_enum_by_value(
        enum_class: Type[E],
        value: Any,
        *,
        default: Optional[E] = None,
    ) -> Optional[E]:
        """
        Find enum member by its value.
        
        Args:
            enum_class: The enum class to search
            value: The value to search for
            default: Default value if not found
        
        Returns:
            The enum member with the given value, or default if not found
        
        Raises:
            TypeError: If enum_class is not an enum type
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            ...     BLUE = 3
            >>> EnumAction.get_enum_by_value(Color, 2)
            <Color.GREEN: 2>
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        try:
            return enum_class(value)
        except ValueError:
            return default
    
    @staticmethod
    def get_enum_by_name(
        enum_class: Type[E],
        name: str,
        *,
        default: Optional[E] = None,
    ) -> Optional[E]:
        """
        Find enum member by its name.
        
        Args:
            enum_class: The enum class to search
            name: The name to search for
            default: Default value if not found
        
        Returns:
            The enum member with the given name, or default if not found
        
        Raises:
            TypeError: If enum_class is not an enum type
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            >>> EnumAction.get_enum_by_name(Color, "GREEN")
            <Color.GREEN: 2>
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return getattr(enum_class, name, default)
    
    @staticmethod
    def list_enum_members(enum_class: Type[E]) -> List[E]:
        """
        Get list of all enum members in order.
        
        Args:
            enum_class: The enum class
        
        Returns:
            List of all enum members
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            ...     BLUE = 3
            >>> EnumAction.list_enum_members(Color)
            [<Color.RED: 1>, <Color.GREEN: 2>, <Color.BLUE: 3>]
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return list(enum_class)
    
    @staticmethod
    def list_enum_names(enum_class: Type[E]) -> List[str]:
        """
        Get list of all enum member names.
        
        Args:
            enum_class: The enum class
        
        Returns:
            List of all enum member names
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            >>> EnumAction.list_enum_names(Color)
            ['RED', 'GREEN']
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return [member.name for member in enum_class]
    
    @staticmethod
    def list_enum_values(enum_class: Type[E]) -> List[Any]:
        """
        Get list of all enum member values.
        
        Args:
            enum_class: The enum class
        
        Returns:
            List of all enum member values
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            >>> EnumAction.list_enum_values(Color)
            [1, 2]
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return [member.value for member in enum_class]
    
    @staticmethod
    def iterate_enum(enum_class: Type[E]) -> Iterator[E]:
        """
        Iterate over all enum members.
        
        Args:
            enum_class: The enum class
        
        Yields:
            Each enum member in order
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            >>> for color in EnumAction.iterate_enum(Color):
            ...     print(color)
            Color.RED
            Color.GREEN
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        yield from enum_class
    
    @staticmethod
    def count_enum_members(enum_class: Type[E]) -> int:
        """
        Count the number of enum members.
        
        Args:
            enum_class: The enum class
        
        Returns:
            Number of enum members
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            ...     BLUE = 3
            >>> EnumAction.count_enum_members(Color)
            3
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return len(enum_class)
    
    @staticmethod
    def is_enum_member(value: Any) -> bool:
        """
        Check if a value is an enum member.
        
        Args:
            value: Value to check
        
        Returns:
            True if value is an enum member, False otherwise
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            >>> EnumAction.is_enum_member(Color.RED)
            True
            >>> EnumAction.is_enum_member(1)
            False
        """
        return isinstance(value, enum.Enum)
    
    @staticmethod
    def get_enum_module(enum_class: Type[E]) -> Optional[str]:
        """
        Get the module name where the enum class is defined.
        
        Args:
            enum_class: The enum class
        
        Returns:
            Module name or None if not set
        
        Example:
            >>> import os
            >>> EnumAction.get_enum_module(os.O_RDONLY.__class__)
            'os'
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return getattr(enum_class, "__module__", None)
    
    @staticmethod
    def get_enum_class_name(enum_class: Type[E]) -> str:
        """
        Get the class name of an enum.
        
        Args:
            enum_class: The enum class
        
        Returns:
            The name of the enum class
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            >>> EnumAction.get_enum_class_name(Color)
            'Color'
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        return enum_class.__name__
    
    @staticmethod
    def convert_to_enum(
        value: Any,
        enum_class: Type[E],
        *,
        ignore_case: bool = False,
    ) -> Optional[E]:
        """
        Convert a value to an enum member, with optional case-insensitive matching.
        
        Args:
            value: The value to convert
            enum_class: Target enum class
            ignore_case: If True, perform case-insensitive name matching
        
        Returns:
            The matching enum member or None if not found
        
        Example:
            >>> class Color(enum.Enum):
            ...     RED = 1
            ...     GREEN = 2
            >>> EnumAction.convert_to_enum(1, Color)
            <Color.RED: 1>
        """
        if not isinstance(enum_class, type) or not issubclass(enum_class, enum.Enum):
            raise TypeError(f"Expected enum class, got {type(enum_class)}")
        
        # Try direct value lookup first
        result = EnumAction.get_enum_by_value(enum_class, value)
        if result is not None:
            return result
        
        # Try name matching if string
        if isinstance(value, str):
            if ignore_case:
                upper_name = value.upper()
                for member in enum_class:
                    if member.name.upper() == upper_name:
                        return member
            else:
                return EnumAction.get_enum_by_name(enum_class, value)
        
        return None
    
    @staticmethod
    def create_flag_enum(
        name: str,
        values: Union[List[str], Dict[str, int]],
        *,
        combine_limit: Optional[int] = None,
    ) -> Type[enum.IntFlag]:
        """
        Create a new IntFlag enum for bitwise operations.
        
        Args:
            name: Name of the flag enum
            values: List of flag names or dict {name: value}
            combine_limit: Maximum combined values before warning
        
        Returns:
            The created IntFlag class
        
        Example:
            >>> Permissions = EnumAction.create_flag_enum("Permissions", 
            ...     {"READ": 1, "WRITE": 2, "EXECUTE": 4})
            >>> Permissions.READ | Permissions.WRITE
            <Permissions.READ|WRITE: 3>
        """
        if isinstance(values, list):
            enum_dict = {v: 1 << i for i, v in enumerate(values)}
        else:
            enum_dict = values
        
        flag_class = enum.IntFlag(name, enum_dict)
        
        if combine_limit is not None:
            # Add warning for combinations exceeding limit
            original_or = flag_class.__or__
            original_and = flag_class.__and__
            
            def warn_or(self, other):
                result = original_or(self, other)
                if result.bit_count() > combine_limit:
                    import warnings
                    warnings.warn(
                        f"Flag combination exceeds limit {combine_limit}",
                        category=UserWarning,
                        stacklevel=2,
                    )
                return result
            
            flag_class.__or__ = warn_or
        
        return flag_class
    
    @staticmethod
    def is_flag_combined(flag_value: enum.IntFlag) -> bool:
        """
        Check if a flag value is a combination of multiple flags.
        
        Args:
            flag_value: A flag enum member
        
        Returns:
            True if value is a combination, False if single flag
        
        Example:
            >>> class Perm(enum.IntFlag):
            ...     R = 1
            ...     W = 2
            ...     X = 4
            >>> EnumAction.is_flag_combined(Perm.R)
            False
            >>> EnumAction.is_flag_combined(Perm.R | Perm.W)
            True
        """
        if not isinstance(flag_value, enum.IntFlag):
            raise TypeError(f"Expected IntFlag member, got {type(flag_value)}")
        
        return flag_value.bit_count() > 1
    
    @staticmethod
    def get_flag_values(flag_value: enum.IntFlag) -> List[enum.IntFlag]:
        """
        Get individual flag values from a combined flag.
        
        Args:
            flag_value: A flag enum member (possibly combined)
        
        Returns:
            List of individual flag members
        
        Example:
            >>> class Perm(enum.IntFlag):
            ...     R = 1
            ...     W = 2
            ...     X = 4
            >>> EnumAction.get_flag_values(Perm.R | Perm.W)
            [<Perm.R: 1>, <Perm.W: 2>]
        """
        if not isinstance(flag_value, enum.IntFlag):
            raise TypeError(f"Expected IntFlag member, got {type(flag_value)}")
        
        result = []
        for member in flag_value.__class__:
            if flag_value & member:
                result.append(member)
        return result


# Convenience function aliases
def create_enum(name: str, values: Any, **kwargs: Any) -> Type[enum.Enum]:
    """Create a new enum class dynamically."""
    return EnumAction.create_enum(name, values, **kwargs)


def get_enum_value(member: enum.Enum) -> Any:
    """Get the raw value of an enum member."""
    return EnumAction.get_enum_value(member)


def get_enum_by_value(enum_class: Type[E], value: Any, **kwargs: Any) -> Optional[E]:
    """Find enum member by its value."""
    return EnumAction.get_enum_by_value(enum_class, value, **kwargs)


def list_members(enum_class: Type[E]) -> List[E]:
    """Get list of all enum members."""
    return EnumAction.list_enum_members(enum_class)


def iterate(enum_class: Type[E]) -> Iterator[E]:
    """Iterate over enum members."""
    return EnumAction.iterate_enum(enum_class)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "EnumAction",
    "create_enum",
    "get_enum_value",
    "get_enum_by_value",
    "list_members",
    "iterate",
]
