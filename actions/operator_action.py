"""
Operator Action Module

Provides operator module utilities including arithmetic, comparison,
logical, and sequence operators for functional programming style operations.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import operator
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

# Type variables
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
N = TypeVar("N", int, float)


class OperatorAction:
    """
    Main operator action handler providing operator utilities.
    
    This class wraps Python's operator module with additional
    utilities for functional programming and automation tasks.
    
    Attributes:
        OPERATORS: Mapping of operator names to operator functions
    """
    
    # Standard operator mappings
    OPERATORS: Dict[str, Callable[..., Any]] = {
        # Arithmetic operators
        "add": operator.add,
        "sub": operator.sub,
        "mul": operator.mul,
        "div": operator.truediv,
        "floordiv": operator.floordiv,
        "mod": operator.mod,
        "pow": operator.pow,
        "neg": operator.neg,
        "pos": operator.pos,
        "abs": operator.abs,
        
        # Comparison operators
        "eq": operator.eq,
        "ne": operator.ne,
        "lt": operator.lt,
        "le": operator.le,
        "gt": operator.gt,
        "ge": operator.ge,
        
        # Logical operators
        "and_": operator.and_,
        "or_": operator.or_,
        "not_": operator.not_,
        "xor": operator.xor,
        
        # Bitwise operators
        "invert": operator.invert,
        "lshift": operator.lshift,
        "rshift": operator.rshift,
        
        # Sequence operators
        "concat": operator.concat,
        "contains": operator.contains,
        "countOf": operator.countOf,
        "indexOf": operator.indexOf,
        "getitem": operator.getitem,
        "setitem": operator.setitem,
        "delitem": operator.delitem,
        
        # Type checking
        "is_": operator.is_,
        "is_not": operator.is_not,
        "truth": operator.truth,
        "is_true": operator.truth,
    }
    
    @staticmethod
    def get_operator(name: str) -> Callable[..., Any]:
        """
        Get an operator function by name.
        
        Args:
            name: Operator name (e.g., 'add', 'eq', 'getitem')
        
        Returns:
            Operator function
        
        Raises:
            ValueError: If operator name is not found
        
        Example:
            >>> add = OperatorAction.get_operator("add")
            >>> add(1, 2)
            3
        """
        if name not in OperatorAction.OPERATORS:
            raise ValueError(f"Unknown operator: {name}")
        return OperatorAction.OPERATORS[name]
    
    @staticmethod
    def add(a: N, b: N) -> N:
        """
        Addition operator: a + b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Sum of a and b
        
        Example:
            >>> OperatorAction.add(1, 2)
            3
        """
        return operator.add(a, b)
    
    @staticmethod
    def subtract(a: N, b: N) -> N:
        """
        Subtraction operator: a - b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Difference of a and b
        
        Example:
            >>> OperatorAction.subtract(5, 3)
            2
        """
        return operator.sub(a, b)
    
    @staticmethod
    def multiply(a: N, b: N) -> N:
        """
        Multiplication operator: a * b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Product of a and b
        
        Example:
            >>> OperatorAction.multiply(4, 5)
            20
        """
        return operator.mul(a, b)
    
    @staticmethod
    def divide(a: N, b: N) -> float:
        """
        Division operator: a / b
        
        Args:
            a: Numerator
            b: Denominator
        
        Returns:
            Quotient of a and b
        
        Example:
            >>> OperatorAction.divide(10, 3)
            3.333...
        """
        return operator.truediv(a, b)
    
    @staticmethod
    def floordiv(a: N, b: N) -> int:
        """
        Floor division operator: a // b
        
        Args:
            a: Numerator
            b: Denominator
        
        Returns:
            Floor of quotient
        
        Example:
            >>> OperatorAction.floordiv(10, 3)
            3
        """
        return operator.floordiv(a, b)
    
    @staticmethod
    def modulo(a: N, b: N) -> N:
        """
        Modulo operator: a % b
        
        Args:
            a: Dividend
            b: Divisor
        
        Returns:
            Remainder
        
        Example:
            >>> OperatorAction.modulo(10, 3)
            1
        """
        return operator.mod(a, b)
    
    @staticmethod
    def power(a: N, b: N) -> N:
        """
        Power operator: a ** b
        
        Args:
            a: Base
            b: Exponent
        
        Returns:
            a raised to the power of b
        
        Example:
            >>> OperatorAction.power(2, 8)
            256
        """
        return operator.pow(a, b)
    
    @staticmethod
    def negate(a: N) -> N:
        """
        Negation operator: -a
        
        Args:
            a: Operand
        
        Returns:
            Negative of a
        
        Example:
            >>> OperatorAction.negate(5)
            -5
        """
        return operator.neg(a)
    
    @staticmethod
    def absolute(a: N) -> N:
        """
        Absolute value: abs(a)
        
        Args:
            a: Operand
        
        Returns:
            Absolute value of a
        
        Example:
            >>> OperatorAction.absolute(-5)
            5
        """
        return operator.abs(a)
    
    @staticmethod
    def equal(a: Any, b: Any) -> bool:
        """
        Equality comparison: a == b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a == b
        
        Example:
            >>> OperatorAction.equal(1, 1)
            True
        """
        return operator.eq(a, b)
    
    @staticmethod
    def not_equal(a: Any, b: Any) -> bool:
        """
        Inequality comparison: a != b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a != b
        
        Example:
            >>> OperatorAction.not_equal(1, 2)
            True
        """
        return operator.ne(a, b)
    
    @staticmethod
    def less_than(a: N, b: N) -> bool:
        """
        Less than comparison: a < b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a < b
        
        Example:
            >>> OperatorAction.less_than(1, 2)
            True
        """
        return operator.lt(a, b)
    
    @staticmethod
    def less_equal(a: N, b: N) -> bool:
        """
        Less than or equal: a <= b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a <= b
        
        Example:
            >>> OperatorAction.less_equal(2, 2)
            True
        """
        return operator.le(a, b)
    
    @staticmethod
    def greater_than(a: N, b: N) -> bool:
        """
        Greater than comparison: a > b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a > b
        
        Example:
            >>> OperatorAction.greater_than(3, 2)
            True
        """
        return operator.gt(a, b)
    
    @staticmethod
    def greater_equal(a: N, b: N) -> bool:
        """
        Greater than or equal: a >= b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a >= b
        
        Example:
            >>> OperatorAction.greater_equal(3, 3)
            True
        """
        return operator.ge(a, b)
    
    @staticmethod
    def logical_and(a: Any, b: Any) -> Any:
        """
        Logical AND: a and b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            a if a is falsy, else b
        
        Example:
            >>> OperatorAction.logical_and(True, False)
            False
        """
        return operator.and_(a, b)
    
    @staticmethod
    def logical_or(a: Any, b: Any) -> Any:
        """
        Logical OR: a or b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            a if a is truthy, else b
        
        Example:
            >>> OperatorAction.logical_or(False, True)
            True
        """
        return operator.or_(a, b)
    
    @staticmethod
    def logical_not(a: Any) -> bool:
        """
        Logical NOT: not a
        
        Args:
            a: Operand
        
        Returns:
            Negated truth value
        
        Example:
            >>> OperatorAction.logical_not(True)
            False
        """
        return operator.not_(a)
    
    @staticmethod
    def bitwise_and(a: int, b: int) -> int:
        """
        Bitwise AND: a & b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Bitwise AND result
        
        Example:
            >>> OperatorAction.bitwise_and(0b1100, 0b1010)
            8
        """
        return operator.and_(a, b)
    
    @staticmethod
    def bitwise_or(a: int, b: int) -> int:
        """
        Bitwise OR: a | b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Bitwise OR result
        
        Example:
            >>> OperatorAction.bitwise_or(0b1100, 0b1010)
            14
        """
        return operator.or_(a, b)
    
    @staticmethod
    def bitwise_xor(a: int, b: int) -> int:
        """
        Bitwise XOR: a ^ b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Bitwise XOR result
        
        Example:
            >>> OperatorAction.bitwise_xor(0b1100, 0b1010)
            6
        """
        return operator.xor(a, b)
    
    @staticmethod
    def bitwise_not(a: int) -> int:
        """
        Bitwise NOT: ~a
        
        Args:
            a: Operand
        
        Returns:
            Bitwise NOT result
        
        Example:
            >>> OperatorAction.bitwise_not(0)
            -1
        """
        return operator.invert(a)
    
    @staticmethod
    def left_shift(a: int, b: int) -> int:
        """
        Left shift: a << b
        
        Args:
            a: Value to shift
            b: Number of bits to shift
        
        Returns:
            Shifted value
        
        Example:
            >>> OperatorAction.left_shift(1, 8)
            256
        """
        return operator.lshift(a, b)
    
    @staticmethod
    def right_shift(a: int, b: int) -> int:
        """
        Right shift: a >> b
        
        Args:
            a: Value to shift
            b: Number of bits to shift
        
        Returns:
            Shifted value
        
        Example:
            >>> OperatorAction.right_shift(256, 8)
            1
        """
        return operator.rshift(a, b)
    
    @staticmethod
    def concat(a: Union[str, List, Tuple], b: Union[str, List, Tuple]) -> Union[str, List, Tuple]:
        """
        Concatenation: a + b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            Concatenated result
        
        Example:
            >>> OperatorAction.concat([1, 2], [3, 4])
            [1, 2, 3, 4]
        """
        return operator.concat(a, b)
    
    @staticmethod
    def get_item(obj: Any, index: Any) -> Any:
        """
        Get item: obj[index]
        
        Args:
            obj: Object to index
            index: Index or key
        
        Returns:
            Item at index
        
        Example:
            >>> OperatorAction.get_item([1, 2, 3], 1)
            2
        """
        return operator.getitem(obj, index)
    
    @staticmethod
    def set_item(obj: Any, index: Any, value: Any) -> None:
        """
        Set item: obj[index] = value
        
        Args:
            obj: Object to modify
            index: Index or key
            value: Value to set
        
        Example:
            >>> lst = [1, 2, 3]
            >>> OperatorAction.set_item(lst, 1, 10)
            >>> lst
            [1, 10, 3]
        """
        operator.setitem(obj, index, value)
    
    @staticmethod
    def delete_item(obj: Any, index: Any) -> None:
        """
        Delete item: del obj[index]
        
        Args:
            obj: Object to modify
            index: Index or key to delete
        
        Example:
            >>> lst = [1, 2, 3]
            >>> OperatorAction.delete_item(lst, 1)
            >>> lst
            [1, 3]
        """
        operator.delitem(obj, index)
    
    @staticmethod
    def contains(container: Any, item: Any) -> bool:
        """
        Contains check: item in container
        
        Args:
            container: Container to check
            item: Item to find
        
        Returns:
            True if item is in container
        
        Example:
            >>> OperatorAction.contains([1, 2, 3], 2)
            True
        """
        return operator.contains(container, item)
    
    @staticmethod
    def count_of(seq: Union[List, Tuple], item: Any) -> int:
        """
        Count occurrences of item in sequence.
        
        Args:
            seq: Sequence to search
            item: Item to count
        
        Returns:
            Count of occurrences
        
        Example:
            >>> OperatorAction.count_of([1, 2, 2, 3, 2], 2)
            3
        """
        return operator.countOf(seq, item)
    
    @staticmethod
    def index_of(seq: Union[List, Tuple], item: Any) -> int:
        """
        Find index of item in sequence.
        
        Args:
            seq: Sequence to search
            item: Item to find
        
        Returns:
            Index of item
        
        Raises:
            ValueError: If item not found
        
        Example:
            >>> OperatorAction.index_of([1, 2, 3], 2)
            1
        """
        return operator.indexOf(seq, item)
    
    @staticmethod
    def is_same(a: Any, b: Any) -> bool:
        """
        Identity check: a is b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a and b are the same object
        
        Example:
            >>> a = [1, 2]
            >>> b = a
            >>> OperatorAction.is_same(a, b)
            True
        """
        return operator.is_(a, b)
    
    @staticmethod
    def is_not_same(a: Any, b: Any) -> bool:
        """
        Identity check: a is not b
        
        Args:
            a: First operand
            b: Second operand
        
        Returns:
            True if a and b are different objects
        
        Example:
            >>> OperatorAction.is_not_same([1], [1])
            True
        """
        return operator.is_not(a, b)
    
    @staticmethod
    def truth(value: Any) -> bool:
        """
        Truth test: bool(value)
        
        Args:
            value: Value to test
        
        Returns:
            True if value is truthy
        
        Example:
            >>> OperatorAction.truth([])
            False
            >>> OperatorAction.truth([1])
            True
        """
        return operator.truth(value)
    
    @staticmethod
    def invoker(name: str) -> Callable[[Any, Any], Any]:
        """
        Create a function that invokes a named method on an object.
        
        Args:
            name: Method name to invoke
        
        Returns:
            Function that calls the named method
        
        Example:
            >>> upper = OperatorAction.invoker("upper")
            >>> upper("hello")
            'HELLO'
        """
        return operator.attrgetter(name)
    
    @staticmethod
    def method_caller(name: str, *args: Any, **kwargs: Any) -> Callable[[Any], Any]:
        """
        Create a function that calls a named method with arguments.
        
        Args:
            name: Method name to call
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method
        
        Returns:
            Function that calls the named method with arguments
        
        Example:
            >>> capitalize = OperatorAction.method_caller("capitalize")
            >>> capitalize("hello")
            'Hello'
            >>> split = OperatorAction.method_caller("split", sep="-")
            >>> split("a-b-c")
            ['a', 'b', 'c']
        """
        def caller(obj: Any) -> Any:
            return getattr(obj, name)(*args, **kwargs)
        return caller
    
    @staticmethod
    def apply_operator(
        op: str,
        a: Any,
        b: Optional[Any] = None,
    ) -> Any:
        """
        Apply an operator by name.
        
        Args:
            op: Operator name
            a: First operand
            b: Second operand (None for unary operators)
        
        Returns:
            Result of applying the operator
        
        Raises:
            ValueError: If operator is unknown or arguments are invalid
        
        Example:
            >>> OperatorAction.apply_operator("add", 1, 2)
            3
            >>> OperatorAction.apply_operator("neg", 5)
            -5
        """
        op_func = OperatorAction.get_operator(op)
        
        if b is None:
            return op_func(a)
        return op_func(a, b)
    
    @staticmethod
    def list_operators() -> List[str]:
        """
        Get a list of all available operator names.
        
        Returns:
            List of operator names
        """
        return list(OperatorAction.OPERATORS.keys())
    
    @staticmethod
    def apply_to_list(
        op: str,
        items: List[Any],
        initial: Optional[Any] = None,
    ) -> Any:
        """
        Apply an operator to a list of items (reduce).
        
        Args:
            op: Operator name
            items: List of items
            initial: Optional initial value
        
        Returns:
            Result of applying operator across all items
        
        Example:
            >>> OperatorAction.apply_to_list("add", [1, 2, 3, 4])
            10
        """
        if not items:
            if initial is not None:
                return initial
            raise ValueError("Cannot apply to empty list without initial value")
        
        result = items[0]
        for item in items[1:]:
            result = OperatorAction.apply_operator(op, result, item)
        
        return result


# Module-level convenience functions
def add(a: N, b: N) -> N:
    """Addition: a + b"""
    return OperatorAction.add(a, b)


def sub(a: N, b: N) -> N:
    """Subtraction: a - b"""
    return OperatorAction.subtract(a, b)


def mul(a: N, b: N) -> N:
    """Multiplication: a * b"""
    return OperatorAction.multiply(a, b)


def eq(a: Any, b: Any) -> bool:
    """Equality: a == b"""
    return OperatorAction.equal(a, b)


def lt(a: N, b: N) -> bool:
    """Less than: a < b"""
    return OperatorAction.less_than(a, b)


def get_item(obj: Any, index: Any) -> Any:
    """Get item: obj[index]"""
    return OperatorAction.get_item(obj, index)


def set_item(obj: Any, index: Any, value: Any) -> None:
    """Set item: obj[index] = value"""
    OperatorAction.set_item(obj, index, value)


def apply(op: str, a: Any, b: Optional[Any] = None) -> Any:
    """Apply operator by name"""
    return OperatorAction.apply_operator(op, a, b)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "OperatorAction",
    "add",
    "sub",
    "mul",
    "eq",
    "lt",
    "get_item",
    "set_item",
    "apply",
]
