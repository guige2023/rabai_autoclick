"""
Conditional logic and evaluation actions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, Callable


def evaluate_condition(
    condition: str,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Evaluate a simple condition string.

    Args:
        condition: Condition string (e.g., 'x > 5').
        context: Variables for evaluation.

    Returns:
        Boolean result.
    """
    if context is None:
        context = {}

    condition = condition.strip()

    operators = ['>=', '<=', '!=', '==', '>', '<']

    for op in operators:
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()

                try:
                    left_val = context.get(left, float(left) if left.replace('.', '').isdigit() else left)
                    right_val = context.get(right, float(right) if right.replace('.', '').isdigit() else right)

                    if op == '>=':
                        return left_val >= right_val
                    elif op == '<=':
                        return left_val <= right_val
                    elif op == '!=':
                        return left_val != right_val
                    elif op == '==':
                        return left_val == right_val
                    elif op == '>':
                        return left_val > right_val
                    elif op == '<':
                        return left_val < right_val
                except (ValueError, TypeError):
                    pass

    return False


def is_truly(value: Any) -> bool:
    """
    Check if value is truthy.

    Args:
        value: Value to check.

    Returns:
        True if truthy.
    """
    return bool(value)


def is_falsy(value: Any) -> bool:
    """
    Check if value is falsy.

    Args:
        value: Value to check.

    Returns:
        True if falsy.
    """
    return not bool(value)


def is_none(value: Any) -> bool:
    """
    Check if value is None.

    Args:
        value: Value to check.

    Returns:
        True if None.
    """
    return value is None


def is_empty(value: Any) -> bool:
    """
    Check if value is empty.

    Args:
        value: Value to check.

    Returns:
        True if empty.
    """
    if value is None:
        return True

    if isinstance(value, (str, list, dict, tuple, set)):
        return len(value) == 0

    return False


def is_equal(val1: Any, val2: Any) -> bool:
    """
    Check if two values are equal.

    Args:
        val1: First value.
        val2: Second value.

    Returns:
        True if equal.
    """
    return val1 == val2


def is_not_equal(val1: Any, val2: Any) -> bool:
    """
    Check if two values are not equal.

    Args:
        val1: First value.
        val2: Second value.

    Returns:
        True if not equal.
    """
    return val1 != val2


def is_greater(val1: Union[int, float], val2: Union[int, float]) -> bool:
    """
    Check if val1 > val2.

    Args:
        val1: First value.
        val2: Second value.

    Returns:
        True if greater.
    """
    return val1 > val2


def is_less(val1: Union[int, float], val2: Union[int, float]) -> bool:
    """
    Check if val1 < val2.

    Args:
        val1: First value.
        val2: Second value.

    Returns:
        True if less.
    """
    return val1 < val2


def is_between(
    value: Union[int, float],
    min_val: Union[int, float],
    max_val: Union[int, float]
) -> bool:
    """
    Check if value is between min and max.

    Args:
        value: Value to check.
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        True if between (inclusive).
    """
    return min_val <= value <= max_val


def is_in_list(value: Any, items: List[Any]) -> bool:
    """
    Check if value is in list.

    Args:
        value: Value to check.
        items: List to check.

    Returns:
        True if in list.
    """
    return value in items


def is_not_in_list(value: Any, items: List[Any]) -> bool:
    """
    Check if value is not in list.

    Args:
        value: Value to check.
        items: List to check.

    Returns:
        True if not in list.
    """
    return value not in items


def is_string(value: Any) -> bool:
    """
    Check if value is a string.

    Args:
        value: Value to check.

    Returns:
        True if string.
    """
    return isinstance(value, str)


def is_number(value: Any) -> bool:
    """
    Check if value is a number.

    Args:
        value: Value to check.

    Returns:
        True if number (int or float).
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_integer(value: Any) -> bool:
    """
    Check if value is an integer.

    Args:
        value: Value to check.

    Returns:
        True if integer.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def is_float(value: Any) -> bool:
    """
    Check if value is a float.

    Args:
        value: Value to check.

    Returns:
        True if float.
    """
    return isinstance(value, float)


def is_boolean(value: Any) -> bool:
    """
    Check if value is a boolean.

    Args:
        value: Value to check.

    Returns:
        True if boolean.
    """
    return isinstance(value, bool)


def is_list(value: Any) -> bool:
    """
    Check if value is a list.

    Args:
        value: Value to check.

    Returns:
        True if list.
    """
    return isinstance(value, list)


def is_dict(value: Any) -> bool:
    """
    Check if value is a dictionary.

    Args:
        value: Value to check.

    Returns:
        True if dict.
    """
    return isinstance(value, dict)


def is_positive(value: Union[int, float]) -> bool:
    """
    Check if value is positive.

    Args:
        value: Value to check.

    Returns:
        True if positive.
    """
    return value > 0


def is_negative(value: Union[int, float]) -> bool:
    """
    Check if value is negative.

    Args:
        value: Value to check.

    Returns:
        True if negative.
    """
    return value < 0


def is_zero(value: Union[int, float]) -> bool:
    """
    Check if value is zero.

    Args:
        value: Value to check.

    Returns:
        True if zero.
    """
    return value == 0


def matches_pattern(value: str, pattern: str) -> bool:
    """
    Check if value matches regex pattern.

    Args:
        value: String to check.
        pattern: Regex pattern.

    Returns:
        True if matches.
    """
    import re
    return bool(re.search(pattern, value))


def contains(substring: str, value: str) -> bool:
    """
    Check if value contains substring.

    Args:
        substring: Substring to find.
        value: String to search.

    Returns:
        True if contains.
    """
    return substring in value


def starts_with(value: str, prefix: str) -> bool:
    """
    Check if value starts with prefix.

    Args:
        value: String to check.
        prefix: Prefix to check.

    Returns:
        True if starts with.
    """
    return value.startswith(prefix)


def ends_with(value: str, suffix: str) -> bool:
    """
    Check if value ends with suffix.

    Args:
        value: String to check.
        suffix: Suffix to check.

    Returns:
        True if ends with.
    """
    return value.endswith(suffix)


def and_all(*conditions: bool) -> bool:
    """
    Logical AND of all conditions.

    Args:
        *conditions: Boolean conditions.

    Returns:
        True if all are True.
    """
    return all(conditions)


def or_any(*conditions: bool) -> bool:
    """
    Logical OR of any conditions.

    Args:
        *conditions: Boolean conditions.

    Returns:
        True if any is True.
    """
    return any(conditions)


def not_value(value: bool) -> bool:
    """
    Logical NOT.

    Args:
        value: Boolean value.

    Returns:
        Negated value.
    """
    return not value


def switch_match(
    value: Any,
    cases: Dict[Any, Callable]
) -> Any:
    """
    Match value against cases.

    Args:
        value: Value to match.
        cases: Dictionary of case -> function.

    Returns:
        Result of matched case function.
    """
    if value in cases:
        return cases[value]()

    if 'default' in cases:
        return cases['default']()

    return None


def ternary(condition: bool, true_val: Any, false_val: Any) -> Any:
    """
    Ternary conditional operator.

    Args:
        condition: Condition to check.
        true_val: Value if True.
        false_val: Value if False.

    Returns:
        Selected value.
    """
    return true_val if condition else false_val
