"""
Stack data structure utilities and algorithms.

Provides:
- Stack implementation with type hints
- Stack-based algorithms (expression evaluation, DFS, etc.)
- History/undo stack
- Expression parsing (infix to postfix)
- Bracket matching
"""

from __future__ import annotations

import operator
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


# ------------------------------------------------------------------------------
# Stack Implementation
# ------------------------------------------------------------------------------

class Stack(Generic[T]):
    """
    Generic type-safe stack implementation.

    Uses a Python list as the underlying storage (LIFO).

    Example:
        >>> s = Stack[int]()
        >>> s.push(1).push(2).push(3)
        >>> s.pop()
        3
        >>> s.peek()
        2
        >>> s.size()
        2
    """

    def __init__(self, initial: Iterable[T] | None = None) -> None:
        """
        Initialize stack.

        Args:
            initial: Optional initial items (pushed in order).
        """
        self._data: list[T] = []
        if initial is not None:
            for item in initial:
                self._data.append(item)

    def push(self, item: T) -> "Stack[T]":
        """
        Push item onto stack.

        Args:
            item: Item to push.

        Returns:
            Self for chaining.
        """
        self._data.append(item)
        return self

    def pop(self) -> T:
        """
        Pop and return top item.

        Returns:
            Top item.

        Raises:
            IndexError: If stack is empty.
        """
        if not self._data:
            raise IndexError("pop from empty stack")
        return self._data.pop()

    def peek(self) -> T:
        """
        Return top item without removing it.

        Returns:
            Top item.

        Raises:
            IndexError: If stack is empty.
        """
        if not self._data:
            raise IndexError("peek from empty stack")
        return self._data[-1]

    def pop_many(self, n: int) -> list[T]:
        """
        Pop n items from stack.

        Args:
            n: Number of items to pop.

        Returns:
            List of popped items (in pop order, so reversed).

        Raises:
            IndexError: If fewer than n items in stack.
        """
        if n > len(self._data):
            raise IndexError(f"pop_many({n}) from stack of size {len(self._data)}")
        items = self._data[-n:]
        self._data[-n:] = []
        return items

    def push_many(self, items: Iterable[T]) -> "Stack[T]":
        """
        Push multiple items onto stack.

        Args:
            items: Items to push (in order).

        Returns:
            Self for chaining.
        """
        for item in items:
            self._data.append(item)
        return self

    def size(self) -> int:
        """Return number of items in stack."""
        return len(self._data)

    def is_empty(self) -> bool:
        """Check if stack is empty."""
        return len(self._data) == 0

    def is_full(self) -> bool:
        """Check if stack is full (always False for unbounded stack)."""
        return False

    def clear(self) -> None:
        """Remove all items from stack."""
        self._data.clear()

    def to_list(self) -> list[T]:
        """Return stack contents as list (top to bottom)."""
        return list(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    def __repr__(self) -> str:
        return f"Stack({self._data!r})"

    def __iter__(self) -> "StackIterator[T]":
        """Iterate from top to bottom."""
        return StackIterator(self._data)


class StackIterator(Generic[T]):
    """Iterator for Stack."""

    def __init__(self, data: list[T]) -> None:
        self._data = data
        self._index = len(data) - 1

    def __iter__(self) -> "StackIterator[T]":
        return self

    def __next__(self) -> T:
        if self._index < 0:
            raise StopIteration
        item = self._data[self._index]
        self._index -= 1
        return item


# ------------------------------------------------------------------------------
# Bounded Stack
# ------------------------------------------------------------------------------

class BoundedStack(Stack[T]):
    """
    Stack with maximum capacity.

    When full, pushing removes the oldest item (bottom of stack).

    Example:
        >>> s = BoundedStack[int](maxsize=3)
        >>> s.push(1).push(2).push(3)
        >>> s.push(4)  # 1 is dropped
        >>> s.to_list()
        [2, 3, 4]
    """

    def __init__(self, maxsize: int, initial: Iterable[T] | None = None) -> None:
        """
        Initialize bounded stack.

        Args:
            maxsize: Maximum capacity.
            initial: Optional initial items.
        """
        if maxsize < 1:
            raise ValueError(f"maxsize must be >= 1, got {maxsize}")
        self._maxsize = maxsize
        items = list(initial) if initial else []
        super().__init__(items[-maxsize:])

    @property
    def maxsize(self) -> int:
        """Maximum stack size."""
        return self._maxsize

    def is_full(self) -> bool:
        """Check if stack is at capacity."""
        return len(self._data) >= self._maxsize

    def push(self, item: T) -> "BoundedStack[T]":
        """
        Push item, dropping oldest if at capacity.

        Args:
            item: Item to push.

        Returns:
            Self for chaining.
        """
        if len(self._data) >= self._maxsize:
            self._data.pop(0)
        self._data.append(item)
        return self


# ------------------------------------------------------------------------------
# History Stack (Undo/Redo)
# ------------------------------------------------------------------------------

class HistoryStack(Generic[T]):
    """
    Stack with undo capability.

    Tracks a history of states. Supports undo (revert to previous)
    and redo (re-apply reverted state).

    Example:
        >>> h = HistoryStack[str](max_history=10)
        >>> h.push("state1")
        >>> h.push("state2")
        >>> h.undo()
        'state1'
        >>> h.redo_stack
        ['state1', 'state2']
    """

    def __init__(self, max_history: int = 100) -> None:
        """
        Initialize history stack.

        Args:
            max_history: Maximum number of states to remember.
        """
        self._undo_stack: list[T] = []
        self._redo_stack: list[T] = []
        self._max_history = max_history

    def push(self, state: T) -> None:
        """
        Push a new state onto history.

        Args:
            state: New state.
        """
        self._undo_stack.append(state)
        self._redo_stack.clear()
        if len(self._undo_stack) > self._max_history:
            self._undo_stack.pop(0)

    def undo(self) -> T | None:
        """
        Revert to previous state.

        Returns:
            Previous state, or None if no history.
        """
        if not self._undo_stack:
            return None
        state = self._undo_stack.pop()
        self._redo_stack.append(state)
        if self._undo_stack:
            return self._undo_stack[-1]
        return None

    def redo(self) -> T | None:
        """
        Re-apply a reverted state.

        Returns:
            Re-applied state, or None if nothing to redo.
        """
        if not self._redo_stack:
            return None
        state = self._redo_stack.pop()
        self._undo_stack.append(state)
        return state

    def can_undo(self) -> bool:
        """Check if undo is available."""
        return len(self._undo_stack) > 1

    def can_redo(self) -> bool:
        """Check if redo is available."""
        return bool(self._redo_stack)

    def current(self) -> T | None:
        """Get current state (top of undo stack)."""
        if not self._undo_stack:
            return None
        return self._undo_stack[-1]

    def clear(self) -> None:
        """Clear all history."""
        self._undo_stack.clear()
        self._redo_stack.clear()


# ------------------------------------------------------------------------------
# Stack-based Algorithms
# ------------------------------------------------------------------------------

def evaluate_postfix(expression: Sequence[str]) -> float:
    """
    Evaluate a postfix (Reverse Polish Notation) expression.

    Args:
        expression: Sequence of operators and operands.

    Returns:
        Result of evaluation.

    Raises:
        ValueError: If expression is invalid.

    Example:
        >>> evaluate_postfix(['3', '4', '+', '2', '*'])
        14.0
    """
    ops: dict[str, Callable[[float, float], float]] = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "//": operator.floordiv,
        "%": operator.mod,
        "**": operator.pow,
    }

    stack: list[float] = []
    for token in expression:
        token = token.strip()
        if not token:
            continue
        if token in ops:
            if len(stack) < 2:
                raise ValueError(f"Invalid postfix: not enough operands for '{token}'")
            b = stack.pop()
            a = stack.pop()
            stack.append(ops[token](a, b))
        else:
            try:
                stack.append(float(token))
            except ValueError:
                raise ValueError(f"Invalid token in postfix: {token!r}")

    if len(stack) != 1:
        raise ValueError(f"Invalid postfix: {len(stack)} items remain on stack")
    return stack[0]


def infix_to_postfix(tokens: list[str]) -> list[str]:
    """
    Convert infix notation tokens to postfix (RPN).

    Args:
        tokens: Infix tokens (operators as strings).

    Returns:
        Postfix token list.

    Example:
        >>> infix_to_postfix(['3', '+', '4', '*', '2'])
        ['3', '4', '2', '*', '+']
    """
    precedence: dict[str, int] = {
        "+": 1,
        "-": 1,
        "*": 2,
        "/": 2,
        "//": 2,
        "%": 2,
        "**": 3,
    }
    left_assoc = {"+", "-", "*", "/", "//", "%"}

    output: list[str] = []
    op_stack: list[str] = []

    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token == "(":
            op_stack.append(token)
        elif token == ")":
            while op_stack and op_stack[-1] != "(":
                output.append(op_stack.pop())
            if op_stack:
                op_stack.pop()  # Remove '('
        elif token in precedence:
            while op_stack and op_stack[-1] != "(":
                top = op_stack[-1]
                if top not in precedence:
                    break
                top_prec = precedence[top]
                token_prec = precedence[token]
                if top_prec > token_prec or (top_prec == token_prec and token in left_assoc):
                    output.append(op_stack.pop())
                else:
                    break
            op_stack.append(token)
        else:
            output.append(token)

    while op_stack:
        output.append(op_stack.pop())

    return output


def evaluate_infix(expression: Sequence[str]) -> float:
    """
    Evaluate an infix expression.

    Args:
        expression: Infix tokens.

    Returns:
        Result.

    Example:
        >>> evaluate_infix(['3', '+', '4', '*', '2'])
        11.0
    """
    postfix = infix_to_postfix(list(expression))
    return evaluate_postfix(postfix)


# ------------------------------------------------------------------------------
# Bracket Matching
# ------------------------------------------------------------------------------

def balance_brackets(text: str) -> bool:
    """
    Check if brackets are balanced in text.

    Handles: (), [], {}, <>

    Args:
        text: Input text.

    Returns:
        True if all brackets are balanced.

    Example:
        >>> balance_brackets('func(a, [b, {c}])')
        True
        >>> balance_brackets('func(a, [b)')
        False
    """
    opening = "([{<"
    closing = ")]}>"
    pairs: dict[str, str] = {")": "(", "]": "[", "}": "{", ">": "<"}

    stack: list[str] = []
    for char in text:
        if char in opening:
            stack.append(char)
        elif char in closing:
            if not stack or stack[-1] != pairs[char]:
                return False
            stack.pop()
    return len(stack) == 0


def match_brackets(text: str) -> dict[int, int]:
    """
    Find all matching bracket pairs.

    Args:
        text: Input text.

    Returns:
        Dict mapping opening bracket index to closing bracket index.

    Example:
        >>> m = match_brackets('func(a, [b])')
        >>> m[5]  # index of '('
        10  # index of ')'
    """
    opening = "([{<"
    closing = ")]}>"
    pairs: dict[str, str] = {")": "(", "]": "[", "}": "{", ">": "<"}
    close_to_open: dict[str, str] = {v: k for k, v in pairs.items()}

    stack: list[int] = []
    open_stack: list[str] = []
    matches: dict[int, int] = {}

    for i, char in enumerate(text):
        if char in opening:
            stack.append(i)
            open_stack.append(char)
        elif char in closing:
            if not stack:
                continue
            expected = close_to_open[char]
            if open_stack[-1] == expected:
                open_idx = stack.pop()
                matches[open_idx] = i
                open_stack.pop()

    return matches


# ------------------------------------------------------------------------------
# DFS with Stack
# ------------------------------------------------------------------------------

def dfs_traverse(
    graph: dict[str, list[str]],
    start: str,
) -> list[str]:
    """
    Depth-first search traversal using explicit stack.

    Args:
        graph: Adjacency list representation.
        start: Starting node.

    Returns:
        List of visited nodes in DFS order.

    Example:
        >>> g = {'A': ['B', 'C'], 'B': ['D'], 'C': [], 'D': []}
        >>> dfs_traverse(g, 'A')
        ['A', 'B', 'D', 'C']
    """
    visited: set[str] = set()
    stack: list[str] = [start]
    result: list[str] = []

    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        result.append(node)
        # Add neighbors in reverse order to maintain left-to-right order
        neighbors = graph.get(node, [])
        for neighbor in reversed(neighbors):
            if neighbor not in visited:
                stack.append(neighbor)

    return result


def topological_sort(graph: dict[str, list[str]]) -> list[str]:
    """
    Topological sort using Kahn's algorithm with explicit stack.

    Args:
        graph: DAG as adjacency list.

    Returns:
        Topologically sorted node list.

    Raises:
        ValueError: If graph contains a cycle.

    Example:
        >>> g = {'A': [], 'B': ['A'], 'C': ['A', 'B']}
        >>> topological_sort(g)
        ['C', 'B', 'A']
    """
    in_degree: dict[str, int] = {node: 0 for node in graph}
    for neighbors in graph.values():
        for neighbor in neighbors:
            in_degree[neighbor] = in_degree.get(neighbor, 0) + 1

    # Handle nodes not in graph keys
    for node in in_degree:
        pass

    queue: list[str] = [node for node, deg in in_degree.items() if deg == 0]
    stack: list[str] = []

    while queue:
        node = queue.pop()
        stack.append(node)
        for neighbor in graph.get(node, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(stack) != len(in_degree):
        raise ValueError("Graph contains a cycle")

    return stack


# ------------------------------------------------------------------------------
# Expression Evaluation with Stack
# ------------------------------------------------------------------------------

def evaluate_with_stack(
    tokens: list[str],
    operators: dict[str, Callable[[], None]],
    operands: Callable[[str], float],
) -> float:
    """
    Generic stack-based expression evaluator.

    Args:
        tokens: Expression tokens.
        operators: Dict of operator token to function (pops args, pushes result).
        operands: Function to convert token to operand value.

    Returns:
        Evaluation result.
    """
    stack: list[float] = []

    def do_bin_op(op: Callable[[float, float], float]) -> None:
        b = stack.pop()
        a = stack.pop()
        stack.append(op(a, b))

    op_handlers: dict[str, Callable[[], None]] = {}
    for op_token, func in operators.items():
        op_handlers[op_token] = lambda f=func: do_bin_op(f)

    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token in op_handlers:
            op_handlers[token]()
        else:
            stack.append(operands(token))

    if len(stack) != 1:
        raise ValueError(f"Invalid expression, {len(stack)} items on stack")
    return stack[0]
