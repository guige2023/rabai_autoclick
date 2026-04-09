"""
Expression Evaluator Action Module

Provides expression parsing and evaluation for dynamic logic in UI automation
workflows. Supports arithmetic, comparison, string operations, and custom functions.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
import math
import operator
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class TokenType(Enum):
    """Token types for expression parser."""
    NUMBER = auto()
    STRING = auto()
    BOOLEAN = auto()
    NULL = auto()
    IDENTIFIER = auto()
    OPERATOR = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    COMMA = auto()
    DOT = auto()
    EOF = auto()


@dataclass
class Token:
    """Represents a token."""
    type: TokenType
    value: Any
    position: int = 0


class Lexer:
    """
    Lexer for tokenizing expressions.

    Example:
        >>> lexer = Lexer("a + b * 2")
        >>> tokens = lexer.tokenize()
    """

    def __init__(self, expression: str) -> None:
        self.expression = expression
        self.position = 0
        self.length = len(expression)

    def tokenize(self) -> list[Token]:
        """Tokenize expression into tokens."""
        tokens: list[Token] = []

        while self.position < self.length:
            token = self._next_token()
            if token.type != TokenType.EOF:
                tokens.append(token)

        tokens.append(Token(type=TokenType.EOF, value=None, position=self.position))
        return tokens

    def _next_token(self) -> Token:
        """Get next token."""
        self._skip_whitespace()

        if self.position >= self.length:
            return Token(type=TokenType.EOF, value=None, position=self.position)

        char = self.expression[self.position]

        if char.isdigit() or (char == "." and self._peek_digits()):
            return self._read_number()
        if char in ("'", '"'):
            return self._read_string()
        if char.isalpha() or char == "_":
            return self._read_identifier()
        if char in "+-*/%^=<>!&|":
            return self._read_operator()
        if char == "(":
            self.position += 1
            return Token(type=TokenType.LPAREN, value="(", position=self.position)
        if char == ")":
            self.position += 1
            return Token(type=TokenType.RPAREN, value=")", position=self.position)
        if char == "[":
            self.position += 1
            return Token(type=TokenType.LBRACKET, value="[", position=self.position)
        if char == "]":
            self.position += 1
            return Token(type=TokenType.RBRACKET, value="]", position=self.position)
        if char == ",":
            self.position += 1
            return Token(type=TokenType.COMMA, value=",", position=self.position)
        if char == ".":
            self.position += 1
            return Token(type=TokenType.DOT, value=".", position=self.position)

        raise ValueError(f"Unexpected character: {char}")

    def _skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while self.position < self.length and self.expression[self.position].isspace():
            self.position += 1

    def _peek_digits(self) -> bool:
        """Check if there are digits after current position."""
        pos = self.position + 1
        while pos < self.length and self.expression[pos].isdigit():
            pos += 1
        return pos > self.position + 1

    def _read_number(self) -> Token:
        """Read number token."""
        start = self.position
        has_dot = False

        while self.position < self.length:
            char = self.expression[self.position]
            if char.isdigit():
                self.position += 1
            elif char == "." and not has_dot:
                has_dot = True
                self.position += 1
            else:
                break

        value_str = self.expression[start:self.position]
        value = float(value_str) if has_dot else int(value_str)
        return Token(type=TokenType.NUMBER, value=value, position=start)

    def _read_string(self) -> Token:
        """Read string token."""
        quote = self.expression[self.position]
        self.position += 1
        start = self.position

        while self.position < self.length:
            char = self.expression[self.position]
            if char == quote:
                value = self.expression[start:self.position]
                self.position += 1
                return Token(type=TokenType.STRING, value=value, position=start)
            if char == "\\" and self.position + 1 < self.length:
                self.position += 2
            else:
                self.position += 1

        raise ValueError("Unterminated string")

    def _read_identifier(self) -> Token:
        """Read identifier token."""
        start = self.position

        while self.position < self.length:
            char = self.expression[self.position]
            if char.isalnum() or char == "_":
                self.position += 1
            else:
                break

        value = self.expression[start:self.position]

        if value == "true":
            return Token(type=TokenType.BOOLEAN, value=True, position=start)
        if value == "false":
            return Token(type=TokenType.BOOLEAN, value=False, position=start)
        if value == "null":
            return Token(type=TokenType.NULL, value=None, position=start)

        return Token(type=TokenType.IDENTIFIER, value=value, position=start)

    def _read_operator(self) -> Token:
        """Read operator token."""
        start = self.position
        char = self.expression[self.position]
        self.position += 1

        if char == "=":
            if self.position < self.length and self.expression[self.position] == "=":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value="==", position=start)
            return Token(type=TokenType.OPERATOR, value="=", position=start)

        if char == "!":
            if self.position < self.length and self.expression[self.position] == "=":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value="!=", position=start)
            return Token(type=TokenType.OPERATOR, value="!", position=start)

        if char == "<":
            if self.position < self.length and self.expression[self.position] == "=":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value="<=", position=start)
            return Token(type=TokenType.OPERATOR, value="<", position=start)

        if char == ">":
            if self.position < self.length and self.expression[self.position] == "=":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value=">=", position=start)
            return Token(type=TokenType.OPERATOR, value=">", position=start)

        if char == "&":
            if self.position < self.length and self.expression[self.position] == "&":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value="&&", position=start)

        if char == "|":
            if self.position < self.length and self.expression[self.position] == "|":
                self.position += 1
                return Token(type=TokenType.OPERATOR, value="||", position=start)

        return Token(type=TokenType.OPERATOR, value=char, position=start)


class ASTNode:
    """Base class for AST nodes."""
    pass


@dataclass
class NumberNode(ASTNode):
    """Number literal node."""
    value: float | int


@dataclass
class StringNode(ASTNode):
    """String literal node."""
    value: str


@dataclass
class BooleanNode(ASTNode):
    """Boolean literal node."""
    value: bool


@dataclass
class NullNode(ASTNode):
    """Null literal node."""
    pass


@dataclass
class IdentifierNode(ASTNode):
    """Identifier/variable node."""
    name: str


@dataclass
class BinaryOpNode(ASTNode):
    """Binary operation node."""
    left: ASTNode
    op: str
    right: ASTNode


@dataclass
class UnaryOpNode(ASTNode):
    """Unary operation node."""
    op: str
    operand: ASTNode


@dataclass
class CallNode(ASTNode):
    """Function call node."""
    name: str
    arguments: list[ASTNode]


@dataclass
class IndexNode(ASTNode):
    """Index access node."""
    object: ASTNode
    index: ASTNode


@dataclass
class MemberNode(ASTNode):
    """Member access node."""
    object: ASTNode
    member: str


class Parser:
    """
    Parser for building AST from tokens.

    Example:
        >>> lexer = Lexer("a + b * 2")
        >>> parser = Parser(lexer.tokenize())
        >>> ast = parser.parse()
    """

    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.position = 0

    def parse(self) -> ASTNode:
        """Parse tokens into AST."""
        return self._parse_expression()

    def _current(self) -> Token:
        """Get current token."""
        if self.position < len(self.tokens):
            return self.tokens[self.position]
        return Token(type=TokenType.EOF, value=None)

    def _advance(self) -> Token:
        """Advance to next token."""
        token = self._current()
        self.position += 1
        return token

    def _parse_expression(self) -> ASTNode:
        """Parse expression with precedence."""
        return self._parse_comparison()

    def _parse_comparison(self) -> ASTNode:
        """Parse comparison operators."""
        left = self._parse_addition()

        while self._current().type == TokenType.OPERATOR and self._current().value in (
            "==", "!=", "<", ">", "<=", ">=", "&&", "||"
        ):
            op = self._advance().value
            right = self._parse_addition()
            left = BinaryOpNode(left=left, op=op, right=right)

        return left

    def _parse_addition(self) -> ASTNode:
        """Parse addition/subtraction."""
        left = self._parse_multiplication()

        while self._current().type == TokenType.OPERATOR and self._current().value in ("+", "-"):
            op = self._advance().value
            right = self._parse_multiplication()
            left = BinaryOpNode(left=left, op=op, right=right)

        return left

    def _parse_multiplication(self) -> ASTNode:
        """Parse multiplication/division."""
        left = self._parse_unary()

        while self._current().type == TokenType.OPERATOR and self._current().value in ("*", "/", "%"):
            op = self._advance().value
            right = self._parse_unary()
            left = BinaryOpNode(left=left, op=op, right=right)

        return left

    def _parse_unary(self) -> ASTNode:
        """Parse unary operators."""
        if self._current().type == TokenType.OPERATOR and self._current().value in ("-", "!"):
            op = self._advance().value
            operand = self._parse_unary()
            return UnaryOpNode(op=op, operand=operand)
        return self._parse_primary()

    def _parse_primary(self) -> ASTNode:
        """Parse primary expressions."""
        token = self._current()

        if token.type == TokenType.NUMBER:
            self._advance()
            return NumberNode(value=token.value)

        if token.type == TokenType.STRING:
            self._advance()
            return StringNode(value=token.value)

        if token.type == TokenType.BOOLEAN:
            self._advance()
            return BooleanNode(value=token.value)

        if token.type == TokenType.NULL:
            self._advance()
            return NullNode()

        if token.type == TokenType.IDENTIFIER:
            return self._parse_identifier_or_call()

        if token.type == TokenType.LPAREN:
            self._advance()
            expr = self._parse_expression()
            if self._current().type != TokenType.RPAREN:
                raise ValueError("Expected closing parenthesis")
            self._advance()
            return expr

        raise ValueError(f"Unexpected token: {token.value}")

    def _parse_identifier_or_call(self) -> ASTNode:
        """Parse identifier, function call, or member access."""
        name = self._advance().value

        if self._current().type == TokenType.LPAREN:
            self._advance()
            arguments: list[ASTNode] = []

            if self._current().type != TokenType.RPAREN:
                arguments.append(self._parse_expression())
                while self._current().type == TokenType.COMMA:
                    self._advance()
                    arguments.append(self._parse_expression())

            if self._current().type != TokenType.RPAREN:
                raise ValueError("Expected closing parenthesis")
            self._advance()

            return CallNode(name=name, arguments=arguments)

        if self._current().type == TokenType.DOT:
            self._advance()
            member = self._advance().value
            return MemberNode(object=IdentifierNode(name=name), member=member)

        if self._current().type == TokenType.LBRACKET:
            self._advance()
            index = self._parse_expression()
            if self._current().type != TokenType.RBRACKET:
                raise ValueError("Expected closing bracket")
            self._advance()
            return IndexNode(object=IdentifierNode(name=name), index=index)

        return IdentifierNode(name=name)


class Evaluator:
    """
    Evaluates AST nodes against context.

    Example:
        >>> evaluator = Evaluator({"x": 10, "y": 5})
        >>> result = evaluator.evaluate(ast)
    """

    def __init__(self, context: Optional[dict[str, Any]] = None) -> None:
        self.context = context or {}
        self._functions: dict[str, Callable] = self._builtin_functions()

    def evaluate(self, node: ASTNode) -> Any:
        """Evaluate AST node."""
        if isinstance(node, NumberNode):
            return node.value
        if isinstance(node, StringNode):
            return node.value
        if isinstance(node, BooleanNode):
            return node.value
        if isinstance(node, NullNode):
            return None
        if isinstance(node, IdentifierNode):
            return self._get_variable(node.name)
        if isinstance(node, BinaryOpNode):
            return self._evaluate_binary(node)
        if isinstance(node, UnaryOpNode):
            return self._evaluate_unary(node)
        if isinstance(node, CallNode):
            return self._call_function(node.name, node.arguments)
        if isinstance(node, IndexNode):
            return self._evaluate_index(node)
        if isinstance(node, MemberNode):
            return self._evaluate_member(node)
        raise ValueError(f"Unknown node type: {type(node)}")

    def _get_variable(self, name: str) -> Any:
        """Get variable from context."""
        if name in self.context:
            return self.context[name]
        if name in self._functions:
            return self._functions[name]
        raise NameError(f"Unknown variable: {name}")

    def _evaluate_binary(self, node: BinaryOpNode) -> Any:
        """Evaluate binary operation."""
        left = self.evaluate(node.left)
        right = self.evaluate(node.right)

        ops = {
            "+": operator.add,
            "-": operator.sub,
            "*": operator.mul,
            "/": operator.truediv,
            "%": operator.mod,
            "==": operator.eq,
            "!=": operator.ne,
            "<": operator.lt,
            ">": operator.gt,
            "<=": operator.le,
            ">=": operator.ge,
            "&&": lambda a, b: bool(a) and bool(b),
            "||": lambda a, b: bool(a) or bool(b),
        }

        if node.op == "&&":
            return bool(left) and bool(right)
        if node.op == "||":
            return bool(left) or bool(right)

        op_func = ops.get(node.op)
        if op_func:
            return op_func(left, right)

        raise ValueError(f"Unknown operator: {node.op}")

    def _evaluate_unary(self, node: UnaryOpNode) -> Any:
        """Evaluate unary operation."""
        operand = self.evaluate(node.operand)

        if node.op == "-":
            return -operand
        if node.op == "!":
            return not operand

        raise ValueError(f"Unknown unary operator: {node.op}")

    def _call_function(self, name: str, arguments: list[ASTNode]) -> Any:
        """Call function with arguments."""
        if name not in self._functions:
            raise NameError(f"Unknown function: {name}")

        args = [self.evaluate(arg) for arg in arguments]
        return self._functions[name](*args)

    def _evaluate_index(self, node: IndexNode) -> Any:
        """Evaluate index access."""
        obj = self.evaluate(node.object)
        index = self.evaluate(node.index)
        return obj[index]

    def _evaluate_member(self, node: MemberNode) -> Any:
        """Evaluate member access."""
        obj = self.evaluate(node.object)
        return getattr(obj, node.member)

    def _builtin_functions(self) -> dict[str, Callable]:
        """Get built-in functions."""
        return {
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "sum": sum,
            "len": len,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
            "type": type,
            "isinstance": isinstance,
            "hasattr": hasattr,
            "getattr": getattr,
            "range": range,
            "enumerate": enumerate,
            "zip": zip,
            "map": map,
            "filter": filter,
            "reversed": reversed,
            "sorted": sorted,
            "any": any,
            "all": all,
            "math": math,
            "ceil": math.ceil,
            "floor": math.floor,
            "sqrt": math.sqrt,
            "pow": math.pow,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "log": math.log,
            "log10": math.log10,
            "exp": math.exp,
            "pi": math.pi,
            "e": math.e,
            "now": lambda: datetime.utcnow(),
            "today": lambda: datetime.utcnow().date(),
        }


class ExpressionEvaluator:
    """
    Complete expression evaluator combining lexer, parser, and evaluator.

    Example:
        >>> evaluator = ExpressionEvaluator()
        >>> result = evaluator.evaluate("a + b * 2", {"a": 10, "b": 5})
        >>> print(result)  # 20
    """

    def __init__(self, context: Optional[dict[str, Any]] = None) -> None:
        self.context = context or {}

    def evaluate(self, expression: str, context: Optional[dict[str, Any]] = None) -> Any:
        """Evaluate expression string."""
        ctx = {**self.context, **(context or {})}

        try:
            lexer = Lexer(expression)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            ast = parser.parse()
            evaluator = Evaluator(ctx)
            return evaluator.evaluate(ast)
        except Exception as e:
            raise ExpressionError(f"Evaluation failed: {e}") from e

    def add_function(self, name: str, func: Callable) -> None:
        """Add custom function to context."""
        self.context[name] = func


class ExpressionError(Exception):
    """Expression evaluation error."""
    pass
