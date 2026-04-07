"""
Tokenizer and parser utilities.

Provides lexing and parsing utilities for building custom DSLs and languages.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional, Union
from enum import Enum, auto


class TokenType(Enum):
    """Base token types for lexer output."""
    EOF = auto()
    UNKNOWN = auto()
    IDENTIFIER = auto()
    NUMBER = auto()
    STRING = auto()
    OPERATOR = auto()
    KEYWORD = auto()
    PUNCTUATION = auto()
    WHITESPACE = auto()
    NEWLINE = auto()
    COMMENT = auto()
    INDENT = auto()


@dataclass
class Token:
    """A single token produced by the lexer."""
    type: TokenType
    value: str
    line: int
    column: int
    span: tuple[int, int] = (0, 0)

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, L{self.line}:C{self.column})"


class LexerError(Exception):
    """Raised when the lexer encounters an invalid token sequence."""
    def __init__(self, message: str, line: int, column: int):
        self.line = line
        self.column = column
        super().__init__(f"Lexer error at L{line}:C{column}: {message}")


class Lexer:
    """A configurable lexer for tokenizing input strings."""

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: list[Token] = []
        self._rules: list[tuple[re.Pattern, Callable[[re.Match], Optional[Token]]]] = []

    def add_rule(self, pattern: str, handler: Callable[[re.Match], Optional[Token]]) -> Lexer:
        """Add a token matching rule. Returns self for chaining."""
        self._rules.append((re.compile(pattern), handler))
        return self

    def tokenize(self) -> list[Token]:
        """Tokenize the entire source and return a list of tokens."""
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens = []

        while self.pos < len(self.source):
            matched = False
            start_pos = self.pos
            for pattern, handler in self._rules:
                match = pattern.match(self.source, self.pos)
                if match:
                    token = handler(match)
                    if token:
                        self.tokens.append(token)
                    self._advance(match.end() - self.pos)
                    matched = True
                    break
            if not matched:
                # Emit unknown token and advance
                self.tokens.append(Token(
                    TokenType.UNKNOWN,
                    self.source[self.pos],
                    self.line,
                    self.column,
                    (self.pos, self.pos + 1),
                ))
                self._advance(1)

        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens

    def _advance(self, count: int) -> None:
        """Advance position and update line/column counters."""
        for _ in range(count):
            if self.pos >= len(self.source):
                break
            if self.source[self.pos] == "\n":
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1


class BasicLexer(Lexer):
    """A pre-configured lexer for common programming languages."""

    KEYWORDS = {
        "if", "else", "elif", "while", "for", "def", "class", "return",
        "import", "from", "as", "try", "except", "finally", "with", "pass",
        "break", "continue", "and", "or", "not", "in", "is", "True", "False",
        "None", "lambda", "yield", "raise", "assert", "global", "nonlocal",
    }

    def __init__(self, source: str):
        super().__init__(source)
        self._build_rules()

    def _build_rules(self) -> None:
        """Set up standard token rules."""
        self.add_rule(r"\s+", lambda m: None)  # Skip whitespace
        self.add_rule(r"#.*", lambda m: None)  # Skip comments
        self.add_rule(r"'''[\s\S]*?'''", self._make_string())
        self.add_rule(r'"""[\s\S]*?"""', self._make_string())
        self.add_rule(r"'[^'\n]*'", self._make_string(escape=False))
        self.add_rule(r'"[^"\n]*"', self._make_string(escape=False))
        self.add_rule(r"\d+\.\d+", self._make_number())
        self.add_rule(r"\d+", self._make_number())
        self.add_rule(r"[a-zA-Z_][a-zA-Z0-9_]*", self._make_identifier())
        self.add_rule(r"==|!=|<=|>=|<<|>>|\+=|-=|\*=|/=|\|=|&=|=>|->|::", self._make_operator())
        self.add_rule(r"[+\-*/%=<>!&|^~]", self._make_operator())
        self.add_rule(r"[()\[\]{}]", self._make_punctuation())
        self.add_rule(r"[,:;.@]", self._make_punctuation())

    def _make_string(self, escape: bool = True) -> Callable[[re.Match], Token]:
        def handler(m: re.Match) -> Token:
            value = m.group()
            return Token(TokenType.STRING, value, self.line, self.column, m.span())
        return handler

    def _make_number(self) -> Callable[[re.Match], Token]:
        def handler(m: re.Match) -> Token:
            return Token(TokenType.NUMBER, m.group(), self.line, self.column, m.span())
        return handler

    def _make_identifier(self) -> Callable[[re.Match], Token]:
        def handler(m: re.Match) -> Token:
            value = m.group()
            ttype = TokenType.KEYWORD if value in self.KEYWORDS else TokenType.IDENTIFIER
            return Token(ttype, value, self.line, self.column, m.span())
        return handler

    def _make_operator(self) -> Callable[[re.Match], Token]:
        def handler(m: re.Match) -> Token:
            return Token(TokenType.OPERATOR, m.group(), self.line, self.column, m.span())
        return handler

    def _make_punctuation(self) -> Callable[[re.Match], Token]:
        def handler(m: re.Match) -> Token:
            return Token(TokenType.PUNCTUATION, m.group(), self.line, self.column, m.span())
        return handler


@dataclass
class ParseRule:
    """A rule for the recursive descent parser."""
    name: str
    precedence: int = 0


class Parser:
    """A simple recursive descent parser operating on tokens."""

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0
        self.errors: list[str] = []

    def current(self) -> Token:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return self.tokens[-1]

    def peek(self, offset: int = 1) -> Token:
        idx = self.pos + offset
        if idx < len(self.tokens):
            return self.tokens[idx]
        return self.tokens[-1]

    def advance(self) -> Token:
        token = self.current()
        if token.type != TokenType.EOF:
            self.pos += 1
        return token

    def match(self, expected: Union[TokenType, str]) -> bool:
        """Check if current token matches and advance if so."""
        current = self.current()
        if isinstance(expected, TokenType):
            if current.type == expected:
                self.advance()
                return True
        elif current.value == expected:
            self.advance()
            return True
        return False

    def expect(self, expected: Union[TokenType, str], message: str = "") -> Token:
        """Consume expected token or raise error."""
        if self.match(expected):
            return self.tokens[self.pos - 1]
        msg = message or f"Expected {expected}, got {self.current().type.name}"
        self.errors.append(msg)
        return self.tokens[self.pos]

    def parse(self) -> Any:
        """Override in subclasses to implement specific grammars."""
        raise NotImplementedError

    def skip_newlines(self) -> None:
        """Skip over newline tokens."""
        while self.current().type == TokenType.NEWLINE:
            self.advance()


@dataclass
class ASTNode:
    """Base class for AST nodes."""
    token: Optional[Token] = None


@dataclass
class NumberNode(ASTNode):
    value: Union[int, float] = 0


@dataclass
class StringNode(ASTNode):
    value: str = ""


@dataclass
class IdentifierNode(ASTNode):
    name: str = ""


@dataclass
class BinaryOpNode(ASTNode):
    left: ASTNode = None
    operator: str = ""
    right: ASTNode = None


@dataclass
class UnaryOpNode(ASTNode):
    operator: str = ""
    operand: ASTNode = None


@dataclass
class CallNode(ASTNode):
    func: ASTNode = None
    args: list[ASTNode] = field(default_factory=list)


@dataclass
class FunctionDefNode(ASTNode):
    name: str = ""
    params: list[str] = field(default_factory=list)
    body: list[ASTNode] = field(default_factory=list)


class ExpressionParser(Parser):
    """Parser for arithmetic expressions."""

    def parse(self) -> ASTNode:
        return self._parse_expression(0)

    def _parse_expression(self, precedence: int) -> ASTNode:
        left = self._parse_unary()
        ops = {
            "+": (1, "left"), "-": (1, "left"),
            "*": (2, "left"), "/": (2, "left"),
            "%": (2, "left"),
            "==": (0, "left"), "!=": (0, "left"),
            "<": (0, "left"), ">": (0, "left"),
            "<=": (0, "left"), ">=": (0, "left"),
        }
        while self.current().type == TokenType.OPERATOR:
            op = self.current().value
            if op not in ops:
                break
            prec, assoc = ops[op]
            if prec < precedence:
                break
            self.advance()
            right = self._parse_expression(prec + 1 if assoc == "left" else prec)
            left = BinaryOpNode(token=self.tokens[self.pos - 1], left=left, operator=op, right=right)
        return left

    def _parse_unary(self) -> ASTNode:
        if self.current().type == TokenType.OPERATOR and self.current().value in "+-":
            op = self.advance().value
            operand = self._parse_unary()
            return UnaryOpNode(operator=op, operand=operand)
        return self._parse_primary()

    def _parse_primary(self) -> ASTNode:
        token = self.advance()
        if token.type == TokenType.NUMBER:
            try:
                if "." in token.value:
                    return NumberNode(value=float(token.value), token=token)
                return NumberNode(value=int(token.value), token=token)
            except ValueError:
                pass
        if token.type == TokenType.STRING:
            return StringNode(value=token.value[1:-1], token=token)
        if token.type == TokenType.IDENTIFIER:
            if self.current().value == "(":
                return self._parse_call(IdentifierNode(name=token.value, token=token))
            return IdentifierNode(name=token.value, token=token)
        self.errors.append(f"Unexpected token: {token}")
        return NumberNode(value=0)

    def _parse_call(self, func: ASTNode) -> CallNode:
        self.advance()  # consume '('
        args: list[ASTNode] = []
        while self.current().value != ")":
            if self.current().type == TokenType.EOF:
                break
            args.append(self._parse_expression(0))
            if not self.match(","):
                break
        self.expect(")", "Expected ')' after arguments")
        return CallNode(func=func, args=args)


def tokenize(source: str) -> list[Token]:
    """Convenience function to tokenize source code."""
    return BasicLexer(source).tokenize()


def parse_expression(source: str) -> ASTNode:
    """Convenience function to parse an expression string."""
    tokens = tokenize(source)
    return ExpressionParser(tokens).parse()
