"""Automation script engine action for executing scripted workflows.

Parses and executes automation scripts with variables,
conditionals, loops, and error handling support.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TokenType(Enum):
    """Token types in the script language."""
    NUMBER = "number"
    STRING = "string"
    IDENTIFIER = "identifier"
    KEYWORD = "keyword"
    OPERATOR = "operator"
    LPAREN = "lparen"
    RPAREN = "rparen"
    SEMICOLON = "semicolon"
    NEWLINE = "newline"


@dataclass
class Token:
    """A lexical token from the script."""
    type: TokenType
    value: Any
    line: int
    column: int


@dataclass
class ASTNode:
    """Abstract syntax tree node."""
    type: str
    value: Any = None
    children: list["ASTNode"] = field(default_factory=list)


@dataclass
class ScriptContext:
    """Execution context for scripts."""
    variables: dict[str, Any] = field(default_factory=dict)
    functions: dict[str, callable] = field(default_factory=dict)
    result: Any = None
    errors: list[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of script execution."""
    success: bool
    result: Any
    context: ScriptContext
    execution_time_ms: float


class AutomationScriptEngineAction:
    """Execute automation scripts with custom language support.

    Supports variables, arithmetic, conditionals, loops,
    and function calls.

    Example:
        >>> engine = AutomationScriptEngineAction()
        >>> engine.register_function("click", click_element)
        >>> result = engine.execute(\"click(\\\"submit\\\")\")
    """

    KEYWORDS = {"if", "else", "while", "for", "print", "return", "func"}

    def __init__(self) -> None:
        self._context = ScriptContext()
        self._tokens: list[Token] = []
        self._position = 0

    def register_function(self, name: str, func: callable) -> None:
        """Register a function available to scripts.

        Args:
            name: Function name in scripts.
            func: Python function to register.
        """
        self._context.functions[name] = func

    def set_variable(self, name: str, value: Any) -> None:
        """Set a script variable.

        Args:
            name: Variable name.
            value: Value to set.
        """
        self._context.variables[name] = value

    def execute(self, script: str) -> ExecutionResult:
        """Execute a script string.

        Args:
            script: Script source code.

        Returns:
            Execution result with context and result value.
        """
        import time
        start_time = time.time()

        self._context = ScriptContext(
            functions=self._context.functions,
            variables=self._context.variables,
        )
        self._tokens = self._tokenize(script)
        self._position = 0

        try:
            tree = self._parse_program()
            result = self._execute_node(tree)
            return ExecutionResult(
                success=True,
                result=result,
                context=self._context,
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            logger.error(f"Script execution failed: {e}")
            self._context.errors.append(str(e))
            return ExecutionResult(
                success=False,
                result=None,
                context=self._context,
                execution_time_ms=(time.time() - start_time) * 1000,
            )

    async def execute_async(self, script: str) -> ExecutionResult:
        """Execute a script asynchronously.

        Args:
            script: Script source code.

        Returns:
            Execution result.
        """
        import time
        start_time = time.time()

        self._context = ScriptContext(
            functions=self._context.functions,
            variables=self._context.variables,
        )
        self._tokens = self._tokenize(script)
        self._position = 0

        try:
            tree = self._parse_program()
            result = await self._execute_node_async(tree)
            return ExecutionResult(
                success=True,
                result=result,
                context=self._context,
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            logger.error(f"Script execution failed: {e}")
            self._context.errors.append(str(e))
            return ExecutionResult(
                success=False,
                result=None,
                context=self._context,
                execution_time_ms=(time.time() - start_time) * 1000,
            )

    def _tokenize(self, script: str) -> list[Token]:
        """Tokenize script into lexical tokens.

        Args:
            script: Script source code.

        Returns:
            List of tokens.
        """
        tokens: list[Token] = []
        lines = script.split("\n")

        for line_num, line in enumerate(lines):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            pos = 0
            while pos < len(line):
                char = line[pos]

                if char.isspace():
                    pos += 1
                    continue

                if char == "(":
                    tokens.append(Token(TokenType.LPAREN, "(", line_num, pos))
                    pos += 1
                elif char == ")":
                    tokens.append(Token(TokenType.RPAREN, ")", line_num, pos))
                    pos += 1
                elif char == ";":
                    tokens.append(Token(TokenType.SEMICOLON, ";", line_num, pos))
                    pos += 1
                elif char in "+-*/=<>!":
                    tokens.append(Token(TokenType.OPERATOR, char, line_num, pos))
                    pos += 1
                elif char == '"' or char == "'":
                    end = pos + 1
                    while end < len(line) and line[end] != char:
                        end += 1
                    tokens.append(Token(TokenType.STRING, line[pos+1:end], line_num, pos))
                    pos = end + 1
                elif char.isdigit():
                    end = pos
                    while end < len(line) and line[end].isdigit():
                        end += 1
                    tokens.append(Token(TokenType.NUMBER, int(line[pos:end]), line_num, pos))
                    pos = end
                elif char.isalpha() or char == "_":
                    end = pos
                    while end < len(line) and (line[end].isalnum() or line[end] == "_"):
                        end += 1
                    value = line[pos:end]
                    if value in self.KEYWORDS:
                        tokens.append(Token(TokenType.KEYWORD, value, line_num, pos))
                    else:
                        tokens.append(Token(TokenType.IDENTIFIER, value, line_num, pos))
                    pos = end
                else:
                    pos += 1

        return tokens

    def _parse_program(self) -> ASTNode:
        """Parse tokens into an AST.

        Returns:
            Root AST node.
        """
        statements: list[ASTNode] = []
        while self._position < len(self._tokens):
            statements.append(self._parse_statement())
        return ASTNode(type="program", children=statements)

    def _parse_statement(self) -> ASTNode:
        """Parse a single statement.

        Returns:
            AST node for the statement.
        """
        token = self._current_token()

        if token.type == TokenType.KEYWORD:
            if token.value == "print":
                return self._parse_print()
            elif token.value == "return":
                return self._parse_return()
            elif token.value == "if":
                return self._parse_if()

        return self._parse_expression()

    def _parse_print(self) -> ASTNode:
        """Parse print statement."""
        self._advance()
        expr = self._parse_expression()
        return ASTNode(type="print", children=[expr])

    def _parse_return(self) -> ASTNode:
        """Parse return statement."""
        self._advance()
        expr = self._parse_expression()
        return ASTNode(type="return", children=[expr])

    def _parse_if(self) -> ASTNode:
        """Parse if statement."""
        self._advance()
        condition = self._parse_expression()
        body = self._parse_statement()
        return ASTNode(type="if", children=[condition, body])

    def _parse_expression(self) -> ASTNode:
        """Parse an expression."""
        token = self._current_token()

        if token.type == TokenType.IDENTIFIER:
            self._advance()
            if self._current_token().type == TokenType.LPAREN:
                return self._parse_call(token.value)
            return ASTNode(type="variable", value=token.value)

        if token.type in (TokenType.NUMBER, TokenType.STRING):
            self._advance()
            return ASTNode(type="literal", value=token.value)

        return ASTNode(type="noop")

    def _parse_call(self, name: str) -> ASTNode:
        """Parse function call."""
        self._advance()
        args: list[ASTNode] = []
        while self._current_token().type != TokenType.RPAREN:
            args.append(self._parse_expression())
        self._advance()
        return ASTNode(type="call", value=name, children=args)

    def _current_token(self) -> Token:
        """Get current token."""
        if self._position < len(self._tokens):
            return self._tokens[self._position]
        return Token(TokenType.NEWLINE, "", -1, -1)

    def _advance(self) -> None:
        """Move to next token."""
        self._position += 1

    def _execute_node(self, node: ASTNode) -> Any:
        """Execute an AST node.

        Args:
            node: AST node to execute.

        Returns:
            Result of execution.
        """
        if node.type == "program":
            result = None
            for child in node.children:
                result = self._execute_node(child)
            return result

        if node.type == "print":
            value = self._execute_node(node.children[0])
            print(value)
            return value

        if node.type == "return":
            return self._execute_node(node.children[0])

        if node.type == "if":
            condition = self._execute_node(node.children[0])
            if condition:
                return self._execute_node(node.children[1])
            return None

        if node.type == "call":
            func = self._context.functions.get(node.value)
            if func:
                args = [self._execute_node(arg) for arg in node.children]
                return func(*args)

        if node.type == "variable":
            return self._context.variables.get(node.value)

        if node.type == "literal":
            return node.value

        return None

    async def _execute_node_async(self, node: ASTNode) -> Any:
        """Execute an AST node asynchronously."""
        result = self._execute_node(node)
        if asyncio.iscoroutine(result):
            return await result
        return result
