"""
Mutation testing utilities for evaluating test suite quality.

Provides mutation operators, test execution with mutations,
mutation score calculation, and surviving mutant analysis.
"""

from __future__ import annotations

import ast
import logging
import random
import subprocess
import sys
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class MutationOperator(Enum):
    """Available mutation operators."""
    AOR = auto()  # Arithmetic Operator Replacement
    ROR = auto()  # Relational Operator Replacement
    LOR = auto()  # Logical Operator Replacement
    CRP = auto()  # Constant Replacement
    DOT = auto()  # Attribute Deletion
    SDD = auto()  # Statement Deletion
    CDI = auto()  # Conditional Decision Inversion


@dataclass
class Mutant:
    """Represents a code mutant."""
    id: str
    operator: MutationOperator
    original_code: str
    mutated_code: str
    line_number: int
    file_path: str
    killed_by: Optional[str] = None
    status: str = "live"  # live, killed, timeout

    @property
    def is_killed(self) -> bool:
        return self.status == "killed"


@dataclass
class MutationReport:
    """Report from a mutation testing run."""
    total_mutants: int = 0
    killed: int = 0
    live: int = 0
    timeout: int = 0
    score: float = 0.0
    mutants: list[Mutant] = field(default_factory=list)

    def calculate_score(self) -> float:
        """Calculate the mutation score (percentage killed)."""
        if self.total_mutants == 0:
            return 0.0
        self.score = (self.killed / self.total_mutants) * 100
        return self.score


class PythonMutator:
    """Applies mutation operators to Python source code."""

    ARITHMETIC_OPS = {"+": "-", "-": "+", "*": "/", "/": "*", "//": "%", "**": "*", "%": "//"}
    RELATIONAL_OPS = {"==": "!=", "!=": "==", "<": ">=", ">": "<=", "<=": ">", ">=": "<"}
    LOGICAL_OPS = {"and": "or", "or": "and"}
    CONSTANT_REPLACEMENTS = {
        "True": "False",
        "False": "True",
        "None": "None",
        "0": "1",
        "1": "0",
        "\'\'": "\'\'",
        "\"\"": "\"\"",
    }

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)
        self._mutant_counter = 0

    def mutate_source(self, source: str, operators: Optional[list[MutationOperator]] = None) -> list[Mutant]:
        """Mutate Python source code using specified operators."""
        operators = operators or list(MutationOperator)
        mutants = []

        try:
            tree = ast.parse(source)
        except SyntaxError:
            return mutants

        for node in ast.walk(tree):
            for op_type in operators:
                if op_type == MutationOperator.AOR:
                    mutants.extend(self._mutate_arithmetic(node, source))
                elif op_type == MutationOperator.ROR:
                    mutants.extend(self._mutate_relational(node, source))
                elif op_type == MutationOperator.LOR:
                    mutants.extend(self._mutate_logical(node, source))
                elif op_type == MutationOperator.CRP:
                    mutants.extend(self._mutate_constants(node, source))

        return mutants

    def _mutate_arithmetic(self, node: ast.AST, source: str) -> list[Mutant]:
        mutants = []
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow)):
            self._mutant_counter += 1
            op_symbol = self._get_op_symbol(node.op)
            replacement = self.ARITHMETIC_OPS.get(op_symbol, op_symbol)
            mutants.append(Mutant(
                id=f"M{self._mutant_counter}",
                operator=MutationOperator.AOR,
                original_code=op_symbol,
                mutated_code=replacement,
                line_number=node.lineno,
                file_path="",
            ))
        return mutants

    def _mutate_relational(self, node: ast.AST, source: str) -> list[Mutant]:
        mutants = []
        if isinstance(node, ast.Compare) and node.ops:
            for op in node.ops:
                if isinstance(op, (ast.Eq, ast.NotEq, ast.Lt, ast.Gt, ast.LtE, ast.GtE)):
                    op_symbol = self._cmp_op_symbol(op)
                    replacement = self.RELATIONAL_OPS.get(op_symbol, op_symbol)
                    self._mutant_counter += 1
                    mutants.append(Mutant(
                        id=f"M{self._mutant_counter}",
                        operator=MutationOperator.ROR,
                        original_code=op_symbol,
                        mutated_code=replacement,
                        line_number=node.lineno,
                        file_path="",
                    ))
        return mutants

    def _mutate_logical(self, node: ast.AST, source: str) -> list[Mutant]:
        mutants = []
        if isinstance(node, ast.BoolOp):
            for op in node.ops:
                if isinstance(op, (ast.And, ast.Or)):
                    op_symbol = "and" if isinstance(op, ast.And) else "or"
                    replacement = self.LOGICAL_OPS.get(op_symbol, op_symbol)
                    self._mutant_counter += 1
                    mutants.append(Mutant(
                        id=f"M{self._mutant_counter}",
                        operator=MutationOperator.LOR,
                        original_code=op_symbol,
                        mutated_code=replacement,
                        line_number=node.lineno,
                        file_path="",
                    ))
        return mutants

    def _mutate_constants(self, node: ast.AST, source: str) -> list[Mutant]:
        mutants = []
        if isinstance(node, ast.Constant):
            original = repr(node.value)
            if original in self.CONSTANT_REPLACEMENTS:
                replacement = self.CONSTANT_REPLACEMENTS[original]
                self._mutant_counter += 1
                mutants.append(Mutant(
                    id=f"M{self._mutant_counter}",
                    operator=MutationOperator.CRP,
                    original_code=original,
                    mutated_code=replacement,
                    line_number=node.lineno,
                    file_path="",
                ))
        return mutants

    def _get_op_symbol(self, op: ast.AST) -> str:
        if isinstance(op, ast.Add): return "+"
        if isinstance(op, ast.Sub): return "-"
        if isinstance(op, ast.Mult): return "*"
        if isinstance(op, ast.Div): return "/"
        if isinstance(op, ast.FloorDiv): return "//"
        if isinstance(op, ast.Mod): return "%"
        if isinstance(op, ast.Pow): return "**"
        return "+"

    def _cmp_op_symbol(self, op: ast.AST) -> str:
        if isinstance(op, ast.Eq): return "=="
        if isinstance(op, ast.NotEq): return "!="
        if isinstance(op, ast.Lt): return "<"
        if isinstance(op, ast.Gt): return ">"
        if isinstance(op, ast.LtE): return "<="
        if isinstance(op, ast.GtE): return ">="
        return "=="


class MutationTestRunner:
    """Runs mutation tests on a Python project."""

    def __init__(
        self,
        test_command: str = "pytest",
        timeout: float = 30.0,
    ) -> None:
        self.test_command = test_command
        self.timeout = timeout
        self.mutator = PythonMutator()
        self.report = MutationReport()

    def run(self, source_files: list[str], operators: Optional[list[MutationOperator]] = None) -> MutationReport:
        """Run mutation testing on source files."""
        all_mutants = []

        for filepath in source_files:
            try:
                with open(filepath) as f:
                    source = f.read()
            except Exception as e:
                logger.error("Failed to read %s: %s", filepath, e)
                continue

            mutants = self.mutator.mutate_source(source, operators)
            for m in mutants:
                m.file_path = filepath
            all_mutants.extend(mutants)

        self.report = MutationReport(total_mutants=len(all_mutants), mutants=all_mutants)

        for mutant in all_mutants:
            self._test_mutant(mutant)

        self.report.calculate_score()
        return self.report

    def _test_mutant(self, mutant: Mutant) -> None:
        """Test a single mutant against the test suite."""
        try:
            result = subprocess.run(
                self.test_command.split(),
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            if result.returncode != 0:
                mutant.status = "killed"
                self.report.killed += 1
            else:
                mutant.status = "live"
                self.report.live += 1
        except subprocess.TimeoutExpired:
            mutant.status = "timeout"
            self.report.timeout += 1
        except Exception as e:
            logger.error("Error testing mutant %s: %s", mutant.id, e)
            mutant.status = "timeout"
            self.report.timeout += 1

    def get_live_mutants(self) -> list[Mutant]:
        """Get all surviving (live) mutants."""
        return [m for m in self.report.mutants if m.status == "live"]

    def get_killed_mutants(self) -> list[Mutant]:
        """Get all killed mutants."""
        return [m for m in self.report.mutants if m.status == "killed"]
