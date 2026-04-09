"""Automation Mutation Testing Action.

Mutation testing framework for automation scripts: injects faults
into automation code to verify test coverage and robustness.
"""
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import random
import re


class MutationType(Enum):
    BOOLEAN_FLIP = "boolean_flip"
    NUMBER_CHANGE = "number_change"
    STRING_TRUNCATE = "string_truncate"
    CONDITION_INVERT = "condition_invert"
    LOOP_SKIP = "loop_skip"
    NULL_INJECT = "null_inject"
    OPERATOR_SWAP = "operator_swap"


@dataclass
class Mutation:
    mutation_type: MutationType
    original: str
    replacement: str
    location: int
    description: str


@dataclass
class MutationResult:
    mutation: Mutation
    survived: bool
    killed_by_test: Optional[str] = None
    execution_error: Optional[str] = None


@dataclass
class MutationReport:
    total_mutations: int
    killed: int
    survived: int
    score: float
    details: List[MutationResult] = field(default_factory=list)


class AutomationMutationTestingAction:
    """Mutation testing for automation scripts."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)
        self._mutation_strategies: Dict[MutationType, Callable[[str], List[str]]] = {
            MutationType.BOOLEAN_FLIP: self._mutate_boolean,
            MutationType.NUMBER_CHANGE: self._mutate_number,
            MutationType.STRING_TRUNCATE: self._mutate_string,
            MutationType.NULL_INJECT: self._mutate_null,
            MutationType.OPERATOR_SWAP: self._mutate_operator,
        }

    def _mutate_boolean(self, code: str) -> List[str]:
        mutations = []
        for bool_lit in ["True", "False", "true", "false"]:
            replacement = "False" if bool_lit in ["True", "true"] else "True"
            mutations.append(code.replace(bool_lit, replacement))
        return mutations

    def _mutate_number(self, code: str) -> List[str]:
        mutations = []
        number_pattern = re.compile(r"\b(\d+)\b")
        for match in number_pattern.finditer(code):
            num = int(match.group(1))
            variants = [num * 2, num // 2 if num > 1 else 0, num + 1, max(0, num - 1)]
            for variant in set(variants):
                if variant != num:
                    mutated = code[:match.start()] + str(variant) + code[match.end():]
                    mutations.append(mutated)
        return mutations

    def _mutate_string(self, code: str) -> List[str]:
        mutations = []
        str_pattern = re.compile(r'"([^"]*)"')
        for match in str_pattern.finditer(code):
            s = match.group(1)
            if len(s) > 0:
                truncated = s[: len(s) // 2]
                mutated = code[: match.start()] + f'"{truncated}"' + code[match.end() :]
                mutations.append(mutated)
        return mutations

    def _mutate_null(self, code: str) -> List[str]:
        mutations = []
        for null_lit in ["None", "null", "undefined"]:
            if null_lit in code:
                mutations.append(code.replace(null_lit, "None", 1))
        return mutations

    def _mutate_operator(self, code: str) -> List[str]:
        mutations = []
        ops = [(" and ", " or "), (" or ", " and "),
               (" == ", " != "), (" != ", " == "),
               (" > ", " < "), (" < ", " > ")]
        for old, new in ops:
            if old in code:
                mutations.append(code.replace(old, new, 1))
        return mutations

    def generate_mutations(
        self,
        code: str,
        mutation_types: Optional[List[MutationType]] = None,
        max_mutations: int = 50,
    ) -> List[Mutation]:
        mutations = []
        types = mutation_types or list(MutationType)
        for mtype in types:
            strategy = self._mutation_strategies.get(mtype)
            if not strategy:
                continue
            try:
                mutated_versions = strategy(code)
                for i, mutated in enumerate(mutated_versions[: max_mutations // len(types)]):
                    if mutated != code:
                        mutations.append(
                            Mutation(
                                mutation_type=mtype,
                                original=code,
                                replacement=mutated,
                                location=i,
                                description=f"{mtype.value} variant {i}",
                            )
                        )
            except Exception:
                continue
        return mutations[:max_mutations]

    def run_mutation_analysis(
        self,
        code: str,
        test_fn: Callable[[str], bool],
        mutation_types: Optional[List[MutationType]] = None,
    ) -> MutationReport:
        mutations = self.generate_mutations(code, mutation_types)
        results: List[MutationResult] = []
        killed = 0
        for mut in mutations:
            try:
                survived = test_fn(mut.replacement)
                result = MutationResult(mutation=mut, survived=survived)
                if not survived:
                    killed += 1
            except Exception as e:
                result = MutationResult(
                    mutation=mut,
                    survived=False,
                    execution_error=str(e),
                )
                killed += 1
            results.append(result)
        total = len(results)
        return MutationReport(
            total_mutations=total,
            killed=killed,
            survived=total - killed,
            score=killed / total if total > 0 else 1.0,
            details=results,
        )
