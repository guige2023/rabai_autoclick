"""
Automaton utilities for finite state machine and regular expression evaluation.

Provides DFA/NFA construction, state machine evaluation, and pattern matching
using finite automata techniques.
"""

from __future__ import annotations

from typing import Callable


class DFA:
    """
    Deterministic Finite Automaton implementation.

    Attributes:
        states: Set of state identifiers
        alphabet: Set of valid input symbols
        transition: Dict mapping (state, symbol) -> next state
        start: Initial state
        accept: Set of accepting/final states
    """

    def __init__(
        self,
        states: set[str],
        alphabet: set[str],
        transition: dict[tuple[str, str], str],
        start: str,
        accept: set[str],
    ) -> None:
        self.states = states
        self.alphabet = alphabet
        self.transition = transition
        self.start = start
        self.accept = accept

    def accepts(self, input_string: str) -> bool:
        """
        Check if the DFA accepts the given input string.

        Args:
            input_string: Sequence of symbols from the alphabet

        Returns:
            True if the string is accepted, False otherwise
        """
        state = self.start
        for symbol in input_string:
            if symbol not in self.alphabet:
                return False
            key = (state, symbol)
            if key not in self.transition:
                return False
            state = self.transition[key]
        return state in self.accept

    def trace(self, input_string: str) -> list[str]:
        """
        Trace the DFA execution path for the input string.

        Args:
            input_string: Sequence of symbols from the alphabet

        Returns:
            List of states visited during execution
        """
        path = [self.start]
        state = self.start
        for symbol in input_string:
            if symbol not in self.alphabet:
                break
            key = (state, symbol)
            if key not in self.transition:
                break
            state = self.transition[key]
            path.append(state)
        return path


class NFA:
    """
    Non-deterministic Finite Automaton implementation.

    Attributes:
        states: Set of state identifiers
        alphabet: Set of valid input symbols (plus epsilon)
        transition: Dict mapping (state, symbol) -> set of next states
        start: Initial state
        accept: Set of accepting/final states
    """

    def __init__(
        self,
        states: set[str],
        alphabet: set[str],
        transition: dict[tuple[str, str | None], set[str]],
        start: str,
        accept: set[str],
    ) -> None:
        self.states = states
        self.alphabet = alphabet
        self.transition = transition
        self.start = start
        self.accept = accept

    def epsilon_closure(self, state_set: set[str]) -> set[str]:
        """
        Compute epsilon closure of a set of states.

        Args:
            state_set: Set of states

        Returns:
            Set of all states reachable via epsilon transitions
        """
        stack = list(state_set)
        closure = set(state_set)
        while stack:
            s = stack.pop()
            key = (s, None)
            if key in self.transition:
                for next_state in self.transition[key]:
                    if next_state not in closure:
                        closure.add(next_state)
                        stack.append(next_state)
        return closure

    def accepts(self, input_string: str) -> bool:
        """
        Check if the NFA accepts the given input string.

        Args:
            input_string: Sequence of symbols from the alphabet

        Returns:
            True if the string is accepted, False otherwise
        """
        current = self.epsilon_closure({self.start})
        for symbol in input_string:
            if symbol not in self.alphabet:
                return False
            next_states: set[str] = set()
            for s in current:
                key = (s, symbol)
                if key in self.transition:
                    next_states.update(self.transition[key])
            current = self.epsilon_closure(next_states)
            if not current:
                return False
        return bool(current & self.accept)


def build_dfa_from_regex(pattern: str) -> DFA:
    """
    Build a DFA from a simple regex pattern.

    Supports: literal characters, '.' (any char), '*' (Kleene star),
              '+' (one or more), '?' (optional)

    Args:
        pattern: Regular expression pattern string

    Returns:
        DFA that accepts strings matching the pattern
    """
    if not pattern:
        return DFA({"q0"}, set(), {}, "q0", {"q0"})

    states: set[str] = {f"q{i}" for i in range(len(pattern) + 2)}
    alphabet: set[str] = set(c for c in pattern if c not in ".*+?()|")
    alphabet.add(".")
    transition: dict[tuple[str, str], str] = {}

    state_map: dict[int | tuple[int, bool], str] = {}
    for i in range(len(pattern) + 2):
        state_map[i] = f"q{i}"
        if i < len(pattern) and pattern[i] == "(":
            state_map[(i, True)] = f"q{i}_branch"

    current = 0
    i = 0
    while i < len(pattern):
        c = pattern[i]
        s_curr = state_map[current]
        if c == ".":
            s_next = state_map[current + 1]
            for ch in alphabet:
                if ch != ".":
                    transition[(s_curr, ch)] = s_next
        elif c == "*":
            s_prev = state_map[current - 1]
            transition[(s_curr, "")] = s_prev
            transition[(s_prev, "")] = s_next = state_map[current + 1]
            transition[(s_curr, "")] = state_map[current + 1]
            current += 1
        elif c == "+":
            s_prev = state_map[current - 1]
            transition[(s_curr, "")] = s_next = state_map[current + 1]
            transition[(s_prev, "")] = s_next
            current += 1
        elif c == "?":
            s_next = state_map[current + 1]
            transition[(s_curr, "")] = s_next
            current += 1
        else:
            s_next = state_map[current + 1]
            transition[(s_curr, c)] = s_next
            current += 1
        i += 1

    start = state_map[0]
    accept = {state_map[len(pattern)]}
    return DFA(states, alphabet, transition, start, accept)


def levenshtein_automaton(pattern: str, max_distance: int) -> DFA:
    """
    Build a DFA that recognizes all strings within edit distance of pattern.

    Args:
        pattern: Reference string
        max_distance: Maximum allowed Levenshtein distance

    Returns:
        DFA accepting all strings within edit distance <= max_distance
    """
    n = len(pattern)
    states = set()
    for i in range(n + 1):
        for d in range(max_distance + 1):
            states.add((i, d))
    state_list = list(states)

    transition: dict[tuple[tuple[int, int], str], tuple[int, int]] = {}
    for i, d in states:
        for ch in "abcdefghijklmnopqrstuvwxyz":
            ni = i + 1
            nd = d
            if ni <= n and (i >= n or ch != pattern[i]):
                if d < max_distance:
                    nd = d + 1
                else:
                    continue
            new_state = (ni, nd)
            if new_state in states:
                transition[((i, d), ch)] = new_state

    accepting = {(n, d) for (_, d) in states if d <= max_distance}
    accepting_states = {f"q{state_list.index(s)}" for s in accepting}
    trans_normalized: dict[tuple[str, str], str] = {}
    for (i, d), ch, (ni, nd) in transition.items():
        si = f"q{state_list.index((i, d))}"
        sn = f"q{state_list.index((ni, nd))}"
        trans_normalized[(si, ch)] = sn

    state_ids = {s: f"q{state_list.index(s)}" for s in states}
    return DFA(
        states={state_ids[s] for s in states},
        alphabet={"abcdefghijklmnopqrstuvwxyz"},
        transition=trans_normalized,
        start=state_ids[(0, 0)],
        accept=accepting_states,
    )
