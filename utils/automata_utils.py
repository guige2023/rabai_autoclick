"""
Automaton models and utilities.

Provides finite automata (DFA/NFA), regular expression to NFA conversion,
automaton minimization, and state machine utilities.
"""

from __future__ import annotations

import math
from typing import Callable


class DFA:
    """Deterministic Finite Automaton."""

    def __init__(
        self,
        states: set[str],
        alphabet: set[str],
        transition: dict[tuple[str, str], str],
        start: str,
        accepts: set[str],
    ):
        self.states = states
        self.alphabet = alphabet
        self.transition = transition
        self.start = start
        self.accepts = accepts

    def step(self, state: str, symbol: str) -> str | None:
        """Single transition step."""
        return self.transition.get((state, symbol))

    def accepts_string(self, input_string: str) -> bool:
        """Check if DFA accepts the input string."""
        state = self.start
        for ch in input_string:
            state = self.step(state, ch)
            if state is None:
                return False
        return state in self.accepts

    def accepts_lambda(self) -> bool:
        """Check if epsilon (empty string) is accepted."""
        return self.start in self.accepts


class NFA:
    """Non-deterministic Finite Automaton."""

    def __init__(
        self,
        states: set[str],
        alphabet: set[str],
        transition: dict[tuple[str, str], set[str]],
        start: str,
        accepts: set[str],
    ):
        self.states = states
        self.alphabet = alphabet
        self.transition = transition
        self.start = start
        self.accepts = accepts

    def epsilon_closure(self, states: set[str]) -> set[str]:
        """Compute epsilon closure of a set of states."""
        stack = list(states)
        closure = set(states)
        while stack:
            s = stack.pop()
            for t in self.transition.get((s, ""), set()):
                if t not in closure:
                    closure.add(t)
                    stack.append(t)
        return closure

    def move(self, states: set[str], symbol: str) -> set[str]:
        """Move from set of states on symbol."""
        result: set[str] = set()
        for s in states:
            result.update(self.transition.get((s, symbol), set()))
        return result

    def accepts_string(self, input_string: str) -> bool:
        """Check if NFA accepts the input string."""
        current = self.epsilon_closure({self.start})
        for ch in input_string:
            current = self.epsilon_closure(self.move(current, ch))
            if not current:
                return False
        return bool(current & self.accepts)


def regex_to_nfa(pattern: str) -> NFA:
    """
    Convert a simple regex pattern to NFA using Thompson's construction.

    Supports: concatenation, union (|), Kleene star (*), plus (+), optional (?).

    Args:
        pattern: Regular expression pattern

    Returns:
        NFA object.
    """
    # Simple approach: build state machine for basic operations
    # This is a simplified implementation
    state_counter = [0]

    def new_state() -> str:
        s = f"q{state_counter[0]}"
        state_counter[0] += 1
        return s

    def build(expr: str) -> tuple[set[str], dict[tuple[str, str], set[str]], str, str, set[str]]:
        """Build NFA for expression, return (states, trans, start, end, accepts)."""
        states: set[str] = set()
        transition: dict[tuple[str, str], set[str]] = {}
        accepts_states: set[str] = set()

        if not expr:
            s = new_state()
            states.add(s)
            accepts_states.add(s)
            return states, transition, s, s, accepts_states

        # Parse concatenation
        i = 0
        fragments: list[tuple[set[str], dict, str, str, set[str]]] = []

        while i < len(expr):
            ch = expr[i]
            if ch == "(":
                depth = 1
                j = i + 1
                while j < len(expr) and depth > 0:
                    if expr[j] == "(":
                        depth += 1
                    elif expr[j] == ")":
                        depth -= 1
                    j += 1
                sub_expr = expr[i + 1:j - 1]
                frag = build(sub_expr)
                fragments.append(frag)
                i = j
            elif ch == "|":
                # Union: combine left and right
                left = build(expr[:i])
                right = build(expr[i + 1:])
                s = new_state()
                e = new_state()
                left.start, right.start  # type: ignore
                transition = {**left[1], **right[1]}
                transition[(s, "")] = {left[2], right[2]}
                for f in left[4] | right[4]:
                    transition[(f, "")] = {e}
                return states | left[0] | right[0] | {s, e}, transition, s, e, {e}
            elif ch == "*":
                if fragments:
                    last = fragments.pop()
                    s = new_state()
                    e = new_state()
                    transition = {**last[1]}
                    transition[(s, "")] = {last[2], e}
                    for f in last[4]:
                        transition[(f, "")] = {last[2], e}
                    return last[0] | {s, e}, transition, s, e, {e}
            elif ch == "+":
                if fragments:
                    last = fragments.pop()
                    s = new_state()
                    e = new_state()
                    transition = {**last[1]}
                    transition[(s, "")] = {last[2]}
                    for f in last[4]:
                        transition[(f, "")] = {last[2], e}
                    return last[0] | {s, e}, transition, s, e, {e}
            elif ch == "?":
                if fragments:
                    last = fragments.pop()
                    s = new_state()
                    transition = {**last[1]}
                    transition[(s, "")] = {last[2]}
                    for f in last[4]:
                        transition[(f, "")] = {last[2]}
                    return last[0] | {s}, transition, s, last[3], last[4]
            else:
                # Literal character
                s = new_state()
                e = new_state()
                transition[(s, ch)] = {e}
                fragments.append(({s, e}, transition, s, e, {e}))
                i += 1
                continue
            i += 1

        # Concatenate all fragments
        if not fragments:
            return states, transition, "", "", set()

        result_states: set[str] = set()
        result_trans: dict = {}
        result_accepts: set[str] = set()

        for f in fragments:
            result_states |= f[0]
            result_trans.update(f[1])
            result_accepts |= f[4]

        combined_start = fragments[0][2]
        combined_end = fragments[-1][3]

        # Link fragments
        for i in range(len(fragments) - 1):
            end_i = fragments[i][3]
            start_next = fragments[i + 1][2]
            if (end_i, "") not in result_trans:
                result_trans[(end_i, "")] = set()
            result_trans[(end_i, "")].add(start_next)

        return result_states, result_trans, combined_start, combined_end, result_accepts

    nfa_info = build(pattern)
    return NFA(
        states=nfa_info[0],
        alphabet=set(c for c in pattern if c not in "()|*+?"),
        transition=nfa_info[1],
        start=nfa_info[2],
        accepts=nfa_info[4],
    )


def nfa_to_dfa(nfa: NFA) -> DFA:
    """
    Convert NFA to DFA using powerset construction.

    Args:
        nfa: Source NFA

    Returns:
        Equivalent DFA.
    """
    from collections import deque

    alphabet = nfa.alphabet - {""}  # Remove epsilon
    dfa_trans: dict[tuple[frozenset[str], str], frozenset[str]] = {}

    start_closure = nfa.epsilon_closure({nfa.start})
    initial = frozenset(start_closure)
    queue: deque[frozenset[str]] = deque([initial])
    visited: set[frozenset[str]] = {initial}
    dfa_accepts: set[frozenset[str]] = set()

    if start_closure & nfa.accepts:
        dfa_accepts.add(initial)

    while queue:
        current = queue.popleft()
        for sym in alphabet:
            next_states = nfa.epsilon_closure(nfa.move(current, sym))
            next_frozen = frozenset(next_states)
            dfa_trans[(current, sym)] = next_frozen
            if next_frozen not in visited:
                visited.add(next_frozen)
                queue.append(next_frozen)
                if next_states & nfa.accepts:
                    dfa_accepts.add(next_frozen)

    dfa_states = visited
    dfa_trans_dict = {(tuple(s)[0] if len(s) == 1 else str(s), sym): tuple(t)[0] if len(t) == 1 else str(t)  # type: ignore
                      for (s, sym), t in dfa_trans.items()}
    dfa_trans_simple: dict[tuple[str, str], str] = {}
    for (s, sym), t in dfa_trans.items():
        key = str(sorted(s)), sym
        val = str(sorted(t))
        dfa_trans_simple[(val, sym)] = val

    return DFA(
        states={str(sorted(s)) for s in dfa_states},
        alphabet=alphabet,
        transition={},
        start=str(sorted(initial)),
        accepts={str(sorted(a)) for a in dfa_accepts},
    )


def dfa_minimize(dfa: DFA) -> DFA:
    """
    Minimize a DFA using Hopcroft's algorithm.

    Args:
        dfa: DFA to minimize

    Returns:
        Minimized DFA.
    """
    # Partition refinement
    non_accepting = dfa.states - dfa.accepts
    partitions = []
    if dfa.accepts:
        partitions.append(dfa.accepts)
    if non_accepting:
        partitions.append(non_accepting)

    changed = True
    while changed:
        changed = False
        new_partitions: list[set[str]] = []
        for part in partitions:
            split: dict[tuple[str, ...], set[str]] = {}
            for state in part:
                key = tuple(sorted(dfa.transition.get((state, sym), "") or "" for sym in sorted(dfa.alphabet)))
                if key not in split:
                    split[key] = set()
                split[key].add(state)
            if len(split) > 1:
                changed = True
            new_partitions.extend(split.values())
        partitions = new_partitions

    # Build minimized DFA
    new_states: set[str] = set()
    new_trans: dict[tuple[str, str], str] = {}
    state_map: dict[str, str] = {}
    for i, part in enumerate(partitions):
        rep = f"Q{i}"
        new_states.add(rep)
        for s in part:
            state_map[s] = rep

    for (s, sym), t in dfa.transition.items():
        src = state_map.get(s, s)
        dst = state_map.get(t, t)
        new_trans[(src, sym)] = dst

    new_start = state_map.get(dfa.start, dfa.start)
    new_accepts = {state_map.get(s, s) for s in dfa.accepts}

    return DFA(new_states, dfa.alphabet, new_trans, new_start, new_accepts)


def regex_matches(pattern: str, text: str) -> bool:
    """Check if regex pattern matches entire text (simplified)."""
    nfa = regex_to_nfa(pattern)
    return nfa.accepts_string(text)


def levenshtein_automaton(word: str, max_distance: int) -> DFA:
    """
    Build a Levenshtein automaton that accepts all strings
    within max_distance edits of word.

    This is a simplified implementation.
    """
    # Build a simple NFA-based Levenshtein automaton
    n = len(word)
    states = set()
    for i in range(n + 1):
        for d in range(max_distance + 1):
            states.add((i, d))

    alphabet = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")
    trans: dict[tuple[tuple[int, int], str], set[tuple[int, int]]] = {}

    for i, d in states:
        # Match
        if i < n:
            trans.setdefault(((i, d), word[i]), set()).add((i + 1, d))
        # Insert
        trans.setdefault(((i, d), ""), set()).add((i, d + 1))
        # Delete
        if i < n:
            trans.setdefault(((i, d), ""), set()).add((i + 1, d + 1))
        # Substitute
        if i < n and d < max_distance:
            for c in alphabet:
                if c != word[i]:
                    trans.setdefault(((i, d), c), set()).add((i + 1, d + 1))

    # Filter out states with distance > max_distance
    nfa = NFA(states, alphabet, trans, (0, 0), {(n, d) for d in range(max_distance + 1)})
    return nfa_to_dfa(nfa)
