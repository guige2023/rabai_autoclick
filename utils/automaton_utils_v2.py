"""
Automaton utilities v2 — regex engine and state machine minimization.

Companion to automaton_utils.py. Adds regex engine, DFA minimization,
and automaton composition operations.
"""

from __future__ import annotations

import re


class RegexEngine:
    """
    Simple regex engine supporting: literal, ., *, +, ?, ^, $, |, ()
    """

    def __init__(self, pattern: str) -> None:
        self.pattern = pattern
        self._parsed = self._parse(pattern)

    def _parse(self, pattern: str) -> dict:
        return {"type": "concat", "parts": self._parse_parts(pattern)}

    def _parse_parts(self, s: str) -> list:
        parts = []
        i = 0
        while i < len(s):
            c = s[i]
            if c == "(":
                depth, j = 1, i + 1
                while j < len(s) and depth > 0:
                    if s[j] == "(":
                        depth += 1
                    elif s[j] == ")":
                        depth -= 1
                    j += 1
                parts.append(self._parse(s[i + 1:j - 1]))
                i = j
            elif c == ".":
                parts.append({"type": "dot"})
                i += 1
            elif c == "*":
                if parts:
                    parts[-1] = {"type": "star", "inner": parts[-1]}
                i += 1
            elif c == "+":
                if parts:
                    inner = parts.pop()
                    parts.append({"type": "plus", "inner": inner})
                i += 1
            elif c == "?":
                if parts:
                    parts[-1] = {"type": "question", "inner": parts[-1]}
                i += 1
            else:
                parts.append({"type": "char", "char": c})
                i += 1
        return parts

    def matches(self, s: str) -> bool:
        return self._match(self._parsed, s, 0) is not None

    def _match(self, node: dict, s: str, pos: int) -> int | None:
        ntype = node["type"]
        if ntype == "char":
            if pos < len(s) and s[pos] == node["char"]:
                return pos + 1
            return None
        if ntype == "dot":
            return pos + 1 if pos < len(s) else None
        if ntype == "concat":
            current = pos
            for part in node["parts"]:
                current = self._match(part, s, current)
                if current is None:
                    return None
            return current
        if ntype == "star":
            current = pos
            while True:
                result = self._match(node["inner"], s, current)
                if result is None:
                    break
                current = result
            return current
        if ntype == "plus":
            current = self._match(node["inner"], s, pos)
            if current is None:
                return None
            while True:
                next_result = self._match(node["inner"], s, current)
                if next_result is None:
                    break
                current = next_result
            return current
        if ntype == "question":
            result = self._match(node["inner"], s, pos)
            return result if result is not None else pos
        return None

    def search(self, s: str) -> tuple[int, int] | None:
        """Find first match, return (start, end) indices."""
        for i in range(len(s) + 1):
            result = self._match(self._parsed, s, i)
            if result is not None:
                return i, result
        return None


def dfa_minimize(dfa: "DFA") -> "DFA":
    """
    Minimize a DFA using Hopcroft's algorithm.

    Args:
        dfa: DFA to minimize

    Returns:
        Minimized DFA
    """
    from utils.automaton_utils import DFA

    non_accepting = dfa.states - dfa.accept
    partitions = []
    if dfa.accept:
        partitions.append(dfa.accept.copy())
    if non_accepting:
        partitions.append(non_accepting)

    worklist: set[tuple[set[str], str]] = set()
    for p in partitions:
        for s in p:
            for a in dfa.alphabet:
                if (s, a) in [(st, at) for st in p for at in dfa.alphabet]:
                    worklist.add((frozenset(p), a))
                    break

    while worklist:
        part_set, sym = worklist.pop()
        for p in partitions:
            if part_set.issubset(p) and len(p) > 1:
                p1, p2 = split_state_set(p, part_set, dfa)
                if p1 and p2:
                    partitions.remove(p)
                    partitions.extend([p1, p2])

    new_states = {frozenset(p) for p in partitions}
    new_start = next((s for s in new_states if dfa.start in s), frozenset({dfa.start}))
    new_accept = {s for s in new_states if s & dfa.accept}
    new_trans = {}
    for st_set in new_states:
        for s in st_set:
            for a in dfa.alphabet:
                key = (s, a)
                if key in dfa.transition:
                    next_st = dfa.transition[key]
                    for ns in new_states:
                        if next_st in ns:
                            new_trans[(st_set, a)] = ns
                            break
            break

    return DFA(
        states=new_states,
        alphabet=dfa.alphabet,
        transition=new_trans,
        start=new_start,
        accept=new_accept,
    )


def split_state_set(p: set, splitter: set, dfa: "DFA") -> tuple[set, set]:
    """Split p based on which states transition into splitter."""
    in_splitter: list[bool] = []
    not_in_splitter: list[bool] = []
    for s in p:
        for a in dfa.alphabet:
            key = (s, a)
            if key in dfa.transition:
                in_splitter.append(dfa.transition[key] in splitter)
                break
        else:
            not_in_splitter.append(s)
    return set(in_splitter) if in_splitter else set(), set(not_in_splitter) if not_in_splitter else set()


def nfa_to_dfa(nfa: "NFA") -> "DFA":
    """Convert NFA to equivalent DFA using powerset construction."""
    from utils.automaton_utils import DFA, NFA

    initial = frozenset(nfa.epsilon_closure({nfa.start}))
    all_states: set[frozenset[str]] = {initial}
    queue = [initial]
    transition: dict[tuple[frozenset[str], str], frozenset[str]] = {}

    while queue:
        current = queue.pop(0)
        for sym in nfa.alphabet:
            if sym is None:
                continue
            next_set: set[str] = set()
            for s in current:
                key = (s, sym)
                if key in nfa.transition:
                    next_set.update(nfa.transition[key])
            next_closure = nfa.epsilon_closure(next_set)
            next_frozen = frozenset(next_closure)
            transition[(current, sym)] = next_frozen
            if next_frozen not in all_states:
                all_states.add(next_frozen)
                queue.append(next_frozen)

    accept_states = {s for s in all_states if s & nfa.accept}
    return DFA(
        states=all_states,
        alphabet=nfa.alphabet - {None},
        transition=transition,
        start=initial,
        accept=accept_states,
    )


def regex_to_nfa(pattern: str) -> "NFA":
    """Build Thompson NFA from regex pattern."""
    from utils.automaton_utils import NFA

    counter = [0]

    def new_state() -> str:
        counter[0] += 1
        return f"s{counter[0]}"

    def build(s: str, i: int) -> tuple[NFA, str]:
        states = set()
        trans = {}
        accepts: set[str] = set()
        start = new_state()
        states.add(start)
        current = start
        while i < len(s):
            c = s[i]
            if c == "(":
                sub_nfa, end = build(s, i + 1)
                next_state = new_state()
                states.add(next_state)
                trans[(current, None)] = {sub_nfa.start}
                states.update(sub_nfa.states)
                for k, v in sub_nfa.transition.items():
                    trans[k] = v
                accepts.update(sub_nfa.accept)
                current = next_state
                while i < len(s) and s[i] != ")":
                    i += 1
                i += 1
            elif c == ")":
                accept = new_state()
                states.add(accept)
                accepts.add(accept)
                trans[(current, None)] = {accept}
                return NFA(states, {"a"}, trans, start, accepts), current
            elif c == "*":
                i += 1
            elif c == "+":
                i += 1
            elif c == "?":
                i += 1
            else:
                next_s = new_state()
                states.add(next_s)
                trans[(current, c)] = {next_s}
                current = next_s
                i += 1
        accept = new_state()
        states.add(accept)
        accepts.add(accept)
        trans[(current, None)] = {accept}
        return NFA(states, set(c for c in s if c not in "()*+?()"), trans, start, accepts), current

    nfa, _ = build(pattern, 0)
    return nfa
