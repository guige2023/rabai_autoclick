"""
Markov chain and random process utilities.

Provides Markov chain analysis, hidden Markov models,
sequence generation, and state probability computations.
"""

from __future__ import annotations

import math
import random
from typing import Any, Callable


class MarkovChain:
    """First-order Markov chain."""

    def __init__(self, states: list[str] | None = None):
        self.states = states or []
        self.transition_probs: dict[str, dict[str, float]] = {}
        self.transition_counts: dict[str, dict[str, int]] = {}
        self._current: str | None = None

    def add_transition(self, from_state: str, to_state: str, count: int = 1) -> None:
        """Add a transition observation."""
        if from_state not in self.transition_counts:
            self.transition_counts[from_state] = {}
        self.transition_counts[from_state][to_state] = (
            self.transition_counts[from_state].get(to_state, 0) + count
        )
        if from_state not in self.states:
            self.states.append(from_state)
        if to_state not in self.states:
            self.states.append(to_state)

    def fit(self) -> "MarkovChain":
        """Compute transition probabilities from counts."""
        self.transition_probs = {}
        for from_state, counts in self.transition_counts.items():
            total = sum(counts.values())
            self.transition_probs[from_state] = {
                to_state: count / total
                for to_state, count in counts.items()
            }
        return self

    def next_state(self, current: str | None = None, seed: int | None = None) -> str | None:
        """Sample next state from transition distribution."""
        rng = random.Random(seed)
        if current is None:
            current = self._current
        if current not in self.transition_probs:
            return None
        probs = self.transition_probs[current]
        r = rng.random()
        cumulative = 0.0
        for state, prob in probs.items():
            cumulative += prob
            if r <= cumulative:
                return state
        return list(probs.keys())[-1]

    def generate_sequence(self, length: int, start_state: str | None = None) -> list[str]:
        """Generate a sequence of states."""
        rng = random.Random()
        if start_state is None:
            start_state = rng.choice(self.states) if self.states else None
        sequence = []
        current = start_state
        for _ in range(length):
            if current is None:
                break
            sequence.append(current)
            current = self.next_state(current)
        return sequence

    def stationary_distribution(self) -> dict[str, float]:
        """
        Compute stationary distribution.

        Finds eigenvector of transition matrix with eigenvalue 1.
        """
        if not self.states or not self.transition_probs:
            return {}
        n = len(self.states)
        # Power iteration
        pi = [1.0 / n] * n
        for _ in range(1000):
            new_pi = [0.0] * n
            for i, s in enumerate(self.states):
                for j, t in enumerate(self.states):
                    p = self.transition_probs.get(s, {}).get(t, 0.0)
                    new_pi[j] += pi[i] * p
            # Normalize
            total = sum(new_pi)
            if total > 0:
                new_pi = [p / total for p in new_pi]
            # Check convergence
            if all(abs(new_pi[i] - pi[i]) < 1e-8 for i in range(n)):
                break
            pi = new_pi
        return {s: pi[i] for i, s in enumerate(self.states)}

    def probability(self, sequence: list[str]) -> float:
        """Compute probability of a sequence."""
        if not sequence:
            return 1.0
        prob = 1.0
        for i in range(len(sequence) - 1):
            p = self.transition_probs.get(sequence[i], {}).get(sequence[i + 1], 0.0)
            prob *= p
        return prob


class HiddenMarkovModel:
    """Hidden Markov Model (simplified)."""

    def __init__(
        self,
        states: list[str],
        observations: list[str],
    ):
        self.states = states
        self.observations = observations
        self.initial_probs: dict[str, float] = {}
        self.transition_probs: dict[str, dict[str, float]] = {}
        self.emission_probs: dict[str, dict[str, float]] = {}

    def fit(
        self,
        sequences: list[list[str]],
        method: str = "max_likelihood",
    ) -> None:
        """
        Fit HMM from observation sequences.

        Args:
            sequences: List of observation sequences
        """
        # Initialize counts
        for s in self.states:
            self.transition_probs[s] = {t: 0.0 for t in self.states}
            self.emission_probs[s] = {o: 0.0 for o in self.observations}
        self.initial_probs = {s: 0.0 for s in self.states}

        trans_counts: dict[str, dict[str, int]] = {s: {t: 0 for t in self.states} for s in self.states}
        emit_counts: dict[str, dict[str, int]] = {s: {o: 0 for o in self.observations} for s in self.states}
        init_counts: dict[str, int] = {s: 0 for s in self.states}

        for seq in sequences:
            if not seq:
                continue
            init_counts[seq[0]] += 1
            for s in self.states:
                emit_counts[s][seq[0]] += 1
            for i in range(len(seq) - 1):
                s = seq[i]
                s_next = seq[i + 1]
                trans_counts[s][s_next] += 1

        total_init = sum(init_counts.values())
        for s in self.states:
            self.initial_probs[s] = init_counts[s] / total_init if total_init > 0 else 0.0
            total_trans = sum(trans_counts[s].values())
            for t in self.states:
                self.transition_probs[s][t] = trans_counts[s][t] / total_trans if total_trans > 0 else 0.0
            total_emit = sum(emit_counts[s].values())
            for o in self.observations:
                self.emission_probs[s][o] = emit_counts[s][o] / total_emit if total_emit > 0 else 0.0

    def viterbi(self, obs_sequence: list[str]) -> list[str]:
        """
        Viterbi algorithm: find most likely state sequence.

        Args:
            obs_sequence: Observed symbols

        Returns:
            Most likely hidden state sequence.
        """
        n_states = len(self.states)
        n_obs = len(obs_sequence)
        if n_obs == 0:
            return []

        # Initialize
        viterbi: list[dict[str, float]] = [{}]
        backpointer: list[dict[str, int]] = [{}]
        for s in self.states:
            viterbi[0][s] = self.initial_probs.get(s, 0.0) * self.emission_probs.get(s, {}).get(obs_sequence[0], 0.0)
            backpointer[0][s] = 0

        # Forward pass
        for t in range(1, n_obs):
            viterbi.append({})
            backpointer.append({})
            for s in self.states:
                max_prob = 0.0
                best_prev = self.states[0]
                for prev_s in self.states:
                    prob = viterbi[t - 1][prev_s] * self.transition_probs.get(prev_s, {}).get(s, 0.0)
                    if prob > max_prob:
                        max_prob = prob
                        best_prev = prev_s
                viterbi[t][s] = max_prob * self.emission_probs.get(s, {}).get(obs_sequence[t], 0.0)
                backpointer[t][s] = self.states.index(best_prev)

        # Backtrack
        best_path_prob = max(viterbi[n_obs - 1].values())
        best_path: list[str] = []
        last_state = max(self.states, key=lambda s: viterbi[n_obs - 1][s])
        best_path.append(last_state)
        for t in range(n_obs - 1, 0, -1):
            bp_idx = backpointer[t][last_state]
            last_state = self.states[bp_idx]
            best_path.insert(0, last_state)
        return best_path

    def forward_algorithm(
        self,
        obs_sequence: list[str],
    ) -> float:
        """
        Forward algorithm: compute probability of observation sequence.

        Returns:
            Log probability of the sequence.
        """
        n_states = len(self.states)
        n_obs = len(obs_sequence)
        if n_obs == 0:
            return 1.0

        alpha: list[dict[str, float]] = []
        alpha.append({
            s: self.initial_probs.get(s, 0.0) * self.emission_probs.get(s, {}).get(obs_sequence[0], 0.0)
            for s in self.states
        })

        for t in range(1, n_obs):
            alpha.append({})
            for s in self.states:
                total = sum(alpha[t - 1][prev] * self.transition_probs.get(prev, {}).get(s, 0.0) for prev in self.states)
                alpha[t][s] = total * self.emission_probs.get(s, {}).get(obs_sequence[t], 0.0)

        return sum(alpha[n_obs - 1].values())


def ngram_markov_chain(
    sequences: list[list[Any]],
    order: int = 2,
) -> MarkovChain:
    """
    Build Markov chain from sequences with n-gram transitions.

    Args:
        sequences: List of sequences
        order: N-gram order (1 = memoryless)

    Returns:
        Fitted MarkovChain.
    """
    chain = MarkovChain()
    for seq in sequences:
        if not seq:
            continue
        for i in range(len(seq) - order + 1):
            ngram = tuple(seq[i:i + order])
            next_val = seq[i + order] if i + order < len(seq) else None
            if next_val is not None:
                chain.add_transition(str(ngram), str((ngram[1:] + (next_val,))))
    chain.fit()
    return chain


def absorbing_states(transition_probs: dict[str, dict[str, float]]) -> list[str]:
    """Find absorbing states in a Markov chain."""
    absorbing = []
    for state, trans in transition_probs.items():
        if state in trans and abs(trans[state] - 1.0) < 1e-8:
            absorbing.append(state)
    return absorbing


def fundamental_matrix(
    transition_probs: dict[str, dict[str, float]],
    transient_states: list[str],
) -> dict[tuple[str, str], float]:
    """
    Compute fundamental matrix N = (I - Q)^-1 for absorbing Markov chain.

    Args:
        transition_probs: Full transition probability matrix
        transient_states: List of transient (non-absorbing) states

    Returns:
        Fundamental matrix N.
    """
    n = len(transient_states)
    if n == 0:
        return {}

    # Build Q matrix (transient to transient)
    Q: list[list[float]] = []
    for i, s_i in enumerate(transient_states):
        row = []
        for j, s_j in enumerate(transient_states):
            row.append(transition_probs.get(s_i, {}).get(s_j, 0.0))
        Q.append(row)

    # I - Q
    I_Q = [[(1.0 if i == j else 0.0) - Q[i][j] for j in range(n)] for i in range(n)]

    # Gauss-Jordan inversion
    aug = [I_Q[i] + [1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            factor = aug[j][i] / aug[i][i]
            for k in range(2 * n):
                aug[j][k] -= factor * aug[i][k]
    for i in range(n - 1, -1, -1):
        for j in range(i - 1, -1, -1):
            factor = aug[j][i] / aug[i][i]
            for k in range(2 * n):
                aug[j][k] -= factor * aug[i][k]
        for k in range(2 * n):
            aug[i][k] /= aug[i][i]

    N = [[aug[i][n + j] for j in range(n)] for i in range(n)]
    result: dict[tuple[str, str], float] = {}
    for i, s_i in enumerate(transient_states):
        for j, s_j in enumerate(transient_states):
            result[(s_i, s_j)] = N[i][j]
    return result
