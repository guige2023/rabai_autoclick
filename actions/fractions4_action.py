"""Fractions action v4 - game theory and economics utilities.

Fraction utilities for game theory, economics,
and combinatorial calculations.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Sequence

__all__ = [
    "fair_division",
    "proportional_allocation",
    "fractional_voting",
    "ranked_choice_tally",
    "borda_count",
    "copeland_winner",
    " Approval_voting_tally",
    "quadratic_voting",
    "fractional_shares",
    "dilution",
    "convertible_ratio",
    "fractional_knapsack",
    "gini_coefficient_fractions",
    "lorenz_curve",
    "fractional_odds",
    "implied_probability",
    "fractional_betting",
    "hhi_fraction",
    "HerfindahlIndex",
    "FractionalEconomics",
]


def fair_division(items: list[tuple[str, Fraction]], players: int) -> list[dict]:
    """Calculate fair division using adjusted winner algorithm.

    Args:
        items: List of (item_name, value_fraction).
        players: Number of players.

    Returns:
        List of player allocations.
    """
    allocations = [{} for _ in range(players)]
    total_value = sum(v.numerator / v.denominator for _, v in items)
    per_player = total_value / players
    remaining = dict(items)
    for i in range(players):
        share = Fraction(0)
        for item, val in list(remaining.items()):
            if share + val <= per_player:
                allocations[i][item] = val
                share += val
                del remaining[item]
    for item, val in remaining.items():
        allocations[0][item] = val
    return allocations


def proportional_allocation(total: Fraction, weights: Sequence[Fraction]) -> list[Fraction]:
    """Allocate total proportionally by weights.

    Args:
        total: Total to allocate.
        weights: List of weights.

    Returns:
        List of allocations.
    """
    total_weight = sum(weights)
    if total_weight == 0:
        return [Fraction(0)] * len(weights)
    return [total * (w / total_weight) for w in weights]


def fractional_voting(votes: Sequence[Fraction], threshold: Fraction = Fraction(1, 2)) -> bool:
    """Check if fraction of votes meets threshold.

    Args:
        votes: Sequence of vote fractions.
        threshold: Fraction needed to pass.

    Returns:
        True if threshold met.
    """
    total = sum(votes)
    return total >= threshold


def ranked_choice_tally(ballots: Sequence[Sequence[str]]) -> dict[str, int]:
    """Ranked choice voting tally.

    Args:
        ballots: List of ranked ballot lists.

    Returns:
        Dict mapping candidate to vote count.
    """
    from collections import Counter
    first_choices = [ballot[0] for ballot in ballots if ballot]
    return dict(Counter(first_choices))


def borda_count(ballots: Sequence[Sequence[str]]) -> dict[str, int]:
    """Borda count voting.

    Args:
        ballots: List of ranked ballot lists.

    Returns:
        Dict mapping candidate to Borda score.
    """
    scores = {}
    for ballot in ballots:
        for rank, candidate in enumerate(ballot):
            n = len(ballot)
            points = n - rank - 1
            scores[candidate] = scores.get(candidate, 0) + points
    return scores


def copeland_winner(ballots: Sequence[Sequence[str]]) -> str | None:
    """Copeland's method pairwise comparison winner.

    Args:
        ballots: List of ranked ballots.

    Returns:
        Winner name or None.
    """
    from collections import defaultdict
    candidates = set()
    for ballot in ballots:
        candidates.update(ballot)
    wins = defaultdict(int)
    for ballot in ballots:
        for i, a in enumerate(ballot):
            for b in ballot[i + 1:]:
                wins[a] += 1
                wins[b] -= 1
    if not wins:
        return None
    return max(wins, key=wins.get)


def Approval_voting_tally(ballots: Sequence[Sequence[str]]) -> dict[str, int]:
    """Approval voting tally.

    Args:
        ballots: List of approved candidate lists.

    Returns:
        Dict mapping candidate to approval count.
    """
    from collections import Counter
    all_approved = [c for ballot in ballots for c in ballot]
    return dict(Counter(all_approved))


def quadratic_voting(credits: int, num_issues: int) -> list[int]:
    """Quadratic voting allocation of credits.

    Args:
        credits: Total voting credits.
        num_issues: Number of issues to vote on.

    Returns:
        List of votes per issue.
    """
    votes = [0] * num_issues
    remaining = credits
    while remaining > 0:
        best_idx = 0
        best_marginal = 0
        for i in range(num_issues):
            marginal_cost = 2 * votes[i] + 1
            if marginal_cost <= remaining and marginal_cost > best_marginal:
                best_marginal = marginal_cost
                best_idx = i
        if best_marginal == 0:
            break
        votes[best_idx] += 1
        remaining -= best_marginal
    return votes


def fractional_shares(total: Fraction, numerators: Sequence[int], denominator: int) -> list[Fraction]:
    """Calculate fractional share allocations.

    Args:
        total: Total shares.
        numerators: List of numerator values.
        denominator: Common denominator.

    Returns:
        List of share fractions.
    """
    total_numerator = sum(numerators)
    if total_numerator == 0:
        return [Fraction(0)] * len(numerators)
    return [total * Fraction(n, total_numerator) for n in numerators]


def dilution(original: Fraction, new_shares: int) -> Fraction:
    """Calculate ownership dilution.

    Args:
        original: Original ownership fraction.
        new_shares: Number of new shares issued.

    Returns:
        New ownership fraction.
    """
    return original / (Fraction(1) + Fraction(new_shares, 1))


def convertible_ratio(debt: Fraction, equity: Fraction, conversion_ratio: Fraction) -> Fraction:
    """Calculate post-conversion ownership.

    Args:
        debt: Debt amount.
        equity: Existing equity.
        conversion_ratio: Conversion ratio.

    Returns:
        Post-conversion equity fraction.
    """
    new_equity = debt * conversion_ratio
    total = equity + new_equity
    return new_equity / total if total > 0 else Fraction(0)


def fractional_knapsack(items: list[tuple[Fraction, Fraction]], capacity: Fraction) -> Fraction:
    """Fractional knapsack solver.

    Args:
        items: List of (value, weight) fractions.
        capacity: Knapsack capacity.

    Returns:
        Maximum value achievable.
    """
    ratios = [(v / w, v, w) for v, w in items if w > 0]
    ratios.sort(key=lambda x: float(x[0]), reverse=True)
    total_value = Fraction(0)
    remaining = capacity
    for ratio, v, w in ratios:
        if remaining >= w:
            total_value += v
            remaining -= w
        else:
            total_value += v * (remaining / w)
            break
    return total_value


def gini_coefficient_fractions(values: list[Fraction]) -> Fraction:
    """Calculate Gini coefficient.

    Args:
        values: List of value fractions.

    Returns:
        Gini coefficient.
    """
    if not values:
        return Fraction(0)
    n = len(values)
    sorted_vals = sorted(values)
    cumsum = Fraction(0)
    for i, v in enumerate(sorted_vals):
        cumsum += (2 * i + 1) * v
    total = sum(values)
    if total == 0:
        return Fraction(0)
    gini = (2 * cumsum) / (n * total) - (n + 1) / n
    return max(Fraction(0), gini)


def lorenz_curve(values: list[Fraction]) -> list[Fraction]:
    """Generate Lorenz curve points.

    Args:
        values: List of value fractions.

    Returns:
        List of cumulative population and income fractions.
    """
    if not values:
        return []
    n = len(values)
    sorted_vals = sorted(values)
    total = sum(sorted_vals)
    if total == 0:
        return [(Fraction(0), Fraction(0))] * (n + 1)
    cumsums = []
    running = Fraction(0)
    for v in sorted_vals:
        running += v
        cumsums.append(running / total)
    return cumsums


def fractional_odds(win: int, lose: int) -> Fraction:
    """Convert odds to probability fraction.

    Args:
        win: Number of wins.
        lose: Number of losses.

    Returns:
        Probability fraction.
    """
    total = win + lose
    if total == 0:
        return Fraction(0)
    return Fraction(win, total)


def implied_probability(odds: Fraction) -> Fraction:
    """Calculate implied probability from odds fraction.

    Args:
        odds: Odds as fraction (e.g., 3/1 means win 3 for each 1 bet).

    Returns:
        Implied probability.
    """
    return Fraction(1, 1) / (odds + Fraction(1, 1))


def fractional_betting(stake: Fraction, odds: Fraction) -> Fraction:
    """Calculate payout from fractional odds.

    Args:
        stake: Amount bet.
        odds: Fractional odds.

    Returns:
        Total payout (including stake).
    """
    return stake + stake * odds


def hhi_fraction(market_shares: list[Fraction]) -> Fraction:
    """Herfindahl-Hirschman Index.

    Args:
        market_shares: List of market share fractions.

    Returns:
        HHI value.
    """
    return sum(s * s for s in market_shares)


class HerfindahlIndex:
    """Herfindahl-Hirschman Index calculator."""

    def __init__(self) -> None:
        self._shares: list[Fraction] = []

    def add_share(self, share: Fraction) -> None:
        """Add a market share."""
        self._shares.append(share)

    def hhi(self) -> Fraction:
        """Calculate HHI."""
        return hhi_fraction(self._shares)

    def is_concentrated(self, threshold: Fraction = Fraction(2500, 1)) -> bool:
        """Check if market is highly concentrated."""
        return self.hhi() >= threshold

    def normalize(self) -> Fraction:
        """Normalize HHI to 0-1 range."""
        n = len(self._shares)
        if n < 2:
            return Fraction(0)
        hhi = self.hhi()
        max_hhi = Fraction(1)
        min_hhi = Fraction(1) / Fraction(n)
        if max_hhi == min_hhi:
            return Fraction(0)
        return (hhi - min_hhi) / (max_hhi - min_hhi)


class FractionalEconomics:
    """Economic calculations with fractions."""

    @staticmethod
    def unemployment_rate(labor_force: Fraction, unemployed: Fraction) -> Fraction:
        """Calculate unemployment rate."""
        if labor_force == 0:
            return Fraction(0)
        return unemployed / labor_force

    @staticmethod
    def labor_force_participation(working_age: Fraction, labor_force: Fraction) -> Fraction:
        """Labor force participation rate."""
        if working_age == 0:
            return Fraction(0)
        return labor_force / working_age

    @staticmethod
    def inflation_rate(old_price: Fraction, new_price: Fraction) -> Fraction:
        """Calculate inflation rate."""
        if old_price == 0:
            return Fraction(0)
        return (new_price - old_price) / old_price

    @staticmethod
    def real_wage(nominal_wage: Fraction, inflation_rate: Fraction) -> Fraction:
        """Calculate real wage."""
        return nominal_wage / (Fraction(1) + inflation_rate)

    @staticmethod
    def price_index(basket: list[tuple[Fraction, Fraction]]) -> Fraction:
        """Calculate price index (Laspeyres).

        Args:
            basket: List of (price, quantity) fractions.

        Returns:
            Price index.
        """
        if not basket:
            return Fraction(0)
        base_total = sum(p * q for p, q in basket)
        return base_total
