"""Weighted random selection utilities.

Provides weighted random choice and sampling for
probability-based selection in automation workflows.
"""

import random
from typing import Any, List, Optional, Tuple


ChoiceItem = Tuple[Any, float]  # (item, weight)


def weighted_choice(choices: List[ChoiceItem]) -> Any:
    """Select a random item based on weights.

    Args:
        choices: List of (item, weight) tuples.

    Returns:
        Selected item.

    Raises:
        ValueError: If choices is empty.
    """
    if not choices:
        raise ValueError("Cannot choose from empty list")

    total = sum(weight for _, weight in choices)
    if total <= 0:
        raise ValueError("Total weight must be positive")

    r = random.uniform(0, total)
    cumulative = 0.0
    for item, weight in choices:
        cumulative += weight
        if r <= cumulative:
            return item
    return choices[-1][0]


def weighted_choice_multi(
    choices: List[ChoiceItem],
    k: int,
    allow_replacement: bool = False,
) -> List[Any]:
    """Select k random items based on weights.

    Args:
        choices: List of (item, weight) tuples.
        k: Number of items to select.
        allow_replacement: If True, items can be selected multiple times.

    Returns:
        List of selected items.
    """
    if k < 0:
        raise ValueError("k must be non-negative")

    if not choices:
        return []

    if allow_replacement:
        return [weighted_choice(choices) for _ in range(k)]

    items = [item for item, _ in choices]
    weights = [weight for _, weight in choices]

    if k >= len(items):
        paired = list(zip(items, weights))
        paired.sort(key=lambda x: x[1], reverse=True)
        return [item for item, _ in paired]

    result = []
    remaining_weights = weights.copy()
    remaining_items = items.copy()

    for _ in range(k):
        total = sum(remaining_weights)
        r = random.uniform(0, total)
        cumulative = 0.0
        for i, weight in enumerate(remaining_weights):
            cumulative += weight
            if r <= cumulative:
                result.append(remaining_items[i])
                del remaining_weights[i]
                del remaining_items[i]
                break

    return result


def build_weighted_choices(
    items: List[Any],
    weight_func: Optional[callable] = None,
) -> List[ChoiceItem]:
    """Build weighted choice list from items.

    Args:
        items: List of items.
        weight_func: Function to compute weight from item.

    Returns:
        List of (item, weight) tuples.
    """
    if weight_func is None:
        weight_func = lambda x: 1.0

    return [(item, weight_func(item)) for item in items]


class WeightedSampler:
    """Reusable weighted random sampler.

    Example:
        sampler = WeightedSampler([("a", 1), ("b", 2), ("c", 3)])
        sampler.sample()  # c more likely than b, b more than a
    """

    def __init__(self, choices: List[ChoiceItem]) -> None:
        self._choices = choices
        self._validate()

    def _validate(self) -> None:
        if not self._choices:
            raise ValueError("Choices cannot be empty")
        total = sum(weight for _, weight in self._choices)
        if total <= 0:
            raise ValueError("Total weight must be positive")

    def sample(self) -> Any:
        """Sample one item."""
        return weighted_choice(self._choices)

    def sample_n(self, n: int) -> List[Any]:
        """Sample n items without replacement."""
        return weighted_choice_multi(self._choices, n)

    def update_weight(self, item: Any, new_weight: float) -> None:
        """Update weight for an item.

        Args:
            item: Item to update.
            new_weight: New weight value.

        Raises:
            ValueError: If item not found.
        """
        for i, (existing_item, _) in enumerate(self._choices):
            if existing_item == item:
                self._choices[i] = (item, new_weight)
                self._validate()
                return
        raise ValueError(f"Item not found: {item}")

    def add_item(self, item: Any, weight: float) -> None:
        """Add a new item with weight.

        Args:
            item: Item to add.
            weight: Item weight.
        """
        self._choices.append((item, weight))
        self._validate()

    def remove_item(self, item: Any) -> bool:
        """Remove an item.

        Args:
            item: Item to remove.

        Returns:
            True if removed.
        """
        for i, (existing_item, _) in enumerate(self._choices):
            if existing_item == item:
                del self._choices[i]
                return True
        return False
