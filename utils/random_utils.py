"""Random data generation utilities for testing and automation.

Provides functions for generating random coordinates, text,
sequences, and structured test data for automation testing.

Example:
    >>> from utils.random_utils import random_coords, random_text, random_delay
    >>> random_coords(screen_w=1920, screen_h=1080)
    (847, 392)
    >>> random_text(length=16)
    'aKj9xPm2QwZbRnYs'
"""

from __future__ import annotations

import random
import string
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


def random_coords(
    *,
    screen_w: int = 1920,
    screen_h: int = 1080,
    margin: int = 0,
) -> Tuple[int, int]:
    """Generate random screen coordinates.

    Args:
        screen_w: Screen width.
        screen_h: Screen height.
        margin: Margin from screen edges.

    Returns:
        Random (x, y) tuple.
    """
    x = random.randint(margin, screen_w - margin - 1)
    y = random.randint(margin, screen_h - margin - 1)
    return x, y


def random_text(
    length: int = 16,
    *,
    charset: Optional[str] = None,
    case_sensitive: bool = True,
) -> str:
    """Generate random text string.

    Args:
        length: Length of string to generate.
        charset: Character set to use.
        case_sensitive: Use mixed case vs lowercase only.

    Returns:
        Random string.
    """
    if charset is None:
        if case_sensitive:
            charset = string.ascii_letters + string.digits
        else:
            charset = string.ascii_lowercase + string.digits

    return "".join(random.choice(charset) for _ in range(length))


def random_delay(
    min_ms: float = 50,
    max_ms: float = 500,
    distribution: str = "uniform",
) -> float:
    """Generate random delay in seconds.

    Args:
        min_ms: Minimum delay in milliseconds.
        max_ms: Maximum delay in milliseconds.
        distribution: "uniform", "normal", or "exponential".

    Returns:
        Random delay in seconds.
    """
    min_s = min_ms / 1000.0
    max_s = max_ms / 1000.0

    if distribution == "uniform":
        return random.uniform(min_s, max_s)
    elif distribution == "normal":
        mean = (min_s + max_s) / 2
        std = (max_s - min_s) / 6
        return max(min_s, min(max_s, random.gauss(mean, std)))
    elif distribution == "exponential":
        lambd = 1.0 / ((min_s + max_s) / 2)
        return min(max_s, random.expovariate(lambd))
    else:
        return random.uniform(min_s, max_s)


def random_choice(
    items: List[T],
    *,
    weights: Optional[List[float]] = None,
) -> T:
    """Randomly select an item.

    Args:
        items: List of items to choose from.
        weights: Optional weights for each item.

    Returns:
        Randomly selected item.
    """
    if not items:
        raise ValueError("Cannot choose from empty list")
    return random.choices(items, weights=weights, k=1)[0]


def random_subset(
    items: List[T],
    min_size: int = 0,
    max_size: Optional[int] = None,
) -> List[T]:
    """Get a random subset of items.

    Args:
        items: Items to choose from.
        min_size: Minimum subset size.
        max_size: Maximum subset size.

    Returns:
        Random subset of items.
    """
    if not items:
        return []
    max_size = max_size or len(items)
    size = random.randint(min_size, max_size)
    return random.sample(items, min(size, len(items)))


def random_element_with_properties(
    properties: Dict[str, List[Any]],
) -> Dict[str, Any]:
    """Generate a random element with random property values.

    Args:
        properties: Dict mapping property name -> list of possible values.

    Returns:
        Dict with randomly selected property values.

    Example:
        >>> random_element_with_properties({
        ...     "color": ["red", "blue", "green"],
        ...     "size": [10, 20, 30],
        ... })
        {"color": "blue", "size": 20}
    """
    return {key: random.choice(values) for key, values in properties.items()}


def random_uuid() -> str:
    """Generate a random UUID string.

    Returns:
        UUID4 string.
    """
    return str(uuid.uuid4())


def random_coordinates_cluster(
    center: Tuple[float, float],
    radius: float,
    count: int = 5,
) -> List[Tuple[float, float]]:
    """Generate random coordinates clustered around a center.

    Args:
        center: (x, y) cluster center.
        radius: Maximum distance from center.
        count: Number of points to generate.

    Returns:
        List of (x, y) coordinate tuples.
    """
    cx, cy = center
    points: List[Tuple[float, float]] = []
    for _ in range(count):
        angle = random.uniform(0, 2 * 3.14159)
        r = radius * random.uniform(0, 1) ** 0.5
        x = cx + r * random.choice([-1, 1]) * abs(random.gauss(0, 0.5))
        y = cy + r * random.choice([-1, 1]) * abs(random.gauss(0, 0.5))
        points.append((x, y))
    return points


def random_mouse_path(
    start: Tuple[int, int],
    end: Tuple[int, int],
    *,
    segments: int = 10,
    jitter: float = 5.0,
) -> List[Tuple[int, int]]:
    """Generate a realistic mouse movement path.

    Args:
        start: Starting (x, y).
        end: Ending (x, y).
        segments: Number of path segments.
        jitter: Random position jitter amount.

    Returns:
        List of (x, y) points along the path.
    """
    sx, sy = start
    ex, ey = end
    path: List[Tuple[int, int]] = []

    for i in range(segments + 1):
        t = i / segments
        x = sx + (ex - sx) * t + random.uniform(-jitter, jitter)
        y = sy + (ey - sy) * t + random.uniform(-jitter, jitter)
        path.append((int(x), int(y)))

    path[0] = start
    path[-1] = end
    return path


def seeded_random(seed: int) -> Callable:
    """Create a seeded random generator.

    Args:
        seed: Random seed.

    Returns:
        Seeded random.Random instance.
    """
    rng = random.Random(seed)
    return rng
