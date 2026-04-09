"""
Input Sequence Fuzzer Utilities

Fuzz test input sequences by generating random variations to
discover edge cases and robustness issues in automation pipelines.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import random
import math
from dataclasses import dataclass, field
from typing import List, Callable, Optional


@dataclass
class FuzzMutation:
    """A mutation applied to an input sequence."""
    mutation_type: str  # 'position_jitter', 'timing_stretch', 'drop_event', 'duplicate_event'
    index: int
    details: dict = field(default_factory=dict)


@dataclass
class FuzzConfig:
    """Configuration for fuzzing."""
    position_jitter_px: float = 5.0
    timing_stretch_factor: float = 1.2
    drop_probability: float = 0.05
    duplicate_probability: float = 0.05
    random_seed: Optional[int] = None


def jitter_position(
    x: float, y: float,
    max_jitter_px: float,
) -> tuple[float, float]:
    """Add random jitter to a position."""
    dx = random.uniform(-max_jitter_px, max_jitter_px)
    dy = random.uniform(-max_jitter_px, max_jitter_px)
    return x + dx, y + dy


def stretch_timing(timestamps: List[float], factor: float) -> List[float]:
    """Stretch or compress the timing of a sequence."""
    if not timestamps:
        return []
    base = timestamps[0]
    return [base + (t - base) * factor for t in timestamps]


def fuzz_input_sequence(
    events: List,
    get_x: Callable[[any], float],
    get_y: Callable[[any], float],
    get_timestamp: Callable[[any], float],
    set_x: Callable[[any, float], None],
    set_y: Callable[[any, float], None],
    set_timestamp: Callable[[any, float], None],
    config: Optional[FuzzConfig] = None,
) -> tuple[List, List[FuzzMutation]]:
    """
    Apply fuzz mutations to an input sequence.

    Args:
        events: List of input events.
        get_x, get_y, get_timestamp: Accessors for event properties.
        set_x, set_y, set_timestamp: Setters for event properties.
        config: Fuzzing configuration.

    Returns:
        Tuple of (mutated_events, list_of_mutations_applied).
    """
    config = config or FuzzConfig()
    if config.random_seed is not None:
        random.seed(config.random_seed)

    mutated_events = list(events)
    mutations: List[FuzzMutation] = []

    # Position jitter
    if config.position_jitter_px > 0:
        for i, event in enumerate(mutated_events):
            x, y = get_x(event), get_y(event)
            nx, ny = jitter_position(x, y, config.position_jitter_px)
            set_x(event, nx)
            set_y(event, ny)
            mutations.append(FuzzMutation(
                mutation_type="position_jitter",
                index=i,
                details={"original_x": x, "original_y": y, "jittered_x": nx, "jittered_y": ny},
            ))

    # Timing stretch
    if config.timing_stretch_factor != 1.0:
        timestamps = [get_timestamp(e) for e in mutated_events]
        new_timestamps = stretch_timing(timestamps, config.timing_stretch_factor)
        for i, event in enumerate(mutated_events):
            set_timestamp(event, new_timestamps[i])
        mutations.append(FuzzMutation(
            mutation_type="timing_stretch",
            index=-1,
            details={"factor": config.timing_stretch_factor},
        ))

    # Drop events
    if config.drop_probability > 0:
        to_drop = []
        for i, event in enumerate(mutated_events):
            if random.random() < config.drop_probability:
                to_drop.append(i)
        for i in reversed(to_drop):
            mutated_events.pop(i)
            mutations.append(FuzzMutation(
                mutation_type="drop_event",
                index=i,
            ))

    return mutated_events, mutations


def compute_mutation_coverage(mutations: List[FuzzMutation]) -> dict:
    """Compute coverage statistics for applied mutations."""
    types = {}
    for m in mutations:
        types[m.mutation_type] = types.get(m.mutation_type, 0) + 1
    return {
        "total_mutations": len(mutations),
        "by_type": types,
    }
