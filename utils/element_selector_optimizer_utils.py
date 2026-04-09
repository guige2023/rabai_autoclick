"""
Element selector optimizer utilities.

This module provides utilities for optimizing UI element selectors
based on specificity, stability, and matching performance.
"""

from __future__ import annotations

import re
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from dataclasses import dataclass as dc


# Type aliases
Selector = str
SelectorScore = float


@dataclass
class SelectorComponent:
    """A component of a selector (tag, class, id, attribute)."""
    component_type: str  # tag, class, id, attribute, nth
    value: str
    specificity_weight: float = 1.0


@dataclass
class OptimizedSelector:
    """Result of selector optimization."""
    selector: Selector
    specificity_score: SelectorScore
    stability_score: SelectorScore
    overall_score: SelectorScore
    is_unique: bool
    estimated_match_time_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SelectorOptimizationConfig:
    """Configuration for selector optimization."""
    max_selector_length: int = 200
    prefer_short_selectors: bool = True
    prefer_stable_attributes: bool = True
    prefer_id_over_xpath: bool = True
    stable_attributes: Tuple[str, ...] = ("id", "data-testid", "accessibility-id", "resource-id")


def parse_selector(selector: Selector) -> List[SelectorComponent]:
    """
    Parse a selector into its components.

    Args:
        selector: CSS or XPath selector string.

    Returns:
        List of SelectorComponent objects.
    """
    components: List[SelectorComponent] = []

    if selector.startswith("//") or selector.startswith("/"):
        # XPath
        parts = re.split(r"/+", selector.strip("/"))
        for part in parts:
            if not part:
                continue
            if part.startswith("@"):
                components.append(SelectorComponent("attribute", part[1:], 0.1))
            elif part.startswith("[") and part.endswith("]"):
                attr_match = re.match(r"\[@?([^\]=]+)=?'?([^'\]]+)'?\]", part)
                if attr_match:
                    components.append(SelectorComponent("attribute", attr_match.group(1), 0.1))
            elif part == "*":
                components.append(SelectorComponent("tag", "*", 0.0))
            else:
                components.append(SelectorComponent("tag", part, 0.1))
    else:
        # CSS selector
        # Match class, id, tag, attribute selectors
        class_matches = re.findall(r'\.([^\s\.#:\[]+)', selector)
        for cls in class_matches:
            components.append(SelectorComponent("class", cls, 0.1))

        id_matches = re.findall(r'#([^\s\.#:\[]+)', selector)
        for id_val in id_matches:
            components.append(SelectorComponent("id", id_val, 1.0))

        tag_matches = re.findall(r'^([a-zA-Z][a-zA-Z0-9]*)|(?<=[^\w-])([a-zA-Z][a-zA-Z0-9]*)', selector)
        for tag in tag_matches:
            if tag:
                components.append(SelectorComponent("tag", tag, 0.0))

        attr_matches = re.findall(r'\[([^\]=]+)=?[^\]]*\]', selector)
        for attr in attr_matches:
            components.append(SelectorComponent("attribute", attr, 0.1))

    return components


def compute_specificity_score(selector: Selector) -> SelectorScore:
    """
    Compute selector specificity (higher = more specific = better).

    Args:
        selector: CSS or XPath selector.

    Returns:
        Specificity score between 0 and 1.
    """
    components = parse_selector(selector)
    if not components:
        return 0.0

    total_weight = sum(c.specificity_weight for c in components)
    # Normalize: id=1.0, class=0.1, attribute=0.1, tag=0.0
    # A selector with id is most specific
    has_id = any(c.component_type == "id" for c in components)
    has_class = any(c.component_type == "class" for c in components)
    has_tag = any(c.component_type == "tag" for c in components)
    has_attr = any(c.component_type == "attribute" for c in components)

    score = 0.0
    if has_id:
        score += 0.6
    if has_class:
        score += 0.2
    if has_attr:
        score += 0.1
    if has_tag:
        score += 0.1

    return min(1.0, score)


def compute_stability_score(
    selector: Selector,
    stable_attributes: Tuple[str, ...] = ("id", "data-testid", "accessibility-id"),
) -> SelectorScore:
    """
    Compute selector stability based on attribute stability.

    Args:
        selector: CSS or XPath selector.
        stable_attributes: Attributes considered stable.

    Returns:
        Stability score between 0 and 1.
    """
    components = parse_selector(selector)
    if not components:
        return 0.0

    stable_count = sum(1 for c in components if c.component_type == "id" or c.value in stable_attributes)
    return stable_count / len(components) if components else 0.0


def estimate_match_time(selector: Selector) -> float:
    """
    Estimate selector matching time in milliseconds.

    Args:
        selector: CSS or XPath selector.

    Returns:
        Estimated match time in ms.
    """
    components = parse_selector(selector)
    if not components:
        return 100.0

    # ID is fastest, then class, then attribute, then tag, then XPath
    has_id = any(c.component_type == "id" for c in components)
    has_xpath = selector.startswith("//")
    has_class = any(c.component_type == "class" for c in components)
    has_attr = any(c.component_type == "attribute" for c in components)
    has_tag = any(c.component_type == "tag" for c in components)

    if has_id and not has_xpath:
        return 1.0
    if has_xpath:
        return 50.0
    if has_class:
        return 10.0
    if has_tag and has_attr:
        return 20.0
    if has_tag:
        return 30.0
    return 50.0


def optimize_selector(
    selector: Selector,
    config: Optional[SelectorOptimizationConfig] = None,
) -> OptimizedSelector:
    """
    Optimize a selector for specificity, stability, and speed.

    Args:
        selector: Original selector.
        config: Optimization configuration.

    Returns:
        OptimizedSelector with scores.
    """
    if config is None:
        config = SelectorOptimizationConfig()

    specificity = compute_specificity_score(selector)
    stability = compute_stability_score(selector, config.stable_attributes)
    match_time = estimate_match_time(selector)

    # Normalize match time to score (faster = higher score)
    match_score = max(0.0, 1.0 - match_time / 100.0)

    # Length penalty
    len_penalty = max(0.0, 1.0 - len(selector) / config.max_selector_length) if config.prefer_short_selectors else 1.0

    overall = (specificity * 0.4 + stability * 0.3 + match_score * 0.3) * len_penalty

    # Check uniqueness heuristic
    is_unique = "id=" in selector or "#" in selector

    return OptimizedSelector(
        selector=selector,
        specificity_score=specificity,
        stability_score=stability,
        overall_score=overall,
        is_unique=is_unique,
        estimated_match_time_ms=match_time,
        metadata={
            "component_count": len(parse_selector(selector)),
            "length": len(selector),
        },
    )


def rank_selectors(
    selectors: List[Selector],
    config: Optional[SelectorOptimizationConfig] = None,
) -> List[OptimizedSelector]:
    """
    Rank and optimize multiple selectors.

    Args:
        selectors: List of candidate selectors.
        config: Optimization configuration.

    Returns:
        List of OptimizedSelectors sorted by overall_score descending.
    """
    optimized = [optimize_selector(s, config) for s in selectors]
    return sorted(optimized, key=lambda x: x.overall_score, reverse=True)
