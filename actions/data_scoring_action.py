"""Data Scoring Action.

Scores and ranks data entities based on configurable scoring functions.
"""
from typing import Any, Callable, Dict, List, Optional, Generic, TypeVar
from dataclasses import dataclass, field


T = TypeVar("T")


@dataclass
class Score:
    entity_id: str
    total_score: float
    factor_scores: Dict[str, float]
    rank: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScoringFactor:
    name: str
    weight: float
    evaluate: Callable[[Any], float]
    higher_is_better: bool = True


class DataScoringAction(Generic[T]):
    """Scores and ranks entities based on weighted factors."""

    def __init__(self, min_score: float = 0.0, max_score: float = 100.0) -> None:
        self.factors: List[ScoringFactor] = []
        self.min_score = min_score
        self.max_score = max_score

    def add_factor(
        self,
        name: str,
        weight: float,
        evaluate_fn: Callable[[Any], float],
        higher_is_better: bool = True,
    ) -> "DataScoringAction":
        self.factors.append(ScoringFactor(
            name=name,
            weight=weight,
            evaluate=evaluate_fn,
            higher_is_better=higher_is_better,
        ))
        return self

    def score(self, entity_id: str, entity: T) -> Score:
        factor_scores = {}
        total = 0.0
        total_weight = sum(f.weight for f in self.factors)
        for factor in self.factors:
            raw = factor.evaluate(entity)
            factor_scores[factor.name] = raw
            normalized = raw / total_weight * factor.weight
            total += normalized
        return Score(
            entity_id=entity_id,
            total_score=total,
            factor_scores=factor_scores,
        )

    def score_batch(
        self,
        entities: List[tuple],
    ) -> List[Score]:
        scores = [self.score(eid, ent) for eid, ent in entities]
        scores.sort(key=lambda s: s.total_score, reverse=True)
        for rank, s in enumerate(scores, 1):
            s.rank = rank
        return scores

    def get_top(
        self,
        entities: List[tuple],
        top_k: int = 10,
    ) -> List[Score]:
        return self.score_batch(entities)[:top_k]

    def get_weight_summary(self) -> Dict[str, float]:
        return {f.name: f.weight for f in self.factors}
