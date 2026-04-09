"""API Cost Estimation Action.

Provides cost tracking and estimation for API calls across different
providers (OpenAI, Anthropic, AWS, Google Cloud, etc.).
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import math


class Provider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AWS = "aws"
    GOOGLE = "google"
    AZURE = "azure"
    VOLC = "volc"
    CUSTOM = "custom"


@dataclass
class TokenCost:
    input_cost_per_1k: float = 0.0
    output_cost_per_1k: float = 0.0
    cache_read_cost_per_1k: float = 0.0


# Provider pricing (example rates, update per usage)
PROVIDER_PRICING: Dict[Provider, TokenCost] = {
    Provider.OPENAI: TokenCost(input_cost_per_1k=0.03, output_cost_per_1k=0.06),
    Provider.ANTHROPIC: TokenCost(input_cost_per_1k=0.015, output_cost_per_1k=0.075),
    Provider.VOLC: TokenCost(input_cost_per_1k=0.02, output_cost_per_1k=0.04),
    Provider.CUSTOM: TokenCost(),
}


@dataclass
class APICallCost:
    provider: Provider
    endpoint: str
    input_tokens: int
    output_tokens: int
    cache_hits: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class APICostEstimationAction:
    """Estimates and tracks API call costs."""

    def __init__(self, default_provider: Provider = Provider.OPENAI) -> None:
        self.default_provider = default_provider
        self._calls: List[APICallCost] = []
        self._budget: Optional[float] = None
        self._spent: float = 0.0

    def estimate(
        self,
        provider: Provider,
        input_tokens: int,
        output_tokens: int,
        cache_hits: int = 0,
    ) -> float:
        pricing = PROVIDER_PRICING.get(provider, TokenCost())
        input_cost = (input_tokens / 1000.0) * pricing.input_cost_per_1k
        output_cost = (output_tokens / 1000.0) * pricing.output_cost_per_1k
        cache_discount = (
            (cache_hits / 1000.0) * pricing.cache_read_cost_per_1k
            if pricing.cache_read_cost_per_1k > 0
            else 0.0
        )
        return max(0.0, input_cost + output_cost - cache_discount)

    def record(
        self,
        endpoint: str,
        input_tokens: int,
        output_tokens: int,
        cache_hits: int = 0,
        provider: Optional[Provider] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> APICallCost:
        prov = provider or self.default_provider
        cost = self.estimate(prov, input_tokens, output_tokens, cache_hits)
        call = APICallCost(
            provider=prov,
            endpoint=endpoint,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_hits=cache_hits,
            metadata=metadata or {},
        )
        self._calls.append(call)
        self._spent += cost
        return call

    def total_cost(self) -> float:
        return sum(
            self.estimate(c.provider, c.input_tokens, c.output_tokens, c.cache_hits)
            for c in self._calls
        )

    def cost_by_endpoint(self) -> Dict[str, float]:
        result: Dict[str, float] = {}
        for call in self._calls:
            cost = self.estimate(
                call.provider, call.input_tokens, call.output_tokens, call.cache_hits
            )
            result[call.endpoint] = result.get(call.endpoint, 0.0) + cost
        return result

    def cost_by_provider(self) -> Dict[Provider, float]:
        result: Dict[Provider, float] = {}
        for call in self._calls:
            cost = self.estimate(
                call.provider, call.input_tokens, call.output_tokens, call.cache_hits
            )
            result[call.provider] = result.get(call.provider, 0.0) + cost
        return result

    def set_budget(self, budget: float) -> None:
        self._budget = budget

    def budget_status(self) -> Dict[str, Any]:
        total = self.total_cost()
        return {
            "spent": total,
            "budget": self._budget,
            "remaining": max(0.0, (self._budget or float("inf")) - total),
            "over_budget": self._budget is not None and total > self._budget,
        }

    def summary(self) -> Dict[str, Any]:
        return {
            "total_cost": self.total_cost(),
            "total_calls": len(self._calls),
            "total_input_tokens": sum(c.input_tokens for c in self._calls),
            "total_output_tokens": sum(c.output_tokens for c in self._calls),
            "budget_status": self.budget_status(),
            "cost_by_provider": {
                str(k): v for k, v in self.cost_by_provider().items()
            },
        }
