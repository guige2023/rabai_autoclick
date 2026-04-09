"""
API Cost Estimation and Budget Tracking Module.

Estimates API call costs based on request complexity, response size,
and provider pricing models. Tracks spend against budgets with alerts.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


class PricingModel(Enum):
    """API pricing model types."""
    PER_REQUEST = "per_request"
    PER_RESPONSE_SIZE = "per_response_size"
    PER_COMPUTE_TIME = "per_compute_time"
    TIERED = "tiered"
    SUBSCRIPTION = "subscription"


class CostAlertLevel(Enum):
    """Budget alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class CostEstimate:
    """Represents a cost estimate for an API call."""
    request_id: str
    estimated_cost: float
    currency: str = "USD"
    breakdown: dict[str, float] = field(default_factory=dict)
    confidence: float = 0.95
    timestamp: float = field(default_factory=time.time)


@dataclass
class Budget:
    """Represents a spending budget."""
    name: str
    limit: float
    spent: float = 0.0
    currency: str = "USD"
    period: str = "monthly"
    alert_thresholds: dict[CostAlertLevel, float] = field(default_factory=dict)


@dataclass
class PricingTier:
    """A single pricing tier."""
    min_units: int
    max_units: Optional[int]
    unit_cost: float
    flat_fee: float = 0.0


class APICostEstimator:
    """
    Estimates and tracks API costs across multiple providers.

    Supports various pricing models and provides budget alerts
    when spending approaches configured thresholds.

    Example:
        estimator = APICostEstimator()
        estimator.add_provider("openai", PricingModel.PER_REQUEST)
        estimator.add_tier("openai", PricingTier(0, 1000, 0.002))
        estimate = estimator.estimate("openai", operation="chat", tokens=500)
        estimator.track("openai", estimate)
    """

    def __init__(self) -> None:
        self._providers: dict[str, PricingModel] = {}
        self._tiers: dict[str, list[PricingTier]] = {}
        self._flat_rates: dict[str, float] = {}
        self._budgets: dict[str, Budget] = {}
        self._usage: dict[str, list[tuple[float, float]]] = {}
        self._callbacks: dict[CostAlertLevel, list[Callable]] = {
            level: [] for level in CostAlertLevel
        }

    def add_provider(
        self,
        provider: str,
        model: PricingModel,
        flat_rate: float = 0.0
    ) -> None:
        """
        Register an API provider with its pricing model.

        Args:
            provider: Provider name (e.g., "openai", "aws")
            model: Pricing model type
            flat_rate: Monthly subscription fee if applicable
        """
        self._providers[provider] = model
        self._flat_rates[provider] = flat_rate
        self._tiers[provider] = []
        self._usage[provider] = []

    def add_tier(self, provider: str, tier: PricingTier) -> None:
        """Add a pricing tier for a provider."""
        if provider not in self._providers:
            raise ValueError(f"Provider {provider} not registered")
        self._tiers[provider].append(tier)
        self._tiers[provider].sort(key=lambda t: t.min_units)

    def add_budget(
        self,
        name: str,
        limit: float,
        period: str = "monthly",
        alerts: Optional[dict[CostAlertLevel, float]] = None
    ) -> Budget:
        """Create a new budget for tracking spend."""
        budget = Budget(
            name=name,
            limit=limit,
            period=period,
            alert_thresholds=alerts or {}
        )
        self._budgets[name] = budget
        return budget

    def on_alert(
        self,
        level: CostAlertLevel,
        callback: Callable[[str, Budget], None]
    ) -> None:
        """Register a callback for budget alerts."""
        self._callbacks[level].append(callback)

    def estimate(
        self,
        provider: str,
        operation: str = "default",
        **params: float
    ) -> CostEstimate:
        """
        Estimate cost for an API call.

        Args:
            provider: Provider name
            operation: Operation type
            **params: Operation parameters (tokens, bytes, compute_time, etc.)

        Returns:
            CostEstimate with breakdown
        """
        if provider not in self._providers:
            raise ValueError(f"Unknown provider: {provider}")

        model = self._providers[provider]
        breakdown: dict[str, float] = {}
        total = self._flat_rates.get(provider, 0.0)

        if model == PricingModel.PER_REQUEST:
            base = params.get("base_cost", 0.001)
            breakdown["base"] = base
            total += base

        elif model == PricingModel.PER_RESPONSE_SIZE:
            bytes_out = params.get("bytes_out", 1000)
            rate = params.get("per_kb", 0.0001)
            cost = (bytes_out / 1024) * rate
            breakdown["data_transfer"] = cost
            total += cost

        elif model == PricingModel.PER_COMPUTE_TIME:
            compute_ms = params.get("compute_ms", 100)
            rate = params.get("per_second", 0.01)
            cost = (compute_ms / 1000) * rate
            breakdown["compute"] = cost
            total += cost

        elif model == PricingModel.TIERED:
            units = int(params.get("units", 1))
            for tier in self._tiers.get(provider, []):
                if tier.min_units <= units < (tier.max_units or float("inf")):
                    breakdown["tier"] = tier.flat_fee + (units * tier.unit_cost)
                    total += breakdown["tier"]
                    break

        request_id = f"{provider}:{operation}:{time.time()}"
        return CostEstimate(
            request_id=request_id,
            estimated_cost=total,
            breakdown=breakdown
        )

    def track(self, provider: str, estimate: CostEstimate) -> None:
        """Record an API call for cost tracking."""
        self._usage[provider].append((estimate.timestamp, estimate.estimated_cost))
        self._cleanup_old_usage(provider)

        for budget in self._budgets.values():
            budget.spent += estimate.estimated_cost
            self._check_budget_alerts(budget)

    def _cleanup_old_usage(self, provider: str, max_age_days: int = 90) -> None:
        """Remove usage records older than max_age_days."""
        cutoff = time.time() - (max_age_days * 86400)
        self._usage[provider] = [
            (ts, cost) for ts, cost in self._usage[provider] if ts >= cutoff
        ]

    def _check_budget_alerts(self, budget: Budget) -> None:
        """Check if budget thresholds are breached."""
        ratio = budget.spent / budget.limit if budget.limit > 0 else 0

        for level, threshold in budget.alert_thresholds.items():
            if ratio >= threshold:
                for callback in self._callbacks[level]:
                    try:
                        callback(budget.name, budget)
                    except Exception:
                        pass

    def get_spend(
        self,
        provider: str,
        days: int = 30
    ) -> float:
        """Get total spend for a provider over N days."""
        cutoff = time.time() - (days * 86400)
        return sum(
            cost for ts, cost in self._usage.get(provider, [])
            if ts >= cutoff
        )

    def get_budget_status(self, name: str) -> Optional[dict]:
        """Get current status of a budget."""
        budget = self._budgets.get(name)
        if not budget:
            return None
        return {
            "name": budget.name,
            "spent": budget.spent,
            "limit": budget.limit,
            "remaining": budget.limit - budget.spent,
            "percent_used": (budget.spent / budget.limit * 100) if budget.limit else 0,
            "period": budget.period
        }

    def get_providers(self) -> list[str]:
        """Get list of registered providers."""
        return list(self._providers.keys())

    def get_tiers(self, provider: str) -> list[PricingTier]:
        """Get pricing tiers for a provider."""
        return self._tiers.get(provider, [])
