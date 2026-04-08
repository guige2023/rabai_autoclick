"""
Data Reconciliation Action Module.

Reconciles data between two systems: detects mismatches,
calculates discrepancies, and generates reconciliation reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class Discrepancy:
    """A data discrepancy found."""
    key: str
    source_value: Any
    target_value: Any
    discrepancy_type: str  # missing, extra, mismatch
    amount_diff: Optional[float] = None


@dataclass
class ReconciliationReport:
    """Data reconciliation report."""
    total_source: int
    total_target: int
    matched: int
    discrepancies: list[Discrepancy]
    source_only: int
    target_only: int
    total_discrepancy_amount: float


class DataReconciliationAction(BaseAction):
    """Reconcile data between two systems."""

    def __init__(self) -> None:
        super().__init__("data_reconciliation")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Reconcile source and target data.

        Args:
            context: Execution context
            params: Parameters:
                - source: Source records
                - target: Target records
                - key_field: Primary key for matching
                - amount_field: Optional amount field for sum comparison
                - tolerance: Tolerance for amount differences

        Returns:
            ReconciliationReport with discrepancy details
        """
        source = params.get("source", [])
        target = params.get("target", [])
        key_field = params.get("key_field", "id")
        amount_field = params.get("amount_field")
        tolerance = params.get("tolerance", 0.01)

        source_by_key = {str(r.get(key_field, "")): r for r in source}
        target_by_key = {str(r.get(key_field, "")): r for r in target}

        source_keys = set(source_by_key.keys())
        target_keys = set(target_by_key.keys())

        matched_keys = source_keys & target_keys
        source_only_keys = source_keys - target_keys
        target_only_keys = target_keys - source_keys

        discrepancies: list[Discrepancy] = []
        total_discrepancy_amount = 0.0

        for key in matched_keys:
            s_rec = source_by_key[key]
            t_rec = target_by_key[key]

            if amount_field:
                s_amount = s_rec.get(amount_field, 0) or 0
                t_amount = t_rec.get(amount_field, 0) or 0
                diff = abs(s_amount - t_amount)
                if diff > tolerance:
                    discrepancies.append(Discrepancy(
                        key=key,
                        source_value=s_amount,
                        target_value=t_amount,
                        discrepancy_type="mismatch",
                        amount_diff=s_amount - t_amount
                    ))
                    total_discrepancy_amount += diff
            else:
                if s_rec != t_rec:
                    discrepancies.append(Discrepancy(
                        key=key,
                        source_value=s_rec,
                        target_value=t_rec,
                        discrepancy_type="mismatch"
                    ))

        return ReconciliationReport(
            total_source=len(source),
            total_target=len(target),
            matched=len(matched_keys),
            discrepancies=discrepancies,
            source_only=len(source_only_keys),
            target_only=len(target_only_keys),
            total_discrepancy_amount=total_discrepancy_amount
        ).__dict__
