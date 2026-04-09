"""Data HyperLogLog Action Module.

Provides HyperLogLog algorithm implementation for
approximate cardinality estimation of large sets.

Author: RabAi Team
"""

from __future__ import annotations

import math
import struct
import sys
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class HyperLogLogConfig:
    """Configuration for HyperLogLog."""
    precision: int = 16
    sparse: bool = True


class HyperLogLog:
    """HyperLogLog probabilistic cardinality estimator.

    Uses minimal memory to estimate cardinality of large sets.
    Standard error is approximately 1.04 / sqrt(register_count).
    """

    REGISTERS_16BIT = 65536
    REGISTERS_14BIT = 16384
    REGISTERS_12BIT = 4096
    REGISTERS_10BIT = 1024

    def __init__(self, precision: int = 16, sparse: bool = True):
        self.precision = precision
        self.sparse = sparse

        self.register_count = 1 << precision
        self.registers: List[int] = [0] * self.register_count

        self._bias_correction: Dict[int, float] = {}

    def _hash(self, value: bytes) -> int:
        """Generate hash for value."""
        import hashlib
        h = hashlib.sha256(value).digest()
        return struct.unpack(">Q", h[:8])[0]

    def _get_register_index(self, hash_value: int) -> int:
        """Get register index from hash."""
        return hash_value & (self.register_count - 1)

    def _get_leading_zeros(self, hash_value: int, max_bits: int = 64) -> int:
        """Count leading zeros in hash."""
        zeros = 0
        for i in range(max_bits - 1, -1, -1):
            if (hash_value >> i) & 1:
                break
            zeros += 1
        return zeros

    def add(self, value: bytes) -> bool:
        """Add value to HyperLogLog."""
        if isinstance(value, str):
            value = value.encode()
        elif not isinstance(value, bytes):
            value = str(value).encode()

        hash_value = self._hash(value)
        index = self._get_register_index(hash_value)

        remaining_bits = 64 - self.precision
        trailing_hash = (hash_value >> self.precision) | (1 << (remaining_bits - 1))
        zeros = self._get_leading_zeros(trailing_hash, remaining_bits)

        new_count = zeros + 1

        if new_count > self.registers[index]:
            self.registers[index] = new_count
            return True

        return False

    def merge(self, other: "HyperLogLog") -> None:
        """Merge another HyperLogLog into this one."""
        if self.register_count != other.register_count:
            raise ValueError("Cannot merge HyperLogLog with different precision")

        for i in range(self.register_count):
            if other.registers[i] > self.registers[i]:
                self.registers[i] = other.registers[i]

    def cardinality(self) -> int:
        """Estimate cardinality."""
        if self.register_count == 0:
            return 0

        Z_inv = 0.0

        for count in self.registers:
            if count > 0:
                Z_inv += 1.0 / (1 << count)

        Z = 1.0 / Z_inv if Z_inv > 0 else 0

        m = self.register_count
        alpha = self._get_alpha(m)

        estimate = alpha * m * m * Z

        if estimate < 2.5 * m:
            zero_count = self.registers.count(0)
            if zero_count > 0:
                estimate = m * math.log(m / zero_count)

        estimate = self._bias_correction.get(self.register_count, estimate)

        return max(0, int(round(estimate)))

    def _get_alpha(self, m: int) -> float:
        """Get alpha constant for given register count."""
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return 0.7213 / (1.0 + 1.079 / m)

    def clear(self) -> None:
        """Clear all registers."""
        self.registers = [0] * self.register_count

    def get_register_summary(self) -> Dict[str, Any]:
        """Get summary of register values."""
        non_zero = sum(1 for r in self.registers if r > 0)
        max_val = max(self.registers) if self.registers else 0
        avg_val = sum(self.registers) / len(self.registers) if self.registers else 0

        return {
            "register_count": self.register_count,
            "non_zero_registers": non_zero,
            "max_value": max_val,
            "avg_value": avg_val,
            "estimated_cardinality": self.cardinality()
        }


class DataHyperLogLogAction(BaseAction):
    """Action for HyperLogLog operations."""

    def __init__(self):
        super().__init__("data_hyperloglog")
        self._hll: HyperLogLog = HyperLogLog(precision=16)

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute HyperLogLog action."""
        try:
            operation = params.get("operation", "add")

            if operation == "add":
                return self._add(params)
            elif operation == "cardinality":
                return self._cardinality(params)
            elif operation == "merge":
                return self._merge(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "configure":
                return self._configure(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add(self, params: Dict[str, Any]) -> ActionResult:
        """Add value(s) to HyperLogLog."""
        values = params.get("values", [])

        if isinstance(values, str):
            values = [values]
        elif not isinstance(values, list):
            values = [values]

        added_count = 0
        for value in values:
            if self._hll.add(str(value).encode()):
                added_count += 1

        cardinality = self._hll.cardinality()

        return ActionResult(
            success=True,
            data={
                "added": added_count,
                "total_values": len(values),
                "estimated_cardinality": cardinality
            }
        )

    def _cardinality(self, params: Dict[str, Any]) -> ActionResult:
        """Get estimated cardinality."""
        cardinality = self._hll.cardinality()

        return ActionResult(
            success=True,
            data={"cardinality": cardinality}
        )

    def _merge(self, params: Dict[str, Any]) -> ActionResult:
        """Merge another HyperLogLog (serialized registers)."""
        other_registers = params.get("registers", [])

        if not other_registers:
            return ActionResult(success=False, message="No registers provided")

        other = HyperLogLog(precision=self._hll.precision)
        other.registers = other_registers[:other.register_count]

        self._hll.merge(other)

        return ActionResult(
            success=True,
            data={"estimated_cardinality": self._hll.cardinality()}
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get HyperLogLog statistics."""
        summary = self._hll.get_register_summary()

        return ActionResult(success=True, data=summary)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear HyperLogLog."""
        self._hll.clear()
        return ActionResult(success=True, message="HyperLogLog cleared")

    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure HyperLogLog."""
        precision = params.get("precision", 16)
        sparse = params.get("sparse", True)

        self._hll = HyperLogLog(precision=precision, sparse=sparse)

        return ActionResult(
            success=True,
            message=f"HyperLogLog configured: precision={precision}"
        )
