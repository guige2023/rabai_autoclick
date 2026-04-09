"""API Chaos Injection Action.

Injects faults (latency, errors, exceptions, network issues) into
API calls for chaos engineering and resilience testing.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import random
import time


T = TypeVar("T")


class FaultType(Enum):
    LATENCY = "latency"
    ERROR_RESPONSE = "error_response"
    TIMEOUT = "timeout"
    DISCONNECT = "disconnect"
    CORRUPT_RESPONSE = "corrupt_response"
    BANDWIDTH_LIMIT = "bandwidth_limit"


@dataclass
class FaultConfig:
    fault_type: FaultType
    probability: float = 0.1
    latency_ms: float = 500.0
    error_code: int = 500
    error_message: str = "Internal Server Error"
    bandwidth_kbps: Optional[float] = None
    enabled: bool = True


@dataclass
class FaultInjectionResult:
    fault_injected: bool
    fault_type: Optional[FaultType]
    original_result: Any
    latency_added_ms: float
    timestamp: datetime


class APIChaosInjectionAction:
    """Chaos engineering toolkit for API fault injection."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)
        self._configs: Dict[str, FaultConfig] = {}
        self._stats: Dict[str, int] = {
            "total_calls": 0,
            "faults_injected": 0,
            "latency_injections": 0,
            "error_injections": 0,
            "timeout_injections": 0,
        }

    def configure(self, endpoint: str, config: FaultConfig) -> None:
        self._configs[endpoint] = config

    def _get_config(self, endpoint: str) -> Optional[FaultConfig]:
        return self._configs.get(endpoint)

    def _inject_latency(self, config: FaultConfig) -> float:
        return config.latency_ms * self.rng.uniform(0.5, 1.5)

    def _should_inject(self, config: FaultConfig) -> bool:
        return config.enabled and self.rng.random() < config.probability

    def execute(
        self,
        endpoint: str,
        fn: Callable[[], T],
        config: Optional[FaultConfig] = None,
    ) -> T:
        fault_cfg = config or self._get_config(endpoint)
        self._stats["total_calls"] += 1
        latency_added = 0.0
        fault_injected = False
        fault_type: Optional[FaultType] = None
        if fault_cfg and self._should_inject(fault_cfg):
            fault_injected = True
            fault_type = fault_cfg.fault_type
            self._stats["faults_injected"] += 1
            if fault_type == FaultType.LATENCY:
                latency_added = self._inject_latency(fault_cfg)
                self._stats["latency_injections"] += 1
                time.sleep(latency_added / 1000.0)
            elif fault_type == FaultType.ERROR_RESPONSE:
                self._stats["error_injections"] += 1
                raise RuntimeError(fault_cfg.error_message)
            elif fault_type == FaultType.TIMEOUT:
                self._stats["timeout_injections"] += 1
                raise TimeoutError(fault_cfg.error_message)
            elif fault_type == FaultType.DISCONNECT:
                raise ConnectionError("Connection reset by peer")
            elif fault_type == FaultType.CORRUPT_RESPONSE:
                raise ValueError("Corrupted response body")
            elif fault_type == FaultType.BANDWIDTH_LIMIT:
                latency_added = self._inject_latency(fault_cfg)
                self._stats["latency_injections"] += 1
                time.sleep(latency_added / 1000.0)
        result = fn()
        return result

    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["total_calls"]
        return {
            "total_calls": total,
            "faults_injected": self._stats["faults_injected"],
            "injection_rate": self._stats["faults_injected"] / total if total > 0 else 0.0,
            "latency_injections": self._stats["latency_injections"],
            "error_injections": self._stats["error_injections"],
            "timeout_injections": self._stats["timeout_injections"],
        }
