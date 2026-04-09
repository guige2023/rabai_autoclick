"""
API Fault Injection Action Module

Injects faults (delays, errors, exceptions) for chaos testing.
Configurable fault types, targeting rules, and failure percentages.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Types of faults that can be injected."""
    
    DELAY = "delay"
    ERROR = "error"
    EXCEPTION = "exception"
    ABORT = "abort"
    TIMEOUT = "timeout"
    BANDWIDTH_LIMIT = "bandwidth_limit"
    DISCONNECT = "disconnect"


class FaultTarget(Enum):
    """Target of fault injection."""
    
    REQUEST = "request"
    RESPONSE = "response"
    ENDPOINT = "endpoint"
    CLIENT = "client"


@dataclass
class FaultConfig:
    """Configuration for a single fault."""
    
    fault_type: FaultType
    target: FaultTarget = FaultTarget.REQUEST
    probability: float = 0.1
    delay_ms: int = 1000
    error_code: int = 500
    error_message: str = "Injected fault"
    duration_ms: Optional[int] = None
    rate_limit: Optional[int] = None
    endpoint_pattern: Optional[str] = None
    header_selector: Optional[str] = None
    enabled: bool = True


@dataclass
class FaultResult:
    """Result of fault injection attempt."""
    
    injected: bool
    fault_type: Optional[FaultType] = None
    delay_ms: int = 0
    error_response: Optional[Dict] = None
    target: Optional[FaultTarget] = None


class FaultInjector:
    """Handles actual fault injection logic."""
    
    def __init__(self, configs: List[FaultConfig]):
        self.configs = configs
        self._request_counts: Dict[str, int] = {}
        self._active_faults: Dict[str, float] = {}
    
    def _should_inject(self, config: FaultConfig, target: str) -> bool:
        """Determine if fault should be injected."""
        if not config.enabled:
            return False
        
        if config.endpoint_pattern:
            if config.endpoint_pattern not in target:
                return False
        
        if config.rate_limit:
            count = self._request_counts.get(target, 0) + 1
            self._request_counts[target] = count
            if count % config.rate_limit != 0:
                return False
        
        return random.random() < config.probability
    
    async def inject(self, target: str) -> FaultResult:
        """Inject a fault based on configuration."""
        for config in self.configs:
            if self._should_inject(config, target):
                return await self._execute_fault(config, target)
        
        return FaultResult(injected=False)
    
    async def _execute_fault(self, config: FaultConfig, target: str) -> FaultResult:
        """Execute a specific fault."""
        fault_key = f"{config.fault_type.value}:{target}"
        self._active_faults[fault_key] = time.time()
        
        try:
            if config.fault_type == FaultType.DELAY:
                await asyncio.sleep(config.delay_ms / 1000)
                return FaultResult(
                    injected=True,
                    fault_type=config.fault_type,
                    delay_ms=config.delay_ms,
                    target=config.target
                )
            
            elif config.fault_type == FaultType.ERROR:
                return FaultResult(
                    injected=True,
                    fault_type=config.fault_type,
                    error_response={
                        "status_code": config.error_code,
                        "body": {"error": config.error_message}
                    },
                    target=config.target
                )
            
            elif config.fault_type == FaultType.EXCEPTION:
                raise Exception(config.error_message)
            
            elif config.fault_type == FaultType.ABORT:
                return FaultResult(
                    injected=True,
                    fault_type=config.fault_type,
                    error_response={
                        "status_code": 0,
                        "aborted": True
                    },
                    target=config.target
                )
            
            elif config.fault_type == FaultType.TIMEOUT:
                await asyncio.sleep(config.duration_ms or 30000)
                return FaultResult(injected=False)
            
            return FaultResult(injected=False)
        
        finally:
            self._active_faults.pop(fault_key, None)
    
    def is_fault_active(self, fault_type: FaultType, target: str) -> bool:
        """Check if a fault is currently active."""
        return f"{fault_type.value}:{target}" in self._active_faults


class APIFaultInjectionAction:
    """
    Main fault injection action handler.
    
    Provides chaos engineering capabilities by injecting
    various faults into API requests and responses.
    """
    
    def __init__(self, configs: Optional[List[FaultConfig]] = None):
        self.configs = configs or []
        self.injector = FaultInjector(self.configs)
        self._middleware: List[Callable] = []
        self._stats = {
            "total_requests": 0,
            "faults_injected": 0,
            "faults_by_type": {}
        }
    
    def add_fault(self, config: FaultConfig) -> None:
        """Add a fault configuration."""
        self.configs.append(config)
        self.injector.configs = self.configs
    
    def remove_fault(self, fault_type: FaultType, target: FaultTarget) -> None:
        """Remove a fault configuration."""
        self.configs = [
            c for c in self.configs
            if not (c.fault_type == fault_type and c.target == target)
        ]
        self.injector.configs = self.configs
    
    def enable_fault(self, index: int) -> None:
        """Enable a fault by index."""
        if 0 <= index < len(self.configs):
            self.configs[index].enabled = True
    
    def disable_fault(self, index: int) -> None:
        """Disable a fault by index."""
        if 0 <= index < len(self.configs):
            self.configs[index].enabled = False
    
    async def process_request(
        self,
        request: Dict,
        target: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process request with potential fault injection."""
        self._stats["total_requests"] += 1
        
        endpoint = target or request.get("path", "/")
        
        result = await self.injector.inject(endpoint)
        
        if result.injected:
            self._stats["faults_injected"] += 1
            fault_type = result.fault_type.value if result.fault_type else "unknown"
            self._stats["faults_by_type"][fault_type] = (
                self._stats["faults_by_type"].get(fault_type, 0) + 1
            )
            
            if result.fault_type == FaultType.ERROR and result.error_response:
                return result.error_response
            
            if result.fault_type == FaultType.ABORT:
                return result.error_response or {"aborted": True}
        
        return request
    
    async def process_response(
        self,
        response: Dict,
        request: Dict
    ) -> Dict[str, Any]:
        """Process response with potential fault injection."""
        endpoint = request.get("path", "/")
        
        for config in self.configs:
            if config.target == FaultTarget.RESPONSE:
                if self.injector._should_inject(config, endpoint):
                    if config.fault_type == FaultType.DELAY:
                        await asyncio.sleep(config.delay_ms / 1000)
        
        return response
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fault injection statistics."""
        injection_rate = (
            self._stats["faults_injected"] / max(self._stats["total_requests"], 1)
        )
        return {
            **self._stats,
            "injection_rate": f"{injection_rate * 100:.2f}%",
            "active_faults": len(self.injector._active_faults)
        }
    
    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = {
            "total_requests": 0,
            "faults_injected": 0,
            "faults_by_type": {}
        }
    
    def get_active_config(self) -> List[Dict[str, Any]]:
        """Get list of active fault configurations."""
        return [
            {
                "fault_type": c.fault_type.value,
                "target": c.target.value,
                "probability": c.probability,
                "enabled": c.enabled
            }
            for c in self.configs
        ]
