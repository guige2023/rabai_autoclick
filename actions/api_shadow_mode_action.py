"""
API Shadow Mode Action Module

Shadow testing - duplicate requests to canary/staging
without affecting production traffic. Response comparison and analysis.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ComparisonStrategy(Enum):
    """Strategy for comparing shadow responses."""
    
    EXACT = "exact"
    SEMANTIC = "semantic"
    SCHEMA = "schema"
    NONE = "none"


@dataclass
class ShadowTarget:
    """Shadow traffic destination."""
    
    name: str
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    timeout_ms: int = 5000
    enabled: bool = True


@dataclass
class ShadowResult:
    """Result of shadow request execution."""
    
    target_name: str
    success: bool
    duration_ms: float
    response: Optional[Dict] = None
    error: Optional[str] = None
    compared: bool = False
    comparison_result: Optional[Dict] = None


@dataclass
class ShadowConfig:
    """Configuration for shadow mode."""
    
    enabled: bool = True
    targets: List[ShadowTarget] = field(default_factory=list)
    comparison_strategy: ComparisonStrategy = ComparisonStrategy.EXACT
    sample_rate: float = 1.0
    record_results: bool = True
    alert_on_difference: bool = False
    difference_threshold: float = 0.0


class ShadowTrafficRouter:
    """Routes traffic to shadow targets."""
    
    def __init__(self, config: ShadowConfig):
        self.config = config
        self._client: Optional[Callable] = None
    
    def set_client(self, client: Callable) -> None:
        """Set the HTTP client to use."""
        self._client = client
    
    async def send_shadow_request(
        self,
        target: ShadowTarget,
        request: Dict
    ) -> ShadowResult:
        """Send a request to a shadow target."""
        if not target.enabled:
            return ShadowResult(
                target_name=target.name,
                success=False,
                duration_ms=0,
                error="Target disabled"
            )
        
        start_time = time.time()
        
        try:
            merged_request = {
                **request,
                "headers": {**request.get("headers", {}), **target.headers},
                "url": target.url
            }
            
            response = await asyncio.wait_for(
                self._execute_request(merged_request),
                timeout=target.timeout_ms / 1000
            )
            
            duration_ms = (time.time() - start_time) * 1000
            
            return ShadowResult(
                target_name=target.name,
                success=True,
                duration_ms=duration_ms,
                response=response
            )
        
        except asyncio.TimeoutError:
            return ShadowResult(
                target_name=target.name,
                success=False,
                duration_ms=(time.time() - start_time) * 1000,
                error="Timeout"
            )
        except Exception as e:
            return ShadowResult(
                target_name=target.name,
                success=False,
                duration_ms=(time.time() - start_time) * 1000,
                error=str(e)
            )
    
    async def _execute_request(self, request: Dict) -> Dict:
        """Execute the actual request."""
        if self._client:
            return await self._client(request)
        
        return {"status_code": 200, "body": {}, "headers": {}}


class ResponseComparator:
    """Compares shadow responses with production."""
    
    def __init__(self, strategy: ComparisonStrategy):
        self.strategy = strategy
    
    def compare(
        self,
        production: Dict,
        shadow: Dict
    ) -> Dict[str, Any]:
        """Compare production and shadow responses."""
        if self.strategy == ComparisonStrategy.NONE:
            return {"identical": None, "differences": []}
        
        if self.strategy == ComparisonStrategy.EXACT:
            return self._exact_compare(production, shadow)
        
        elif self.strategy == ComparisonStrategy.SCHEMA:
            return self._schema_compare(production, shadow)
        
        return {"identical": None, "differences": []}
    
    def _exact_compare(
        self,
        production: Dict,
        shadow: Dict
    ) -> Dict[str, Any]:
        """Perform exact comparison."""
        differences = []
        
        if production.get("status_code") != shadow.get("status_code"):
            differences.append({
                "field": "status_code",
                "production": production.get("status_code"),
                "shadow": shadow.get("status_code")
            })
        
        prod_body = production.get("body", {})
        shadow_body = shadow.get("body", {})
        
        if prod_body != shadow_body:
            differences.append({
                "field": "body",
                "production": prod_body,
                "shadow": shadow_body
            })
        
        return {
            "identical": len(differences) == 0,
            "differences": differences
        }
    
    def _schema_compare(
        self,
        production: Dict,
        shadow: Dict
    ) -> Dict[str, Any]:
        """Compare response schemas."""
        differences = []
        
        prod_keys = set(production.get("body", {}).keys())
        shadow_keys = set(shadow.get("body", {}).keys())
        
        missing_in_shadow = prod_keys - shadow_keys
        extra_in_shadow = shadow_keys - prod_keys
        
        if missing_in_shadow:
            differences.append({
                "type": "missing_fields",
                "fields": list(missing_in_shadow)
            })
        
        if extra_in_shadow:
            differences.append({
                "type": "extra_fields",
                "fields": list(extra_in_shadow)
            })
        
        return {
            "identical": len(differences) == 0,
            "differences": differences
        }


class APIShadowModeAction:
    """
    Main shadow mode action handler.
    
    Duplicates production traffic to canary/staging targets
    and compares responses without affecting production.
    """
    
    def __init__(self, config: Optional[ShadowConfig] = None):
        self.config = config or ShadowConfig()
        self.router = ShadowTrafficRouter(self.config)
        self.comparator = ResponseComparator(self.config.comparison_strategy)
        self._results: List[Dict] = []
        self._middleware: List[Callable] = []
    
    def add_target(self, target: ShadowTarget) -> None:
        """Add a shadow traffic target."""
        self.config.targets.append(target)
    
    def remove_target(self, name: str) -> None:
        """Remove a shadow traffic target by name."""
        self.config.targets = [t for t in self.config.targets if t.name != name]
    
    def set_client(self, client: Callable) -> None:
        """Set the HTTP client for shadow requests."""
        self.router.set_client(client)
    
    async def process_request(
        self,
        request: Dict
    ) -> Dict[str, Any]:
        """Process request with shadow traffic."""
        if not self.config.enabled:
            return {"request": request, "shadow_results": []}
        
        if self.config.sample_rate < 1.0:
            import random
            if random.random() > self.config.sample_rate:
                return {"request": request, "shadow_results": []}
        
        shadow_tasks = [
            self.router.send_shadow_request(target, request)
            for target in self.config.targets
        ]
        
        results = await asyncio.gather(*shadow_tasks)
        
        shadow_results = []
        for result in results:
            shadow_results.append({
                "target": result.target_name,
                "success": result.success,
                "duration_ms": result.duration_ms,
                "error": result.error
            })
            
            if self.config.record_results:
                self._results.append({
                    "timestamp": datetime.now().isoformat(),
                    "target": result.target_name,
                    "result": result
                })
        
        return {
            "request": request,
            "shadow_results": shadow_results
        }
    
    async def compare_responses(
        self,
        production_response: Dict,
        shadow_responses: List[ShadowResult]
    ) -> Dict[str, Any]:
        """Compare production and shadow responses."""
        comparisons = []
        
        for shadow in shadow_responses:
            if shadow.success and shadow.response:
                comparison = self.comparator.compare(
                    production_response,
                    shadow.response
                )
                shadow.compared = True
                shadow.comparison_result = comparison
                comparisons.append({
                    "target": shadow.target_name,
                    "comparison": comparison
                })
        
        return {
            "production": production_response,
            "comparisons": comparisons,
            "all_identical": all(
                c["comparison"].get("identical", False)
                for c in comparisons
            )
        }
    
    def get_results(
        self,
        limit: Optional[int] = None,
        target: Optional[str] = None
    ) -> List[Dict]:
        """Get recorded shadow results."""
        results = self._results
        
        if target:
            results = [r for r in results if r["target"] == target]
        
        if limit:
            results = results[-limit:]
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get shadow mode statistics."""
        total = len(self._results)
        successful = sum(1 for r in self._results if r["result"].success)
        failed = total - successful
        
        return {
            "total_requests": total,
            "successful": successful,
            "failed": failed,
            "success_rate": f"{(successful / max(total, 1)) * 100:.1f}%",
            "targets": [t.name for t in self.config.targets],
            "enabled": self.config.enabled
        }
    
    def clear_results(self) -> None:
        """Clear recorded results."""
        self._results.clear()
