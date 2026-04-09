"""API A/B Testing Action Module.

Provides A/B testing capabilities for API endpoints including
variant allocation, metrics tracking, and statistical analysis.

Example:
    >>> from actions.api.api_ab_testing_action import ABTest, ABTester
    >>> tester = ABTester()
    >>> variant = tester.get_variant(user_id, test_name="homepage_v1")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import hashlib
import random
import threading
import time
import uuid


class Variant(Enum):
    """Test variant types."""
    CONTROL = "control"
    TREATMENT_A = "treatment_a"
    TREATMENT_B = "treatment_b"
    TREATMENT_C = "treatment_c"


class TestStatus(Enum):
    """A/B test status."""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"


@dataclass
class VariantConfig:
    """Configuration for a test variant.
    
    Attributes:
        variant: Variant type
        weight: Allocation weight (0-100)
        config: Variant-specific configuration
        description: Variant description
    """
    variant: Variant
    weight: float
    config: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class ABTest:
    """A/B test definition.
    
    Attributes:
        test_id: Unique test identifier
        name: Test name
        description: Test description
        variants: List of variant configurations
        target_metric: Primary metric to optimize
        start_time: Test start time
        end_time: Test end time
        status: Current test status
        min_sample_size: Minimum sample size per variant
    """
    test_id: str
    name: str
    description: str = ""
    variants: List[VariantConfig] = field(default_factory=list)
    target_metric: str = "conversion_rate"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: TestStatus = TestStatus.DRAFT
    min_sample_size: int = 1000


@dataclass
class VariantAssignment:
    """Variant assignment result.
    
    Attributes:
        test_id: Associated test ID
        variant: Assigned variant
        user_id: User identifier
        assigned_at: Assignment timestamp
    """
    test_id: str
    variant: Variant
    user_id: str
    assigned_at: datetime = field(default_factory=datetime.now)


@dataclass
class TestMetrics:
    """A/B test metrics.
    
    Attributes:
        test_id: Associated test ID
        variant: Variant type
        sample_size: Number of participants
        conversions: Number of conversions
        conversion_rate: Calculated conversion rate
        confidence_level: Statistical confidence
    """
    test_id: str
    variant: Variant
    sample_size: int
    conversions: int
    conversion_rate: float = 0.0
    confidence_level: float = 0.0


class ABTester:
    """A/B testing controller for API operations.
    
    Manages variant allocation, metrics tracking, and
    statistical analysis for A/B testing.
    
    Attributes:
        _tests: Registered A/B tests
        _assignments: User variant assignments
        _metrics: Test metrics by variant
        _lock: Thread safety lock
    """
    
    def __init__(self, seed: Optional[int] = None) -> None:
        """Initialize A/B tester.
        
        Args:
            seed: Random seed for reproducibility
        """
        self._tests: Dict[str, ABTest] = {}
        self._assignments: Dict[str, Dict[str, VariantAssignment]] = {}
        self._metrics: Dict[str, Dict[Variant, TestMetrics]] = {}
        self._lock = threading.RLock()
        if seed is not None:
            random.seed(seed)
    
    def create_test(
        self,
        name: str,
        variants: List[VariantConfig],
        target_metric: str = "conversion_rate",
        min_sample_size: int = 1000,
        description: str = "",
    ) -> str:
        """Create a new A/B test.
        
        Args:
            name: Test name
            variants: Variant configurations
            target_metric: Metric to optimize
            min_sample_size: Minimum sample size
            description: Test description
            
        Returns:
            Created test ID
        """
        test_id = str(uuid.uuid4())[:8]
        
        # Validate weights sum to 100
        total_weight = sum(v.weight for v in variants)
        if abs(total_weight - 100.0) > 0.01:
            raise ValueError(f"Variant weights must sum to 100, got {total_weight}")
        
        test = ABTest(
            test_id=test_id,
            name=name,
            description=description,
            variants=variants,
            target_metric=target_metric,
            min_sample_size=min_sample_size,
        )
        
        with self._lock:
            self._tests[test_id] = test
            self._metrics[test_id] = {
                v.variant: TestMetrics(test_id=test_id, variant=v.variant, sample_size=0, conversions=0)
                for v in variants
            }
        
        return test_id
    
    def start_test(self, test_id: str) -> None:
        """Start a test.
        
        Args:
            test_id: Test ID to start
        """
        with self._lock:
            if test_id not in self._tests:
                raise KeyError(f"Test {test_id} not found")
            test = self._tests[test_id]
            if test.status == TestStatus.RUNNING:
                raise ValueError(f"Test {test_id} is already running")
            test.status = TestStatus.RUNNING
            test.start_time = datetime.now()
    
    def stop_test(self, test_id: str) -> None:
        """Stop a test.
        
        Args:
            test_id: Test ID to stop
        """
        with self._lock:
            if test_id not in self._tests:
                raise KeyError(f"Test {test_id} not found")
            test = self._tests[test_id]
            test.status = TestStatus.COMPLETED
            test.end_time = datetime.now()
    
    def get_variant(
        self,
        user_id: str,
        test_id: str,
        force_variant: Optional[Variant] = None,
    ) -> Variant:
        """Get variant assignment for user.
        
        Args:
            user_id: User identifier
            test_id: Test ID
            force_variant: Force specific variant (for debugging)
            
        Returns:
            Assigned variant
        """
        if force_variant is not None:
            return force_variant
        
        with self._lock:
            # Check existing assignment
            if test_id in self._assignments:
                if user_id in self._assignments[test_id]:
                    return self._assignments[test_id][user_id].variant
            
            # Allocate new variant
            test = self._tests.get(test_id)
            if test is None:
                raise KeyError(f"Test {test_id} not found")
            
            if test.status != TestStatus.RUNNING:
                raise ValueError(f"Test {test_id} is not running")
            
            variant = self._allocate_variant(user_id, test)
            
            # Store assignment
            assignment = VariantAssignment(
                test_id=test_id,
                variant=variant,
                user_id=user_id,
            )
            if test_id not in self._assignments:
                self._assignments[test_id] = {}
            self._assignments[test_id][user_id] = assignment
            
            # Increment sample size
            self._metrics[test_id][variant].sample_size += 1
            
            return variant
    
    def _allocate_variant(self, user_id: str, test: ABTest) -> Variant:
        """Allocate variant based on user hash.
        
        Args:
            user_id: User identifier
            test: A/B test configuration
            
        Returns:
            Allocated variant
        """
        # Create deterministic hash for consistent assignment
        hash_input = f"{user_id}:{test.test_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        normalized = (hash_value % 10000) / 10000.0 * 100.0
        
        cumulative = 0.0
        for variant_config in test.variants:
            cumulative += variant_config.weight
            if normalized < cumulative:
                return variant_config.variant
        
        return test.variants[-1].variant
    
    def record_conversion(
        self,
        user_id: str,
        test_id: str,
        value: float = 1.0,
    ) -> None:
        """Record a conversion for user.
        
        Args:
            user_id: User identifier
            test_id: Test ID
            value: Conversion value
        """
        with self._lock:
            if test_id not in self._assignments:
                return
            if user_id not in self._assignments[test_id]:
                return
            
            variant = self._assignments[test_id][user_id].variant
            metrics = self._metrics[test_id][variant]
            metrics.conversions += int(value)
            
            # Update conversion rate
            if metrics.sample_size > 0:
                metrics.conversion_rate = metrics.conversions / metrics.sample_size
    
    def get_metrics(self, test_id: str) -> Dict[Variant, TestMetrics]:
        """Get test metrics.
        
        Args:
            test_id: Test ID
            
        Returns:
            Dictionary of variant metrics
        """
        with self._lock:
            if test_id not in self._metrics:
                raise KeyError(f"Test {test_id} not found")
            return dict(self._metrics[test_id])
    
    def analyze_test(self, test_id: str) -> Dict[str, Any]:
        """Perform statistical analysis on test.
        
        Args:
            test_id: Test ID
            
        Returns:
            Analysis results with significance testing
        """
        with self._lock:
            if test_id not in self._metrics:
                raise KeyError(f"Test {test_id} not found")
            
            metrics = self._metrics[test_id]
            results = {
                "test_id": test_id,
                "variants": {},
                "recommendation": " inconclusive",
                "confidence": 0.0,
            }
            
            # Find control variant
            control = metrics.get(Variant.CONTROL)
            if control is None:
                return results
            
            # Compare each variant to control
            for variant, m in metrics.items():
                if variant == Variant.CONTROL:
                    continue
                
                # Calculate z-score (simplified)
                diff = m.conversion_rate - control.conversion_rate
                pooled_rate = (
                    (m.conversions + control.conversions) /
                    (m.sample_size + control.sample_size)
                    if (m.sample_size + control.sample_size) > 0 else 0
                )
                se = (pooled_rate * (1 - pooled_rate) * (
                    1/m.sample_size + 1/control.sample_size
                )) ** 0.5 if se := pooled_rate * (1 - pooled_rate) * (
                    1/m.sample_size + 1/control.sample_size
                ) ** 0.5 else 0.001
                
                z_score = diff / se if se > 0 else 0
                
                results["variants"][variant.value] = {
                    "sample_size": m.sample_size,
                    "conversions": m.conversions,
                    "conversion_rate": m.conversion_rate,
                    "lift": (diff / control.conversion_rate * 100) if control.conversion_rate > 0 else 0,
                    "z_score": z_score,
                }
                
                # Check significance (|z| > 1.96 for 95% confidence)
                if abs(z_score) > 1.96:
                    if diff > 0:
                        results["recommendation"] = f"{variant.value} wins"
                    else:
                        results["recommendation"] = "control wins"
                    results["confidence"] = min(abs(z_score) / 3.0 * 100, 99.9)
            
            return results
    
    def get_test_status(self, test_id: str) -> Dict[str, Any]:
        """Get test status and progress.
        
        Args:
            test_id: Test ID
            
        Returns:
            Test status information
        """
        with self._lock:
            if test_id not in self._tests:
                raise KeyError(f"Test {test_id} not found")
            
            test = self._tests[test_id]
            metrics = self._metrics.get(test_id, {})
            total_samples = sum(m.sample_size for m in metrics.values())
            
            return {
                "test_id": test_id,
                "name": test.name,
                "status": test.status.value,
                "start_time": test.start_time.isoformat() if test.start_time else None,
                "end_time": test.end_time.isoformat() if test.end_time else None,
                "total_samples": total_samples,
                "min_sample_size": test.min_sample_size,
                "progress_pct": min(total_samples / (test.min_sample_size * len(test.variants)) * 100, 100),
            }
