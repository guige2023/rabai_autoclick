"""Input delay calibrator for measuring and compensating input latency."""
from typing import Dict, List, Optional
from dataclasses import dataclass
import time


@dataclass
class DelayMeasurement:
    """A single delay measurement."""
    timestamp: float
    input_type: str
    delay_ms: float
    success: bool


class InputDelayCalibrator:
    """Measures and compensates for input system delays.
    
    Calibrates by measuring actual vs expected response times
    and builds compensation profiles.
    
    Example:
        calibrator = InputDelayCalibrator()
        calibrator.measure_click(response_time_ms=45.2)
        profile = calibrator.get_compensation_profile("click")
    """

    def __init__(self, sample_size: int = 20) -> None:
        self._sample_size = sample_size
        self._measurements: Dict[str, List[DelayMeasurement]] = {
            "click": [], "touch": [], "swipe": [], "key": [],
        }
        self._profiles: Dict[str, Dict] = {}

    def measure(self, input_type: str, delay_ms: float, success: bool = True) -> None:
        """Record a delay measurement."""
        if input_type not in self._measurements:
            self._measurements[input_type] = []
        measurement = DelayMeasurement(time.time(), input_type, delay_ms, success)
        self._measurements[input_type].append(measurement)
        if len(self._measurements[input_type]) > self._sample_size:
            self._measurements[input_type] = self._measurements[input_type][-self._sample_size:]
        self._recompute_profile(input_type)

    def measure_click(self, response_time_ms: float, success: bool = True) -> None:
        """Measure a click input delay."""
        self.measure("click", response_time_ms, success)

    def measure_touch(self, response_time_ms: float, success: bool = True) -> None:
        """Measure a touch input delay."""
        self.measure("touch", response_time_ms, success)

    def get_compensation_profile(self, input_type: str) -> Dict:
        """Get compensation profile for an input type."""
        return self._profiles.get(input_type, {"mean_ms": 0, "compensation_ms": 0, "sample_count": 0})

    def get_recommended_delay(self, input_type: str) -> float:
        """Get recommended delay compensation for an input type."""
        return self._profiles.get(input_type, {}).get("compensation_ms", 0)

    def _recompute_profile(self, input_type: str) -> None:
        """Recompute profile after new measurements."""
        measurements = self._measurements.get(input_type, [])
        successful = [m for m in measurements if m.success]
        if not successful:
            return
        delays = [m.delay_ms for m in successful]
        mean = sum(delays) / len(delays)
        variance = sum((d - mean) ** 2 for d in delays) / len(delays)
        self._profiles[input_type] = {
            "mean_ms": round(mean, 2),
            "std_dev_ms": round(variance ** 0.5, 2),
            "compensation_ms": round(mean, 1),
            "sample_count": len(successful),
        }

    def reset(self) -> None:
        """Reset all measurements and profiles."""
        for k in self._measurements:
            self._measurements[k] = []
        self._profiles.clear()
