"""
Touch Prediction Utilities for UI Automation.

This module provides utilities for predicting touch positions
to reduce perceived latency in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class PredictionModel(Enum):
    """Available prediction models."""
    LINEAR = "linear"
    QUADRATIC = "quadratic"
    KALMAN = "kalman"
    EXPONENTIAL = "exponential"


@dataclass
class PredictedPosition:
    """A predicted touch position."""
    x: float
    y: float
    confidence: float
    timestamp: float
    model: PredictionModel
    latency_ms: float


@dataclass
class PredictionState:
    """Internal state for prediction."""
    x: float = 0.0
    y: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    ax: float = 0.0
    ay: float = 0.0
    last_update: float = 0.0
    history_x: List[float] = field(default_factory=list)
    history_y: List[float] = field(default_factory=list)
    history_t: List[float] = field(default_factory=list)


@dataclass
class TouchPredictorConfig:
    """Configuration for touch prediction."""
    model: PredictionModel = PredictionModel.LINEAR
    lookahead_ms: float = 16.0
    history_size: int = 5
    min_history_for_prediction: int = 2
    confidence_threshold: float = 0.5


class TouchPredictor:
    """Predicts touch positions to reduce perceived latency."""

    def __init__(self, config: Optional[TouchPredictorConfig] = None) -> None:
        self._config = config or TouchPredictorConfig()
        self._state = PredictionState()
        self._max_history: int = self._config.history_size

    def set_config(self, config: TouchPredictorConfig) -> None:
        """Update the prediction configuration."""
        self._config = config
        self._max_history = config.history_size

    def update(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Update the predictor with a new touch position."""
        if timestamp is None:
            timestamp = time.time()

        self._state.x = x
        self._state.y = y
        self._state.last_update = timestamp

        self._state.history_x.append(x)
        self._state.history_y.append(y)
        self._state.history_t.append(timestamp)

        if len(self._state.history_x) > self._max_history:
            self._state.history_x.pop(0)
            self._state.history_y.pop(0)
            self._state.history_t.pop(0)

        self._update_velocity()

    def _update_velocity(self) -> None:
        """Update velocity estimates from history."""
        if len(self._state.history_x) < 2:
            return

        history_len = len(self._state.history_x)
        t0 = self._state.history_t[0]
        t1 = self._state.history_t[-1]
        dt = t1 - t0

        if dt > 0:
            self._state.vx = (self._state.history_x[-1] - self._state.history_x[0]) / dt
            self._state.vy = (self._state.history_y[-1] - self._state.history_y[0]) / dt
        else:
            self._state.vx = 0.0
            self._state.vy = 0.0

        if len(self._state.history_x) >= 3:
            dt2 = (t1 - self._state.history_t[-3]) / 2 if history_len >= 3 else dt
            if dt2 > 0:
                vx2 = (self._state.history_x[-1] - self._state.history_x[-3]) / dt2
                vy2 = (self._state.history_y[-1] - self._state.history_y[-3]) / dt2
                self._state.ax = (self._state.vx - vx2) / dt2 if dt2 > 0 else 0.0
                self._state.ay = (self._state.vy - vy2) / dt2 if dt2 > 0 else 0.0

    def predict(
        self,
        lookahead_ms: Optional[float] = None,
    ) -> Optional[PredictedPosition]:
        """Predict the touch position at a future time."""
        if lookahead_ms is None:
            lookahead_ms = self._config.lookahead_ms

        if len(self._state.history_x) < self._config.min_history_for_prediction:
            return None

        dt = lookahead_ms / 1000.0

        if self._config.model == PredictionModel.LINEAR:
            pred_x = self._state.x + self._state.vx * dt
            pred_y = self._state.y + self._state.vy * dt
        elif self._config.model == PredictionModel.QUADRATIC:
            pred_x = self._state.x + self._state.vx * dt + 0.5 * self._state.ax * dt * dt
            pred_y = self._state.y + self._state.vy * dt + 0.5 * self._state.ay * dt * dt
        elif self._config.model == PredictionModel.EXPONENTIAL:
            alpha = 0.5
            decay = math.exp(-alpha * dt)
            pred_x = self._state.x + self._state.vx * dt * decay
            pred_y = self._state.y + self._state.vy * dt * decay
        else:
            pred_x = self._state.x
            pred_y = self._state.y

        confidence = self._calculate_confidence()

        return PredictedPosition(
            x=pred_x,
            y=pred_y,
            confidence=confidence,
            timestamp=time.time() + dt,
            model=self._config.model,
            latency_ms=lookahead_ms,
        )

    def _calculate_confidence(self) -> float:
        """Calculate prediction confidence based on motion consistency."""
        if len(self._state.history_x) < 2:
            return 0.0

        velocities = []
        for i in range(1, len(self._state.history_x)):
            dt = self._state.history_t[i] - self._state.history_t[i - 1]
            if dt > 0:
                vx = (self._state.history_x[i] - self._state.history_x[i - 1]) / dt
                vy = (self._state.history_y[i] - self._state.history_y[i - 1]) / dt
                velocities.append(math.sqrt(vx * vx + vy * vy))

        if not velocities:
            return 0.5

        avg_vel = sum(velocities) / len(velocities)
        variance = sum((v - avg_vel) ** 2 for v in velocities) / len(velocities)
        std_dev = math.sqrt(variance)

        confidence = max(0.0, 1.0 - std_dev / (avg_vel + 1.0))
        return min(confidence, 1.0)

    def reset(self) -> None:
        """Reset all prediction state."""
        self._state = PredictionState()
