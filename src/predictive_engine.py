"""
预测性自动化引擎 v22
P0级差异化功能 - 基于用户历史行为预测下一个可能需要的动作
ML增强版 - 支持模式学习、异常检测、性能预测、自适应置信度等高级功能
"""
import json
import time
import os
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from collections import defaultdict, Counter
import re


# ============== Data Classes ==============

@dataclass
class UserAction:
    """用户动作记录"""
    timestamp: float
    action_type: str  # click, type, hotkey, app_launch, workflow_trigger
    target: str  # 目标元素/应用/工作流名称
    context: Dict[str, Any]  # 上下文信息
    result: str = "success"  # success, failed, cancelled
    duration: float = 0.0  # 耗时(秒)


@dataclass
class Prediction:
    """预测结果"""
    predicted_action: str
    confidence: float  # 0-1 置信度
    reasoning: str
    suggested_workflow: Optional[str] = None
    alternatives: List[str] = field(default_factory=list)
    context_match: Dict[str, Any] = field(default_factory=dict)
    failure_risk: float = 0.0  # 失败风险 0-1
    predicted_duration: float = 0.0  # 预测耗时(秒)
    quality_score: float = 0.0  # 质量分数 0-1


@dataclass
class ActionPattern:
    """动作模式"""
    sequence: List[str]
    frequency: int
    avg_interval: float  # 平均间隔(秒)
    time_of_day: str
    day_of_week: str
    trigger_context: Dict[str, Any]


@dataclass
class AnomalyReport:
    """异常报告"""
    timestamp: float
    anomaly_type: str  # unusual_time, unusual_sequence, performance_degradation, repeated_failure
    severity: str  # low, medium, high, critical
    description: str
    action_context: Dict[str, Any]
    recommended_action: str


@dataclass
class OCRQualityPrediction:
    """OCR质量预测"""
    image_hash: str
    predicted_accuracy: float  # 0-1
    confidence_factors: Dict[str, float]
    low_confidence_regions: List[Dict[str, Any]] = field(default_factory=list)
    suggested_improvements: List[str] = field(default_factory=list)


@dataclass
class ImageMatchQuality:
    """图像匹配质量"""
    match_score: float  # 基础匹配分数
    quality_grade: str  # excellent, good, fair, poor
    feature_stability: float  # 特征稳定性 0-1
    environmental_factors: Dict[str, float] = field(default_factory=dict)
    recommended_confidence_threshold: float = 0.0


@dataclass
class RetrySchedule:
    """重试调度计划"""
    target_action: str
    recommended_delay: float  # 秒
    max_attempts: int
    success_probability: float
    best_time_window: Tuple[float, float]  # (start, end) timestamp
    reason: str


@dataclass
class PerformanceMetrics:
    """性能指标"""
    action: str
    avg_duration: float
    std_deviation: float
    min_duration: float
    max_duration: float
    percentile_95: float
    sample_count: int
    trend: str  # improving, stable, degrading


# ============== ML-Based Learning ==============

class MLPatternLearner:
    """ML-based pattern learning using simple statistical methods"""

    def __init__(self):
        self.action_embeddings: Dict[str, List[float]] = {}
        self.sequence_models: Dict[str, Dict] = defaultdict(dict)
        self.context_weights: Dict[str, float] = defaultdict(float)
        self.feature_importance: Dict[str, float] = defaultdict(float)
        self._init_default_weights()

    def _init_default_weights(self):
        """Initialize default feature weights"""
        self.context_weights = {
            "time_of_day": 0.25,
            "day_of_week": 0.15,
            "active_app": 0.30,
            "recent_actions": 0.20,
            "sequence_pattern": 0.10
        }

    def learn_from_sequence(self, actions: List[UserAction]) -> Dict[str, Any]:
        """Learn patterns from action sequences"""
        if len(actions) < 3:
            return {}

        features = self._extract_sequence_features(actions)
        self._update_sequence_model(actions, features)
        return features

    def _extract_sequence_features(self, actions: List[UserAction]) -> Dict[str, Any]:
        """Extract features from action sequence"""
        targets = [a.target for a in actions]
        types = [a.action_type for a in actions]
        durations = [a.duration for a in actions]

        # Calculate transition probabilities
        transitions = {}
        for i in range(len(targets) - 1):
            key = f"{targets[i]}->{targets[i+1]}"
            transitions[key] = transitions.get(key, 0) + 1

        # Normalize transitions
        total = sum(transitions.values())
        for k in transitions:
            transitions[k] /= total

        return {
            "targets": targets,
            "types": types,
            "avg_duration": sum(durations) / len(durations) if durations else 0,
            "transitions": transitions,
            "sequence_length": len(actions),
            "unique_targets": len(set(targets))
        }

    def _update_sequence_model(self, actions: List[UserAction], features: Dict):
        """Update sequence model based on features"""
        key = f"{actions[0].target}_{actions[-1].target}"
        self.sequence_models[key] = {
            "features": features,
            "count": self.sequence_models[key].get("count", 0) + 1,
            "last_seen": time.time()
        }

    def predict_next_in_sequence(self, recent_actions: List[str], context: Dict) -> Optional[Tuple[str, float]]:
        """Predict next action in sequence with confidence"""
        if len(recent_actions) < 2:
            return None

        # Find matching sequence patterns
        best_match = None
        best_score = 0.0

        for model_key, model in self.sequence_models.items():
            seq_targets = model["features"].get("targets", [])
            if len(seq_targets) >= len(recent_actions):
                # Check if recent actions match the end of a known sequence
                match_len = 0
                for i in range(len(recent_actions)):
                    if i < len(seq_targets) and seq_targets[-(i+1)] == recent_actions[-(i+1)]:
                        match_len += 1
                    else:
                        break

                if match_len >= 2:
                    score = match_len * model["features"].get("sequence_length", 1)
                    if score > best_score:
                        best_score = score
                        # Get the next action after the matched sequence
                        pattern_end = len(seq_targets) - match_len
                        if pattern_end < len(seq_targets):
                            best_match = seq_targets[pattern_end]

        if best_match:
            confidence = min(0.95, 0.5 + (best_score * 0.1))
            return (best_match, confidence)

        return None

    def calculate_similarity(self, action1: str, action2: str) -> float:
        """Calculate similarity between two actions using embeddings"""
        if action1 not in self.action_embeddings:
            self.action_embeddings[action1] = self._create_embedding(action1)
        if action2 not in self.action_embeddings:
            self.action_embeddings[action2] = self._create_embedding(action2)

        emb1 = self.action_embeddings[action1]
        emb2 = self.action_embeddings[action2]

        # Cosine similarity
        dot = sum(a * b for a, b in zip(emb1, emb2))
        norm1 = math.sqrt(sum(a * a for a in emb1))
        norm2 = math.sqrt(sum(a * a for a in emb2))

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot / (norm1 * norm2)

    def _create_embedding(self, action: str) -> List[float]:
        """Create a simple embedding for an action based on its characteristics"""
        # Simple hash-based embedding for demo - in production use proper embeddings
        import hashlib
        hash_val = int(hashlib.md5(action.encode()).hexdigest()[:8], 16)

        # Create 8-dimensional embedding
        embedding = []
        for i in range(8):
            embedding.append(((hash_val >> (i * 4)) & 0xF) / 15.0)

        return embedding


# ============== Anomaly Detection ==============

class AnomalyDetector:
    """Detect unusual behavior patterns"""

    def __init__(self, history: List[UserAction]):
        self.history = history
        self.baseline_stats: Dict[str, Dict] = {}
        self.anomaly_threshold = 2.5  # Standard deviations

    def detect_all(self, current_action: UserAction, context: Dict) -> List[AnomalyReport]:
        """Run all anomaly detection methods"""
        reports = []

        time_report = self._detect_unusual_time(current_action)
        if time_report:
            reports.append(time_report)

        sequence_report = self._detect_unusual_sequence(current_action)
        if sequence_report:
            reports.append(sequence_report)

        performance_report = self._detect_performance_degradation(current_action)
        if performance_report:
            reports.append(performance_report)

        return reports

    def _detect_unusual_time(self, action: UserAction) -> Optional[AnomalyReport]:
        """Detect if action is at unusual time"""
        if len(self.history) < 50:
            return None

        dt = datetime.fromtimestamp(action.timestamp)
        hour = dt.hour

        # Build hour distribution
        hour_counts = Counter([datetime.fromtimestamp(a.timestamp).hour for a in self.history[-500:]])

        if hour not in hour_counts or hour_counts[hour] < 2:
            # Unusual hour
            return AnomalyReport(
                timestamp=action.timestamp,
                anomaly_type="unusual_time",
                severity="low",
                description=f"Action '{action.target}' executed at unusual hour ({hour}:00)",
                action_context={"hour": hour, "typical_hours": list(hour_counts.keys())},
                recommended_action="Verify this action is intentional"
            )

        return None

    def _detect_unusual_sequence(self, action: UserAction) -> Optional[AnomalyReport]:
        """Detect unusual action sequences"""
        if len(self.history) < 20:
            return None

        recent_targets = [a.target for a in self.history[-10:]]

        # Check for repetition anomaly
        if len(recent_targets) >= 5:
            last_5 = recent_targets[-5:]
            if len(set(last_5)) == 1:
                return AnomalyReport(
                    timestamp=action.timestamp,
                    anomaly_type="repeated_failure",
                    severity="high",
                    description=f"Same action '{action.target}' repeated 5+ times - possible stuck state",
                    action_context={"repeated_action": action.target, "count": 5},
                    recommended_action="Check for UI freeze or repeated automation loop"
                )

        return None

    def _detect_performance_degradation(self, action: UserAction) -> Optional[AnomalyReport]:
        """Detect if action is taking longer than expected"""
        if len(self.history) < 20 or action.duration == 0:
            return None

        # Find similar actions
        similar = [a for a in self.history[-500:] if a.target == action.target and a.duration > 0]

        if len(similar) < 5:
            return None

        durations = [a.duration for a in similar]
        avg = sum(durations) / len(durations)
        variance = sum((d - avg) ** 2 for d in durations) / len(durations)
        std = math.sqrt(variance)

        if action.duration > avg + (self.anomaly_threshold * std):
            return AnomalyReport(
                timestamp=action.timestamp,
                anomaly_type="performance_degradation",
                severity="medium",
                description=f"Action '{action.target}' taking {action.duration:.1f}s vs expected {avg:.1f}s",
                action_context={
                    "actual_duration": action.duration,
                    "expected_duration": avg,
                    "std_deviation": std
                },
                recommended_action="Investigate system load or target element changes"
            )

        return None


# ============== Performance Predictor ==============

class PerformancePredictor:
    """Predict how long actions will take"""

    def __init__(self):
        self.action_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            "sum": 0.0,
            "count": 0,
            "min": float('inf'),
            "max": 0.0,
            "squared_sum": 0.0,
            "recent_values": []
        })
        self.trend_window = 20

    def record_duration(self, action: str, duration: float) -> None:
        """Record action duration for prediction model"""
        stats = self.action_stats[action]

        stats["sum"] += duration
        stats["count"] += 1
        stats["min"] = min(stats["min"], duration)
        stats["max"] = max(stats["max"], duration)
        stats["squared_sum"] += duration ** 2

        # Keep rolling window for trend detection
        stats["recent_values"].append(duration)
        if len(stats["recent_values"]) > self.trend_window:
            stats["recent_values"].pop(0)

    def predict(self, action: str, context: Dict = None) -> Tuple[float, PerformanceMetrics]:
        """Predict duration for an action"""
        stats = self.action_stats.get(action)

        if not stats or stats["count"] < 3:
            # No data - return defaults based on action type
            return self._default_prediction(action)

        avg = stats["sum"] / stats["count"]

        # Calculate std dev
        variance = (stats["squared_sum"] / stats["count"]) - (avg ** 2)
        std = math.sqrt(max(0, variance))

        # Detect trend using recent values
        trend = self._detect_trend(stats["recent_values"])

        # Adjust prediction based on context (time of day, app state, etc.)
        context_multiplier = 1.0
        if context:
            hour = datetime.now().hour
            if 13 <= hour <= 15:  # Post-lunch dip
                context_multiplier *= 1.15
            if context.get("active_app"):
                context_multiplier *= 1.1  # App switching adds overhead

        predicted = avg * context_multiplier

        metrics = PerformanceMetrics(
            action=action,
            avg_duration=avg,
            std_deviation=std,
            min_duration=stats["min"],
            max_duration=stats["max"],
            percentile_95=self._calculate_percentile(stats["recent_values"], 95),
            sample_count=stats["count"],
            trend=trend
        )

        return predicted, metrics

    def _detect_trend(self, values: List[float]) -> str:
        """Detect trend in recent values"""
        if len(values) < 5:
            return "stable"

        # Simple linear regression slope
        n = len(values)
        x_vals = list(range(n))
        x_mean = sum(x_vals) / n
        y_mean = sum(values) / n

        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, values))
        denominator = sum((x - x_mean) ** 2 for x in x_vals)

        if denominator == 0:
            return "stable"

        slope = numerator / denominator

        # Normalize slope by mean value
        if y_mean > 0:
            normalized_slope = slope / y_mean
        else:
            normalized_slope = 0

        if normalized_slope > 0.05:
            return "degrading"
        elif normalized_slope < -0.05:
            return "improving"
        else:
            return "stable"

    def _calculate_percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of values"""
        if not values:
            return 0.0

        sorted_vals = sorted(values)
        index = int(len(sorted_vals) * percentile / 100)
        return sorted_vals[min(index, len(sorted_vals) - 1)]

    def _default_prediction(self, action: str) -> Tuple[float, PerformanceMetrics]:
        """Get default prediction for unknown actions"""
        # Heuristics based on action type
        defaults = {
            "click": 0.5,
            "type": 1.0,
            "hotkey": 0.1,
            "app_launch": 2.0,
            "workflow_trigger": 1.5,
            "image_match": 0.3,
            "ocr": 0.8
        }

        base = defaults.get(action, 1.0)

        metrics = PerformanceMetrics(
            action=action,
            avg_duration=base,
            std_deviation=base * 0.5,
            min_duration=base * 0.5,
            max_duration=base * 2.0,
            percentile_95=base * 1.8,
            sample_count=0,
            trend="unknown"
        )

        return base, metrics


# ============== Adaptive Confidence ==============

class AdaptiveConfidenceEngine:
    """Adjust confidence thresholds based on success rate"""

    def __init__(self):
        self.action_success_rates: Dict[str, List[bool]] = defaultdict(list)
        self.confidence_multipliers: Dict[str, float] = defaultdict(lambda: 1.0)
        self.window_size = 50

    def record_result(self, action: str, success: bool) -> None:
        """Record action result for adaptive confidence"""
        self.action_success_rates[action].append(success)

        # Keep window size
        if len(self.action_success_rates[action]) > self.window_size:
            self.action_success_rates[action].pop(0)

        self._update_multiplier(action)

    def _update_multiplier(self, action: str) -> None:
        """Update confidence multiplier based on recent success rate"""
        results = self.action_success_rates.get(action, [])

        if len(results) < 5:
            return

        recent = results[-20:] if len(results) >= 20 else results
        success_rate = sum(recent) / len(recent)

        # Map success rate to multiplier
        # 100% success -> 1.2 multiplier (boost confidence)
        # 50% success -> 0.8 multiplier
        # 0% success -> 0.3 multiplier
        self.confidence_multipliers[action] = 0.3 + (success_rate * 0.9)

    def get_adjusted_confidence(self, base_confidence: float, action: str) -> float:
        """Get confidence adjusted by historical success rate"""
        multiplier = self.confidence_multipliers.get(action, 1.0)
        adjusted = base_confidence * multiplier

        # Clamp to [0.1, 1.0]
        return max(0.1, min(1.0, adjusted))

    def get_success_rate(self, action: str) -> float:
        """Get current success rate for an action"""
        results = self.action_success_rates.get(action, [])
        if not results:
            return 0.5  # Default

        return sum(results) / len(results)


# ============== Failure Predictor ==============

class FailurePredictor:
    """Predict likely failures before they happen"""

    def __init__(self, history: List[UserAction]):
        self.history = history
        self.failure_patterns: Dict[str, Dict] = defaultdict(lambda: {
            "count": 0,
            "contexts": [],
            "time_patterns": [],
            "retry_success_rate": 0.5
        })

    def analyze_failure_risk(self, action: UserAction, context: Dict) -> float:
        """Analyze and return failure risk (0-1)"""
        if len(self.history) < 20:
            return 0.3  # Default moderate risk

        risk_score = 0.3  # Base risk

        # Check similar past actions
        similar_failures = self._find_similar_failures(action)
        if similar_failures:
            failure_rate = sum(1 for f in similar_failures if f.result == "failed") / len(similar_failures)
            risk_score = max(risk_score, failure_rate)

        # Check for environmental factors
        risk_score += self._environmental_risk(action, context)

        # Check for repetition patterns
        if self._check_repeated_failure(action):
            risk_score = min(1.0, risk_score + 0.3)

        return min(1.0, risk_score)

    def _find_similar_failures(self, action: UserAction) -> List[UserAction]:
        """Find similar past actions and their results"""
        recent = [a for a in self.history[-500:] if a.target == action.target]
        return recent

    def _environmental_risk(self, action: UserAction, context: Dict) -> float:
        """Calculate environmental risk factors"""
        risk = 0.0

        # Check time-based risk
        dt = datetime.fromtimestamp(action.timestamp)
        if dt.hour < 6 or dt.hour > 23:
            risk += 0.1  # Late night operations

        # Check for known failure contexts
        if context:
            if context.get("screen_locked"):
                risk += 0.4
            if context.get("app_not_responding"):
                risk += 0.3

        return risk

    def _check_repeated_failure(self, action: UserAction) -> bool:
        """Check if same action has failed repeatedly"""
        recent = [a for a in self.history[-20:] if a.target == action.target]

        if len(recent) >= 3:
            failures = sum(1 for a in recent if a.result == "failed")
            if failures >= 2:
                return True
        return False

    def get_warning_message(self, action: UserAction, risk: float) -> Optional[str]:
        """Get warning message for high risk actions"""
        if risk < 0.6:
            return None

        warnings = {
            0.6: f"Caution: '{action.target}' has elevated failure risk",
            0.75: f"Warning: '{action.target}' likely to fail - consider retry strategy",
            0.9: f"Critical: '{action.target}' almost certain to fail - abort recommended"
        }

        if risk >= 0.9:
            return warnings[0.9]
        elif risk >= 0.75:
            return warnings[0.75]
        elif risk >= 0.6:
            return warnings[0.6]

        return None


# ============== Smart Retry Scheduler ==============

class SmartRetryScheduler:
    """Predict best times to retry failed actions"""

    def __init__(self, history: List[UserAction]):
        self.history = history
        self.retry_outcomes: Dict[str, List[Dict]] = defaultdict(list)

    def get_retry_schedule(self, failed_action: str, context: Dict = None) -> RetrySchedule:
        """Get optimal retry schedule for a failed action"""
        past_retries = self.retry_outcomes.get(failed_action, [])

        # Analyze past retry outcomes
        if past_retries:
            best_delay = self._calculate_optimal_delay(past_retries)
            success_prob = self._calculate_success_probability(past_retries)
            max_attempts = min(5, self._calculate_max_attempts(past_retries))
        else:
            best_delay = 2.0  # Default 2 seconds
            success_prob = 0.5
            max_attempts = 3

        # Adjust based on context
        if context:
            if context.get("active_app") == "System Preferences":
                best_delay *= 1.5  # Slower apps need longer delays
            if context.get("is_automation_running"):
                best_delay *= 0.8  # Can retry faster during automation

        # Calculate best time window
        now = time.time()
        best_window = (now, now + best_delay * max_attempts * 2)

        return RetrySchedule(
            target_action=failed_action,
            recommended_delay=best_delay,
            max_attempts=max_attempts,
            success_probability=success_prob,
            best_time_window=best_window,
            reason=self._get_recommendation_reason(past_retries, success_prob)
        )

    def record_retry_outcome(self, action: str, delay: float, success: bool) -> None:
        """Record retry attempt outcome"""
        self.retry_outcomes[action].append({
            "delay": delay,
            "success": success,
            "timestamp": time.time()
        })

        # Keep recent history
        if len(self.retry_outcomes[action]) > 50:
            self.retry_outcomes[action] = self.retry_outcomes[action][-50:]

    def _calculate_optimal_delay(self, outcomes: List[Dict]) -> float:
        """Calculate optimal delay from past outcomes"""
        successful = [o for o in outcomes if o["success"]]
        if not successful:
            return 3.0  # Default

        return sum(o["delay"] for o in successful) / len(successful)

    def _calculate_success_probability(self, outcomes: List[Dict]) -> float:
        """Calculate success probability for retries"""
        if not outcomes:
            return 0.5

        recent = outcomes[-10:]
        return sum(1 for o in recent if o["success"]) / len(recent)

    def _calculate_max_attempts(self, outcomes: List[Dict]) -> int:
        """Calculate max recommended attempts"""
        recent = outcomes[-20:]
        successes_after_retries = sum(1 for o in recent if o["success"])

        if successes_after_retries >= 5:
            return 5
        elif successes_after_retries >= 3:
            return 4
        elif successes_after_retries >= 1:
            return 3
        else:
            return 2

    def _get_recommendation_reason(self, outcomes: List[Dict], prob: float) -> str:
        """Get reason for recommendation"""
        if not outcomes:
            return "No historical data - using defaults"

        if prob >= 0.7:
            return f"High retry success rate ({prob:.0%}) - confident retry"
        elif prob >= 0.4:
            return f"Moderate retry success rate ({prob:.0%}) - consider alternatives"
        else:
            return f"Low retry success rate ({prob:.0%}) - may need different approach"


# ============== OCR Quality Predictor ==============

class OCRQualityPredictor:
    """Predict OCR accuracy before processing"""

    def __init__(self):
        self.ocr_outcomes: Dict[str, Dict] = defaultdict(lambda: {
            "success": [],
            "failure": [],
            "low_confidence_patterns": []
        })
        self.known_hashes: Dict[str, float] = {}

    def predict_quality(self, image_hash: str, image_context: Dict = None) -> OCRQualityPrediction:
        """Predict OCR quality for an image"""
        context = image_context or {}

        factors = {}
        predicted_accuracy = 0.7  # Base accuracy

        # Factor 1: Known previous results for similar images
        if image_hash in self.known_hashes:
            predicted_accuracy = self.known_hashes[image_hash]
            factors["historical"] = predicted_accuracy
        else:
            factors["historical"] = 0.7

        # Factor 2: Image size and quality indicators
        width = context.get("width", 0)
        height = context.get("height", 0)
        if width > 0 and height > 0:
            resolution_factor = min(1.0, (width * height) / (1920 * 1080))
            factors["resolution"] = resolution_factor
            predicted_accuracy = predicted_accuracy * 0.6 + resolution_factor * 0.4

        # Factor 3: Contrast and brightness estimates
        contrast = context.get("estimated_contrast", 0.5)
        brightness = context.get("estimated_brightness", 0.5)
        factors["contrast"] = contrast
        factors["brightness"] = brightness

        # Penalize extreme values
        brightness_penalty = abs(brightness - 0.5) * 0.2
        predicted_accuracy -= brightness_penalty

        # Factor 4: Known low-quality patterns
        patterns = context.get("detected_patterns", [])
        low_quality_patterns = ["blur", "noise", "compression_artifact", "low_dpi"]
        detected_low_quality = [p for p in patterns if p in low_quality_patterns]

        if detected_low_quality:
            factors["low_quality_patterns"] = len(detected_low_quality) / len(low_quality_patterns)
            predicted_accuracy *= (1 - factors["low_quality_patterns"] * 0.3)

        # Factor 5: Text density
        text_density = context.get("text_density", 0.5)
        factors["text_density"] = text_density
        # Optimal is around 0.3-0.7
        density_factor = 1.0 - abs(text_density - 0.5)
        predicted_accuracy = predicted_accuracy * 0.7 + density_factor * 0.3

        # Clamp final prediction
        predicted_accuracy = max(0.3, min(0.98, predicted_accuracy))

        # Generate improvements
        improvements = []
        if brightness_penalty > 0.1:
            improvements.append("Adjust image brightness")
        if resolution_factor < 0.7:
            improvements.append("Increase image resolution")
        if detected_low_quality:
            improvements.append("Reduce noise/blur in image")

        return OCRQualityPrediction(
            image_hash=image_hash,
            predicted_accuracy=predicted_accuracy,
            confidence_factors=factors,
            low_confidence_regions=[],
            suggested_improvements=improvements
        )

    def record_result(self, image_hash: str, actual_accuracy: float) -> None:
        """Record actual OCR result for future predictions"""
        self.known_hashes[image_hash] = actual_accuracy


# ============== Image Match Quality Scorer ==============

class ImageMatchQualityScorer:
    """Advanced quality scoring for image matches"""

    def __init__(self):
        self.match_history: List[Dict] = []

    def score_match(self, match_result: Dict, context: Dict = None) -> ImageMatchQuality:
        """Score the quality of an image match"""
        base_score = match_result.get("confidence", 0.5)
        context = context or {}

        # Grade assignment
        if base_score >= 0.95:
            grade = "excellent"
            recommended_threshold = 0.9
        elif base_score >= 0.85:
            grade = "good"
            recommended_threshold = 0.8
        elif base_score >= 0.7:
            grade = "fair"
            recommended_threshold = 0.65
        else:
            grade = "poor"
            recommended_threshold = 0.5

        # Environmental factors
        env_factors = {}

        # Factor 1: Match stability over time
        stability = self._calculate_stability(match_result)
        env_factors["stability"] = stability

        # Factor 2: Screen state consistency
        screen_locked = context.get("screen_locked", False)
        app_focused = context.get("app_focused", True)

        if screen_locked:
            env_factors["screen_locked"] = -0.2  # Penalty
        if not app_focused:
            env_factors["app_not_focused"] = -0.1  # Penalty

        # Factor 3: Historical reliability of this target
        target = match_result.get("target", "")
        historical_score = self._get_historical_score(target)
        env_factors["historical_reliability"] = historical_score

        # Calculate quality score
        quality_score = base_score
        quality_score += stability * 0.1
        quality_score += historical_score * 0.1
        quality_score = max(0.0, min(1.0, quality_score))

        # Adjust recommended threshold based on environment
        if screen_locked:
            recommended_threshold += 0.1
        if not app_focused:
            recommended_threshold += 0.05

        return ImageMatchQuality(
            match_score=base_score,
            quality_grade=grade,
            feature_stability=stability,
            environmental_factors=env_factors,
            recommended_confidence_threshold=recommended_threshold
        )

    def _calculate_stability(self, match_result: Dict) -> float:
        """Calculate match stability from repeated attempts"""
        target = match_result.get("target", "")

        recent_matches = [
            m for m in self.match_history[-10:]
            if m.get("target") == target
        ]

        if len(recent_matches) < 2:
            return 0.7  # Default moderate stability

        scores = [m.get("confidence", 0) for m in recent_matches]
        variance = sum((s - sum(scores)/len(scores))**2 for s in scores) / len(scores)

        # Low variance = high stability
        stability = 1.0 - min(1.0, variance * 10)
        return stability

    def _get_historical_score(self, target: str) -> float:
        """Get historical reliability score for target"""
        recent = [m for m in self.match_history[-20:] if m.get("target") == target]

        if not recent:
            return 0.5  # Default neutral

        success_rate = sum(1 for m in recent if m.get("result") == "success") / len(recent)
        return success_rate

    def record_match(self, target: str, confidence: float, result: str) -> None:
        """Record match result for future quality scoring"""
        self.match_history.append({
            "target": target,
            "confidence": confidence,
            "result": result,
            "timestamp": time.time()
        })

        if len(self.match_history) > 1000:
            self.match_history = self.match_history[-1000:]


# ============== User Behavior Learner ==============

class UserBehaviorLearner:
    """Learn user's typical workflows and preferences"""

    def __init__(self):
        self.workflow_sequences: Dict[str, List[str]] = defaultdict(list)
        self.preferred_actions: Dict[str, int] = defaultdict(int)
        self.context_preferences: Dict[str, Dict] = defaultdict(lambda: defaultdict(int))
        self.habit_strength: Dict[str, float] = {}
        self.learning_rate = 0.1

    def learn_from_action(self, action: UserAction) -> None:
        """Learn from a user action"""
        target = action.target
        action_type = action.action_type
        context = action.context

        # Update action frequency
        self.preferred_actions[f"{action_type}:{target}"] += 1

        # Update context preferences
        if "active_app" in context:
            self.context_preferences["active_app"][context["active_app"]] += 1

        # Update workflow sequences
        if len(self.workflow_sequences["current"]) > 0:
            self.workflow_sequences["current"].append(target)

        # Update habit strength
        self._update_habit_strength(target)

    def _update_habit_strength(self, target: str) -> None:
        """Update how strong a habit this action is"""
        current = self.habit_strength.get(target, 0.5)
        frequency = self.preferred_actions.get(target, 1)

        # More frequent = stronger habit, but with diminishing returns
        frequency_factor = min(1.0, frequency / 50)

        # Exponential moving average
        new_strength = current + self.learning_rate * (frequency_factor - current)
        self.habit_strength[target] = new_strength

    def get_habit_strength(self, target: str) -> float:
        """Get habit strength for a target (0-1)"""
        return self.habit_strength.get(target, 0.5)

    def predict_workflow_next(self, current_sequence: List[str]) -> Optional[Tuple[str, float]]:
        """Predict next step in workflow"""
        if len(current_sequence) < 2:
            return None

        # Build transition probabilities
        transitions = Counter()
        for i in range(len(current_sequence) - 1):
            key = f"{current_sequence[i]}->{current_sequence[i+1]}"
            transitions[key] += 1

        # Find most likely next action
        last_action = current_sequence[-1]
        candidates = []

        for (from_a, to_a), count in transitions.items():
            if from_a == last_action:
                candidates.append((to_a, count))

        if candidates:
            # Sort by frequency
            candidates.sort(key=lambda x: x[1], reverse=True)
            best = candidates[0]
            confidence = min(0.95, 0.5 + (best[1] / 10))
            return (best[0], confidence)

        return None

    def get_workflow_suggestion(self, current_context: Dict) -> Optional[str]:
        """Suggest a complete workflow based on context"""
        app = current_context.get("active_app", "")

        # Find workflows that start with this app
        app_workflows = [
            seq for seq in self.workflow_sequences.get("current", [])
            if seq and seq[0] == app
        ]

        if len(app_workflows) >= 3:
            # This app triggers workflows regularly
            return f"Consider automating: {app} workflow ({len(app_workflows)} times detected)"

        return None


# ============== Main Engine ==============

class PredictiveAutomationEngine:
    """预测性自动化引擎"""

    def __init__(self, data_dir: str = "./data", flow_engine_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.action_history: List[UserAction] = []
        self.patterns: List[ActionPattern] = []
        self.time_patterns: Dict[str, List[str]] = defaultdict(list)
        self.app_patterns: Dict[str, List[str]] = defaultdict(list)
        self.weekly_patterns: Dict[str, List[str]] = defaultdict(list)
        self.prediction_cache: Dict[str, Prediction] = {}
        self.last_prediction_time: float = 0

        # User correction learning
        self.user_corrections: Dict[str, int] = defaultdict(int)
        self.correction_history: List[Dict] = []

        # FlowEngine callback integration
        self.flow_engine_callback = flow_engine_callback
        self._ensure_data_dir()

        # Initialize ML components
        self.ml_learner = MLPatternLearner()
        self.anomaly_detector = None  # Initialized when needed
        self.performance_predictor = PerformancePredictor()
        self.confidence_engine = AdaptiveConfidenceEngine()
        self.failure_predictor = None  # Initialized when needed
        self.retry_scheduler = None  # Initialized when needed
        self.ocr_predictor = OCRQualityPredictor()
        self.image_scorer = ImageMatchQualityScorer()
        self.behavior_learner = UserBehaviorLearner()

        self._load_history()
        self._load_corrections()
        self._load_ml_models()

    def _load_history(self) -> None:
        """加载历史数据"""
        try:
            with open(f"{self.data_dir}/action_history.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    self.action_history.append(UserAction(**item))

            # Initialize detectors with history
            self.anomaly_detector = AnomalyDetector(self.action_history)
            self.failure_predictor = FailurePredictor(self.action_history)
            self.retry_scheduler = SmartRetryScheduler(self.action_history)

            # Learn from history
            for action in self.action_history[-1000:]:
                self.performance_predictor.record_duration(action.target, action.duration)
                success = action.result == "success"
                self.confidence_engine.record_result(action.target, success)
                self.ml_learner.learn_from_sequence(self.action_history[-10:])
                self.behavior_learner.learn_from_action(action)
        except FileNotFoundError:
            pass

    def _save_history(self) -> None:
        """保存历史数据"""
        data = []
        for action in self.action_history[-10000:]:
            data.append({
                "timestamp": action.timestamp,
                "action_type": action.action_type,
                "target": action.target,
                "context": action.context,
                "result": action.result,
                "duration": action.duration
            })
        with open(f"{self.data_dir}/action_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _ensure_data_dir(self) -> None:
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)

    def _load_corrections(self) -> None:
        """加载用户纠正数据"""
        try:
            with open(f"{self.data_dir}/user_corrections.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                self.user_corrections = defaultdict(int, data.get("corrections", {}))
                self.correction_history = data.get("history", [])
        except FileNotFoundError:
            pass

    def _save_corrections(self) -> None:
        """保存用户纠正数据"""
        data = {
            "corrections": dict(self.user_corrections),
            "history": self.correction_history[-1000:]
        }
        with open(f"{self.data_dir}/user_corrections.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_ml_models(self) -> None:
        """Load persisted ML models"""
        try:
            with open(f"{self.data_dir}/ml_models.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                self.ml_learner.action_embeddings = data.get("embeddings", {})
                self.ml_learner.sequence_models = defaultdict(dict, data.get("sequence_models", {}))
                self.behavior_learner.habit_strength = data.get("habit_strength", {})
        except FileNotFoundError:
            pass

    def _save_ml_models(self) -> None:
        """Persist ML models"""
        data = {
            "embeddings": self.ml_learner.action_embeddings,
            "sequence_models": dict(self.ml_learner.sequence_models),
            "habit_strength": self.behavior_learner.habit_strength
        }
        with open(f"{self.data_dir}/ml_models.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def record_user_correction(self, predicted_action: str, user_action: str,
                               context: Dict[str, Any] = None) -> None:
        """记录用户纠正，学习用户偏好"""
        if predicted_action != user_action:
            self.user_corrections[user_action] += 1
            self.correction_history.append({
                "timestamp": time.time(),
                "predicted": predicted_action,
                "actual": user_action,
                "context": context or {}
            })
            if len(self.correction_history) % 10 == 0:
                self._save_corrections()

    def get_action_confidence(self, action: str) -> float:
        """获取动作的置信度（基于用户纠正历史）"""
        total = sum(self.user_corrections.values())
        if total == 0:
            return 0.5

        correction_count = self.user_corrections.get(action, 0)
        confidence = max(0.1, 1.0 - (correction_count / max(total, 1)) * 0.8)
        return confidence

    # ============== Advanced Prediction Methods ==============

    def predict_next_action_advanced(self, current_context: Dict[str, Any] = None) -> Optional[Prediction]:
        """Advanced prediction with all ML enhancements"""
        # Check cache (5 minutes)
        if time.time() - self.last_prediction_time < 300 and self.prediction_cache:
            return list(self.prediction_cache.values())[0] if self.prediction_cache else None

        context = current_context or {}
        now = datetime.now()
        time_key = self._get_time_key(now)
        day_key = self._get_day_key(now)

        predictions = []

        # 1. ML-based sequence prediction
        recent_targets = [a.target for a in self.action_history[-5:]]
        seq_pred = self.ml_learner.predict_next_in_sequence(recent_targets, context)
        if seq_pred:
            predictions.append(Prediction(
                predicted_action=seq_pred[0],
                confidence=seq_pred[1],
                reasoning="Based on learned action sequence pattern",
                context_match={"pattern": "ml_sequence"}
            ))

        # 2. User behavior workflow prediction
        workflow_pred = self.behavior_learner.predict_workflow_next(recent_targets)
        if workflow_pred:
            predictions.append(Prediction(
                predicted_action=workflow_pred[0],
                confidence=workflow_pred[1] * 0.9,
                reasoning="Based on your typical workflow patterns",
                context_match={"pattern": "workflow"}
            ))

        # 3. Time-based prediction
        if time_key in self.time_patterns:
            recent = self.time_patterns[time_key][-10:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                predictions.append(Prediction(
                    predicted_action=most_common[0],
                    confidence=0.6,
                    reasoning=f"Based on {time_key} time patterns",
                    context_match={"pattern": "time_based"}
                ))

        # 4. Day-based prediction
        if day_key in self.weekly_patterns:
            recent = self.weekly_patterns[day_key][-10:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                predictions.append(Prediction(
                    predicted_action=most_common[0],
                    confidence=0.5,
                    reasoning=f"Based on {day_key} patterns",
                    context_match={"pattern": "day_based"}
                ))

        # 5. App context prediction
        active_app = context.get("active_app")
        if active_app and active_app in self.app_patterns:
            recent = self.app_patterns[active_app][-5:]
            if recent:
                most_common = Counter(recent).most_common(1)[0]
                predictions.append(Prediction(
                    predicted_action=most_common[0],
                    confidence=0.7,
                    reasoning=f"Common action when using {active_app}",
                    context_match={"pattern": "app_based", "app": active_app}
                ))

        # Sort and select best
        if predictions:
            predictions.sort(key=lambda p: p.confidence, reverse=True)
            best = predictions[0]

            # Apply adaptive confidence
            base_confidence = best.confidence
            adjusted_confidence = self.confidence_engine.get_adjusted_confidence(
                base_confidence, best.predicted_action
            )
            best.confidence = adjusted_confidence

            # Apply user correction factor
            action_confidence = self.get_action_confidence(best.predicted_action)
            best.confidence = best.confidence * 0.7 + action_confidence * 0.3

            # Add failure risk prediction
            temp_action = UserAction(
                timestamp=time.time(),
                action_type="unknown",
                target=best.predicted_action,
                context=context
            )
            best.failure_risk = self.failure_predictor.analyze_failure_risk(temp_action, context)

            # Add performance prediction
            predicted_duration, _ = self.performance_predictor.predict(best.predicted_action, context)
            best.predicted_duration = predicted_duration

            # Add quality score
            best.quality_score = self.behavior_learner.get_habit_strength(best.predicted_action)

            best.alternatives = [p.predicted_action for p in predictions[1:3]]

            # Cache result
            self.prediction_cache = {best.predicted_action: best}
            self.last_prediction_time = time.time()

            # Notify FlowEngine
            self._notify_flow_engine("prediction_made", {
                "predicted_action": best.predicted_action,
                "confidence": best.confidence,
                "failure_risk": best.failure_risk
            })

            return best

        return None

    def detect_anomalies(self, current_context: Dict = None) -> List[AnomalyReport]:
        """Detect anomalies in current state"""
        if not self.anomaly_detector or len(self.action_history) < 10:
            return []

        current_action = self.action_history[-1] if self.action_history else None
        if not current_action:
            return []

        context = current_context or current_action.context
        return self.anomaly_detector.detect_all(current_action, context)

    def get_failure_warning(self, action: str, context: Dict = None) -> Optional[str]:
        """Get warning message if action is likely to fail"""
        temp_action = UserAction(
            timestamp=time.time(),
            action_type="unknown",
            target=action,
            context=context or {}
        )
        risk = self.failure_predictor.analyze_failure_risk(temp_action, context)
        return self.failure_predictor.get_warning_message(temp_action, risk)

    def get_retry_schedule(self, failed_action: str, context: Dict = None) -> RetrySchedule:
        """Get optimal retry schedule for a failed action"""
        return self.retry_scheduler.get_retry_schedule(failed_action, context)

    def record_retry_outcome(self, action: str, delay: float, success: bool) -> None:
        """Record retry outcome for smart scheduling"""
        self.retry_scheduler.record_retry_outcome(action, delay, success)

    def predict_ocr_quality(self, image_hash: str, image_context: Dict = None) -> OCRQualityPrediction:
        """Predict OCR quality for an image"""
        return self.ocr_predictor.predict_quality(image_hash, image_context)

    def record_ocr_result(self, image_hash: str, actual_accuracy: float) -> None:
        """Record actual OCR result"""
        self.ocr_predictor.record_result(image_hash, actual_accuracy)

    def score_image_match(self, match_result: Dict, context: Dict = None) -> ImageMatchQuality:
        """Score an image match quality"""
        score = self.image_scorer.score_match(match_result, context)

        # Record for future scoring
        if context:
            self.image_scorer.record_match(
                match_result.get("target", ""),
                match_result.get("confidence", 0),
                "success" if match_result.get("confidence", 0) > 0.7 else "failure"
            )

        return score

    def get_execution_analysis(self) -> Dict[str, Any]:
        """Analyze execution history for optimization insights"""
        if len(self.action_history) < 10:
            return {"status": "insufficient_data"}

        recent = self.action_history[-100:]
        now = time.time()
        day_ago = now - 86400
        week_ago = now - 604800

        # Actions in different time windows
        day_actions = [a for a in recent if a.timestamp > day_ago]
        week_actions = [a for a in self.action_history if a.timestamp > week_ago]

        # Success rates
        recent_success_rate = sum(1 for a in recent if a.result == "success") / len(recent)
        day_success_rate = sum(1 for a in day_actions if a.result == "success") / len(day_actions) if day_actions else 0

        # Performance trends
        performance_trends = {}
        for target in set(a.target for a in recent[:20]):
            _, metrics = self.performance_predictor.predict(target)
            if metrics.sample_count > 0:
                performance_trends[target] = metrics.trend

        # Habit analysis
        habit_analysis = []
        for target, strength in self.behavior_learner.habit_strength.items():
            if strength > 0.7:
                habit_analysis.append({
                    "target": target,
                    "strength": strength,
                    "suggestion": "Could be automated"
                })

        return {
            "total_actions": len(self.action_history),
            "recent_success_rate": recent_success_rate,
            "day_success_rate": day_success_rate,
            "performance_trends": performance_trends,
            "habit_opportunities": habit_analysis[:5],
            "anomaly_count": len(self.detect_anomalies()),
            "most_reliable_actions": self._get_top_actions_by_reliability(5),
            "actions_needing_attention": self._get_actions_needing_attention(5)
        }

    def _get_top_actions_by_reliability(self, limit: int) -> List[Dict]:
        """Get most reliable actions"""
        action_rates = []
        for target in set(a.target for a in self.action_history[-500:]):
            rate = self.confidence_engine.get_success_rate(target)
            action_rates.append({"action": target, "success_rate": rate})

        action_rates.sort(key=lambda x: x["success_rate"], reverse=True)
        return action_rates[:limit]

    def _get_actions_needing_attention(self, limit: int) -> List[Dict]:
        """Get actions that may need optimization"""
        attention = []

        for target in set(a.target for a in self.action_history[-200:]):
            rate = self.confidence_engine.get_success_rate(target)
            habit = self.behavior_learner.get_habit_strength(target)

            if rate < 0.7 and habit > 0.3:
                attention.append({
                    "action": target,
                    "success_rate": rate,
                    "habit_strength": habit,
                    "issue": "Low success rate for frequent action"
                })

        attention.sort(key=lambda x: x["habit_strength"] * (1 - x["success_rate"]), reverse=True)
        return attention[:limit]

    # ============== Legacy Methods (kept for compatibility) ==============

    def record_action(self, action_type: str, target: str,
                     context: Dict[str, Any] = None,
                     result: str = "success",
                     duration: float = 0.0) -> None:
        """记录用户动作"""
        action = UserAction(
            timestamp=time.time(),
            action_type=action_type,
            target=target,
            context=context or {},
            result=result,
            duration=duration
        )
        self.action_history.append(action)

        # Update ML components
        self.performance_predictor.record_duration(target, duration)
        self.confidence_engine.record_result(target, result == "success")
        self.ml_learner.learn_from_sequence(self.action_history[-10:])
        self.behavior_learner.learn_from_action(action)

        # Update anomaly detector
        if self.anomaly_detector:
            self.anomaly_detector.history = self.action_history
        else:
            self.anomaly_detector = AnomalyDetector(self.action_history)

        # Update failure predictor
        if self.failure_predictor:
            self.failure_predictor.history = self.action_history
        else:
            self.failure_predictor = FailurePredictor(self.action_history)

        # Update retry scheduler
        if self.retry_scheduler:
            self.retry_scheduler.history = self.action_history
        else:
            self.retry_scheduler = SmartRetryScheduler(self.action_history)

        # Save periodically
        if len(self.action_history) % 100 == 0:
            self._save_history()
            self._save_ml_models()

        # Notify FlowEngine
        self._notify_flow_engine("action_recorded", {
            "action_type": action_type,
            "target": target,
            "result": result
        })

    def _update_patterns(self, action: UserAction) -> None:
        """更新动作模式"""
        dt = datetime.fromtimestamp(action.timestamp)
        time_key = self._get_time_key(dt)
        day_key = self._get_day_key(dt)

        self.time_patterns[time_key].append(action.target)
        self.weekly_patterns[day_key].append(action.target)

        if "active_app" in action.context:
            self.app_patterns[action.context["active_app"]].append(action.target)

    def _get_time_key(self, dt: datetime) -> str:
        """获取时间键"""
        hour = dt.hour
        if 6 <= hour < 9:
            return "morning_early"
        elif 9 <= hour < 12:
            return "morning"
        elif 12 <= hour < 14:
            return "noon"
        elif 14 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 21:
            return "evening"
        elif 21 <= hour < 24:
            return "night_late"
        else:
            return "night"

    def _get_day_key(self, dt: datetime) -> str:
        """获取星期键"""
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        return days[dt.weekday()]

    def predict_next_action(self, current_context: Dict[str, Any] = None) -> Optional[Prediction]:
        """Predict next action (legacy wrapper for advanced method)"""
        return self.predict_next_action_advanced(current_context)

    def _find_sequence_next(self, pattern: List[str], last_action: str) -> List[str]:
        """查找序列模式后的下一个动作"""
        if len(self.action_history) < 10:
            return []

        history_targets = [a.target for a in self.action_history[-1000:]]

        for i in range(len(history_targets) - len(pattern) - 1):
            if history_targets[i:i+len(pattern)] == pattern:
                next_action = history_targets[i+len(pattern)]
                return [next_action]

        return []

    def suggest_workflow_creation(self) -> Optional[str]:
        """建议创建自动化工作流"""
        if len(self.action_history) < 20:
            return None

        recent_targets = [a.target for a in self.action_history[-50:]]
        target_counts = Counter(recent_targets)

        repeated = [t for t, count in target_counts.items() if count >= 5]

        if repeated:
            most_frequent = target_counts.most_common(1)[0]
            return f"检测到您经常执行「{most_frequent[0]}」，建议创建自动化工作流"

        return None

    def get_time_based_suggestions(self) -> List[Dict[str, Any]]:
        """获取基于时间的建议"""
        now = datetime.now()
        time_key = self._get_time_key(now)
        day_key = self._get_day_key(now)

        suggestions = []

        if day_key == "monday" and time_key in ["morning", "morning_early"]:
            suggestions.append({
                "type": "workflow",
                "title": "周一早上工作准备",
                "description": "一键打开工作文档、邮件、通讯工具",
                "workflow": "monday_morning_routine"
            })

        if time_key == "afternoon":
            suggestions.append({
                "type": "workflow",
                "title": "下午茶时间",
                "description": "自动整理上午工作、准备下午任务",
                "workflow": "afternoon_tea_routine"
            })

        if time_key == "evening":
            suggestions.append({
                "type": "workflow",
                "title": "下班Shutdown",
                "description": "自动保存文件、清理桌面、锁屏",
                "workflow": "shutdown_routine"
            })

        return suggestions

    def analyze_user_behavior(self) -> Dict[str, Any]:
        """分析用户行为"""
        if not self.action_history:
            return {"status": "no_data"}

        recent = self.action_history[-100:]

        action_types = Counter([a.action_type for a in recent])
        targets = Counter([a.target for a in recent])
        avg_duration = sum(a.duration for a in recent) / len(recent) if recent else 0
        success_count = sum(1 for a in recent if a.result == "success")
        success_rate = success_count / len(recent) if recent else 0

        return {
            "total_actions": len(self.action_history),
            "recent_actions": len(recent),
            "action_type_distribution": dict(action_types),
            "top_targets": dict(targets.most_common(10)),
            "avg_duration": round(avg_duration, 2),
            "success_rate": round(success_rate, 2),
            "time_patterns": {k: len(v) for k, v in self.time_patterns.items()},
            "app_patterns": {k: len(v) for k, v in self.app_patterns.items()}
        }

    def enable_predictive_execution(self, workflow_name: str) -> bool:
        """启用预测性执行"""
        prediction = self.predict_next_action()
        if prediction and prediction.confidence > 0.7:
            return True
        return False

    def get_prediction_for_workflow(self, workflow_name: str) -> Optional[Prediction]:
        """获取特定工作流的预测信息"""
        return self.prediction_cache.get(workflow_name)

    def export_learned_patterns(self, filepath: str = None) -> str:
        """导出学习到的模式"""
        if filepath is None:
            filepath = os.path.join(self.data_dir, "patterns_export.json")

        export_data = {
            "version": "2.0",
            "export_time": time.time(),
            "time_patterns": dict(self.time_patterns),
            "app_patterns": dict(self.app_patterns),
            "weekly_patterns": dict(self.weekly_patterns),
            "user_corrections": dict(self.user_corrections),
            "action_history_count": len(self.action_history),
            "ml_sequence_models": {k: v for k, v in self.ml_learner.sequence_models.items()},
            "habit_strengths": self.behavior_learner.habit_strength
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)

        return filepath

    def import_learned_patterns(self, filepath: str) -> bool:
        """导入学习到的模式"""
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.time_patterns = defaultdict(list, data.get("time_patterns", {}))
            self.app_patterns = defaultdict(list, data.get("app_patterns", {}))
            self.weekly_patterns = defaultdict(list, data.get("weekly_patterns", {}))
            self.user_corrections = defaultdict(int, data.get("user_corrections", {}))
            self.ml_learner.sequence_models = defaultdict(dict, data.get("ml_sequence_models", {}))
            self.behavior_learner.habit_strength = data.get("habit_strengths", {})

            self._save_corrections()
            self._save_ml_models()
            return True
        except Exception as e:
            print(f"Import failed: {e}")
            return False

    def set_flow_engine_callback(self, callback: Callable) -> None:
        """设置 FlowEngine 回调函数"""
        self.flow_engine_callback = callback

    def _notify_flow_engine(self, event_type: str, data: Dict) -> None:
        """通知 FlowEngine 事件"""
        if self.flow_engine_callback:
            try:
                self.flow_engine_callback(event_type, data)
            except Exception:
                pass


def create_predictive_engine(data_dir: str = "./data") -> PredictiveAutomationEngine:
    """创建预测性自动化引擎实例"""
    return PredictiveAutomationEngine(data_dir)


# Test
if __name__ == "__main__":
    engine = create_predictive_engine("./data")

    # Simulate recording some actions
    engine.record_action("app_launch", "Chrome", {"active_app": None}, "success", 2.5)
    engine.record_action("click", "Email button", {"active_app": "Chrome"}, "success", 0.3)
    engine.record_action("workflow_trigger", "Read emails", {"active_app": "Mail"}, "success", 1.2)

    # Advanced prediction
    prediction = engine.predict_next_action_advanced({"active_app": "Mail"})
    if prediction:
        print("=== Advanced Prediction ===")
        print(f"Predicted action: {prediction.predicted_action}")
        print(f"Confidence: {prediction.confidence:.2%}")
        print(f"Failure risk: {prediction.failure_risk:.2%}")
        print(f"Predicted duration: {prediction.predicted_duration:.1f}s")
        print(f"Reasoning: {prediction.reasoning}")

    # Anomaly detection
    anomalies = engine.detect_anomalies()
    if anomalies:
        print("\n=== Anomalies Detected ===")
        for a in anomalies:
            print(f"[{a.severity}] {a.description}")

    # Execution analysis
    analysis = engine.get_execution_analysis()
    print("\n=== Execution Analysis ===")
    print(f"Total actions: {analysis.get('total_actions', 0)}")
    print(f"Recent success rate: {analysis.get('recent_success_rate', 0):.1%}")

    # Behavior analysis
    behavior = engine.analyze_user_behavior()
    print("\n=== Behavior Analysis ===")
    print(f"Total actions: {behavior.get('total_actions', 0)}")
    print(f"Success rate: {behavior.get('success_rate', 0):.1%}")
