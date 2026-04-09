"""
Automation Intent Detection Action Module

Detects user intent from UI interaction patterns and
routes to appropriate automation handlers.

Author: RabAi Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Pattern

import logging

logger = logging.getLogger(__name__)


class IntentType(Enum):
    """Known intent types for UI automation."""

    NAVIGATE = auto()
    CLICK = auto()
    INPUT = auto()
    SUBMIT = auto()
    SEARCH = auto()
    FILTER = auto()
    SORT = auto()
    EXPORT = auto()
    REFRESH = auto()
    CANCEL = auto()
    BACK = auto()
    SELECT = auto()
    SCROLL = auto()
    ZOOM = auto()
    DRAG = auto()
    HOVER = auto()
    UNKNOWN = auto()


@dataclass
class IntentPattern:
    """A pattern for detecting user intent."""

    intent_type: IntentType
    name: str
    trigger_patterns: List[str]
    confidence_boost: float = 0.0
    priority: int = 0

    def __post_init__(self) -> None:
        self._compiled: List[Pattern] = []
        for p in self.trigger_patterns:
            import re
            self._compiled.append(re.compile(p, re.IGNORECASE))


@dataclass
class DetectedIntent:
    """Result of intent detection."""

    intent_type: IntentType
    confidence: float
    matched_pattern: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class IntentRoute:
    """Route mapping from intent to handler."""

    intent_type: IntentType
    handler: Callable[[DetectedIntent], Any]
    fallback_handler: Optional[Callable[[DetectedIntent], Any]] = None
    required_confidence: float = 0.5


@dataclass
class IntentSequence:
    """A sequence of detected intents forming a flow."""

    sequence_id: str
    intents: List[DetectedIntent]
    start_time: float
    end_time: Optional[float] = None

    def add_intent(self, intent: DetectedIntent) -> None:
        self.intents.append(intent)


class IntentDetector:
    """Detects user intent from UI interaction data."""

    DEFAULT_PATTERNS = [
        IntentPattern(
            IntentType.NAVIGATE,
            "navigation",
            [r"go to", r"navigate to", r"open", r"visit", r"前往", r"导航"],
            priority=10,
        ),
        IntentPattern(
            IntentType.CLICK,
            "click",
            [r"click", r"press", r"tap", r"选择", r"点击", r"按钮"],
            priority=5,
        ),
        IntentPattern(
            IntentType.INPUT,
            "input",
            [r"type", r"enter", r"input", r"fill", r"write", r"输入", r"填写"],
            priority=5,
        ),
        IntentPattern(
            IntentType.SUBMIT,
            "submit",
            [r"submit", r"confirm", r"save", r"apply", r"提交", r"确认", r"保存"],
            priority=8,
        ),
        IntentPattern(
            IntentType.SEARCH,
            "search",
            [r"search", r"find", r"lookup", r"query", r"搜索", r"查找"],
            priority=7,
        ),
        IntentPattern(
            IntentType.FILTER,
            "filter",
            [r"filter", r"筛选", r"过滤", r"条件"],
            priority=6,
        ),
        IntentPattern(
            IntentType.SORT,
            "sort",
            [r"sort by", r"order by", r"排列", r"排序"],
            priority=6,
        ),
        IntentPattern(
            IntentType.EXPORT,
            "export",
            [r"export", r"download", r"导出", r"下载"],
            priority=7,
        ),
        IntentPattern(
            IntentType.REFRESH,
            "refresh",
            [r"refresh", r"reload", r"刷新", r"重载"],
            priority=4,
        ),
        IntentPattern(
            IntentType.CANCEL,
            "cancel",
            [r"cancel", r"abort", r"stop", r"取消", r"中止"],
            priority=8,
        ),
        IntentPattern(
            IntentType.BACK,
            "back",
            [r"go back", r"return", r"返回", r"后退"],
            priority=7,
        ),
        IntentPattern(
            IntentType.SCROLL,
            "scroll",
            [r"scroll", r"滚动"],
            priority=3,
        ),
    ]

    def __init__(self, custom_patterns: Optional[List[IntentPattern]] = None) -> None:
        self.patterns = custom_patterns or self.DEFAULT_PATTERNS
        self.patterns.sort(key=lambda p: p.priority, reverse=True)

    def detect(self, interaction_text: str, context: Optional[Dict[str, Any]] = None) -> DetectedIntent:
        """Detect intent from interaction text."""
        import re

        best_match: Optional[IntentPattern] = None
        best_confidence = 0.0
        best_pattern: Optional[str] = None

        for pattern in self.patterns:
            for compiled in pattern._compiled:
                if compiled.search(interaction_text):
                    confidence = 0.5 + pattern.confidence_boost
                    if best_match is None or confidence > best_confidence:
                        best_match = pattern
                        best_confidence = confidence
                        best_pattern = compiled.pattern
                    break

        if best_match is None:
            return DetectedIntent(
                intent_type=IntentType.UNKNOWN,
                confidence=0.0,
                context=context or {},
            )

        return DetectedIntent(
            intent_type=best_match.intent_type,
            confidence=min(best_confidence, 1.0),
            matched_pattern=best_pattern,
            context=context or {},
        )


class IntentRoutingAction:
    """Action class for intent-based automation routing."""

    def __init__(self, detector: Optional[IntentDetector] = None) -> None:
        self.detector = detector or IntentDetector()
        self._routes: Dict[IntentType, IntentRoute] = {}
        self._sequences: List[IntentSequence] = []
        self._current_sequence: Optional[IntentSequence] = None

    def register_route(
        self,
        intent_type: IntentType,
        handler: Callable[[DetectedIntent], Any],
        fallback_handler: Optional[Callable[[DetectedIntent], Any]] = None,
        required_confidence: float = 0.5,
    ) -> None:
        """Register a handler for an intent type."""
        self._routes[intent_type] = IntentRoute(
            intent_type=intent_type,
            handler=handler,
            fallback_handler=fallback_handler,
            required_confidence=required_confidence,
        )

    def start_sequence(self) -> None:
        """Start tracking a new intent sequence."""
        import uuid
        self._current_sequence = IntentSequence(
            sequence_id=str(uuid.uuid4()),
            intents=[],
            start_time=time.time(),
        )

    def detect_and_route(
        self,
        interaction_text: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Detect intent and route to appropriate handler."""
        intent = self.detector.detect(interaction_text, context)

        if self._current_sequence:
            self._current_sequence.add_intent(intent)

        route = self._routes.get(intent.intent_type)
        if route is None:
            logger.warning(f"No route registered for intent: {intent.intent_type.name}")
            return None

        if intent.confidence < route.required_confidence:
            if route.fallback_handler:
                return route.fallback_handler(intent)
            return None

        return route.handler(intent)

    def end_sequence(self) -> Optional[IntentSequence]:
        """End current sequence and return it."""
        if self._current_sequence:
            self._current_sequence.end_time = time.time()
            seq = self._current_sequence
            self._sequences.append(seq)
            self._current_sequence = None
            return seq
        return None

    def get_sequences(self) -> List[IntentSequence]:
        """Return all recorded intent sequences."""
        return self._sequences.copy()
