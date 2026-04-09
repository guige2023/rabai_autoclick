"""Intent analysis for automation workflows.

Analyzes user intent and action context to intelligently route
and adapt automation execution paths.
"""

from __future__ import annotations

import difflib
import re
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class IntentConfidence(Enum):
    """Confidence level of intent detection."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class IntentCategory(Enum):
    """Broad categories of user intent."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    SEARCH = "search"
    ANALYZE = "analyze"
    AUTOMATE = "automate"
    ORCHESTRATE = "orchestrate"
    MONITOR = "monitor"
    CONFIGURE = "configure"
    REPORT = "report"
    NOTIFY = "notify"
    IMPORT = "import"
    EXPORT = "export"
    VALIDATE = "validate"
    TRANSFORM = "transform"


@dataclass
class Intent:
    """A detected user intent."""
    intent_id: str
    category: IntentCategory
    confidence: IntentConfidence
    entities: Dict[str, Any] = field(default_factory=dict)
    raw_query: str = ""
    tokens: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    alternatives: List[IntentCategory] = field(default_factory=list)


@dataclass
class IntentPattern:
    """A pattern for recognizing intent."""
    pattern_id: str
    category: IntentCategory
    keywords: List[str] = field(default_factory=list)
    regex_patterns: List[str] = field(default_factory=list)
    phrases: List[str] = field(default_factory=list)
    action_verbs: List[str] = field(default_factory=list)
    priority: int = 50
    enabled: bool = True


@dataclass
class IntentMatch:
    """A match of an intent pattern against input."""
    pattern_id: str
    category: IntentCategory
    match_score: float
    matched_tokens: List[str] = field(default_factory=list)
    match_type: str = "keyword"


class IntentAnalyzer:
    """Analyzes and classifies user intent from text input."""

    def __init__(self):
        self._patterns: Dict[str, IntentPattern] = {}
        self._entity_extractors: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._history: List[Intent] = []
        self._max_history = 1000
        self._setup_default_patterns()

    def _setup_default_patterns(self) -> None:
        """Set up default intent recognition patterns."""
        default_patterns = [
            IntentPattern(
                pattern_id="create_pattern",
                category=IntentCategory.CREATE,
                keywords=["create", "add", "new", "make", "generate", "build", "insert"],
                phrases=["create a", "add a new", "make a", "build a new", "generate a"],
                action_verbs=["create", "add", "make", "generate", "build"],
                priority=40,
            ),
            IntentPattern(
                pattern_id="read_pattern",
                category=IntentCategory.READ,
                keywords=["get", "fetch", "retrieve", "show", "display", "view", "list", "find"],
                phrases=["get the", "show me", "display the", "view the", "list all", "find the"],
                action_verbs=["get", "fetch", "retrieve", "show", "display", "view", "list"],
                priority=35,
            ),
            IntentPattern(
                pattern_id="update_pattern",
                category=IntentCategory.UPDATE,
                keywords=["update", "modify", "edit", "change", "set", "replace", "alter"],
                phrases=["update the", "modify the", "edit the", "change the", "set the"],
                action_verbs=["update", "modify", "edit", "change", "set", "alter"],
                priority=42,
            ),
            IntentPattern(
                pattern_id="delete_pattern",
                category=IntentCategory.DELETE,
                keywords=["delete", "remove", "drop", "clear", "purge", "destroy"],
                phrases=["delete the", "remove the", "drop the", "clear the"],
                action_verbs=["delete", "remove", "drop", "clear", "purge"],
                priority=45,
            ),
            IntentPattern(
                pattern_id="search_pattern",
                category=IntentCategory.SEARCH,
                keywords=["search", "find", "lookup", "query", "filter", "locate"],
                phrases=["search for", "find all", "search in", "query the"],
                action_verbs=["search", "find", "lookup", "query"],
                priority=38,
            ),
            IntentPattern(
                pattern_id="analyze_pattern",
                category=IntentCategory.ANALYZE,
                keywords=["analyze", "examine", "inspect", "review", "assess", "evaluate", "audit"],
                phrases=["analyze the", "examine the", "look at the", "review the"],
                action_verbs=["analyze", "examine", "inspect", "review", "assess"],
                priority=40,
            ),
            IntentPattern(
                pattern_id="automate_pattern",
                category=IntentCategory.AUTOMATE,
                keywords=["automate", "schedule", "run", "execute", "trigger", "start", "stop"],
                phrases=["automate the", "schedule the", "run the", "execute the", "trigger the"],
                action_verbs=["automate", "schedule", "run", "execute", "trigger"],
                priority=43,
            ),
            IntentPattern(
                pattern_id="orchestrate_pattern",
                category=IntentCategory.ORCHESTRATE,
                keywords=["orchestrate", "coordinate", "sequence", "pipeline", "chain", "compose"],
                phrases=["orchestrate the", "coordinate the", "chain the", "pipeline the"],
                action_verbs=["orchestrate", "coordinate", "sequence", "chain"],
                priority=44,
            ),
            IntentPattern(
                pattern_id="monitor_pattern",
                category=IntentCategory.MONITOR,
                keywords=["monitor", "watch", "track", "observe", "check", "measure"],
                phrases=["monitor the", "watch the", "track the", "check the"],
                action_verbs=["monitor", "watch", "track", "observe", "check"],
                priority=39,
            ),
            IntentPattern(
                pattern_id="configure_pattern",
                category=IntentCategory.CONFIGURE,
                keywords=["configure", "setup", "initialize", "setup", "install", "deploy"],
                phrases=["configure the", "set up the", "initialize the", "setup the"],
                action_verbs=["configure", "setup", "initialize", "install", "deploy"],
                priority=41,
            ),
            IntentPattern(
                pattern_id="report_pattern",
                category=IntentCategory.REPORT,
                keywords=["report", "summarize", "aggregate", "count", "total", "calculate"],
                phrases=["report on", "summarize the", "count the", "aggregate the"],
                action_verbs=["report", "summarize", "aggregate", "count"],
                priority=36,
            ),
            IntentPattern(
                pattern_id="notify_pattern",
                category=IntentCategory.NOTIFY,
                keywords=["notify", "alert", "send", "message", "email", "publish", "broadcast"],
                phrases=["notify the", "alert the", "send a", "publish the"],
                action_verbs=["notify", "alert", "send", "message", "publish"],
                priority=37,
            ),
            IntentPattern(
                pattern_id="validate_pattern",
                category=IntentCategory.VALIDATE,
                keywords=["validate", "verify", "check", "confirm", "test", "ensure"],
                phrases=["validate the", "verify the", "check the", "confirm the"],
                action_verbs=["validate", "verify", "check", "confirm", "test"],
                priority=38,
            ),
            IntentPattern(
                pattern_id="transform_pattern",
                category=IntentCategory.TRANSFORM,
                keywords=["transform", "convert", "parse", "format", "encode", "decode", "map"],
                phrases=["transform the", "convert the", "parse the", "format the"],
                action_verbs=["transform", "convert", "parse", "format", "encode", "decode"],
                priority=40,
            ),
        ]

        for pattern in default_patterns:
            self._patterns[pattern.pattern_id] = pattern

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize input text."""
        text = text.lower()
        tokens = re.findall(r'\b\w+\b', text)
        return tokens

    def _calculate_similarity(
        self,
        query_tokens: List[str],
        pattern_tokens: List[str],
    ) -> float:
        """Calculate similarity between query and pattern tokens."""
        if not pattern_tokens:
            return 0.0

        matches = 0
        for qt in query_tokens:
            best_match = 0.0
            for pt in pattern_tokens:
                ratio = difflib.SequenceMatcher(None, qt, pt).ratio()
                if ratio > best_match:
                    best_match = ratio
            if best_match > 0.8:
                matches += best_match

        return matches / len(pattern_tokens)

    def _match_pattern(
        self,
        query: str,
        tokens: List[str],
        pattern: IntentPattern,
    ) -> Optional[IntentMatch]:
        """Match a pattern against query."""
        matched_tokens = []
        match_score = 0.0
        match_type = "none"

        for keyword in pattern.keywords:
            keyword_lower = keyword.lower()
            for token in tokens:
                if token == keyword_lower:
                    matched_tokens.append(token)
                    match_score += 1.0
                    match_type = "keyword"

        for phrase in pattern.phrases:
            phrase_lower = phrase.lower()
            if phrase_lower in query.lower():
                matched_tokens.append(phrase)
                match_score += 2.0
                match_type = "phrase"

        for action_verb in pattern.action_verbs:
            action_lower = action_verb.lower()
            for token in tokens:
                if token == action_lower:
                    matched_tokens.append(token)
                    match_score += 1.5
                    match_type = "action_verb"

        for regex_pat in pattern.regex_patterns:
            try:
                regex = re.compile(regex_pat, re.IGNORECASE)
                if regex.search(query):
                    match_score += 2.0
                    match_type = "regex"
            except Exception:
                pass

        if match_score > 0:
            normalized_score = min(match_score / (len(pattern.keywords) + len(pattern.phrases) + 1), 1.0)
            return IntentMatch(
                pattern_id=pattern.pattern_id,
                category=pattern.category,
                match_score=normalized_score,
                matched_tokens=matched_tokens,
                match_type=match_type,
            )

        return None

    def analyze(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        threshold: float = 0.3,
    ) -> Intent:
        """Analyze a query to detect user intent."""
        tokens = self._tokenize(query)
        matches: List[IntentMatch] = []

        context_intent = None
        if context and "last_intent" in context:
            context_intent = context["last_intent"]

        for pattern in self._patterns.values():
            if not pattern.enabled:
                continue

            match = self._match_pattern(query, tokens, pattern)
            if match and match.match_score >= threshold:
                matches.append(match)

        matches.sort(key=lambda m: m.match_score, reverse=True)

        if not matches:
            return Intent(
                intent_id=str(uuid.uuid4())[:12],
                category=IntentCategory.READ,
                confidence=IntentConfidence.UNKNOWN,
                raw_query=query,
                tokens=tokens,
            )

        best_match = matches[0]
        alternatives = [m.category for m in matches[1:4]]

        if best_match.match_score >= 0.7:
            confidence = IntentConfidence.HIGH
        elif best_match.match_score >= 0.5:
            confidence = IntentConfidence.MEDIUM
        else:
            confidence = IntentConfidence.LOW

        if context_intent and best_match.match_score < 0.6:
            if context_intent in [m.category for m in matches[:3]]:
                confidence = IntentConfidence.MEDIUM

        entities = self._extract_entities(query, tokens, best_match.category)

        intent = Intent(
            intent_id=str(uuid.uuid4())[:12],
            category=best_match.category,
            confidence=confidence,
            entities=entities,
            raw_query=query,
            tokens=tokens,
            alternatives=alternatives,
        )

        with self._lock:
            self._history.append(intent)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

        return intent

    def _extract_entities(
        self,
        query: str,
        tokens: List[str],
        category: IntentCategory,
    ) -> Dict[str, Any]:
        """Extract entities from query based on category."""
        entities: Dict[str, Any] = {}

        number_match = re.search(r'\d+', query)
        if number_match:
            entities["numeric_value"] = int(number_match.group())

        path_patterns = [
            r'[/\\][\w\-\./]+',
            r'[\w\-\.]+\.(py|js|ts|json|yaml|yml|xml|csv|txt|md)',
        ]
        for pat in path_patterns:
            matches = re.findall(pat, query)
            if matches:
                entities["paths"] = matches

        quoted_match = re.findall(r'["\']([^"\']+)["\']', query)
        if quoted_match:
            entities["quoted_strings"] = quoted_matches = quoted_match

        return entities

    def register_pattern(self, pattern: IntentPattern) -> None:
        """Register a new intent pattern."""
        with self._lock:
            self._patterns[pattern.pattern_id] = pattern

    def get_history(
        self,
        limit: int = 100,
        category: Optional[IntentCategory] = None,
    ) -> List[Intent]:
        """Get intent history."""
        with self._lock:
            history = list(reversed(self._history))
            if category:
                history = [i for i in history if i.category == category]
            return history[:limit]


class AutomationIntentAction:
    """Action providing intent analysis for automation workflows."""

    def __init__(self, analyzer: Optional[IntentAnalyzer] = None):
        self._analyzer = analyzer or IntentAnalyzer()

    def analyze(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        threshold: float = 0.3,
    ) -> Dict[str, Any]:
        """Analyze user intent from a query."""
        intent = self._analyzer.analyze(query, context, threshold)

        return {
            "intent_id": intent.intent_id,
            "category": intent.category.value,
            "confidence": intent.confidence.value,
            "entities": intent.entities,
            "raw_query": intent.raw_query,
            "tokens": intent.tokens,
            "alternatives": [a.value for a in intent.alternatives],
            "created_at": datetime.fromtimestamp(intent.created_at).isoformat(),
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute intent analysis.

        Required params:
            query: str - The text to analyze for intent
            intent_mapping: dict - Map of intent categories to operations

        Optional params:
            threshold: float - Minimum confidence threshold (default 0.3)
            fallback: str - Fallback intent if analysis fails
        """
        query = params.get("query")
        intent_mapping = params.get("intent_mapping", {})
        threshold = params.get("threshold", 0.3)
        fallback = params.get("fallback", "read")
        operation = params.get("operation")

        if not query:
            raise ValueError("query is required")

        intent_result = self.analyze(query, context, threshold)

        if callable(operation):
            operation_result = operation(
                intent=intent_result,
                context=context,
                params=params,
            )
            intent_result["operation_result"] = operation_result

        elif intent_mapping:
            category = intent_result["category"]
            if category in intent_mapping:
                mapped_operation = intent_mapping[category]
                if callable(mapped_operation):
                    intent_result["operation_result"] = mapped_operation(
                        intent=intent_result,
                        context=context,
                        params=params,
                    )
                else:
                    intent_result["fallback_used"] = True
                    intent_result["mapped_to"] = mapped_operation

        return intent_result

    def add_pattern(
        self,
        pattern_id: str,
        category: str,
        keywords: Optional[List[str]] = None,
        phrases: Optional[List[str]] = None,
        priority: int = 50,
    ) -> Dict[str, Any]:
        """Add a custom intent pattern."""
        try:
            category_enum = IntentCategory(category.lower())
        except ValueError:
            return {"success": False, "error": f"Invalid category: {category}"}

        pattern = IntentPattern(
            pattern_id=pattern_id,
            category=category_enum,
            keywords=keywords or [],
            phrases=phrases or [],
            priority=priority,
        )

        self._analyzer.register_pattern(pattern)

        return {
            "success": True,
            "pattern_id": pattern_id,
            "category": category,
        }

    def get_intent_distribution(self) -> Dict[str, int]:
        """Get distribution of intents from history."""
        history = self._analyzer.get_history(limit=1000)
        distribution = defaultdict(int)
        for intent in history:
            distribution[intent.category.value] += 1
        return dict(distribution)
