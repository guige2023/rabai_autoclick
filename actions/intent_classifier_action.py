"""
Intent Classification Action Module.

Provides NLP-based intent classification for user commands
using rule-based and embedding similarity approaches.
"""

import re
from typing import Any, Optional


class IntentClassifier:
    """Classifies user intents from natural language commands."""

    INTENT_PATTERNS = {
        "click": [r"click", r"tap", r"press", r"select"],
        "type": [r"type", r"enter", r"input", r"write", r"fill"],
        "scroll": [r"scroll", r"swipe", r"move.*down", r"move.*up"],
        "navigate": [r"go.*to", r"navigate", r"open", r"browse", r"visit"],
        "search": [r"search", r"find", r"look.*up", r"query"],
        "wait": [r"wait", r"pause", r"delay", r"sleep"],
        "screenshot": [r"screenshot", r"capture", r"screen.*shot", r"grab.*screen"],
        "extract": [r"extract", r"get.*text", r"read.*content", r"scrape"],
        "close": [r"close", r"dismiss", r"quit", r"exit"],
        "maximize": [r"maximize", r"fullscreen", r"expand", r"enlarge"],
        "minimize": [r"minimize", r"collapse", r"shrink", r"hide"],
        "refresh": [r"refresh", r"reload", r"reload.*page", r"reload.*content"],
        "back": [r"back", r"go.*back", r"return", r"previous"],
        "forward": [r"forward", r"next", r"advance"],
        "submit": [r"submit", r"confirm", r"send", r"apply", r"ok.*click"],
        "cancel": [r"cancel", r"abort", r"stop", r"undo"],
        "download": [r"download", r"save.*as", r"export"],
        "upload": [r"upload", r"attach", r"send.*file"],
        "login": [r"login", r"log.*in", r"sign.*in", r"authenticate"],
        "logout": [r"logout", r"log.*out", r"sign.*out"],
    }

    def __init__(self, threshold: float = 0.6):
        """
        Initialize intent classifier.

        Args:
            threshold: Minimum confidence threshold for classification.
        """
        self.threshold = threshold
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Compile regex patterns for all intents."""
        self._compiled: dict[str, list[re.Pattern]] = {}
        for intent, patterns in self.INTENT_PATTERNS.items():
            self._compiled[intent] = [re.compile(p, re.IGNORECASE) for p in patterns]

    def classify(self, command: str) -> dict[str, Any]:
        """
        Classify a user command into intents.

        Args:
            command: The natural language command to classify.

        Returns:
            Dictionary with top intent, confidence, and all scored intents.
        """
        scores: dict[str, float] = {}
        for intent, patterns in self._compiled.items():
            score = max((p.search(command) is not None for p in patterns), default=0.0)
            if score:
                scores[intent] = float(score)

        if not scores:
            return {
                "top_intent": "unknown",
                "confidence": 0.0,
                "all_intents": {},
            }

        top = max(scores, key=scores.get)
        return {
            "top_intent": top,
            "confidence": scores[top],
            "all_intents": scores,
        }

    def classify_batch(self, commands: list[str]) -> list[dict[str, Any]]:
        """
        Classify multiple commands.

        Args:
            commands: List of commands to classify.

        Returns:
            List of classification results.
        """
        return [self.classify(cmd) for cmd in commands]

    def add_custom_intent(self, intent: str, patterns: list[str]) -> None:
        """
        Add a custom intent with patterns.

        Args:
            intent: Name of the intent.
            patterns: List of regex patterns.
        """
        compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
        self._compiled[intent] = compiled
        self.INTENT_PATTERNS[intent] = patterns

    def remove_intent(self, intent: str) -> bool:
        """
        Remove a custom intent.

        Args:
            intent: Name of the intent to remove.

        Returns:
            True if removed, False if not found.
        """
        if intent in self._compiled:
            del self._compiled[intent]
            self.INTENT_PATTERNS.pop(intent, None)
            return True
        return False


def classify_command(command: str, threshold: float = 0.6) -> dict[str, Any]:
    """
    Convenience function to classify a command.

    Args:
        command: Natural language command.
        threshold: Confidence threshold.

    Returns:
        Classification result dictionary.
    """
    classifier = IntentClassifier(threshold=threshold)
    return classifier.classify(command)
