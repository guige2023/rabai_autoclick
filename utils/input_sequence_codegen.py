"""
Input sequence code generator for replay automation.

Converts recorded input sequences into executable
Python/playwright code.

Author: AutoClick Team
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


class ActionType(Enum):
    """Types of input actions."""

    CLICK = auto()
    DOUBLE_CLICK = auto()
    RIGHT_CLICK = auto()
    HOVER = auto()
    TYPE = auto()
    PRESS = auto()
    SCROLL = auto()
    DRAG = auto()
    WAIT = auto()


@dataclass
class InputAction:
    """Represents a single input action."""

    action_type: ActionType
    x: float | None = None
    y: float | None = None
    text: str | None = None
    key: str | None = None
    delta_x: int | None = None
    delta_y: int | None = None
    duration_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SequenceMetadata:
    """Metadata for a recorded sequence."""

    name: str = "RecordedSequence"
    target_app: str | None = None
    recorded_at: str | None = None
    author: str | None = None
    description: str | None = None


class InputSequenceCodeGenerator:
    """
    Generates executable code from input sequences.

    Converts recorded actions into Python/playwright
    or vanilla Python automation code.

    Example:
        generator = InputSequenceCodeGenerator()
        code = generator.generate_python(actions)
        exec(code)
    """

    def __init__(
        self,
        use_playwright: bool = True,
        indent_size: int = 4,
    ) -> None:
        """
        Initialize code generator.

        Args:
            use_playwright: Generate playwright code vs vanilla
            indent_size: Spaces per indent level
        """
        self._use_playwright = use_playwright
        self._indent = " " * indent_size

    def generate_python(
        self,
        actions: list[InputAction],
        metadata: SequenceMetadata | None = None,
    ) -> str:
        """
        Generate Python code from input actions.

        Args:
            actions: List of input actions to convert
            metadata: Optional sequence metadata

        Returns:
            Python code as string
        """
        lines = [
            "#!/usr/bin/env python3",
            f'"""Auto-generated from {metadata.name if metadata else "recorded sequence"}."""',
            "",
        ]

        if self._use_playwright:
            lines.extend(self._generate_playwright_header())
        else:
            lines.extend(self._generate_vanilla_header())

        lines.append("")

        for action in actions:
            lines.extend(self._action_to_code(action))

        lines.append("")
        lines.append("def main():")
        lines.append(f"{self._indent}sequence()")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append(f"{self._indent}main()")

        return "\n".join(lines)

    def _generate_playwright_header(self) -> list[str]:
        """Generate playwright import header."""
        return [
            "from playwright.sync_api import sync_playwright",
            "",
            "",
            "def sequence():",
            f"{self._indent}with sync_playwright() as p:",
            f"{self._indent}{self._indent}p.chromium.launch(headless=False).new_context()",
        ]

    def _generate_vanilla_header(self) -> list[str]:
        """Generate vanilla Python header."""
        return [
            "import time",
            "",
            "# Note: This requires pyautogui or similar",
            "# pip install pyautogui",
            "import pyautogui",
            "",
            "# Disable fail-safe",
            "pyautogui.FAILSAFE = False",
            "",
        ]

    def _action_to_code(self, action: InputAction) -> list[str]:
        """Convert single action to code lines."""
        lines = []

        if action.action_type == ActionType.CLICK:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.click({action.x}, {action.y})"
            )
        elif action.action_type == ActionType.DOUBLE_CLICK:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.doubleClick({action.x}, {action.y})"
            )
        elif action.action_type == ActionType.RIGHT_CLICK:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.click({action.x}, {action.y}, button='right')"
            )
        elif action.action_type == ActionType.HOVER:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.moveTo({action.x}, {action.y})"
            )
        elif action.action_type == ActionType.TYPE:
            escaped = action.text.replace('"', '\\"')
            lines.append(f'{self._indent}{self._indent}pyautogui.typewrite("{escaped}")')
        elif action.action_type == ActionType.PRESS:
            lines.append(f'{self._indent}{self._indent}pyautogui.press("{action.key}")')
        elif action.action_type == ActionType.SCROLL:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.scroll({action.delta_y}, {action.x}, {action.y})"
            )
        elif action.action_type == ActionType.DRAG:
            lines.append(
                f"{self._indent}{self._indent}pyautogui.drag({action.delta_x}, {action.delta_y}, {action.duration_ms / 1000})"
            )
        elif action.action_type == ActionType.WAIT:
            lines.append(f"{self._indent}{self._indent}time.sleep({action.duration_ms / 1000})")

        if action.duration_ms > 0 and action.action_type != ActionType.WAIT:
            lines.append(f"{self._indent}{self._indent}time.sleep({action.duration_ms / 1000})")

        return lines


def actions_to_json(actions: list[InputAction]) -> list[dict[str, Any]]:
    """Serialize actions to JSON-serializable format."""
    return [
        {
            "type": action.action_type.name,
            "x": action.x,
            "y": action.y,
            "text": action.text,
            "key": action.key,
            "delta_x": action.delta_x,
            "delta_y": action.delta_y,
            "duration_ms": action.duration_ms,
            "metadata": action.metadata,
        }
        for action in actions
    ]


def json_to_actions(data: list[dict[str, Any]]) -> list[InputAction]:
    """Deserialize actions from JSON format."""
    return [
        InputAction(
            action_type=ActionType[item["type"]],
            x=item.get("x"),
            y=item.get("y"),
            text=item.get("text"),
            key=item.get("key"),
            delta_x=item.get("delta_x"),
            delta_y=item.get("delta_y"),
            duration_ms=item.get("duration_ms", 0),
            metadata=item.get("metadata", {}),
        )
        for item in data
    ]
