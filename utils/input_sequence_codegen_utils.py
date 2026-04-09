"""
Input sequence code generation utilities.

Generate code from recorded input sequences.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional


@dataclass
class InputAction:
    """An input action to be converted to code."""
    action_type: str
    x: Optional[float] = None
    y: Optional[float] = None
    key: Optional[str] = None
    text: Optional[str] = None
    duration_ms: float = 0
    metadata: dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class CodeGenerator:
    """Generate code from input sequences."""
    
    def __init__(self, language: str = "python"):
        self.language = language
    
    def generate(
        self,
        actions: list[InputAction],
        class_name: str = "RecordedSequence"
    ) -> str:
        """Generate code from input actions."""
        if self.language == "python":
            return self._generate_python(actions, class_name)
        elif self.language == "javascript":
            return self._generate_javascript(actions, class_name)
        else:
            return self._generate_generic(actions, class_name)
    
    def _generate_python(
        self,
        actions: list[InputAction],
        class_name: str
    ) -> str:
        """Generate Python code."""
        lines = [
            f"class {class_name}:",
            "    def __init__(self, controller):",
            "        self.controller = controller",
            "",
            "    def play(self):",
        ]
        
        for action in actions:
            if action.action_type == "tap":
                lines.append(f"        self.controller.tap({action.x}, {action.y})")
            elif action.action_type == "drag":
                lines.append(f"        self.controller.drag({action.x}, {action.y}, {action.metadata.get('end_x')}, {action.metadata.get('end_y')})")
            elif action.action_type == "type":
                lines.append(f"        self.controller.type_text('{action.text}')")
            elif action.action_type == "key":
                lines.append(f"        self.controller.press_key('{action.key}')")
            elif action.action_type == "swipe":
                lines.append(f"        self.controller.swipe({action.x}, {action.y}, {action.metadata.get('end_x')}, {action.metadata.get('end_y')})")
            elif action.action_type == "wait":
                lines.append(f"        self.controller.wait({action.duration_ms / 1000})")
        
        lines.append("")
        return "\n".join(lines)
    
    def _generate_javascript(
        self,
        actions: list[InputAction],
        class_name: str
    ) -> str:
        """Generate JavaScript code."""
        lines = [
            f"class {class_name} {{",
            "  constructor(controller) {",
            "    this.controller = controller;",
            "  }",
            "",
            "  async play() {",
        ]
        
        for action in actions:
            if action.action_type == "tap":
                lines.append(f"    await this.controller.tap({action.x}, {action.y});")
            elif action.action_type == "drag":
                lines.append(f"    await this.controller.drag({action.x}, {action.y}, {action.metadata.get('end_x')}, {action.metadata.get('end_y')});")
            elif action.action_type == "type":
                lines.append(f"    await this.controller.typeText('{action.text}');")
            elif action.action_type == "key":
                lines.append(f"    await this.controller.pressKey('{action.key}');")
            elif action.action_type == "swipe":
                lines.append(f"    await this.controller.swipe({action.x}, {action.y}, {action.metadata.get('end_x')}, {action.metadata.get('end_y')});")
            elif action.action_type == "wait":
                lines.append(f"    await this.controller.wait({action.duration_ms});")
        
        lines.append("  }")
        lines.append("}")
        lines.append("")
        return "\n".join(lines)
    
    def _generate_generic(
        self,
        actions: list[InputAction],
        class_name: str
    ) -> str:
        """Generate generic code."""
        return json.dumps([{"type": a.action_type, "x": a.x, "y": a.y, "key": a.key, "text": a.text} for a in actions], indent=2)


class SequenceExporter:
    """Export input sequences to various formats."""
    
    @staticmethod
    def to_json(actions: list[InputAction]) -> str:
        """Export to JSON."""
        return json.dumps([
            {
                "type": a.action_type,
                "x": a.x,
                "y": a.y,
                "key": a.key,
                "text": a.text,
                "duration_ms": a.duration_ms,
                "metadata": a.metadata
            }
            for a in actions
        ], indent=2)
    
    @staticmethod
    def to_csv(actions: list[InputAction]) -> str:
        """Export to CSV."""
        lines = ["action_type,x,y,key,text,duration_ms"]
        for action in actions:
            lines.append(f"{action.action_type},{action.x or ''},{action.y or ''},{action.key or ''},{action.text or ''},{action.duration_ms}")
        return "\n".join(lines)
