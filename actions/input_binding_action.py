"""
Input Binding Action Module

Binds automation actions to input triggers including
keyboard shortcuts, mouse buttons, and gesture patterns.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class InputModifier(Enum):
    """Input modifier keys."""

    CTRL = "ctrl"
    ALT = "alt"
    SHIFT = "shift"
    META = "meta"
    FN = "fn"


class InputButton(Enum):
    """Mouse button identifiers."""

    LEFT = "left"
    MIDDLE = "middle"
    RIGHT = "right"
    BACK = "back"
    FORWARD = "forward"


@dataclass
class InputBinding:
    """Binds input trigger to action."""

    id: str
    trigger: Tuple[Set[InputModifier], str]
    callback: Callable[[], Any]
    description: str = ""
    enabled: bool = True
    repeat: bool = False


@dataclass
class InputBindingConfig:
    """Configuration for input bindings."""

    default_timeout: float = 5.0
    enable_mouse: bool = True
    enable_keyboard: bool = True
    enable_gestures: bool = True
    global_bindings: bool = False


class InputBindingManager:
    """
    Manages input-to-action bindings.

    Supports keyboard shortcuts, mouse button combos,
    and gesture-based triggers.
    """

    def __init__(
        self,
        config: Optional[InputBindingConfig] = None,
        action_executor: Optional[Callable[[str], None]] = None,
    ):
        self.config = config or InputBindingConfig()
        self.action_executor = action_executor or self._default_executor
        self._bindings: Dict[str, InputBinding] = {}

    def _default_executor(self, binding_id: str) -> None:
        """Default action executor."""
        logger.debug(f"Executing binding: {binding_id}")

    def bind(
        self,
        binding_id: str,
        modifiers: List[InputModifier],
        key: str,
        callback: Callable[[], Any],
        description: str = "",
        enabled: bool = True,
    ) -> bool:
        """
        Create an input binding.

        Args:
            binding_id: Binding identifier
            modifiers: Modifier keys
            key: Input key/button
            callback: Function to call
            description: Description
            enabled: Initial enabled state

        Returns:
            True if successful
        """
        binding = InputBinding(
            id=binding_id,
            trigger=(set(modifiers), key),
            callback=callback,
            description=description,
            enabled=enabled,
        )

        self._bindings[binding_id] = binding
        logger.info(f"Bound {binding_id}: {modifiers}+{key}")
        return True

    def unbind(self, binding_id: str) -> bool:
        """Remove a binding."""
        if binding_id in self._bindings:
            del self._bindings[binding_id]
            return True
        return False

    def handle_input(
        self,
        modifiers: Set[InputModifier],
        key: str,
        is_repeat: bool = False,
    ) -> bool:
        """
        Handle an input event.

        Args:
            modifiers: Active modifiers
            key: Input key
            is_repeat: Key repeat flag

        Returns:
            True if binding was triggered
        """
        for binding in self._bindings.values():
            if not binding.enabled:
                continue

            if binding.repeat or not is_repeat:
                mod_set, trigger_key = binding.trigger
                if mod_set == modifiers and trigger_key == key:
                    try:
                        binding.callback()
                        self.action_executor(binding.id)
                        return True
                    except Exception as e:
                        logger.error(f"Binding failed: {e}")

        return False

    def enable(self, binding_id: str) -> bool:
        """Enable a binding."""
        if binding_id in self._bindings:
            self._bindings[binding_id].enabled = True
            return True
        return False

    def disable(self, binding_id: str) -> bool:
        """Disable a binding."""
        if binding_id in self._bindings:
            self._bindings[binding_id].enabled = False
            return True
        return False

    def list_bindings(self) -> List[Dict[str, Any]]:
        """List all bindings."""
        return [
            {
                "id": b.id,
                "modifiers": [m.value for m in b.trigger[0]],
                "key": b.trigger[1],
                "description": b.description,
                "enabled": b.enabled,
            }
            for b in self._bindings.values()
        ]


def create_input_binding_manager(
    config: Optional[InputBindingConfig] = None,
) -> InputBindingManager:
    """Factory function."""
    return InputBindingManager(config=config)
