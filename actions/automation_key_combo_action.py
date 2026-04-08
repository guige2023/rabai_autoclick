"""Automation Key Combo Action Module for RabAI AutoClick.

Keyboard combination automation with modifier key support,
sequence execution, and combo chaining.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationKeyComboAction(BaseAction):
    """Keyboard combination and sequence automation.

    Executes keyboard shortcuts, key sequences, and chained
    combos with precise timing control. Supports modifier keys
    and hold-release sequences.
    """
    action_type = "automation_key_combo"
    display_name = "组合键自动化"
    description = "键盘组合键和序列自动化"

    _modifier_keys: Set[str] = {
        'ctrl', 'control', 'alt', 'option', 'shift', 'cmd',
        'command', 'super', 'meta', 'fn', 'capslock'
    }

    _key_map: Dict[str, int] = {
        'a': 0, 'b': 11, 'c': 8, 'd': 2, 'e': 14, 'f': 3, 'g': 5,
        'h': 4, 'i': 34, 'j': 38, 'k': 40, 'l': 37, 'm': 46, 'n': 45,
        'o': 31, 'p': 35, 'q': 12, 'r': 15, 's': 1, 't': 17, 'u': 32,
        'v': 9, 'w': 13, 'x': 7, 'y': 16, 'z': 6,
        '0': 29, '1': 18, '2': 19, '3': 20, '4': 21, '5': 23,
        '6': 22, '7': 26, '8': 28, '9': 25,
        'return': 36, 'enter': 36, 'tab': 48, 'space': 49,
        'delete': 51, 'backspace': 51,
        'escape': 53, 'esc': 53,
        'up': 126, 'down': 125, 'left': 123, 'right': 124,
        'f1': 122, 'f2': 120, 'f3': 99, 'f4': 118, 'f5': 96,
        'f6': 97, 'f7': 98, 'f8': 100, 'f9': 101, 'f10': 109,
        'f11': 103, 'f12': 118,
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute key combo operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'combo', 'sequence', 'hold', 'type_text'
                - keys: list (optional) - list of keys for combo (e.g., ['ctrl', 'c'])
                - sequence: list (optional) - list of key combos to execute in sequence
                - text: str (optional) - text to type
                - hold_duration: float (optional) - how long to hold keys
                - interval: float (optional) - interval between keys in sequence
                - mods: list (optional) - modifier keys

        Returns:
            ActionResult with key combo result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'combo')

            if operation == 'combo':
                return self._execute_combo(params, start_time)
            elif operation == 'sequence':
                return self._execute_sequence(params, start_time)
            elif operation == 'hold':
                return self._hold_keys(params, start_time)
            elif operation == 'type_text':
                return self._type_text(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Key combo action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _execute_combo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a key combination (e.g., Ctrl+C)."""
        keys = params.get('keys', [])
        mods = params.get('mods', [])

        if not keys:
            return ActionResult(
                success=False,
                message="keys list is required",
                duration=time.time() - start_time
            )

        all_keys = mods + keys if mods else keys
        modifiers = [k for k in all_keys if k.lower() in self._modifier_keys]
        main_key = keys[-1] if keys else None

        self._press_combo(modifiers, main_key)

        return ActionResult(
            success=True,
            message=f"Combo executed: {'+'.join(all_keys)}",
            data={
                'combo': all_keys,
                'modifiers': modifiers,
                'main_key': main_key
            },
            duration=time.time() - start_time
        )

    def _execute_sequence(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a sequence of key combos."""
        sequence = params.get('sequence', [])
        interval = params.get('interval', 0.1)

        if not sequence:
            return ActionResult(
                success=False,
                message="sequence list is required",
                duration=time.time() - start_time
            )

        executed = 0
        for combo in sequence:
            if isinstance(combo, str):
                combo = [combo]
            elif isinstance(combo, dict):
                combo = combo.get('keys', [])

            if combo:
                modifiers = [k for k in combo if k.lower() in self._modifier_keys]
                main_key = combo[-1] if combo else None
                if main_key:
                    self._press_combo(modifiers, main_key)
                    executed += 1

            time.sleep(interval)

        return ActionResult(
            success=True,
            message=f"Sequence executed: {executed} combos",
            data={
                'sequence_length': len(sequence),
                'executed': executed,
                'interval': interval
            },
            duration=time.time() - start_time
        )

    def _hold_keys(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Hold keys for a duration then release."""
        keys = params.get('keys', [])
        hold_duration = params.get('hold_duration', 1.0)

        if not keys:
            return ActionResult(
                success=False,
                message="keys list is required",
                duration=time.time() - start_time
            )

        modifiers = [k for k in keys if k.lower() in self._modifier_keys]
        main_key = keys[-1] if keys else None

        self._press_combo(modifiers, main_key)
        time.sleep(hold_duration)
        self._release_combo(modifiers, main_key)

        return ActionResult(
            success=True,
            message=f"Hold executed: {'+'.join(keys)} for {hold_duration}s",
            data={
                'keys': keys,
                'hold_duration': hold_duration
            },
            duration=time.time() - start_time
        )

    def _type_text(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Type a string of text."""
        text = params.get('text', '')
        interval = params.get('interval', 0.01)

        if not text:
            return ActionResult(
                success=False,
                message="text is required",
                duration=time.time() - start_time
            )

        typed_chars = 0
        for char in text:
            if char == '\n':
                self._press_combo([], 'return')
            elif char == '\t':
                self._press_combo([], 'tab')
            else:
                self._type_char(char)
            typed_chars += 1
            time.sleep(interval)

        return ActionResult(
            success=True,
            message=f"Typed {typed_chars} characters",
            data={
                'characters': typed_chars,
                'interval': interval
            },
            duration=time.time() - start_time
        )

    def _press_combo(self, modifiers: List[str], main_key: Optional[str]) -> None:
        """Press a key combination."""
        try:
            import Quartz
        except ImportError:
            return

        flags = 0
        for mod in modifiers:
            mod_lower = mod.lower()
            if mod_lower in ('ctrl', 'control'):
                flags |= Quartz.kCGEventFlagMaskControl
            elif mod_lower in ('alt', 'option'):
                flags |= Quartz.kCGEventFlagMaskAlternate
            elif mod_lower in ('shift'):
                flags |= Quartz.kCGEventFlagMaskShift
            elif mod_lower in ('cmd', 'command', 'super', 'meta'):
                flags |= Quartz.kCGEventFlagMaskCommand

        key_code = self._get_key_code(main_key) if main_key else None

        if key_code is not None:
            key_down = Quartz.CGEventCreateKeyboardEvent(None, key_code, True)
            key_up = Quartz.CGEventCreateKeyboardEvent(None, key_code, False)

            if key_down:
                Quartz.CGEventSetFlags(key_down, flags)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_down)
            if key_up:
                Quartz.CGEventSetFlags(key_up, flags)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_up)

    def _release_combo(self, modifiers: List[str], main_key: Optional[str]) -> None:
        """Release a key combination."""
        self._press_combo(modifiers, main_key)

    def _type_char(self, char: str) -> None:
        """Type a single character using Quartz."""
        try:
            import Quartz
        except ImportError:
            return

        char_lower = char.lower()
        key_code = self._get_key_code(char_lower)

        if key_code is not None:
            key_down = Quartz.CGEventCreateKeyboardEvent(None, key_code, True)
            key_up = Quartz.CGEventCreateKeyboardEvent(None, key_code, False)

            if char.isupper():
                flags = Quartz.kCGEventFlagMaskShift
            else:
                flags = 0

            if key_down:
                Quartz.CGEventSetFlags(key_down, flags)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_down)
            if key_up:
                Quartz.CGEventSetFlags(key_up, flags)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_up)

    def _get_key_code(self, key: Optional[str]) -> Optional[int]:
        """Map key name to Quartz key code."""
        if key is None:
            return None
        key_lower = key.lower()
        return self._key_map.get(key_lower)
