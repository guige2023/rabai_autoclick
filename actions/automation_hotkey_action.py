"""Automation Hotkey Action Module for RabAI AutoClick.

Registers and triggers keyboard hotkey combinations for
quick automation workflow activation.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationHotkeyAction(BaseAction):
    """Register and trigger keyboard hotkey combinations.

    Manages global hotkey bindings that can trigger automation
    sequences from any application. Supports modifier keys,
    key sequences, and chord combinations.
    """
    action_type = "automation_hotkey"
    display_name = "热键自动化"
    description = "注册和触发键盘热键组合"

    _registered_hotkeys: Dict[str, Dict[str, Any]] = {}
    _active_hotkeys: Set[str] = set()

    MODIFIER_KEYS = {'ctrl', 'control', 'alt', 'shift', 'cmd', 'command', 'super', 'meta'}
    KEY_ALIASES = {
        'esc': 'escape',
        'return': 'enter',
        'backspace': 'backspace',
        'delete': 'delete',
        'tab': 'tab',
        'space': 'space',
        'up': 'up',
        'down': 'down',
        'left': 'left',
        'right': 'right',
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hotkey operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'register', 'unregister', 'trigger', 'list', 'press'
                - hotkey: str - hotkey string like 'ctrl+shift+a' or 'cmd+option+b'
                - action_id: str (optional) - action ID to trigger when hotkey fires
                - description: str (optional) - description of the hotkey
                - repeat: int (optional) - number of times to press, default 1

        Returns:
            ActionResult with hotkey registration or trigger result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'trigger')
            hotkey = params.get('hotkey', '')
            action_id = params.get('action_id')
            description = params.get('description', '')
            repeat = params.get('repeat', 1)

            if operation == 'register':
                return self._register_hotkey(
                    hotkey, action_id, description, start_time
                )
            elif operation == 'unregister':
                return self._unregister_hotkey(hotkey, start_time)
            elif operation == 'trigger':
                return self._trigger_hotkey(hotkey, repeat, start_time)
            elif operation == 'list':
                return self._list_hotkeys(start_time)
            elif operation == 'press':
                return self._press_keys(hotkey, repeat, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hotkey action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _register_hotkey(
        self,
        hotkey: str,
        action_id: Optional[str],
        description: str,
        start_time: float
    ) -> ActionResult:
        """Register a hotkey binding."""
        if not hotkey:
            return ActionResult(
                success=False,
                message="Hotkey string is required",
                duration=time.time() - start_time
            )

        parsed = self._parse_hotkey(hotkey)
        if parsed is None:
            return ActionResult(
                success=False,
                message=f"Invalid hotkey format: {hotkey}",
                duration=time.time() - start_time
            )

        self._registered_hotkeys[hotkey.lower()] = {
            'hotkey': hotkey,
            'action_id': action_id,
            'description': description,
            'modifiers': parsed['modifiers'],
            'key': parsed['key'],
            'registered_at': time.time()
        }
        self._active_hotkeys.add(hotkey.lower())

        return ActionResult(
            success=True,
            message=f"Hotkey registered: {hotkey}",
            data={
                'hotkey': hotkey,
                'action_id': action_id,
                'modifiers': parsed['modifiers'],
                'key': parsed['key']
            },
            duration=time.time() - start_time
        )

    def _unregister_hotkey(self, hotkey: str, start_time: float) -> ActionResult:
        """Unregister a hotkey binding."""
        key = hotkey.lower()
        if key in self._registered_hotkeys:
            del self._registered_hotkeys[key]
            self._active_hotkeys.discard(key)
            return ActionResult(
                success=True,
                message=f"Hotkey unregistered: {hotkey}",
                duration=time.time() - start_time
            )
        return ActionResult(
            success=False,
            message=f"Hotkey not found: {hotkey}",
            duration=time.time() - start_time
        )

    def _trigger_hotkey(self, hotkey: str, repeat: int, start_time: float) -> ActionResult:
        """Trigger a registered hotkey press sequence."""
        key = hotkey.lower()
        if key not in self._registered_hotkeys:
            return ActionResult(
                success=False,
                message=f"Hotkey not registered: {hotkey}",
                duration=time.time() - start_time
            )

        hotkey_info = self._registered_hotkeys[key]
        for i in range(repeat):
            self._simulate_key_press(
                hotkey_info['modifiers'],
                hotkey_info['key']
            )
            if i < repeat - 1:
                time.sleep(0.05)

        return ActionResult(
            success=True,
            message=f"Hotkey triggered: {hotkey} x{repeat}",
            data={'hotkey': hotkey, 'repeat': repeat},
            duration=time.time() - start_time
        )

    def _press_keys(self, hotkey: str, repeat: int, start_time: float) -> ActionResult:
        """Press key combination without requiring registration."""
        parsed = self._parse_hotkey(hotkey)
        if parsed is None:
            return ActionResult(
                success=False,
                message=f"Invalid hotkey format: {hotkey}",
                duration=time.time() - start_time
            )

        for i in range(repeat):
            self._simulate_key_press(parsed['modifiers'], parsed['key'])
            if i < repeat - 1:
                time.sleep(0.05)

        return ActionResult(
            success=True,
            message=f"Keys pressed: {hotkey} x{repeat}",
            data={'hotkey': hotkey, 'repeat': repeat},
            duration=time.time() - start_time
        )

    def _list_hotkeys(self, start_time: float) -> ActionResult:
        """List all registered hotkeys."""
        hotkeys = [
            {
                'hotkey': info['hotkey'],
                'action_id': info['action_id'],
                'description': info['description']
            }
            for info in self._registered_hotkeys.values()
        ]
        return ActionResult(
            success=True,
            message=f"Registered hotkeys: {len(hotkeys)}",
            data={'hotkeys': hotkeys, 'count': len(hotkeys)},
            duration=time.time() - start_time
        )

    def _parse_hotkey(self, hotkey: str) -> Optional[Dict[str, Any]]:
        """Parse hotkey string into modifiers and key."""
        parts = [p.strip().lower() for p in hotkey.split('+')]
        if not parts:
            return None

        modifiers = []
        key = parts[-1]

        for part in parts[:-1]:
            normalized = self.KEY_ALIASES.get(part, part)
            if normalized in self.MODIFIER_KEYS or part in self.MODIFIER_KEYS:
                modifiers.append(normalized)
            else:
                modifiers.append(normalized)

        return {'modifiers': modifiers, 'key': key}

    def _simulate_key_press(self, modifiers: List[str], key: str) -> None:
        """Simulate keyboard press using Quartz."""
        try:
            import Quartz
        except ImportError:
            return

        key_code = self._get_key_code(key)
        if key_code is None:
            return

        flags = 0
        for mod in modifiers:
            if mod in ('ctrl', 'control'):
                flags |= Quartz.kCGEventFlagMaskControl
            elif mod in ('alt', 'option'):
                flags |= Quartz.kCGEventFlagMaskAlternate
            elif mod in ('shift'):
                flags |= Quartz.kCGEventFlagMaskShift
            elif mod in ('cmd', 'command', 'super', 'meta'):
                flags |= Quartz.kCGEventFlagMaskCommand

        key_down = Quartz.CGEventCreateKeyboardEvent(None, key_code, True)
        key_up = Quartz.CGEventCreateKeyboardEvent(None, key_code, False)

        if key_down:
            Quartz.CGEventSetFlags(key_down, flags)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_down)
        if key_up:
            Quartz.CGEventSetFlags(key_up, flags)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, key_up)

    def _get_key_code(self, key: str) -> Optional[int]:
        """Map key name to Quartz key code."""
        key_map = {
            'a': 0, 'b': 11, 'c': 8, 'd': 2, 'e': 14, 'f': 3, 'g': 5,
            'h': 4, 'i': 34, 'j': 38, 'k': 40, 'l': 37, 'm': 46, 'n': 45,
            'o': 31, 'p': 35, 'q': 12, 'r': 15, 's': 1, 't': 17, 'u': 32,
            'v': 9, 'w': 13, 'x': 7, 'y': 16, 'z': 6,
            '0': 29, '1': 18, '2': 19, '3': 20, '4': 21, '5': 23,
            '6': 22, '7': 26, '8': 28, '9': 25,
            'return': 36, 'enter': 36, 'tab': 48, 'space': 49, 'delete': 51,
            'escape': 53, 'esc': 53,
            'up': 126, 'down': 125, 'left': 123, 'right': 124,
            'f1': 122, 'f2': 120, 'f3': 99, 'f4': 118, 'f5': 96,
            'f6': 97, 'f7': 98, 'f8': 100, 'f9': 101, 'f10': 109,
            'f11': 103, 'f12': 118,
        }
        return key_map.get(key.lower())
