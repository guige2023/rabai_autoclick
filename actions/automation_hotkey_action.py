"""Automation Hotkey Action Module.

Manages global hotkey registration and handling for automation triggers
with configurable key combinations, scopes, and action bindings.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class HotkeyBinding:
    hotkey_id: str
    keys: Set[str]
    modifiers: Set[str]
    action_fn: Callable
    scope: str = "global"
    enabled: bool = True
    description: str = ""
    cooldown_ms: float = 0.0


@dataclass
class HotkeyEvent:
    hotkey_id: str
    keys: Set[str]
    modifiers: Set[str]
    timestamp: float
    trigger_count: int = 1


class AutomationHotkeyAction:
    """Manages global hotkey bindings for automation triggers."""

    MODIFIERS: Set[str] = {"ctrl", "alt", "shift", "meta", "cmd", "win", "super"}

    def __init__(self) -> None:
        self._bindings: Dict[str, HotkeyBinding] = {}
        self._last_trigger: Dict[str, float] = {}
        self._trigger_counts: Dict[str, int] = {}
        self._listeners: Dict[str, List[Callable]] = {
            "hotkey_pressed": [],
            "hotkey_released": [],
            "cooldown_active": [],
        }
        self._enabled = True

    def register(
        self,
        hotkey_id: str,
        keys: List[str],
        action_fn: Callable,
        modifiers: Optional[List[str]] = None,
        scope: str = "global",
        description: str = "",
        cooldown_ms: float = 0.0,
    ) -> bool:
        if hotkey_id in self._bindings:
            logger.warning(f"Hotkey {hotkey_id} already registered")
            return False
        key_set = set(k.lower() for k in keys)
        mod_set = set(m.lower() for m in (modifiers or []))
        binding = HotkeyBinding(
            hotkey_id=hotkey_id,
            keys=key_set,
            modifiers=mod_set,
            action_fn=action_fn,
            scope=scope,
            description=description,
            cooldown_ms=cooldown_ms,
        )
        self._bindings[hotkey_id] = binding
        self._trigger_counts[hotkey_id] = 0
        logger.info(f"Registered hotkey {hotkey_id}: {mod_set}+{key_set}")
        return True

    def unregister(self, hotkey_id: str) -> bool:
        if hotkey_id in self._bindings:
            del self._bindings[hotkey_id]
            self._last_trigger.pop(hotkey_id, None)
            self._trigger_counts.pop(hotkey_id, None)
            return True
        return False

    def trigger(
        self,
        keys: Set[str],
        modifiers: Set[str],
    ) -> Optional[Any]:
        if not self._enabled:
            return None
        binding = self._match_binding(keys, modifiers)
        if not binding:
            return None
        if binding.cooldown_ms > 0:
            last = self._last_trigger.get(binding.hotkey_id, 0)
            if time.time() - last < binding.cooldown_ms / 1000.0:
                self._notify("cooldown_active", binding)
                return None
        self._last_trigger[binding.hotkey_id] = time.time()
        self._trigger_counts[binding.hotkey_id] = self._trigger_counts.get(
            binding.hotkey_id, 0
        ) + 1
        event = HotkeyEvent(
            hotkey_id=binding.hotkey_id,
            keys=binding.keys,
            modifiers=binding.modifiers,
            timestamp=time.time(),
            trigger_count=self._trigger_counts[binding.hotkey_id],
        )
        self._notify("hotkey_pressed", event)
        try:
            result = binding.action_fn(event)
            return result
        except Exception as e:
            logger.error(f"Hotkey action failed for {binding.hotkey_id}: {e}")
            return None

    def _match_binding(
        self,
        keys: Set[str],
        modifiers: Set[str],
    ) -> Optional[HotkeyBinding]:
        for binding in self._bindings.values():
            if not binding.enabled:
                continue
            if binding.keys != keys:
                continue
            if binding.modifiers != modifiers:
                continue
            return binding
        return None

    def enable(self, hotkey_id: str) -> bool:
        binding = self._bindings.get(hotkey_id)
        if binding:
            binding.enabled = True
            return True
        return False

    def disable(self, hotkey_id: str) -> bool:
        binding = self._bindings.get(hotkey_id)
        if binding:
            binding.enabled = False
            return True
        return False

    def enable_all(self) -> None:
        self._enabled = True

    def disable_all(self) -> None:
        self._enabled = False

    def list_bindings(self) -> List[Dict[str, Any]]:
        return [
            {
                "hotkey_id": b.hotkey_id,
                "keys": list(b.keys),
                "modifiers": list(b.modifiers),
                "scope": b.scope,
                "description": b.description,
                "enabled": b.enabled,
                "cooldown_ms": b.cooldown_ms,
                "trigger_count": self._trigger_counts.get(b.hotkey_id, 0),
                "last_trigger": self._last_trigger.get(b.hotkey_id),
            }
            for b in self._bindings.values()
        ]

    def add_listener(self, event: str, callback: Callable) -> None:
        if event in self._listeners:
            self._listeners[event].append(callback)

    def _notify(self, event: str, data: Any) -> None:
        for cb in self._listeners.get(event, []):
            try:
                cb(data)
            except Exception as e:
                logger.error(f"Hotkey listener error for {event}: {e}")

    def reset_stats(self, hotkey_id: Optional[str] = None) -> None:
        if hotkey_id:
            self._trigger_counts[hotkey_id] = 0
            self._last_trigger.pop(hotkey_id, None)
        else:
            self._trigger_counts.clear()
            self._last_trigger.clear()
