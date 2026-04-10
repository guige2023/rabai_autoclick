"""
WorkflowHotkeyManager - Global hotkey system for RabAI AutoClick

Features:
- Global hotkey registration (works in background)
- Hotkey to workflow mapping
- Hotkey to action mapping
- Context-aware hotkeys (different apps/windows)
- Hotkey sequences (e.g., C-x C-c like Emacs)
- Hotkey profiles (work, personal, gaming)
- Quick-launch hotkeys
- Hotkey conflict detection
- macOS integration via pynput
- Hotkey recording

Author: RabAI Team
Version: 1.0.0
"""

import threading
import time
import json
import os
import logging
from typing import Dict, List, Optional, Tuple, Callable, Any, Set
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from collections import defaultdict
from contextlib import contextmanager
import uuid

# pynput for global keyboard capture
from pynput import keyboard
from pynput.keyboard import Key, KeyCode, Listener as KeyboardListener

# For macOS accessibility/permissions handling
import sys
import platform

logger = logging.getLogger(__name__)


class HotkeyActionType(Enum):
    """Types of actions that can be triggered by hotkeys"""
    WORKFLOW_LAUNCH = auto()
    WORKFLOW_STOP = auto()
    WORKFLOW_PAUSE = auto()
    ACTION_EXECUTE = auto()
    PROFILE_SWITCH = auto()
    SEQUENCE_PART = auto()
    QUICK_LAUNCH = auto()
    CONTEXT_SWITCH = auto()
    CUSTOM = auto()


@dataclass
class HotkeyBinding:
    """Represents a single hotkey binding"""
    id: str
    keys: str  # String representation like "ctrl+shift+a"
    action_type: HotkeyActionType
    action_target: str  # workflow_id, action_id, profile_name, etc.
    contexts: List[str] = field(default_factory=list)  # Empty = all contexts
    profile: str = "default"
    description: str = ""
    enabled: bool = True
    is_sequence_start: bool = False
    sequence_continuation: Optional[str] = None  # Next key in sequence
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "keys": self.keys,
            "action_type": self.action_type.name,
            "action_target": self.action_target,
            "contexts": self.contexts,
            "profile": self.profile,
            "description": self.description,
            "enabled": self.enabled,
            "is_sequence_start": self.is_sequence_start,
            "sequence_continuation": self.sequence_continuation,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HotkeyBinding":
        data["action_type"] = HotkeyActionType[data["action_type"]]
        return cls(**data)


@dataclass
class HotkeyProfile:
    """A named profile containing hotkey bindings"""
    name: str
    description: str = ""
    bindings: List[HotkeyBinding] = field(default_factory=list)
    is_active: bool = False
    priority: int = 0  # Higher priority profiles override lower

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "bindings": [b.to_dict() for b in self.bindings],
            "is_active": self.is_active,
            "priority": self.priority,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HotkeyProfile":
        bindings = [HotkeyBinding.from_dict(b) for b in data.get("bindings", [])]
        return cls(
            name=data["name"],
            description=data.get("description", ""),
            bindings=bindings,
            is_active=data.get("is_active", False),
            priority=data.get("priority", 0),
        )


@dataclass
class ContextRule:
    """Rules for context-aware hotkey activation"""
    app_bundle_id: Optional[str] = None  # macOS app bundle ID
    app_name: Optional[str] = None  # Generic app name
    window_title_pattern: Optional[str] = None
    active: bool = True

    def matches(self, app_info: Dict) -> bool:
        """Check if this context rule matches the given app info"""
        if not self.active:
            return False
        
        if self.app_bundle_id and app_info.get("bundle_id") != self.app_bundle_id:
            return False
        
        if self.app_name:
            app_name_lower = app_info.get("name", "").lower()
            if self.app_name.lower() not in app_name_lower:
                return False
        
        if self.window_title_pattern:
            title = app_info.get("window_title", "")
            import re
            if not re.search(self.window_title_pattern, title):
                return False
        
        return True


class ConflictType(Enum):
    """Types of hotkey conflicts"""
    EXACT_MATCH = auto()
    SEQUENCE_CONFLICT = auto()
    CONTEXT_CONFLICT = auto()
    PROFILE_CONFLICT = auto()


@dataclass
class HotkeyConflict:
    """Represents a conflict between hotkey bindings"""
    conflict_type: ConflictType
    binding1: HotkeyBinding
    binding2: HotkeyBinding
    message: str


class WorkflowHotkeyManager:
    """
    Global hotkey management system for RabAI AutoClick.
    
    Features:
    - Register global hotkeys that work even when the app is in background
    - Map hotkeys to workflows or individual actions
    - Support context-aware hotkeys (active only in specific apps/windows)
    - Support key sequences (like Emacs: C-x C-c)
    - Multiple named profiles (work, personal, gaming)
    - Quick-launch any workflow with a single hotkey
    - Detect and warn about conflicting hotkeys
    - Record new hotkeys by pressing keys
    """

    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the hotkey manager.
        
        Args:
            config_dir: Directory to store hotkey configurations
        """
        self.config_dir = config_dir or os.path.expanduser("~/.rabai_autoclick")
        os.makedirs(self.config_dir, exist_ok=True)
        
        # Core state
        self._profiles: Dict[str, HotkeyProfile] = {}
        self._active_profile_name: str = "default"
        self._bindings_by_key: Dict[str, List[HotkeyBinding]] = defaultdict(list)
        self._bindings_by_context: Dict[str, List[HotkeyBinding]] = defaultdict(list)
        self._sequence_buffer: List[str] = []
        self._sequence_timeout: float = 1.0  # seconds
        self._last_key_time: float = 0
        
        # Context tracking
        self._current_context: str = "default"
        self._context_rules: List[ContextRule] = []
        self._foreground_app_info: Dict = {"name": "", "bundle_id": None, "window_title": ""}
        
        # Hotkey recording
        self._is_recording: bool = False
        self._recorded_keys: List[Tuple[bool, Any]] = []  # (is_press, key)
        self._recording_callback: Optional[Callable] = None
        
        # Action callbacks
        self._workflow_launcher: Optional[Callable[[str], None]] = None
        self._action_executor: Optional[Callable[[str, Any], Any]] = None
        self._profile_switcher: Optional[Callable[[str], None]] = None
        
        # Conflict tracking
        self._conflicts: List[HotkeyConflict] = []
        
        # Listener thread
        self._listener: Optional[KeyboardListener] = None
        self._listener_thread: Optional[threading.Thread] = None
        self._running: bool = False
        
        # Key normalization map
        self._key_map = self._build_key_map()
        
        # Platform detection
        self._platform = platform.system()
        
        # Initialize default profile
        self._init_default_profile()
        
        logger.info(f"WorkflowHotkeyManager initialized on {self._platform}")

    def _build_key_map(self) -> Dict[str, str]:
        """Build a mapping of normalized key names"""
        return {
            "ctrl": "ctrl",
            "control": "ctrl",
            "alt": "alt",
            "option": "alt",
            "shift": "shift",
            "cmd": "cmd",
            "command": "cmd",
            "meta": "cmd",
            "windows": "cmd",
            "space": "space",
            "tab": "tab",
            "enter": "enter",
            "return": "enter",
            "escape": "escape",
            "esc": "escape",
            "backspace": "backspace",
            "delete": "delete",
            "up": "up",
            "down": "down",
            "left": "left",
            "right": "right",
            "home": "home",
            "end": "end",
            "page_up": "page_up",
            "page_down": "page_down",
            "f1": "f1", "f2": "f2", "f3": "f3", "f4": "f4",
            "f5": "f5", "f6": "f6", "f7": "f7", "f8": "f8",
            "f9": "f9", "f10": "f10", "f11": "f11", "f12": "f12",
        }

    def _init_default_profile(self) -> None:
        """Initialize the default hotkey profile"""
        default_profile = HotkeyProfile(
            name="default",
            description="Default hotkey profile",
            is_active=True,
            priority=0,
        )
        self._profiles["default"] = default_profile
        
        # Add some useful default quick-launch hotkeys
        self.register_hotkey(
            keys="ctrl+shift+r",
            action_type=HotkeyActionType.QUICK_LAUNCH,
            action_target="record_workflow",
            description="Quick launch workflow recording",
            profile="default",
        )
        
        logger.info("Default hotkey profile initialized")

    # ==================== Global Hotkey Registration ====================

    def register_hotkey(
        self,
        keys: str,
        action_type: HotkeyActionType,
        action_target: str,
        contexts: Optional[List[str]] = None,
        profile: str = "default",
        description: str = "",
        enabled: bool = True,
        is_sequence_start: bool = False,
        sequence_continuation: Optional[str] = None,
    ) -> Optional[HotkeyConflict]:
        """
        Register a global hotkey binding.
        
        Args:
            keys: Hotkey string like "ctrl+shift+a" or "cmd+shift+b"
            action_type: Type of action to trigger
            action_target: Target of the action (workflow_id, action_id, etc.)
            contexts: List of context names where this hotkey is active
            profile: Profile name to add this binding to
            description: Human-readable description
            enabled: Whether the hotkey is initially enabled
            is_sequence_start: If True, this starts a key sequence
            sequence_continuation: Next key in sequence (e.g., "ctrl+x" for C-x C-c)
            
        Returns:
            HotkeyConflict if there's a conflict, None otherwise
        """
        # Ensure profile exists
        if profile not in self._profiles:
            self._profiles[profile] = HotkeyProfile(name=profile)
        
        # Normalize and validate keys
        normalized_keys = self._normalize_keys(keys)
        
        # Check for conflicts
        conflict = self._detect_conflict(normalized_keys, profile, contexts)
        if conflict:
            self._conflicts.append(conflict)
            logger.warning(f"Hotkey conflict detected: {conflict.message}")
        
        # Create binding
        binding = HotkeyBinding(
            id=str(uuid.uuid4()),
            keys=normalized_keys,
            action_type=action_type,
            action_target=action_target,
            contexts=contexts or [],
            profile=profile,
            description=description,
            enabled=enabled,
            is_sequence_start=is_sequence_start,
            sequence_continuation=sequence_continuation,
        )
        
        # Add to profile
        self._profiles[profile].bindings.append(binding)
        
        # Index by key
        self._bindings_by_key[normalized_keys].append(binding)
        
        # Index by context
        for ctx in binding.contexts:
            self._bindings_by_context[ctx].append(binding)
        
        logger.info(f"Registered hotkey: {normalized_keys} -> {action_type.name}:{action_target}")
        
        return conflict

    def unregister_hotkey(self, binding_id: str) -> bool:
        """
        Unregister a hotkey binding by ID.
        
        Returns:
            True if binding was found and removed, False otherwise
        """
        for profile in self._profiles.values():
            for i, binding in enumerate(profile.bindings):
                if binding.id == binding_id:
                    # Remove from indexes
                    self._bindings_by_key[binding.keys].remove(binding)
                    if not self._bindings_by_key[binding.keys]:
                        del self._bindings_by_key[binding.keys]
                    
                    for ctx in binding.contexts:
                        self._bindings_by_context[ctx].remove(binding)
                    
                    # Remove from profile
                    profile.bindings.pop(i)
                    logger.info(f"Unregistered hotkey: {binding.keys}")
                    return True
        
        return False

    def _normalize_keys(self, keys: str) -> str:
        """Normalize a key combination string"""
        parts = keys.lower().replace(" ", "").split("+")
        normalized = []
        
        for part in parts:
            if part in self._key_map:
                normalized.append(self._key_map[part])
            elif len(part) == 1:
                normalized.append(part)
            elif part.startswith("f") and part[1:].isdigit():
                normalized.append(part.lower())
            else:
                normalized.append(part)
        
        # Sort modifiers first
        modifiers = {"ctrl", "alt", "shift", "cmd"}
        sorted_parts = sorted(normalized, key=lambda x: (0 if x in modifiers else 1, x))
        return "+".join(sorted_parts)

    def _keys_from_event(self, key: Any, is_press: bool) -> Optional[str]:
        """Convert a pynput key event to a normalized key string"""
        if key is None:
            return None
        
        # Handle KeyCode (regular character keys)
        if isinstance(key, KeyCode):
            if key.char:
                return key.char.lower()
            return None
        
        # Handle special keys
        if isinstance(key, Key):
            key_map = {
                Key.ctrl: "ctrl",
                Key.alt: "alt",
                Key.alt_gr: "alt",
                Key.shift: "shift",
                Key.shift_r: "shift",
                Key.shift_l: "shift",
                Key.cmd: "cmd",
                Key.cmd_r: "cmd",
                Key.cmd_l: "cmd",
                Key.space: "space",
                Key.tab: "tab",
                Key.enter: "enter",
                Key.return_: "enter",
                Key.escape: "escape",
                Key.esc: "escape",
                Key.backspace: "backspace",
                Key.delete: "delete",
                Key.up: "up",
                Key.down: "down",
                Key.left: "left",
                Key.right: "right",
                Key.home: "home",
                Key.end: "end",
                Key.page_up: "page_up",
                Key.page_down: "page_down",
                Key.f1: "f1", Key.f2: "f2", Key.f3: "f3", Key.f4: "f4",
                Key.f5: "f5", Key.f6: "f6", Key.f7: "f7", Key.f8: "f8",
                Key.f9: "f9", Key.f10: "f10", Key.f11: "f11", Key.f12: "f12",
            }
            return key_map.get(key)
        
        return None

    # ==================== Hotkey to Workflow/Action Mapping ====================

    def map_hotkey_to_workflow(
        self,
        keys: str,
        workflow_id: str,
        contexts: Optional[List[str]] = None,
        profile: str = "default",
    ) -> Optional[HotkeyConflict]:
        """Map a hotkey to launch a specific workflow"""
        return self.register_hotkey(
            keys=keys,
            action_type=HotkeyActionType.WORKFLOW_LAUNCH,
            action_target=workflow_id,
            contexts=contexts,
            profile=profile,
            description=f"Launch workflow: {workflow_id}",
        )

    def map_hotkey_to_action(
        self,
        keys: str,
        action_id: str,
        contexts: Optional[List[str]] = None,
        profile: str = "default",
    ) -> Optional[HotkeyConflict]:
        """Map a hotkey to execute a specific action"""
        return self.register_hotkey(
            keys=keys,
            action_type=HotkeyActionType.ACTION_EXECUTE,
            action_target=action_id,
            contexts=contexts,
            profile=profile,
            description=f"Execute action: {action_id}",
        )

    def set_workflow_launcher(self, launcher: Callable[[str], None]) -> None:
        """Set the callback function to launch workflows"""
        self._workflow_launcher = launcher

    def set_action_executor(self, executor: Callable[[str, Any], Any]) -> None:
        """Set the callback function to execute actions"""
        self._action_executor = executor

    # ==================== Context-Aware Hotkeys ====================

    def add_context_rule(
        self,
        context_name: str,
        app_bundle_id: Optional[str] = None,
        app_name: Optional[str] = None,
        window_title_pattern: Optional[str] = None,
    ) -> ContextRule:
        """Add a rule for context-aware hotkey activation"""
        rule = ContextRule(
            app_bundle_id=app_bundle_id,
            app_name=app_name,
            window_title_pattern=window_title_pattern,
            active=True,
        )
        self._context_rules.append(rule)
        
        # Create context-specific bindings
        if context_name not in self._bindings_by_context:
            self._bindings_by_context[context_name] = []
        
        logger.info(f"Added context rule: {context_name}")
        return rule

    def set_foreground_app(self, app_info: Dict) -> None:
        """Update the current foreground application info"""
        self._foreground_app_info = app_info
        
        # Determine active context
        for rule in self._context_rules:
            if rule.matches(app_info):
                if self._current_context != rule.app_name or rule.app_bundle_id:
                    old_context = self._current_context
                    self._current_context = rule.app_name or rule.app_bundle_id or "default"
                    logger.debug(f"Context switched: {old_context} -> {self._current_context}")
                break
        else:
            self._current_context = "default"

    def get_active_bindings(self) -> List[HotkeyBinding]:
        """Get all bindings active in the current context and profile"""
        active_profile = self._profiles.get(self._active_profile_name)
        if not active_profile:
            return []
        
        active_bindings = []
        for binding in active_profile.bindings:
            if not binding.enabled:
                continue
            
            # Check if binding matches current context
            if not binding.contexts or self._current_context in binding.contexts or self._current_context == "default":
                active_bindings.append(binding)
        
        return active_bindings

    @contextmanager
    def temporary_context(self, context_name: str):
        """Temporarily switch to a different context"""
        old_context = self._current_context
        self._current_context = context_name
        try:
            yield
        finally:
            self._current_context = old_context

    # ==================== Hotkey Sequences ====================

    def register_sequence(
        self,
        keys: List[str],
        action_type: HotkeyActionType,
        action_target: str,
        profile: str = "default",
    ) -> List[Optional[HotkeyConflict]]:
        """
        Register a sequence of hotkeys (like Emacs C-x C-c).
        
        Args:
            keys: List of key combinations in order (e.g., ["ctrl+x", "ctrl+c"])
            action_type: Type of action to trigger on sequence completion
            action_target: Target of the action
            profile: Profile name
            
        Returns:
            List of conflicts (one per key in sequence)
        """
        conflicts = []
        
        for i, key in enumerate(keys):
            is_start = (i == 0)
            continuation = keys[i + 1] if i < len(keys) - 1 else None
            
            conflict = self.register_hotkey(
                keys=key,
                action_type=HotkeyActionType.SEQUENCE_PART if i > 0 else action_type,
                action_target=action_target if i == len(keys) - 1 else keys[i + 1],
                profile=profile,
                description=f"Sequence {'start' if is_start else 'part ' + str(i)}",
                is_sequence_start=is_start,
                sequence_continuation=continuation,
            )
            conflicts.append(conflict)
        
        return conflicts

    def _process_sequence(self, key_str: str) -> Optional[HotkeyBinding]:
        """Process a potential sequence key press"""
        current_time = time.time()
        
        # Check timeout
        if current_time - self._last_key_time > self._sequence_timeout:
            self._sequence_buffer.clear()
        
        self._last_key_time = current_time
        self._sequence_buffer.append(key_str)
        
        # Check for sequence match
        active_bindings = self.get_active_bindings()
        
        for binding in active_bindings:
            if binding.is_sequence_start and binding.keys == key_str:
                # This is a sequence start
                if binding.sequence_continuation:
                    # Wait for continuation
                    return None
                else:
                    # Complete sequence (single key)
                    self._sequence_buffer.clear()
                    return binding
        
        # Check if current buffer matches any sequence
        buffer_str = "+".join(self._sequence_buffer)
        for binding in active_bindings:
            if binding.keys == buffer_str:
                self._sequence_buffer.clear()
                return binding
        
        return None

    # ==================== Hotkey Profiles ====================

    def create_profile(
        self,
        name: str,
        description: str = "",
        priority: int = 0,
        copy_from: Optional[str] = None,
    ) -> HotkeyProfile:
        """Create a new hotkey profile"""
        if name in self._profiles:
            raise ValueError(f"Profile '{name}' already exists")
        
        if copy_from and copy_from in self._profiles:
            source = self._profiles[copy_from]
            bindings = [HotkeyBinding(**asdict(b)) for b in source.bindings]
            profile = HotkeyProfile(
                name=name,
                description=description or source.description,
                bindings=bindings,
                priority=priority,
            )
        else:
            profile = HotkeyProfile(
                name=name,
                description=description,
                priority=priority,
            )
        
        self._profiles[name] = profile
        logger.info(f"Created hotkey profile: {name}")
        return profile

    def delete_profile(self, name: str) -> bool:
        """Delete a hotkey profile"""
        if name == "default":
            return False
        
        if name in self._profiles:
            # Remove bindings from indexes
            for binding in self._profiles[name].bindings:
                self._bindings_by_key[binding.keys].remove(binding)
                for ctx in binding.contexts:
                    self._bindings_by_context[ctx].remove(binding)
            
            del self._profiles[name]
            logger.info(f"Deleted hotkey profile: {name}")
            return True
        
        return False

    def activate_profile(self, name: str) -> bool:
        """Activate a hotkey profile"""
        if name not in self._profiles:
            return False
        
        # Deactivate all profiles
        for profile in self._profiles.values():
            profile.is_active = False
        
        # Activate selected profile
        self._profiles[name].is_active = True
        self._active_profile_name = name
        
        # Call profile switcher if set
        if self._profile_switcher:
            self._profile_switcher(name)
        
        logger.info(f"Activated hotkey profile: {name}")
        return True

    def get_profile(self, name: str) -> Optional[HotkeyProfile]:
        """Get a profile by name"""
        return self._profiles.get(name)

    def list_profiles(self) -> List[str]:
        """List all profile names"""
        return list(self._profiles.keys())

    def set_profile_switcher(self, switcher: Callable[[str], None]) -> None:
        """Set the callback for profile switches"""
        self._profile_switcher = switcher

    # ==================== Quick-Launch Hotkeys ====================

    def set_quick_launch_hotkey(
        self,
        keys: str,
        workflow_id: str,
        profile: str = "default",
    ) -> Optional[HotkeyConflict]:
        """Set a hotkey to quick-launch a workflow"""
        return self.register_hotkey(
            keys=keys,
            action_type=HotkeyActionType.QUICK_LAUNCH,
            action_target=workflow_id,
            profile=profile,
            description=f"Quick launch: {workflow_id}",
        )

    def get_quick_launch_bindings(self) -> List[HotkeyBinding]:
        """Get all quick-launch bindings in the active profile"""
        active_profile = self._profiles.get(self._active_profile_name)
        if not active_profile:
            return []
        
        return [
            b for b in active_profile.bindings
            if b.action_type == HotkeyActionType.QUICK_LAUNCH and b.enabled
        ]

    # ==================== Hotkey Conflict Detection ====================

    def _detect_conflict(
        self,
        keys: str,
        profile: str,
        contexts: Optional[List[str]] = None,
    ) -> Optional[HotkeyConflict]:
        """Detect conflicts for a new hotkey binding"""
        target_profile = self._profiles.get(profile)
        if not target_profile:
            return None
        
        # Check for exact match conflict
        for binding in target_profile.bindings:
            if binding.keys == keys:
                # Check if contexts overlap
                if not binding.contexts or not contexts:
                    return HotkeyConflict(
                        conflict_type=ConflictType.EXACT_MATCH,
                        binding1=binding,
                        binding2=HotkeyBinding(
                            id="new",
                            keys=keys,
                            action_type=HotkeyActionType.CUSTOM,
                            action_target="",
                            contexts=contexts or [],
                            profile=profile,
                        ),
                        message=f"Exact conflict: '{keys}' already bound in profile '{profile}'",
                    )
                
                # Check context overlap
                context_overlap = set(binding.contexts) & set(contexts)
                if context_overlap:
                    return HotkeyConflict(
                        conflict_type=ConflictType.CONTEXT_CONFLICT,
                        binding1=binding,
                        binding2=HotkeyBinding(
                            id="new",
                            keys=keys,
                            action_type=HotkeyActionType.CUSTOM,
                            action_target="",
                            contexts=contexts,
                            profile=profile,
                        ),
                        message=f"Context conflict: '{keys}' overlaps in contexts {context_overlap}",
                    )
        
        return None

    def detect_all_conflicts(self) -> List[HotkeyConflict]:
        """Detect all hotkey conflicts across all profiles"""
        self._conflicts.clear()
        
        all_bindings: List[HotkeyBinding] = []
        for profile in self._profiles.values():
            all_bindings.extend(profile.bindings)
        
        # Check each pair
        for i, binding1 in enumerate(all_bindings):
            for binding2 in all_bindings[i + 1:]:
                if binding1.keys != binding2.keys:
                    continue
                
                # Check context overlap
                if binding1.contexts and binding2.contexts:
                    overlap = set(binding1.contexts) & set(binding2.contexts)
                    if overlap:
                        conflict = HotkeyConflict(
                            conflict_type=ConflictType.CONTEXT_CONFLICT,
                            binding1=binding1,
                            binding2=binding2,
                            message=f"Context conflict in '{overlap}': '{binding1.keys}' bound to both {binding1.action_target} and {binding2.action_target}",
                        )
                        self._conflicts.append(conflict)
                elif not binding1.contexts or not binding2.contexts:
                    # One has no context (applies everywhere)
                    conflict = HotkeyConflict(
                        conflict_type=ConflictType.EXACT_MATCH,
                        binding1=binding1,
                        binding2=binding2,
                        message=f"Conflict: '{binding1.keys}' bound to both {binding1.action_target} and {binding2.action_target}",
                    )
                    self._conflicts.append(conflict)
        
        return self._conflicts

    def get_conflicts(self) -> List[HotkeyConflict]:
        """Get the list of detected conflicts"""
        return self._conflicts.copy()

    # ==================== Hotkey Recording ====================

    def start_recording(self, callback: Optional[Callable[[str], None]] = None) -> None:
        """
        Start recording hotkeys.
        
        Args:
            callback: Optional callback to invoke with each recorded key combination
        """
        self._is_recording = True
        self._recorded_keys.clear()
        self._recording_callback = callback
        logger.info("Hotkey recording started")

    def stop_recording(self) -> List[str]:
        """
        Stop recording and return the recorded hotkey combinations.
        
        Returns:
            List of normalized hotkey strings
        """
        self._is_recording = False
        logger.info(f"Hotkey recording stopped. Recorded {len(self._recorded_keys)} events")
        
        # Convert recorded keys to combinations
        combinations = self._extract_combinations()
        return combinations

    def _extract_combinations(self) -> List[str]:
        """Extract key combinations from recorded key events"""
        if not self._recorded_keys:
            return []
        
        combinations = []
        current_combo: Set[str] = set()
        last_was_press = False
        
        for is_press, key in self._recorded_keys:
            key_str = self._keys_from_event(key, is_press)
            if key_str is None:
                continue
            
            if is_press:
                current_combo.add(key_str)
                last_was_press = True
            else:
                if last_was_press and current_combo:
                    combinations.append("+".join(sorted(current_combo)))
                    current_combo = set()
                last_was_press = False
        
        # Handle any remaining keys
        if current_combo:
            combinations.append("+".join(sorted(current_combo)))
        
        return combinations

    def _on_key_event(self, key: Any, is_press: bool) -> None:
        """Handle a key event from the listener"""
        # Handle recording
        if self._is_recording:
            self._recorded_keys.append((is_press, key))
            if self._recording_callback:
                key_str = self._keys_from_event(key, is_press)
                if key_str:
                    self._recording_callback(key_str)
        
        # Get normalized key string
        key_str = self._keys_from_event(key, is_press)
        if key_str is None:
            return
        
        # Only process key press events
        if not is_press:
            return
        
        # Try to process as sequence
        binding = self._process_sequence(key_str)
        
        if binding and binding.enabled:
            self._execute_binding(binding)

    def _execute_binding(self, binding: HotkeyBinding) -> None:
        """Execute the action associated with a binding"""
        logger.info(f"Executing hotkey: {binding.keys} -> {binding.action_type.name}")
        
        try:
            if binding.action_type == HotkeyActionType.WORKFLOW_LAUNCH:
                if self._workflow_launcher:
                    self._workflow_launcher(binding.action_target)
            
            elif binding.action_type == HotkeyActionType.QUICK_LAUNCH:
                if self._workflow_launcher:
                    self._workflow_launcher(binding.action_target)
            
            elif binding.action_type == HotkeyActionType.ACTION_EXECUTE:
                if self._action_executor:
                    self._action_executor(binding.action_target, binding.metadata)
            
            elif binding.action_type == HotkeyActionType.SEQUENCE_PART:
                # Sequence continuation handled by _process_sequence
                pass
            
            elif binding.action_type == HotkeyActionType.PROFILE_SWITCH:
                self.activate_profile(binding.action_target)
            
            elif binding.action_type == HotkeyActionType.WORKFLOW_STOP:
                if self._workflow_launcher:
                    self._workflow_launcher(f"stop:{binding.action_target}")
            
            elif binding.action_type == HotkeyActionType.WORKFLOW_PAUSE:
                if self._workflow_launcher:
                    self._workflow_launcher(f"pause:{binding.action_target}")
            
            else:
                logger.warning(f"Unknown action type: {binding.action_type}")
        
        except Exception as e:
            logger.error(f"Error executing hotkey binding: {e}")

    # ==================== macOS Integration ====================

    def _get_macos_bundle_id(self, app_name: str) -> Optional[str]:
        """Get macOS bundle ID from app name (simplified)"""
        # Common bundle IDs for macOS
        bundle_id_map = {
            "safari": "com.apple.Safari",
            "chrome": "com.google.Chrome",
            "firefox": "org.mozilla.firefox",
            "terminal": "com.apple.Terminal",
            "iterm": "com.googlecode.iterm2",
            "xcode": "com.apple.dt.Xcode",
            "vscode": "com.microsoft.VSCode",
            "sublime": "com.sublimetext.4",
            " finder": "com.apple.finder",
        }
        return bundle_id_map.get(app_name.lower())

    def update_foreground_app_macos(self) -> None:
        """Update foreground app info using macOS APIs (requires accessibility permissions)"""
        if self._platform != "Darwin":
            return
        
        try:
            # Use AppleScript to get frontmost app info
            import subprocess
            script = '''
            tell application "System Events"
                set frontApp to first application process whose frontmost is true
                set appName to name of frontApp
                set bundleId to bundle identifier of frontApp
            end tell
            
            tell application "System Events"
                set windowTitle to name of front window of frontApp
            end tell
            
            return appName & "|" & bundleId & "|" & windowTitle
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2,
            )
            
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split("|")
                if len(parts) >= 2:
                    self.set_foreground_app({
                        "name": parts[0],
                        "bundle_id": parts[1],
                        "window_title": parts[2] if len(parts) > 2 else "",
                    })
        except Exception as e:
            logger.debug(f"Could not get foreground app: {e}")

    def request_accessibility_permissions(self) -> bool:
        """Request accessibility permissions on macOS"""
        if self._platform != "Darwin":
            return True
        
        try:
            import subprocess
            # Open System Preferences to Accessibility pane
            subprocess.run(
                ["open", "x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility"],
                check=False,
            )
            logger.info("Opened macOS accessibility permissions settings")
            return True
        except Exception as e:
            logger.error(f"Could not open accessibility settings: {e}")
            return False

    def check_accessibility_permissions(self) -> bool:
        """Check if accessibility permissions are granted on macOS"""
        if self._platform != "Darwin":
            return True
        
        try:
            import subprocess
            result = subprocess.run(
                ["osascript", "-e", "tell application \"System Events\" to return true"],
                capture_output=True,
                timeout=2,
            )
            return result.returncode == 0
        except Exception:
            return False

    # ==================== Lifecycle Management ====================

    def start(self) -> bool:
        """
        Start the global hotkey listener.
        
        Returns:
            True if started successfully, False otherwise
        """
        if self._running:
            logger.warning("Hotkey manager already running")
            return True
        
        # Check permissions on macOS
        if self._platform == "Darwin":
            if not self.check_accessibility_permissions():
                logger.warning("Accessibility permissions not granted. Hotkeys may not work in background.")
                # Don't fail, just warn
        
        try:
            self._running = True
            
            # Create and start listener
            self._listener = KeyboardListener(
                on_press=lambda k: self._on_key_event(k, True),
                on_release=lambda k: self._on_key_event(k, False),
                suppress=False,
            )
            
            # Start in a separate thread
            self._listener_thread = threading.Thread(
                target=self._listener.run,
                daemon=True,
            )
            self._listener_thread.start()
            
            logger.info("Hotkey manager started")
            return True
        
        except Exception as e:
            self._running = False
            logger.error(f"Failed to start hotkey manager: {e}")
            return False

    def stop(self) -> None:
        """Stop the global hotkey listener"""
        if not self._running:
            return
        
        self._running = False
        
        if self._listener:
            self._listener.stop()
            self._listener = None
        
        logger.info("Hotkey manager stopped")

    def is_running(self) -> bool:
        """Check if the hotkey manager is running"""
        return self._running

    # ==================== Configuration Persistence ====================

    def save_config(self, path: Optional[str] = None) -> str:
        """
        Save hotkey configuration to a file.
        
        Returns:
            Path to the saved configuration file
        """
        config_path = path or os.path.join(self.config_dir, "hotkeys_config.json")
        
        config = {
            "profiles": {name: profile.to_dict() for name, profile in self._profiles.items()},
            "active_profile": self._active_profile_name,
            "context_rules": [
                {
                    "app_bundle_id": rule.app_bundle_id,
                    "app_name": rule.app_name,
                    "window_title_pattern": rule.window_title_pattern,
                    "active": rule.active,
                }
                for rule in self._context_rules
            ],
            "sequence_timeout": self._sequence_timeout,
        }
        
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Hotkey configuration saved to {config_path}")
        return config_path

    def load_config(self, path: Optional[str] = None) -> bool:
        """
        Load hotkey configuration from a file.
        
        Returns:
            True if loaded successfully, False otherwise
        """
        config_path = path or os.path.join(self.config_dir, "hotkeys_config.json")
        
        if not os.path.exists(config_path):
            logger.warning(f"Configuration file not found: {config_path}")
            return False
        
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # Clear existing profiles
            self._profiles.clear()
            self._bindings_by_key.clear()
            self._bindings_by_context.clear()
            
            # Load profiles
            for name, profile_data in config.get("profiles", {}).items():
                profile = HotkeyProfile.from_dict(profile_data)
                self._profiles[name] = profile
                
                # Re-index bindings
                for binding in profile.bindings:
                    self._bindings_by_key[binding.keys].append(binding)
                    for ctx in binding.contexts:
                        self._bindings_by_context[ctx].append(binding)
            
            # Set active profile
            active = config.get("active_profile", "default")
            if active in self._profiles:
                self.activate_profile(active)
            
            # Load context rules
            self._context_rules.clear()
            for rule_data in config.get("context_rules", []):
                rule = ContextRule(
                    app_bundle_id=rule_data.get("app_bundle_id"),
                    app_name=rule_data.get("app_name"),
                    window_title_pattern=rule_data.get("window_title_pattern"),
                    active=rule_data.get("active", True),
                )
                self._context_rules.append(rule)
            
            # Load sequence timeout
            self._sequence_timeout = config.get("sequence_timeout", 1.0)
            
            logger.info(f"Hotkey configuration loaded from {config_path}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return False

    # ==================== Utility Methods ====================

    def get_binding(self, binding_id: str) -> Optional[HotkeyBinding]:
        """Get a binding by its ID"""
        for profile in self._profiles.values():
            for binding in profile.bindings:
                if binding.id == binding_id:
                    return binding
        return None

    def get_bindings_for_workflow(self, workflow_id: str) -> List[HotkeyBinding]:
        """Get all bindings associated with a workflow"""
        bindings = []
        for profile in self._profiles.values():
            for binding in profile.bindings:
                if binding.action_target == workflow_id:
                    bindings.append(binding)
        return bindings

    def get_bindings_for_key(self, keys: str) -> List[HotkeyBinding]:
        """Get all bindings for a specific key combination"""
        normalized = self._normalize_keys(keys)
        return self._bindings_by_key.get(normalized, []).copy()

    def enable_binding(self, binding_id: str) -> bool:
        """Enable a hotkey binding"""
        binding = self.get_binding(binding_id)
        if binding:
            binding.enabled = True
            return True
        return False

    def disable_binding(self, binding_id: str) -> bool:
        """Disable a hotkey binding"""
        binding = self.get_binding(binding_id)
        if binding:
            binding.enabled = False
            return True
        return False

    def update_binding(self, binding_id: str, **kwargs) -> bool:
        """Update properties of a binding"""
        binding = self.get_binding(binding_id)
        if not binding:
            return False
        
        for key, value in kwargs.items():
            if hasattr(binding, key):
                setattr(binding, key, value)
        
        return True

    def __enter__(self) -> "WorkflowHotkeyManager":
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.stop()


# ==================== Factory Function ====================

def create_hotkey_manager(config_dir: Optional[str] = None) -> WorkflowHotkeyManager:
    """Create and return a new WorkflowHotkeyManager instance"""
    return WorkflowHotkeyManager(config_dir=config_dir)


# ==================== Main Entry Point ====================

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    manager = WorkflowHotkeyManager()
    
    # Register some hotkeys
    manager.map_hotkey_to_workflow("ctrl+shift+r", "my_workflow")
    manager.register_sequence(
        ["ctrl+x", "ctrl+c"],
        HotkeyActionType.WORKFLOW_LAUNCH,
        "emacs_like_workflow",
    )
    
    # Create a profile
    manager.create_profile("gaming", priority=10)
    manager.set_quick_launch_hotkey("f1", "quick_game_workflow", profile="gaming")
    
    # Start listening
    with manager:
        print("Hotkey manager running. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nExiting...")
