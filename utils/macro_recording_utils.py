"""Macro recording utilities for capturing and replaying multi-step workflows.

Supports recording sequences of automation actions as named macros,
storing them in a registry, and replaying them on demand with
variable substitution and conditional execution.

Example:
    >>> from utils.macro_recording_utils import MacroRecorder, MacroRegistry
    >>> registry = MacroRegistry()
    >>> recorder = MacroRecorder('login_flow')
    >>> recorder.start()
    >>> # ... perform steps ...
    >>> macro = recorder.stop()
    >>> registry.save(macro)
    >>> registry.play('login_flow')
"""

from __future__ import annotations

import time
import uuid
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

__all__ = [
    "Macro",
    "MacroStep",
    "MacroRecorder",
    "MacroReplayer",
    "MacroRegistry",
    "MacroError",
]


@dataclass
class MacroStep:
    """A single step within a macro.

    Attributes:
        id: Unique step identifier.
        name: Human-readable step name.
        action_type: Type of action (click, type, wait, etc.).
        params: Action parameters as a dictionary.
        timestamp: When the step was recorded.
        description: Optional description of what this step does.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    action_type: str = "custom"
    params: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    description: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "action_type": self.action_type,
            "params": self.params,
            "timestamp": self.timestamp,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MacroStep":
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            name=d.get("name", ""),
            action_type=d.get("action_type", "custom"),
            params=d.get("params", {}),
            timestamp=d.get("timestamp", 0.0),
            description=d.get("description", ""),
        )


@dataclass
class Macro:
    """A named macro containing multiple steps.

    Attributes:
        id: Unique macro identifier.
        name: Human-readable name.
        description: What this macro does.
        steps: Ordered list of MacroSteps.
        created_at: Creation timestamp.
        modified_at: Last modification timestamp.
        tags: Tags for categorization.
        variables: Variable definitions for this macro.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    steps: list[MacroStep] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    tags: list[str] = field(default_factory=list)
    variables: dict[str, Any] = field(default_factory=dict)

    def add_step(self, step: MacroStep) -> "Macro":
        self.steps.append(step)
        self.modified_at = time.time()
        return self

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "steps": [s.to_dict() for s in self.steps],
            "created_at": self.created_at,
            "modified_at": self.modified_at,
            "tags": self.tags,
            "variables": self.variables,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Macro":
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            name=d.get("name", ""),
            description=d.get("description", ""),
            steps=[MacroStep.from_dict(s) for s in d.get("steps", [])],
            created_at=d.get("created_at", time.time()),
            modified_at=d.get("modified_at", time.time()),
            tags=d.get("tags", []),
            variables=d.get("variables", {}),
        )

    def substitute(self, text: str, context: Optional[dict[str, Any]] = None) -> str:
        """Substitute {{variable}} placeholders in text.

        Args:
            text: Text containing {{var}} placeholders.
            context: Optional variable context (uses self.variables if None).

        Returns:
            Text with substituted values.
        """
        ctx = context or self.variables
        result = text
        for key, val in ctx.items():
            result = result.replace(f"{{{{{key}}}}}", str(val))
        return result

    def resolve_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Resolve {{variable}} placeholders in step parameters.

        Args:
            params: Raw parameters from a step.

        Returns:
            Parameters with resolved variables.
        """
        resolved: dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, str):
                resolved[key] = self.substitute(value)
            elif isinstance(value, dict):
                resolved[key] = {
                    k: self.substitute(v) if isinstance(v, str) else v
                    for k, v in value.items()
                }
            elif isinstance(value, list):
                resolved[key] = [
                    self.substitute(v) if isinstance(v, str) else v
                    for v in value
                ]
            else:
                resolved[key] = value
        return resolved


class MacroError(Exception):
    """Raised when a macro operation fails."""
    pass


class MacroRecorder:
    """Records a sequence of actions as a named macro.

    Example:
        >>> recorder = MacroRecorder('my_macro', description='Does X')
        >>> recorder.start()
        >>> recorder.record_step('click', {'x': 100, 'y': 200}, name='Click button')
        >>> recorder.record_step('type', {'text': 'hello'}, name='Type text')
        >>> macro = recorder.stop()
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        tags: Optional[list[str]] = None,
    ):
        self.name = name
        self.description = description
        self.tags = tags or []
        self._macro = Macro(name=name, description=description, tags=self.tags)
        self._start_time: Optional[float] = None
        self._running = False

    def start(self) -> None:
        """Start recording steps."""
        self._macro = Macro(
            name=self.name,
            description=self.description,
            tags=self.tags,
        )
        self._start_time = time.time()
        self._running = True

    def stop(self) -> Macro:
        """Stop recording and return the completed macro."""
        self._running = False
        return self._macro

    @property
    def is_recording(self) -> bool:
        return self._running

    def record_step(
        self,
        action_type: str,
        params: Optional[dict[str, Any]] = None,
        name: str = "",
        description: str = "",
    ) -> MacroStep:
        """Record a single action step.

        Args:
            action_type: Type of action (click, type, etc.).
            params: Action parameters.
            name: Human-readable step name.
            description: Optional description.

        Returns:
            The created MacroStep.
        """
        if not self._running:
            raise MacroError("Recorder is not running. Call start() first.")

        step = MacroStep(
            name=name or action_type,
            action_type=action_type,
            params=params or {},
            description=description,
        )
        self._macro.add_step(step)
        return step

    def get_macro(self) -> Macro:
        """Get the current macro (without stopping)."""
        return self._macro


class MacroReplayer:
    """Replays a macro with variable substitution and error handling.

    Example:
        >>> replayer = MacroReplayer(macro)
        >>> replayer.set_variable('username', 'test')
        >>> replayer.play()
    """

    def __init__(self, macro: Macro):
        self.macro = macro
        self._context: dict[str, Any] = dict(macro.variables)
        self._aborted = False

    def set_variable(self, key: str, value: Any) -> "MacroReplayer":
        """Set a variable value for substitution during replay."""
        self._context[key] = value
        return self

    def set_variables(self, variables: dict[str, Any]) -> "MacroReplayer":
        """Set multiple variable values at once."""
        self._context.update(variables)
        return self

    def abort(self) -> None:
        """Abort the currently playing replay."""
        self._aborted = True

    def play(
        self,
        stop_on_error: bool = True,
    ) -> list[tuple[Optional[MacroStep], Optional[Exception]]]:
        """Replay all macro steps.

        Args:
            stop_on_error: If True, stop on the first error.

        Returns:
            List of (step, error) tuples for each step.
        """
        self._aborted = False
        results: list[tuple[Optional[MacroStep], Optional[Exception]]] = []

        try:
            from utils.input_simulation_utils import (
                click,
                double_click,
                right_click,
                drag,
                scroll,
                type_text,
                press_key,
            )
        except ImportError:
            pass

        for step in self.macro.steps:
            if self._aborted:
                break

            try:
                params = self.macro.resolve_params(step.params)
                self._dispatch_step(step, params)
                results.append((step, None))
            except Exception as e:
                if stop_on_error:
                    results.append((step, e))
                    return results
                results.append((step, e))

        return results

    def _dispatch_step(self, step: MacroStep, params: dict[str, Any]) -> None:
        """Dispatch a single step to the input layer."""
        from utils.input_simulation_utils import (
            click,
            double_click,
            right_click,
            drag,
            scroll,
            type_text,
            press_key,
        )
        from utils.timing_utils import precise_delay

        at = step.action_type

        if at == "click":
            click(params.get("x", 0), params.get("y", 0), button=params.get("button", "left"))
        elif at == "right_click":
            right_click(params.get("x", 0), params.get("y", 0))
        elif at == "double_click":
            double_click(params.get("x", 0), params.get("y", 0))
        elif at == "drag":
            drag(
                params.get("x1", 0),
                params.get("y1", 0),
                params.get("x2", 0),
                params.get("y2", 0),
                params.get("duration", 0.3),
            )
        elif at == "scroll":
            scroll(params.get("dx", 0), params.get("dy", 0))
        elif at == "key_press":
            press_key(params.get("key", ""))
        elif at == "type":
            type_text(params.get("text", ""))
        elif at == "wait":
            precise_delay(params.get("duration", 1.0))
        elif at == "custom":
            # Custom actions can be dispatched via callback
            callback = params.get("_callback")
            if callback and callable(callback):
                callback(params)


class MacroRegistry:
    """Persistent storage for macros.

    Example:
        >>> registry = MacroRegistry('/tmp/macros')
        >>> registry.save(my_macro)
        >>> macros = registry.list()
        >>> registry.play('login')
    """

    def __init__(self, storage_dir: str | Path = "/tmp/macros"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._index_path = self.storage_dir / "index.json"
        self._load_index()

    def _load_index(self) -> None:
        if self._index_path.exists():
            try:
                self._index = json.loads(self._index_path.read_text())
            except Exception:
                self._index = {}
        else:
            self._index: dict[str, dict] = {}

    def _save_index(self) -> None:
        self._index_path.write_text(json.dumps(self._index, indent=2))

    def save(self, macro: Macro, overwrite: bool = True) -> Macro:
        """Save a macro to the registry.

        Args:
            macro: Macro to save.
            overwrite: If False, raise an error if name already exists.

        Returns:
            The saved macro.
        """
        if macro.name in self._index and not overwrite:
            raise MacroError(f"Macro '{macro.name}' already exists")

        macro_file = self.storage_dir / f"{macro.id}.json"
        macro_file.write_text(json.dumps(macro.to_dict(), indent=2))
        self._index[macro.name] = {
            "id": macro.id,
            "name": macro.name,
            "file": macro_file.name,
        }
        self._save_index()
        return macro

    def load(self, name: str) -> Macro:
        """Load a macro by name.

        Args:
            name: Macro name.

        Returns:
            Loaded Macro object.
        """
        if name not in self._index:
            raise MacroError(f"Macro '{name}' not found")

        meta = self._index[name]
        macro_file = self.storage_dir / meta["file"]
        return Macro.from_dict(json.loads(macro_file.read_text()))

    def delete(self, name: str) -> None:
        """Delete a macro from the registry."""
        if name not in self._index:
            raise MacroError(f"Macro '{name}' not found")

        meta = self._index.pop(name)
        macro_file = self.storage_dir / meta["file"]
        if macro_file.exists():
            macro_file.unlink()
        self._save_index()

    def list(self) -> list[str]:
        """List all macro names in the registry."""
        return list(self._index.keys())

    def exists(self, name: str) -> bool:
        """Check if a macro exists."""
        return name in self._index

    def play(
        self,
        name: str,
        variables: Optional[dict[str, Any]] = None,
    ) -> list[tuple[Optional[MacroStep], Optional[Exception]]]:
        """Load and play a macro by name.

        Args:
            name: Macro name.
            variables: Optional variable overrides.

        Returns:
            List of (step, error) tuples.
        """
        macro = self.load(name)
        replayer = MacroReplayer(macro)
        if variables:
            replayer.set_variables(variables)
        return replayer.play()
