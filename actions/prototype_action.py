"""Prototype action module for RabAI AutoClick.

Provides prototype pattern implementation:
- Prototype: Abstract prototype interface
- ConcretePrototype: Specific prototype implementations
- CloneRegistry: Registry for prototypes
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar
from abc import ABC, abstractmethod
import copy
import uuid
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Prototype(ABC):
    """Abstract prototype interface."""

    @abstractmethod
    def clone(self) -> "Prototype":
        """Create a clone."""
        pass

    @abstractmethod
    def deep_clone(self) -> "Prototype":
        """Create a deep clone."""
        pass


class DataPrototype(Prototype):
    """Prototype for data objects."""

    def __init__(self, data: Dict[str, Any], prototype_id: Optional[str] = None):
        self._data = data
        self._prototype_id = prototype_id or str(uuid.uuid4())

    @property
    def prototype_id(self) -> str:
        """Get prototype ID."""
        return self._prototype_id

    @property
    def data(self) -> Dict[str, Any]:
        """Get data."""
        return self._data

    def clone(self) -> "DataPrototype":
        """Create a shallow clone."""
        return DataPrototype(self._data.copy(), f"{self._prototype_id}_clone")

    def deep_clone(self) -> "DataPrototype":
        """Create a deep clone."""
        return DataPrototype(copy.deepcopy(self._data), f"{self._prototype_id}_deep_clone")

    def set_data(self, key: str, value: Any) -> None:
        """Set data field."""
        self._data[key] = value

    def get_data(self, key: str) -> Any:
        """Get data field."""
        return self._data.get(key)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "prototype_id": self._prototype_id,
            "data": self._data,
        }


class ConfigPrototype(Prototype):
    """Prototype for configuration objects."""

    def __init__(self, config: Dict[str, Any], name: str = ""):
        self._config = config
        self._name = name or str(uuid.uuid4())
        self._version = 1

    @property
    def name(self) -> str:
        """Get name."""
        return self._name

    @property
    def version(self) -> int:
        """Get version."""
        return self._version

    def clone(self) -> "ConfigPrototype":
        """Create a shallow clone."""
        new_config = self._config.copy()
        clone = ConfigPrototype(new_config, f"{self._name}_v{self._version + 1}")
        return clone

    def deep_clone(self) -> "ConfigPrototype":
        """Create a deep clone."""
        new_config = copy.deepcopy(self._config)
        clone = ConfigPrototype(new_config, f"{self._name}_v{self._version + 1}")
        return clone

    def get(self, key: str, default: Any = None) -> Any:
        """Get config value."""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    def set(self, key: str, value: Any) -> None:
        """Set config value."""
        keys = key.split(".")
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value


class DocumentPrototype(Prototype):
    """Prototype for document objects."""

    def __init__(self, title: str = "", body: str = "", metadata: Optional[Dict] = None):
        self._title = title
        self._body = body
        self._metadata = metadata or {}
        self._document_id = str(uuid.uuid4())
        self._created_at = 0
        self._modified_at = 0

    @property
    def document_id(self) -> str:
        """Get document ID."""
        return self._document_id

    @property
    def title(self) -> str:
        """Get title."""
        return self._title

    @property
    def body(self) -> str:
        """Get body."""
        return self._body

    def clone(self) -> "DocumentPrototype":
        """Create a shallow clone."""
        doc = DocumentPrototype(self._title, self._body, self._metadata.copy())
        return doc

    def deep_clone(self) -> "DocumentPrototype":
        """Create a deep clone."""
        doc = DocumentPrototype(
            self._title,
            self._body,
            copy.deepcopy(self._metadata),
        )
        return doc

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "document_id": self._document_id,
            "title": self._title,
            "body": self._body,
            "metadata": self._metadata,
        }


class CloneRegistry:
    """Registry for managing prototypes."""

    def __init__(self):
        self._prototypes: Dict[str, Prototype] = {}
        self._lock = False

    def register(self, key: str, prototype: Prototype) -> None:
        """Register a prototype."""
        self._prototypes[key] = prototype

    def unregister(self, key: str) -> bool:
        """Unregister a prototype."""
        if key in self._prototypes:
            del self._prototypes[key]
            return True
        return False

    def get(self, key: str) -> Optional[Prototype]:
        """Get a prototype."""
        return self._prototypes.get(key)

    def clone(self, key: str, deep: bool = False) -> Optional[Prototype]:
        """Clone a prototype."""
        prototype = self._prototypes.get(key)
        if prototype is None:
            return None
        if deep:
            return prototype.deep_clone()
        return prototype.clone()

    def list_keys(self) -> List[str]:
        """List all registered keys."""
        return list(self._prototypes.keys())

    def clear(self) -> None:
        """Clear all prototypes."""
        self._prototypes.clear()


class PrototypeAction(BaseAction):
    """Prototype pattern action."""
    action_type = "prototype"
    display_name = "原型模式"
    description = "对象克隆原型"

    def __init__(self):
        super().__init__()
        self._registry = CloneRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "clone")

            if operation == "register":
                return self._register(params)
            elif operation == "clone":
                return self._clone(params)
            elif operation == "deep_clone":
                return self._deep_clone(params)
            elif operation == "list":
                return self._list_prototypes()
            elif operation == "create_data":
                return self._create_data_prototype(params)
            elif operation == "create_config":
                return self._create_config_prototype(params)
            elif operation == "create_document":
                return self._create_document_prototype(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Prototype error: {str(e)}")

    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a prototype."""
        key = params.get("key")
        prototype_type = params.get("type", "data")
        data = params.get("data", {})

        if not key:
            return ActionResult(success=False, message="key is required")

        if prototype_type == "data":
            prototype = DataPrototype(data, key)
        elif prototype_type == "config":
            prototype = ConfigPrototype(data, key)
        elif prototype_type == "document":
            prototype = DocumentPrototype(
                title=data.get("title", ""),
                body=data.get("body", ""),
                metadata=data.get("metadata", {}),
            )
        else:
            return ActionResult(success=False, message=f"Unknown type: {prototype_type}")

        self._registry.register(key, prototype)

        return ActionResult(success=True, message=f"Prototype registered: {key}")

    def _clone(self, params: Dict[str, Any]) -> ActionResult:
        """Clone a prototype."""
        key = params.get("key")
        new_key = params.get("new_key")

        if not key:
            return ActionResult(success=False, message="key is required")

        prototype = self._registry.clone(key, deep=False)

        if prototype is None:
            return ActionResult(success=False, message=f"Prototype not found: {key}")

        if new_key and isinstance(prototype, DataPrototype):
            prototype._prototype_id = new_key

        return ActionResult(success=True, message=f"Cloned: {key}", data={"prototype": prototype.to_dict() if hasattr(prototype, "to_dict") else {}})

    def _deep_clone(self, params: Dict[str, Any]) -> ActionResult:
        """Deep clone a prototype."""
        key = params.get("key")
        new_key = params.get("new_key")

        if not key:
            return ActionResult(success=False, message="key is required")

        prototype = self._registry.clone(key, deep=True)

        if prototype is None:
            return ActionResult(success=False, message=f"Prototype not found: {key}")

        if new_key and isinstance(prototype, DataPrototype):
            prototype._prototype_id = new_key

        return ActionResult(success=True, message=f"Deep cloned: {key}", data={"prototype": prototype.to_dict() if hasattr(prototype, "to_dict") else {}})

    def _list_prototypes(self) -> ActionResult:
        """List all prototypes."""
        keys = self._registry.list_keys()
        return ActionResult(success=True, message=f"{len(keys)} prototypes", data={"keys": keys})

    def _create_data_prototype(self, params: Dict[str, Any]) -> ActionResult:
        """Create a data prototype."""
        key = params.get("key", str(uuid.uuid4()))
        data = params.get("data", {})

        prototype = DataPrototype(data, key)
        self._registry.register(key, prototype)

        return ActionResult(success=True, message=f"Data prototype created: {key}", data={"key": key})

    def _create_config_prototype(self, params: Dict[str, Any]) -> ActionResult:
        """Create a config prototype."""
        key = params.get("key", str(uuid.uuid4()))
        config = params.get("config", {})

        prototype = ConfigPrototype(config, key)
        self._registry.register(key, prototype)

        return ActionResult(success=True, message=f"Config prototype created: {key}", data={"key": key})

    def _create_document_prototype(self, params: Dict[str, Any]) -> ActionResult:
        """Create a document prototype."""
        key = params.get("key", str(uuid.uuid4()))
        title = params.get("title", "")
        body = params.get("body", "")
        metadata = params.get("metadata", {})

        prototype = DocumentPrototype(title, body, metadata)
        self._registry.register(key, prototype)

        return ActionResult(success=True, message=f"Document prototype created: {key}", data={"key": key})
