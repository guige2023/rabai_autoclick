"""UI state persistence for saving and restoring UI state."""
from typing import Dict, Any, Optional
import json
import time


class UIStatePersistence:
    """Persists and restores UI state across sessions.
    
    Saves element states, scroll positions, and form data
    to enable session recovery and state restoration.
    
    Example:
        persister = UIStatePersistence()
        persister.save_state("dialog", {"scroll": 100, "values": {"name": "test"}})
        state = persister.load_state("dialog")
    """

    def __init__(self, storage_path: Optional[str] = None) -> None:
        self._storage_path = storage_path or "/tmp/ui_state.json"
        self._memory: Dict[str, Dict[str, Any]] = {}
        self._load()

    def save_state(self, key: str, state: Dict[str, Any], metadata: Optional[Dict] = None) -> bool:
        """Save UI state for a key."""
        self._memory[key] = {
            "state": state,
            "timestamp": time.time(),
            "metadata": metadata or {},
        }
        return self._persist()

    def load_state(self, key: str) -> Optional[Dict[str, Any]]:
        """Load saved UI state for a key."""
        entry = self._memory.get(key)
        return entry.get("state") if entry else None

    def delete_state(self, key: str) -> bool:
        """Delete saved state for a key."""
        if key in self._memory:
            del self._memory[key]
            return self._persist()
        return False

    def list_states(self) -> list:
        """List all saved state keys."""
        return list(self._memory.keys())

    def get_metadata(self, key: str) -> Optional[Dict]:
        """Get metadata for a saved state."""
        entry = self._memory.get(key)
        return entry.get("metadata") if entry else None

    def _persist(self) -> bool:
        """Persist memory to disk."""
        try:
            with open(self._storage_path, "w") as f:
                json.dump(self._memory, f, indent=2)
            return True
        except Exception:
            return False

    def _load(self) -> None:
        """Load from disk into memory."""
        try:
            with open(self._storage_path) as f:
                self._memory = json.load(f)
        except Exception:
            self._memory = {}
