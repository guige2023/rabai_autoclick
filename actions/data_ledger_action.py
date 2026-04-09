"""Data Ledger Action Module.

Implements an immutable append-only ledger with entry validation,
hash chaining, and audit trail for data integrity.
"""

import time
import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class LedgerEntry:
    entry_id: str
    sequence: int
    timestamp: float
    entry_type: str
    data: Dict[str, Any]
    prev_hash: str
    entry_hash: str
    signature: Optional[str] = None


@dataclass
class LedgerConfig:
    max_entries: int = 1000000
    hash_algorithm: str = "sha256"
    enable_signatures: bool = False


class DataLedgerAction:
    """Append-only ledger with hash chaining for data integrity."""

    def __init__(self, config: Optional[LedgerConfig] = None) -> None:
        self._config = config or LedgerConfig()
        self._entries: List[LedgerEntry] = []
        self._genesis_hash = self._compute_hash("genesis")
        self._entry_counter = 0

    def append(
        self,
        entry_type: str,
        data: Dict[str, Any],
        signature: Optional[str] = None,
    ) -> str:
        if len(self._entries) >= self._config.max_entries:
            raise RuntimeError("Ledger at maximum capacity")
        self._entry_counter += 1
        sequence = self._entry_counter
        timestamp = time.time()
        prev_hash = self._entries[-1].entry_hash if self._entries else self._genesis_hash
        entry_dict = {
            "sequence": sequence,
            "timestamp": timestamp,
            "type": entry_type,
            "data": data,
            "prev_hash": prev_hash,
        }
        entry_hash = self._compute_hash(json.dumps(entry_dict, sort_keys=True))
        entry = LedgerEntry(
            entry_id=f"entry_{sequence}",
            sequence=sequence,
            timestamp=timestamp,
            entry_type=entry_type,
            data=data,
            prev_hash=prev_hash,
            entry_hash=entry_hash,
            signature=signature,
        )
        self._entries.append(entry)
        logger.debug(f"Appended ledger entry {entry.entry_id}")
        return entry.entry_id

    def get_entry(self, sequence: int) -> Optional[LedgerEntry]:
        for entry in self._entries:
            if entry.sequence == sequence:
                return entry
        return None

    def get_entry_by_id(self, entry_id: str) -> Optional[LedgerEntry]:
        for entry in self._entries:
            if entry.entry_id == entry_id:
                return entry
        return None

    def verify_chain(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self._entries:
            return True, []
        if self._entries[0].prev_hash != self._genesis_hash:
            errors.append("Genesis hash mismatch")
        for i in range(1, len(self._entries)):
            prev = self._entries[i - 1]
            curr = self._entries[i]
            if curr.prev_hash != prev.entry_hash:
                errors.append(f"Chain broken at entry {curr.entry_id}")
            entry_dict = {
                "sequence": curr.sequence,
                "timestamp": curr.timestamp,
                "type": curr.entry_type,
                "data": curr.data,
                "prev_hash": curr.prev_hash,
            }
            expected_hash = self._compute_hash(json.dumps(entry_dict, sort_keys=True))
            if expected_hash != curr.entry_hash:
                errors.append(f"Hash mismatch at entry {curr.entry_id}")
        return len(errors) == 0, errors

    def get_range(
        self,
        start_seq: Optional[int] = None,
        end_seq: Optional[int] = None,
    ) -> List[LedgerEntry]:
        start = start_seq or 1
        end = end_seq or self._entry_counter
        return [e for e in self._entries if start <= e.sequence <= end]

    def get_by_type(
        self,
        entry_type: str,
        limit: int = 100,
    ) -> List[LedgerEntry]:
        return [e for e in self._entries if e.entry_type == entry_type][-limit:]

    def get_stats(self) -> Dict[str, Any]:
        verified, errors = self.verify_chain()
        return {
            "total_entries": len(self._entries),
            "next_sequence": self._entry_counter + 1,
            "chain_valid": verified,
            "chain_errors": len(errors),
            "max_entries": self._config.max_entries,
            "utilization": len(self._entries) / self._config.max_entries,
        }

    def export_to_json(self) -> str:
        return json.dumps(
            [
                {
                    "entry_id": e.entry_id,
                    "sequence": e.sequence,
                    "timestamp": e.timestamp,
                    "type": e.entry_type,
                    "data": e.data,
                    "prev_hash": e.prev_hash,
                    "entry_hash": e.entry_hash,
                    "signature": e.signature,
                }
                for e in self._entries
            ],
            indent=2,
        )

    def import_from_json(self, json_str: str) -> int:
        raw = json.loads(json_str)
        self._entries.clear()
        self._entry_counter = 0
        for item in raw:
            entry = LedgerEntry(**item)
            self._entries.append(entry)
            self._entry_counter = max(self._entry_counter, entry.sequence)
        return len(self._entries)

    def _compute_hash(self, content: str) -> str:
        if self._config.hash_algorithm == "sha256":
            return hashlib.sha256(content.encode()).hexdigest()
        elif self._config.hash_algorithm == "sha512":
            return hashlib.sha512(content.encode()).hexdigest()
        elif self._config.hash_algorithm == "md5":
            return hashlib.md5(content.encode()).hexdigest()
        return hashlib.sha256(content.encode()).hexdigest()
