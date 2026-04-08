"""Data deduplication action module for RabAI AutoClick.

Provides data deduplication with multiple strategies:
exact match, fuzzy match, and key-based deduplication.
"""

import sys
import os
import json
import hashlib
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DedupStrategy(Enum):
    """Deduplication strategy types."""
    EXACT = "exact"
    FINGERPRINT = "fingerprint"
    KEY = "key"
    FUZZY = "fuzzy"
    SEQUENCE = "sequence"


class DataDedupAction(BaseAction):
    """Remove duplicate data entries.
    
    Supports exact match, fingerprint-based, key-based,
    fuzzy matching, and sequence-based deduplication.
    """
    action_type = "data_dedup"
    display_name = "数据去重"
    description = "多种去重策略：精确/指纹/键值/模糊匹配"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Remove duplicates from data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list, data to deduplicate
                - strategy: str (exact/fingerprint/key/fuzzy/sequence)
                - key: str, field name for key-based dedup
                - threshold: float, similarity threshold for fuzzy (0-1)
                - normalize: bool, normalize strings before comparison
                - keep: str (first/last), which duplicate to keep
                - save_to_var: str
        
        Returns:
            ActionResult with deduplicated data.
        """
        data = params.get('data', [])
        strategy = params.get('strategy', 'exact')
        key = params.get('key', None)
        threshold = params.get('threshold', 0.9)
        normalize = params.get('normalize', False)
        keep = params.get('keep', 'first')
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        if strategy == 'exact':
            unique, removed = self._exact_dedup(data, keep)
        elif strategy == 'fingerprint':
            unique, removed = self._fingerprint_dedup(data, keep, normalize)
        elif strategy == 'key':
            unique, removed = self._key_dedup(data, key, keep)
        elif strategy == 'fuzzy':
            unique, removed = self._fuzzy_dedup(data, keep, threshold, normalize)
        elif strategy == 'sequence':
            unique, removed = self._sequence_dedup(data, keep)
        else:
            return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = unique

        return ActionResult(
            success=True,
            message=f"Deduplicated: {len(unique)} unique, {removed} duplicates removed",
            data={
                'unique': unique,
                'count': len(unique),
                'removed': removed
            }
        )

    def _exact_dedup(self, data: List, keep: str) -> Tuple[List, int]:
        """Exact match deduplication."""
        seen: Set[Any] = set()
        result = []
        removed = 0

        if keep == 'last':
            data = list(reversed(data))

        for item in data:
            if item not in seen:
                seen.add(item)
                result.append(item)
            else:
                removed += 1

        if keep == 'last':
            result = list(reversed(result))

        return result, removed

    def _fingerprint_dedup(
        self, data: List, keep: str, normalize: bool
    ) -> Tuple[List, int]:
        """Fingerprint-based deduplication using hash."""
        seen: Set[str] = set()
        result = []
        removed = 0

        if keep == 'last':
            data = list(reversed(data))

        for item in data:
            fp = self._fingerprint(item, normalize)
            if fp not in seen:
                seen.add(fp)
                result.append(item)
            else:
                removed += 1

        if keep == 'last':
            result = list(reversed(result))

        return result, removed

    def _fingerprint(self, item: Any, normalize: bool) -> str:
        """Generate fingerprint for an item."""
        if isinstance(item, str):
            s = item.lower().strip() if normalize else item
        elif isinstance(item, dict):
            s = json.dumps(item, sort_keys=True, default=str)
        elif isinstance(item, (list, tuple)):
            s = json.dumps(list(item), sort_keys=True, default=str)
        else:
            s = str(item)
        return hashlib.md5(s.encode('utf-8')).hexdigest()

    def _key_dedup(
        self, data: List, key: Optional[str], keep: str
    ) -> Tuple[List, int]:
        """Key-based deduplication."""
        if not key:
            return self._exact_dedup(data, keep)

        seen: Set[Any] = set()
        result = []
        removed = 0

        if keep == 'last':
            data = list(reversed(data))

        for item in data:
            if isinstance(item, dict) and key in item:
                k = item[key]
            else:
                k = key
            if k not in seen:
                seen.add(k)
                result.append(item)
            else:
                removed += 1

        if keep == 'last':
            result = list(reversed(result))

        return result, removed

    def _fuzzy_dedup(
        self, data: List, keep: str, threshold: float, normalize: bool
    ) -> Tuple[List, int]:
        """Fuzzy deduplication using string similarity."""
        result = []
        removed = 0

        if keep == 'last':
            data = list(reversed(data))

        for item in data:
            is_dup = False
            item_str = self._normalize_str(str(item)) if normalize else str(item)

            for existing in result:
                existing_str = self._normalize_str(str(existing)) if normalize else str(existing)
                if self._similarity(item_str, existing_str) >= threshold:
                    is_dup = True
                    break

            if not is_dup:
                result.append(item)
            else:
                removed += 1

        if keep == 'last':
            result = list(reversed(result))

        return result, removed

    def _normalize_str(self, s: str) -> str:
        """Normalize string for comparison."""
        s = s.lower().strip()
        s = re.sub(r'\s+', ' ', s)
        s = re.sub(r'[^\w\s]', '', s)
        return s

    def _similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity (Jaccard)."""
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        set1 = set(s1.split())
        set2 = set(s2.split())
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0

    def _sequence_dedup(self, data: List, keep: str) -> Tuple[List, int]:
        """Remove consecutive duplicates."""
        if not data:
            return [], 0

        result = [data[0]]
        removed = 0

        for item in data[1:]:
            if item != result[-1]:
                result.append(item)
            else:
                removed += 1

        if keep == 'last':
            # Rebuild reversing consecutive dups
            result = []
            seen_last = {}
            for item in reversed(data):
                if item not in seen_last:
                    seen_last[item] = True
                    result.append(item)
            result = list(reversed(result))
            removed = len(data) - len(result)

        return result, removed

    def get_required_params(self) -> List[str]:
        return ['data', 'strategy']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': None,
            'threshold': 0.9,
            'normalize': False,
            'keep': 'first',
            'save_to_var': None,
        }
