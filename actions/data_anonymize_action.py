"""Data anonymization action module for RabAI AutoClick.

Provides data anonymization and pseudonymization with
techniques: masking, hashing, generalization, and perturbation.
"""

import sys
import os
import re
import hashlib
import random
import string
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AnonymizeStrategy(Enum):
    """Anonymization strategies."""
    MASK = "mask"
    HASH = "hash"
    GENERALIZE = "generalize"
    PERTURB = "perturb"
    PSEUDONYMIZE = "pseudonymize"
    REDACT = "redact"


@dataclass
class FieldSpec:
    """Specification for field anonymization."""
    field_name: str
    strategy: str
    params: Dict[str, Any] = None


class DataAnonymizeAction(BaseAction):
    """Anonymize sensitive data fields.
    
    Supports masking, hashing, generalization, perturbation,
    pseudonymization, and redaction techniques.
    """
    action_type = "data_anonymize"
    display_name = "数据脱敏"
    description = "敏感数据脱敏：掩码/哈希/泛化/扰动/伪标识"

    _pseudonym_map: Dict[str, Dict[str, str]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Anonymize sensitive data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts or dict, data to anonymize
                - field_specs: list of {field, strategy, params}
                    strategies: mask/hash/generalize/perturb/pseudonymize/redact
                - salt: str, salt for hashing (for reproducibility)
                - seed: int, random seed for perturbation
                - save_to_var: str
        
        Returns:
            ActionResult with anonymized data.
        """
        data = params.get('data', [])
        field_specs = params.get('field_specs', [])
        salt = params.get('salt', '')
        seed = params.get('seed', None)
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        if seed is not None:
            random.seed(seed)

        if not field_specs:
            # Auto-detect common sensitive fields
            field_specs = self._auto_detect_fields(data)

        if isinstance(data, dict):
            data = [data]

        result = []
        for record in data:
            anonymized = dict(record)
            for spec in field_specs:
                field_name = spec.get('field_name', '')
                strategy = spec.get('strategy', 'mask')
                spec_params = spec.get('params', {})

                if field_name in anonymized:
                    original = anonymized[field_name]
                    anonymized[field_name] = self._anonymize_field(
                        original, strategy, spec_params, salt
                    )
            result.append(anonymized)

        # Return single dict if input was single
        if isinstance(params.get('data'), dict):
            result = result[0] if result else {}

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Anonymized {len(result)} record(s) using {len(field_specs)} field specs",
            data=result
        )

    def _auto_detect_fields(self, data: List[Dict]) -> List[Dict]:
        """Auto-detect sensitive fields."""
        if not data or not isinstance(data[0], dict):
            return []

        specs = []
        common_sensitive = {
            'email': 'hash',
            'phone': 'mask',
            'mobile': 'mask',
            'name': 'pseudonymize',
            'username': 'pseudonymize',
            'password': 'redact',
            'ssn': 'mask',
            'id_card': 'mask',
            'credit_card': 'mask',
            'address': 'generalize',
            'ip': 'hash',
            'ip_address': 'hash',
        }

        first_record = data[0]
        for field_name in first_record.keys():
            lower_name = field_name.lower()
            for sensitive_key, strategy in common_sensitive.items():
                if sensitive_key in lower_name:
                    specs.append({
                        'field_name': field_name,
                        'strategy': strategy,
                        'params': {}
                    })
                    break

        return specs

    def _anonymize_field(
        self, value: Any, strategy: str, params: Dict, salt: str
    ) -> Any:
        """Anonymize a single field value."""
        if value is None:
            return None

        value_str = str(value)

        if strategy == 'mask':
            return self._mask_value(value_str, params)
        elif strategy == 'hash':
            return self._hash_value(value_str, salt, params)
        elif strategy == 'generalize':
            return self._generalize_value(value_str, params)
        elif strategy == 'perturb':
            return self._perturb_value(value, params)
        elif strategy == 'pseudonymize':
            return self._pseudonymize_value(value_str, salt, params)
        elif strategy == 'redact':
            return self._redact_value(value_str, params)
        else:
            return value

    def _mask_value(self, value: str, params: Dict) -> str:
        """Mask value showing only last N characters."""
        visible = params.get('visible_chars', 4)
        mask_char = params.get('mask_char', '*')
        if visible >= len(value):
            return value
        return mask_char * (len(value) - visible) + value[-visible:]

    def _hash_value(self, value: str, salt: str, params: Dict) -> str:
        """Hash value with optional truncation."""
        algo = params.get('algorithm', 'sha256')
        raw = f"{salt}{value}" if salt else value
        if algo == 'md5':
            h = hashlib.md5(raw.encode()).hexdigest()
        elif algo == 'sha1':
            h = hashlib.sha1(raw.encode()).hexdigest()
        else:
            h = hashlib.sha256(raw.encode()).hexdigest()
        truncate = params.get('truncate', 0)
        return h[:truncate] if truncate > 0 else h

    def _generalize_value(self, value: str, params: Dict) -> str:
        """Generalize value (e.g., age ranges, date to year)."""
        gtype = params.get('type', 'auto')

        if gtype == 'date' or re.match(r'\d{4}-\d{2}-\d{2}', value):
            # Generalize date to year
            match = re.match(r'(\d{4})', value)
            if match:
                return match.group(1)
        elif gtype == 'age' or value.isdigit():
            age = int(value)
            if 0 <= age < 18:
                return '0-17'
            elif 18 <= age < 30:
                return '18-29'
            elif 30 <= age < 50:
                return '30-49'
            elif 50 <= age < 70:
                return '50-69'
            else:
                return '70+'
        elif gtype == 'number':
            num = float(value)
            bucket = params.get('bucket_size', 100)
            lower = int(num // bucket) * bucket
            return f"{lower}-{lower + bucket}"
        elif gtype == 'city':
            return 'City Group'
        elif gtype == 'country':
            return 'Country Group'

        return value

    def _perturb_value(self, value: Any, params: Dict) -> Any:
        """Add noise to numeric values."""
        if not isinstance(value, (int, float)):
            return value

        scale = params.get('scale', 0.1)
        delta = value * scale * (random.random() * 2 - 1)
        return round(value + delta, params.get('precision', 2))

    def _pseudonymize_value(self, value: str, salt: str, params: Dict) -> str:
        """Replace with consistent pseudonym (same input -> same output)."""
        key = params.get('key', 'default')
        if key not in self._pseudonym_map:
            self._pseudonym_map[key] = {}

        if value not in self._pseudonym_map[key]:
            prefix = params.get('prefix', 'P')
            raw = f"{salt}{key}{value}" if salt else f"{key}{value}"
            pseudo = f"{prefix}{hashlib.sha256(raw.encode()).hexdigest()[:12]}"
            self._pseudonym_map[key][value] = pseudo

        return self._pseudonym_map[key][value]

    def _redact_value(self, value: str, params: Dict) -> str:
        """Completely redact value."""
        return '[REDACTED]'

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field_specs': [],
            'salt': '',
            'seed': None,
            'save_to_var': None,
        }
