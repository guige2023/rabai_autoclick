"""Anonymizer action module for RabAI AutoClick.

Provides data anonymization and pseudonymization operations.
"""

import hashlib
import random
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AnonymizerAction(BaseAction):
    """Data anonymization and pseudonymization.
    
    Supports masking, hashing, generalization, and substitution
    for personal identifiable information (PII) and sensitive data.
    """
    action_type = "anonymizer"
    display_name = "数据脱敏"
    description = "PII数据脱敏与匿名化处理"
    
    EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    PHONE_PATTERN = re.compile(r'(\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}')
    ID_PATTERN = re.compile(r'\b\d{17}[\dXx]\b')
    CREDIT_CARD_PATTERN = re.compile(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b')
    IP_PATTERN = re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b')
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute anonymization operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'mask', 'hash', 'pseudonymize', 'detect_pii', 'generalize'
                - data: String or data to anonymize
                - fields: List of field names to anonymize (for dict data)
                - preserve_prefix: Keep first N chars visible (default 0)
                - salt: Salt for hashing (optional)
                - algorithm: Hash algorithm (sha256, md5, sha1)
        
        Returns:
            ActionResult with anonymized data.
        """
        command = params.get('command', 'mask')
        data = params.get('data')
        fields = params.get('fields', [])
        preserve_prefix = params.get('preserve_prefix', 0)
        salt = params.get('salt', '')
        algorithm = params.get('algorithm', 'sha256')
        
        if command == 'mask':
            if isinstance(data, str):
                return self._mask_string(data, preserve_prefix)
            elif isinstance(data, dict):
                result = {}
                for key, value in data.items():
                    if key in fields or not fields:
                        if isinstance(value, str):
                            result[key] = self._mask_string(value, preserve_prefix)
                        else:
                            result[key] = value
                    else:
                        result[key] = value
                return ActionResult(success=True, message="Masked data", data={'result': result})
            else:
                return ActionResult(success=False, message="data must be str or dict")
        
        if command == 'hash':
            if not isinstance(data, (str, dict)):
                return ActionResult(success=False, message="data must be str or dict for hash")
            if isinstance(data, str):
                return self._hash_value(data, algorithm, salt)
            result = {}
            for key, value in data.items():
                if key in fields or not fields:
                    if isinstance(value, str):
                        result[key] = self._hash_value(value, algorithm, salt).data.get('hashed', value)
                    else:
                        result[key] = str(value)
                else:
                    result[key] = value
            return ActionResult(success=True, message=f"Hashed with {algorithm}", data={'result': result})
        
        if command == 'pseudonymize':
            if isinstance(data, str):
                return self._pseudonymize(data, salt)
            elif isinstance(data, dict):
                result = {}
                for key, value in data.items():
                    if key in fields or not fields:
                        if isinstance(value, str):
                            result[key] = self._pseudonymize(value, salt).data.get('pseudonym', value)
                        else:
                            result[key] = value
                    else:
                        result[key] = value
                return ActionResult(success=True, message="Pseudonymized", data={'result': result})
            return ActionResult(success=False, message="data must be str or dict")
        
        if command == 'detect_pii':
            if not isinstance(data, str):
                return ActionResult(success=False, message="data must be str for detect_pii")
            return self._detect_pii(data)
        
        if command == 'generalize':
            if not isinstance(data, str):
                return ActionResult(success=False, message="data must be str for generalize")
            return self._generalize(data)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _mask_string(self, value: str, preserve_prefix: int) -> ActionResult:
        """Mask a string value."""
        if len(value) <= preserve_prefix:
            return ActionResult(success=True, message="Value too short to mask", data={'result': value})
        if preserve_prefix > 0:
            masked = value[:preserve_prefix] + '*' * (len(value) - preserve_prefix)
        else:
            masked = '*' * len(value)
        return ActionResult(success=True, message=f"Masked {len(value)} chars", data={'result': masked})
    
    def _hash_value(self, value: str, algorithm: str, salt: str) -> ActionResult:
        """Hash a value."""
        hasher = hashlib.new(algorithm)
        hasher.update(f"{salt}{value}".encode('utf-8'))
        return ActionResult(
            success=True,
            message=f"Hashed with {algorithm}",
            data={'hashed': hasher.hexdigest(), 'algorithm': algorithm}
        )
    
    def _pseudonymize(self, value: str, salt: str) -> ActionResult:
        """Create consistent pseudonym for a value."""
        key = hashlib.pbkdf2_hmac('sha256', value.encode('utf-8'), salt.encode('utf-8'), 100000)
        pseudonym = key[:16].hex()[:8]
        return ActionResult(success=True, message="Pseudonymized", data={'pseudonym': pseudonym})
    
    def _detect_pii(self, text: str) -> ActionResult:
        """Detect PII types in text."""
        detected = []
        if self.EMAIL_PATTERN.search(text):
            detected.append('email')
        if self.PHONE_PATTERN.search(text):
            detected.append('phone')
        if self.ID_PATTERN.search(text):
            detected.append('id_number')
        if self.CREDIT_CARD_PATTERN.search(text):
            detected.append('credit_card')
        if self.IP_PATTERN.search(text):
            detected.append('ip_address')
        return ActionResult(
            success=True,
            message=f"Detected {len(detected)} PII types",
            data={'pii_types': detected, 'count': len(detected)}
        )
    
    def _generalize(self, value: str) -> ActionResult:
        """Generalize a value (e.g., age ranges, date truncation)."""
        generalized = value
        age_match = re.search(r'\b(\d{1,3})\b', value)
        if age_match:
            age = int(age_match.group(1))
            if 0 <= age < 18:
                generalized = re.sub(r'\b\d{1,3}\b', 'minor', value, count=1)
            elif 18 <= age < 30:
                generalized = re.sub(r'\b\d{1,3}\b', '18-29', value, count=1)
            elif 30 <= age < 50:
                generalized = re.sub(r'\b\d{1,3}\b', '30-49', value, count=1)
            elif 50 <= age < 70:
                generalized = re.sub(r'\b\d{1,3}\b', '50-69', value, count=1)
            else:
                generalized = re.sub(r'\b\d{1,3}\b', '70+', value, count=1)
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', value)
        if date_match:
            generalized = value.replace(date_match.group(0), date_match.group(0)[:7] + '-01')
        return ActionResult(success=True, message="Generalized", data={'result': generalized})
