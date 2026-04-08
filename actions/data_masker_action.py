"""Data masker action module for RabAI AutoClick.

Provides data masking with multiple strategies,
pattern-based masking, and format preservation.
"""

import re
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
import random
import string

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataMaskerAction(BaseAction):
    """Mask sensitive data with various strategies.
    
    Supports full masking, partial masking, format-preserving masking,
    and pattern-based masking for common data types.
    """
    action_type = "data_masker"
    display_name = "数据脱敏"
    description = "敏感数据脱敏，支持多种脱敏策略"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data masking operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, masking_rules, mask_type.
        
        Returns:
            ActionResult with masked data.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        if isinstance(records, dict):
            records = [records]
        
        masking_rules = params.get('masking_rules', [])
        if not masking_rules:
            masking_rules = [{'field': '*', 'strategy': 'full_mask'}]
        
        masked_records = []
        
        for record in records:
            masked = self._mask_record(record, masking_rules)
            masked_records.append(masked)
        
        return ActionResult(
            success=True,
            message=f"Masked {len(masked_records)} records",
            data={
                'records': masked_records,
                'count': len(masked_records)
            }
        )
    
    def _mask_record(
        self,
        record: Dict[str, Any],
        masking_rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Mask a single record based on rules."""
        masked = {}
        
        for field, value in record.items():
            field_masked = False
            
            for rule in masking_rules:
                rule_field = rule.get('field', '*')
                strategy = rule.get('strategy', 'full_mask')
                
                if rule_field == '*' or rule_field == field:
                    if isinstance(value, str):
                        masked[field] = self._mask_value(value, strategy, rule)
                        field_masked = True
                    elif isinstance(value, (int, float)):
                        masked[field] = self._mask_numeric(value, strategy)
                        field_masked = True
                    elif isinstance(value, list):
                        masked[field] = [
                            self._mask_value(str(v), strategy, rule) if isinstance(v, str) else v
                            for v in value
                        ]
                        field_masked = True
                    else:
                        masked[field] = value
                        field_masked = True
                    break
            
            if not field_masked:
                masked[field] = value
        
        return masked
    
    def _mask_value(
        self,
        value: str,
        strategy: str,
        rule: Dict[str, Any]
    ) -> str:
        """Mask a string value based on strategy."""
        if not value:
            return value
        
        if strategy == 'full_mask':
            return '*' * len(value)
        
        elif strategy == 'partial_mask':
            visible_start = rule.get('visible_start', 2)
            visible_end = rule.get('visible_end', 2)
            mask_char = rule.get('mask_char', '*')
            
            if len(value) <= visible_start + visible_end:
                return mask_char * len(value)
            
            return (
                value[:visible_start] + 
                mask_char * (len(value) - visible_start - visible_end) + 
                value[-visible_end:]
            )
        
        elif strategy == 'email_mask':
            if '@' in value:
                parts = value.split('@')
                local = parts[0]
                domain = parts[1]
                masked_local = local[0] + '*' * (len(local) - 1) if len(local) > 1 else '*'
                return f"{masked_local}@{domain}"
            return self._mask_value(value, 'partial_mask', rule)
        
        elif strategy == 'phone_mask':
            digits = re.sub(r'\D', '', value)
            if len(digits) >= 4:
                return '*' * (len(digits) - 4) + digits[-4:]
            return '*' * len(digits)
        
        elif strategy == 'credit_card_mask':
            digits = re.sub(r'\D', '', value)
            if len(digits) >= 4:
                return '*' * (len(digits) - 4) + digits[-4:]
            return '*' * len(digits)
        
        elif strategy == 'ssn_mask':
            digits = re.sub(r'\D', '', value)
            if len(digits) >= 4:
                return '*' * (len(digits) - 4) + digits[-4:]
            return '*' * len(digits)
        
        elif strategy == 'name_mask':
            parts = value.split()
            masked_parts = []
            for part in parts:
                if len(part) > 1:
                    masked_parts.append(part[0] + '*' * (len(part) - 1))
                else:
                    masked_parts.append(part)
            return ' '.join(masked_parts)
        
        elif strategy == 'random':
            length = rule.get('length', len(value))
            chars = rule.get('chars', string.ascii_letters + string.digits)
            return ''.join(random.choice(chars) for _ in range(length))
        
        elif strategy == 'hash':
            import hashlib
            return hashlib.sha256(value.encode()).hexdigest()[:rule.get('hash_length', 16)]
        
        else:
            return '*' * len(value)
    
    def _mask_numeric(
        self,
        value: Union[int, float],
        strategy: str
    ) -> Union[int, float]:
        """Mask a numeric value."""
        if strategy == 'full_mask':
            return 0
        elif strategy == 'round':
            magnitude = 10 ** (len(str(int(abs(value)))) - 1)
            return round(value / magnitude) * magnitude
        elif strategy == 'range':
            return random.randint(0, 100)
        else:
            return 0
