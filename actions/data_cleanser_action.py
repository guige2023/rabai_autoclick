"""Data cleanser action module for RabAI AutoClick.

Provides data cleansing and normalization operations including
handling whitespace, special characters, encoding issues,
and inconsistent formatting.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCleanserAction(BaseAction):
    """Cleanse and normalize data values.
    
    Applies various cleaning operations including trimming,
    case normalization, character removal, and pattern replacement.
    """
    action_type = "data_cleanser"
    display_name = "数据清洗"
    description = "清洗和规范化数据值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cleanse data values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, operations (list of
                   {field, op, value}), result_field.
        
        Returns:
            ActionResult with cleansed data.
        """
        data = params.get('data', [])
        operations = params.get('operations', [])
        result_field = params.get('result_field', 'cleaned')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not operations:
            operations = [
                {'field': '', 'op': 'trim'},
                {'field': '', 'op': 'normalize_whitespace'}
            ]

        results = []
        stats = {'trimmed': 0, 'normalized': 0, 'replaced': 0, 'converted': 0}

        for row in data:
            new_row = dict(row)
            if isinstance(row, dict):
                for op_def in operations:
                    field = op_def.get('field', '')
                    op = op_def.get('op', 'trim')
                    value = op_def.get('value', '')

                    if field:
                        current_val = new_row.get(field, '')
                    else:
                        current_val = str(row) if not isinstance(row, dict) else ''

                    cleaned = self._apply_clean_op(current_val, op, value, stats)
                    if field:
                        new_row[field] = cleaned
                    else:
                        new_row[result_field] = cleaned

            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Cleansed {len(results)} records",
            data={
                'data': results,
                'count': len(results),
                'stats': stats
            },
            duration=time.time() - start_time
        )

    def _apply_clean_op(self, value: Any, op: str, param: Any, stats: Dict) -> str:
        """Apply a cleaning operation to a value."""
        s = str(value) if value is not None else ''

        if op == 'trim':
            stats['trimmed'] += 1
            return s.strip()
        elif op == 'normalize_whitespace':
            stats['normalized'] += 1
            return re.sub(r'\s+', ' ', s).strip()
        elif op == 'remove_special_chars':
            stats['replaced'] += 1
            return re.sub(r'[^\w\s]', '', s)
        elif op == 'remove_digits':
            stats['replaced'] += 1
            return re.sub(r'\d', '', s)
        elif op == 'remove_whitespace':
            stats['replaced'] += 1
            return re.sub(r'\s', '', s)
        elif op == 'to_upper':
            stats['converted'] += 1
            return s.upper()
        elif op == 'to_lower':
            stats['converted'] += 1
            return s.lower()
        elif op == 'to_title':
            stats['converted'] += 1
            return s.title()
        elif op == 'replace':
            stats['replaced'] += 1
            return s.replace(str(param.get('old', '')), str(param.get('new', '')))
        elif op == 'regex_replace':
            stats['replaced'] += 1
            pattern = param.get('pattern', '')
            repl = param.get('replacement', '')
            return re.sub(pattern, repl, s)
        elif op == 'remove_prefix':
            stats['replaced'] += 1
            prefix = str(param) if param else ''
            return s[len(prefix):] if s.startswith(prefix) else s
        elif op == 'remove_suffix':
            stats['replaced'] += 1
            suffix = str(param) if param else ''
            return s[:-len(suffix)] if s.endswith(suffix) else s
        elif op == 'pad_zero':
            stats['converted'] += 1
            length = int(param) if param else 1
            return s.zfill(length)
        return s


class PhoneNormalizerAction(BaseAction):
    """Normalize phone numbers to standard format.
    
    Parses and normalizes phone numbers from various
    input formats to E.164 or national format.
    """
    action_type = "phone_normalizer"
    display_name = "电话标准化"
    description = "标准化电话号码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize phone numbers.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, output_format (e164|national|international),
                   default_country_code.
        
        Returns:
            ActionResult with normalized phones.
        """
        data = params.get('data', [])
        field = params.get('field', 'phone')
        output_format = params.get('output_format', 'e164')
        default_country = params.get('default_country_code', '1')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        normalized_count = 0

        for row in data:
            new_row = dict(row)
            phone = self._get_field(row, field)
            if phone:
                normalized = self._normalize_phone(str(phone), output_format, default_country)
                new_row[field] = normalized
                if normalized != str(phone):
                    normalized_count += 1
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Normalized {normalized_count}/{len(results)} phone numbers",
            data={
                'data': results,
                'count': len(results),
                'normalized_count': normalized_count
            },
            duration=time.time() - start_time
        )

    def _normalize_phone(self, phone: str, fmt: str, default_country: str) -> str:
        """Normalize phone number to target format."""
        digits = re.sub(r'\D', '', phone)
        if len(digits) < 7 or len(digits) > 15:
            return phone

        if len(digits) == 10:
            digits = default_country + digits

        if fmt == 'e164':
            return f"+{digits}"
        elif fmt == 'national':
            if len(digits) == 11 and digits[0] == '1':
                return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
            if len(digits) == 10:
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif fmt == 'international':
            if len(digits) == 11 and digits[0] == '1':
                return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return phone

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class EmailNormalizerAction(BaseAction):
    """Normalize email addresses to standard format.
    
    Validates and normalizes email addresses, fixing common
    formatting issues and optionally handling case sensitivity.
    """
    action_type = "email_normalizer"
    display_name = "邮箱标准化"
    description = "标准化邮箱地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize email addresses.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, lowercase_domain,
                   remove_alias, result_field.
        
        Returns:
            ActionResult with normalized emails.
        """
        data = params.get('data', [])
        field = params.get('field', 'email')
        lowercase_domain = params.get('lowercase_domain', True)
        remove_alias = params.get('remove_alias', False)
        result_field = params.get('result_field', 'email')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        normalized_count = 0
        invalid_count = 0

        email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'

        for row in data:
            new_row = dict(row)
            email = str(self._get_field(row, field) or '').strip()
            original = email

            if not email:
                new_row[result_field] = ''
            elif not re.match(email_pattern, email):
                new_row[result_field] = email
                invalid_count += 1
            else:
                if '+' in email and remove_alias:
                    local, domain = email.rsplit('@', 1)
                    local = local.split('+')[0]
                    email = f"{local}@{domain}"

                if lowercase_domain:
                    local, domain = email.rsplit('@', 1)
                    email = f"{local}@{domain.lower()}"

                new_row[result_field] = email
                if email != original:
                    normalized_count += 1

            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Normalized {normalized_count} emails, {invalid_count} invalid",
            data={
                'data': results,
                'count': len(results),
                'normalized_count': normalized_count,
                'invalid_count': invalid_count
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class AddressCleanerAction(BaseAction):
    """Clean and standardize address data.
    
    Normalizes street addresses, removes redundant
    components, and standardizes abbreviations.
    """
    action_type = "address_cleaner"
    display_name = "地址清洗"
    description = "清洗标准化地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clean address data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, standardize_abbreviations.
        
        Returns:
            ActionResult with cleaned addresses.
        """
        data = params.get('data', [])
        field = params.get('field', 'address')
        standardize_abbreviations = params.get('standardize_abbreviations', True)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        abbreviation_map = {
            'st': 'Street', 'st.': 'Street',
            'ave': 'Avenue', 'ave.': 'Avenue',
            'blvd': 'Boulevard', 'blvd.': 'Boulevard',
            'rd': 'Road', 'rd.': 'Road',
            'dr': 'Drive', 'dr.': 'Drive',
            'ln': 'Lane', 'ln.': 'Lane',
            'ct': 'Court', 'ct.': 'Court',
            'pl': 'Place', 'pl.': 'Place',
            'cir': 'Circle', 'cir.': 'Circle',
            'hwy': 'Highway', 'hwy.': 'Highway',
            'pkwy': 'Parkway', 'pkwy.': 'Parkway',
            'apt': 'Apartment', 'apt.': 'Apartment',
            'ste': 'Suite', 'ste.': 'Suite',
            'fl': 'Floor', 'fl.': 'Floor',
            'n': 'North', 'n.': 'North',
            's': 'South', 's.': 'South',
            'e': 'East', 'e.': 'East',
            'w': 'West', 'w.': 'West',
            'ne': 'Northeast', 'nw': 'Northwest',
            'se': 'Southeast', 'sw': 'Southwest',
        }

        results = []
        cleaned_count = 0

        for row in data:
            new_row = dict(row)
            address = str(self._get_field(row, field) or '').strip()
            original = address

            if standardize_abbreviations and address:
                words = address.split()
                cleaned_words = []
                for word in words:
                    lower = word.lower()
                    if lower in abbreviation_map:
                        cleaned_words.append(abbreviation_map[lower])
                    else:
                        cleaned_words.append(word)
                address = ' '.join(cleaned_words)

            address = re.sub(r'\s+', ' ', address).strip()
            new_row[field] = address
            if address != original:
                cleaned_count += 1
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Cleaned {cleaned_count}/{len(results)} addresses",
            data={
                'data': results,
                'count': len(results),
                'cleaned_count': cleaned_count
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value
