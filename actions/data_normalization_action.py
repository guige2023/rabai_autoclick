"""Data Normalization Action Module.

Provides data normalization, standardization, and
transformation capabilities for various data formats.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from datetime import datetime
import urllib.parse


class NormalizationType(Enum):
    """Types of normalization operations."""
    CASE = "case"
    WHITESPACE = "whitespace"
    PUNCTUATION = "punctuation"
    ACCENTS = "accents"
    SPECIAL_CHARS = "special_chars"
    WHITELIST = "whitelist"
    BLACKLIST = "blacklist"


@dataclass
class NormalizationConfig:
    """Configuration for normalization."""
    normalization_type: NormalizationType
    options: Dict[str, Any] = field(default_factory=dict)


class TextNormalizer:
    """Normalizes text data."""

    WHITESPACE_PATTERN = re.compile(r'\s+')
    PUNCTUATION_PATTERN = re.compile(r'[^\w\s]')

    def __init__(self):
        self._custom_rules: Dict[str, Callable] = {}

    def register_rule(self, name: str, rule: Callable):
        """Register a custom normalization rule."""
        self._custom_rules[name] = rule

    def normalize(
        self,
        text: str,
        config: NormalizationConfig,
    ) -> str:
        """Normalize text according to config."""
        if not isinstance(text, str):
            return str(text)

        norm_type = config.normalization_type
        options = config.options

        if norm_type == NormalizationType.CASE:
            return self._normalize_case(text, options)
        elif norm_type == NormalizationType.WHITESPACE:
            return self._normalize_whitespace(text, options)
        elif norm_type == NormalizationType.PUNCTUATION:
            return self._normalize_punctuation(text, options)
        elif norm_type == NormalizationType.ACCENTS:
            return self._normalize_accents(text, options)
        elif norm_type == NormalizationType.SPECIAL_CHARS:
            return self._normalize_special_chars(text, options)
        elif norm_type == NormalizationType.WHITELIST:
            return self._whitelist_chars(text, options)
        elif norm_type == NormalizationType.BLACKLIST:
            return self._blacklist_chars(text, options)

        return text

    def _normalize_case(self, text: str, options: Dict[str, Any]) -> str:
        """Normalize text case."""
        case_type = options.get("case_type", "lower")
        if case_type == "lower":
            return text.lower()
        elif case_type == "upper":
            return text.upper()
        elif case_type == "title":
            return text.title()
        elif case_type == "sentence":
            return text.capitalize()
        return text

    def _normalize_whitespace(self, text: str, options: Dict[str, Any]) -> str:
        """Normalize whitespace."""
        strip = options.get("strip", True)
        collapse = options.get("collapse", True)

        if strip:
            text = text.strip()
        if collapse:
            text = self.WHITESPACE_PATTERN.sub(' ', text)

        return text

    def _normalize_punctuation(self, text: str, options: Dict[str, Any]) -> str:
        """Remove or replace punctuation."""
        action = options.get("action", "remove")
        if action == "remove":
            return self.PUNCTUATION_PATTERN.sub('', text)
        elif action == "normalize":
            replacements = options.get("replacements", {})
            for old, new in replacements.items():
                text = text.replace(old, new)
            return text
        return text

    def _normalize_accents(self, text: str, options: Dict[str, Any]) -> str:
        """Remove accents from text."""
        accent_map = {
            'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
            'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
            'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
            'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
            'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
            'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
            'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
            'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
            'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
            'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
            'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
            'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
            'đ': 'd',
        }
        for char, replacement in accent_map.items():
            text = text.replace(char, replacement)
            text = text.replace(char.upper(), replacement.upper())
        return text

    def _normalize_special_chars(self, text: str, options: Dict[str, Any]) -> str:
        """Remove special characters."""
        allowed = options.get("allowed", "")
        pattern = f'[^{re.escape(allowed + "a-zA-Z0-9 ")}]'
        return re.sub(pattern, '', text)

    def _whitelist_chars(self, text: str, options: Dict[str, Any]) -> str:
        """Keep only whitelisted characters."""
        allowed = options.get("allowed", "")
        allowed_set = set(allowed)
        return ''.join(c for c in text if c in allowed_set)

    def _blacklist_chars(self, text: str, options: Dict[str, Any]) -> str:
        """Remove blacklisted characters."""
        blocked = set(options.get("blocked", ""))
        return ''.join(c for c in text if c not in blocked)


class DataNormalizer:
    """Normalizes structured data."""

    def __init__(self, text_normalizer: Optional[TextNormalizer] = None):
        self.text_normalizer = text_normalizer or TextNormalizer()
        self._field_configs: Dict[str, List[NormalizationConfig]] = {}

    def register_field_normalization(
        self,
        field_name: str,
        configs: List[NormalizationConfig],
    ):
        """Register normalization configs for a field."""
        self._field_configs[field_name] = configs

    def normalize_record(
        self,
        record: Dict[str, Any],
        field_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Normalize a single record."""
        result = {}

        for field_name, value in record.items():
            target_field = field_name
            if field_mappings and field_name in field_mappings:
                target_field = field_mappings[field_name]

            if isinstance(value, str):
                normalized = self._normalize_field_value(value, field_name)
            elif isinstance(value, dict):
                normalized = self.normalize_record(value, field_mappings)
            elif isinstance(value, list):
                normalized = [
                    self._normalize_field_value(v, field_name)
                    if isinstance(v, str) else v
                    for v in value
                ]
            else:
                normalized = value

            result[target_field] = normalized

        return result

    def _normalize_field_value(
        self,
        value: str,
        field_name: str,
    ) -> str:
        """Normalize a single field value."""
        configs = self._field_configs.get(field_name, [])
        result = value

        for config in configs:
            result = self.text_normalizer.normalize(result, config)

        return result

    def normalize_batch(
        self,
        records: List[Dict[str, Any]],
        field_mappings: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Normalize batch of records."""
        return [
            self.normalize_record(record, field_mappings)
            for record in records
        ]


class URLNormalizer:
    """Normalizes URLs."""

    def __init__(self):
        self._default_params = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}

    def normalize(self, url: str, remove_tracking: bool = True) -> str:
        """Normalize URL."""
        if not url:
            return url

        parsed = urllib.parse.urlparse(url)

        scheme = parsed.scheme.lower() if parsed.scheme else "http"
        netloc = parsed.netloc.lower()

        if "www." in netloc:
            netloc = netloc.replace("www.", "")

        path = self._normalize_path(parsed.path)
        query = self._normalize_query(parsed.query, remove_tracking)
        fragment = ""

        return urllib.parse.urlunparse((
            scheme, netloc, path, parsed.params, query, fragment
        ))

    def _normalize_path(self, path: str) -> str:
        """Normalize URL path."""
        path = urllib.parse.unquote(path)
        path = re.sub(r'/+', '/', path)
        if path != '/' and path.endswith('/'):
            path = path.rstrip('/')
        return path or '/'

    def _normalize_query(self, query: str, remove_tracking: bool) -> str:
        """Normalize and clean query string."""
        if not query:
            return ""

        params = urllib.parse.parse_qsl(query, keep_blank_values=True)

        if remove_tracking:
            params = [(k, v) for k, v in params if k not in self._default_params]

        params = [(k.strip().lower(), v.strip()) for k, v in params]
        params = [(k, v) for k, v in params if v]

        return urllib.parse.urlencode(params)


class EmailNormalizer:
    """Normalizes email addresses."""

    def __init__(self):
        self._disposable_domains = {
            "tempmail.com", "guerrillamail.com", "mailinator.com",
            "10minutemail.com", "throwaway.email",
        }

    def normalize(self, email: str) -> str:
        """Normalize email address."""
        if not email or '@' not in email:
            return email

        local, domain = email.rsplit('@', 1)

        local = local.lower().strip()
        domain = domain.lower().strip()

        local = local.split('+')[0]

        local = local.replace('.', '')

        return f"{local}@{domain}"

    def is_disposable(self, email: str) -> bool:
        """Check if email is from disposable domain."""
        if '@' not in email:
            return False
        _, domain = email.rsplit('@', 1)
        return domain.lower() in self._disposable_domains

    def validate_format(self, email: str) -> bool:
        """Basic email format validation."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))


class DataNormalizationAction:
    """High-level data normalization action."""

    def __init__(
        self,
        normalizer: Optional[DataNormalizer] = None,
        text_normalizer: Optional[TextNormalizer] = None,
    ):
        self.data_normalizer = normalizer or DataNormalizer(text_normalizer)
        self.text_normalizer = text_normalizer or TextNormalizer()
        self.url_normalizer = URLNormalizer()
        self.email_normalizer = EmailNormalizer()

    def normalize_text(
        self,
        text: str,
        normalization_type: str,
        **options,
    ) -> str:
        """Normalize text with specified configuration."""
        config = NormalizationConfig(
            normalization_type=NormalizationType(normalization_type),
            options=options,
        )
        return self.text_normalizer.normalize(text, config)

    def normalize_record(
        self,
        record: Dict[str, Any],
        field_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Normalize a data record."""
        return self.data_normalizer.normalize_record(record, field_mappings)

    def normalize_batch(
        self,
        records: List[Dict[str, Any]],
        field_mappings: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Normalize batch of records."""
        return self.data_normalizer.normalize_batch(records, field_mappings)

    def normalize_url(self, url: str, remove_tracking: bool = True) -> str:
        """Normalize URL."""
        return self.url_normalizer.normalize(url, remove_tracking)

    def normalize_email(self, email: str) -> str:
        """Normalize email address."""
        return self.email_normalizer.normalize(email)

    def register_field_normalization(
        self,
        field_name: str,
        normalizations: List[Dict[str, Any]],
    ):
        """Register field normalization configurations."""
        configs = []
        for norm in normalizations:
            configs.append(NormalizationConfig(
                normalization_type=NormalizationType(norm["type"]),
                options=norm.get("options", {}),
            ))
        self.data_normalizer.register_field_normalization(field_name, configs)


# Module exports
__all__ = [
    "DataNormalizationAction",
    "DataNormalizer",
    "TextNormalizer",
    "URLNormalizer",
    "EmailNormalizer",
    "NormalizationConfig",
    "NormalizationType",
]
