"""Data Anonymizer Action Module.

Provides PII (Personally Identifiable Information) anonymization
including name masking, email hashing, phone scrambling, and
data de-identification for privacy compliance.

Example:
    >>> from actions.data.data_anonymizer_action import DataAnonymizer, PIIType
    >>> anonymizer = DataAnonymizer()
    >>> result = anonymizer.anonymize("user@example.com", PIIType.EMAIL)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import hashlib
import re
import threading


class PIIType(Enum):
    """Types of PII data."""
    NAME = "name"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"


class AnonymizationStrategy(Enum):
    """Anonymization strategies."""
    MASK = "mask"
    HASH = "hash"
    REDACT = "redact"
    GENERALIZE = "generalize"
    SHUFFLE = "shuffle"
    REPLACE = "replace"


@dataclass
class PIIPattern:
    """PII detection pattern.
    
    Attributes:
        pii_type: Type of PII
        pattern: Regex pattern
        sample: Example of the PII type
    """
    pii_type: PIIType
    pattern: str
    sample: str = ""


@dataclass
class AnonymizationResult:
    """Anonymization result.
    
    Attributes:
        original: Original value
        anonymized: Anonymized value
        pii_type: Detected PII type
        strategy: Applied strategy
        confidence: Detection confidence
    """
    original: str
    anonymized: str
    pii_type: PIIType
    strategy: AnonymizationStrategy
    confidence: float = 1.0


@dataclass
class AnonymizationConfig:
    """Configuration for anonymization.
    
    Attributes:
        strategy: Default strategy to use
        hash_salt: Salt for hashing
        preserve_format: Preserve original format
        mask_char: Character to use for masking
        replacement_token: Token to use for replacement
    """
    strategy: AnonymizationStrategy = AnonymizationStrategy.MASK
    hash_salt: str = ""
    preserve_format: bool = True
    mask_char: str = "*"
    replacement_token: str = "[REDACTED]"


class DataAnonymizer:
    """PII anonymization and de-identification engine.
    
    Detects and anonymizes various types of PII including
    emails, phone numbers, addresses, and identification numbers.
    
    Attributes:
        _patterns: Registered PII detection patterns
        _config: Anonymization configuration
        _custom_handlers: Custom anonymization handlers
        _lock: Thread safety lock
    """
    
    # Default PII patterns
    DEFAULT_PATTERNS = [
        PIIPattern(PIIType.EMAIL, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', "user@example.com"),
        PIIPattern(PIIType.PHONE, r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', "555-123-4567"),
        PIIPattern(PIIType.CREDIT_CARD, r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', "4111-1111-1111-1111"),
        PIIPattern(PIIType.SSN, r'\b\d{3}-\d{2}-\d{4}\b', "123-45-6789"),
        PIIPattern(PIIType.IP_ADDRESS, r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', "192.168.1.1"),
        PIIPattern(PIIType.DATE_OF_BIRTH, r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', "01/15/1990"),
    ]
    
    def __init__(self, config: Optional[AnonymizationConfig] = None) -> None:
        """Initialize anonymizer.
        
        Args:
            config: Anonymization configuration
        """
        self._patterns: Dict[PIIType, re.Pattern] = {}
        self._config = config or AnonymizationConfig()
        self._custom_handlers: Dict[PIIType, Callable] = {}
        self._lock = threading.RLock()
        
        # Register default patterns
        for pii_pattern in self.DEFAULT_PATTERNS:
            self.register_pattern(pii_pattern)
    
    def register_pattern(self, pii_pattern: PIIPattern) -> None:
        """Register a PII detection pattern.
        
        Args:
            pii_pattern: PII pattern to register
        """
        with self._lock:
            self._patterns[pii_pattern.pii_type] = re.compile(pii_pattern.pattern)
    
    def register_handler(
        self,
        pii_type: PIIType,
        handler: Callable[[str, AnonymizationConfig], str],
    ) -> None:
        """Register custom anonymization handler.
        
        Args:
            pii_type: PII type to handle
            handler: Handler function
        """
        with self._lock:
            self._custom_handlers[pii_type] = handler
    
    def anonymize(
        self,
        value: str,
        pii_type: Optional[PIIType] = None,
        strategy: Optional[AnonymizationStrategy] = None,
    ) -> AnonymizationResult:
        """Anonymize a value.
        
        Args:
            value: Value to anonymize
            pii_type: Specific PII type (auto-detect if None)
            strategy: Strategy to use (use config default if None)
            
        Returns:
            Anonymization result
        """
        strategy = strategy or self._config.strategy
        pii_type = pii_type or self._detect_pii_type(value)
        
        # Use custom handler if registered
        if pii_type in self._custom_handlers:
            anonymized = self._custom_handlers[pii_type](value, self._config)
            return AnonymizationResult(
                original=value,
                anonymized=anonymized,
                pii_type=pii_type,
                strategy=strategy,
            )
        
        # Apply strategy
        if pii_type == PIIType.EMAIL:
            anonymized = self._anonymize_email(value, strategy)
        elif pii_type == PIIType.PHONE:
            anonymized = self._anonymize_phone(value, strategy)
        elif pii_type == PIIType.CREDIT_CARD:
            anonymized = self._anonymize_credit_card(value, strategy)
        elif pii_type == PIIType.SSN:
            anonymized = self._anonymize_ssn(value, strategy)
        elif pii_type == PIIType.IP_ADDRESS:
            anonymized = self._anonymize_ip(value, strategy)
        else:
            anonymized = self._apply_strategy(value, strategy)
        
        return AnonymizationResult(
            original=value,
            anonymized=anonymized,
            pii_type=pii_type or PIIType.NAME,
            strategy=strategy,
        )
    
    def _detect_pii_type(self, value: str) -> Optional[PIIType]:
        """Auto-detect PII type.
        
        Args:
            value: Value to check
            
        Returns:
            Detected PII type or None
        """
        for pii_type, pattern in self._patterns.items():
            if pattern.search(value):
                return pii_type
        return None
    
    def _anonymize_email(
        self,
        email: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Anonymize email address.
        
        Args:
            email: Email address
            strategy: Anonymization strategy
            
        Returns:
            Anonymized email
        """
        if strategy == AnonymizationStrategy.HASH:
            local, domain = email.rsplit("@", 1)
            hash_val = self._hash_value(local)
            return f"{hash_val}@{domain}"
        elif strategy == AnonymizationStrategy.MASK:
            local, domain = email.rsplit("@", 1)
            masked_local = self._mask_string(local, show_chars=2)
            return f"{masked_local}@{domain}"
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        else:
            return self._apply_strategy(email, strategy)
    
    def _anonymize_phone(
        self,
        phone: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Anonymize phone number.
        
        Args:
            phone: Phone number
            strategy: Anonymization strategy
            
        Returns:
            Anonymized phone
        """
        digits = re.sub(r'\D', '', phone)
        if strategy == AnonymizationStrategy.MASK:
            if len(digits) >= 10:
                return f"***-***-{digits[-4:]}"
            return f"***-{digits[-4:]}"
        elif strategy == AnonymizationStrategy.HASH:
            return self._hash_value(digits)
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        else:
            return self._apply_strategy(phone, strategy)
    
    def _anonymize_credit_card(
        self,
        card: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Anonymize credit card number.
        
        Args:
            card: Credit card number
            strategy: Anonymization strategy
            
        Returns:
            Anonymized card number
        """
        digits = re.sub(r'\D', '', card)
        if strategy == AnonymizationStrategy.MASK:
            return f"****-****-****-{digits[-4:]}"
        elif strategy == AnonymizationStrategy.HASH:
            return self._hash_value(digits)
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        else:
            return self._apply_strategy(card, strategy)
    
    def _anonymize_ssn(
        self,
        ssn: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Anonymize SSN.
        
        Args:
            ssn: Social Security Number
            strategy: Anonymization strategy
            
        Returns:
            Anonymized SSN
        """
        if strategy == AnonymizationStrategy.MASK:
            return f"***-**-{ssn[-4:]}"
        elif strategy == AnonymizationStrategy.HASH:
            return self._hash_value(ssn)
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        else:
            return self._apply_strategy(ssn, strategy)
    
    def _anonymize_ip(
        self,
        ip: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Anonymize IP address.
        
        Args:
            ip: IP address
            strategy: Anonymization strategy
            
        Returns:
            Anonymized IP
        """
        if strategy == AnonymizationStrategy.GENERALIZE:
            parts = ip.split(".")
            return f"{parts[0]}.x.x.x"
        elif strategy == AnonymizationStrategy.HASH:
            return self._hash_value(ip)
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        else:
            return self._apply_strategy(ip, strategy)
    
    def _apply_strategy(
        self,
        value: str,
        strategy: AnonymizationStrategy,
    ) -> str:
        """Apply general anonymization strategy.
        
        Args:
            value: Value to anonymize
            strategy: Strategy to apply
            
        Returns:
            Anonymized value
        """
        if strategy == AnonymizationStrategy.MASK:
            return self._mask_string(value, show_chars=0)
        elif strategy == AnonymizationStrategy.HASH:
            return self._hash_value(value)
        elif strategy == AnonymizationStrategy.REDACT:
            return self._config.replacement_token
        elif strategy == AnonymizationStrategy.GENERALIZE:
            return self._generalize(value)
        else:
            return value
    
    def _mask_string(self, value: str, show_chars: int = 2) -> str:
        """Mask a string, showing last N characters.
        
        Args:
            value: String to mask
            show_chars: Number of characters to show
            
        Returns:
            Masked string
        """
        if len(value) <= show_chars:
            return self._config.mask_char * len(value)
        visible = value[-show_chars:] if show_chars > 0 else ""
        masked = self._config.mask_char * (len(value) - show_chars)
        return masked + visible
    
    def _hash_value(self, value: str) -> str:
        """Hash a value with salt.
        
        Args:
            value: Value to hash
            
        Returns:
            Hashed value (first 16 chars of hex)
        """
        salt = self._config.hash_salt.encode()
        hash_obj = hashlib.sha256(salt + value.encode())
        return hash_obj.hexdigest()[:16]
    
    def _generalize(self, value: str) -> str:
        """Generalize a value.
        
        Args:
            value: Value to generalize
            
        Returns:
            Generalized value
        """
        # For numeric values, round to nearest 10
        try:
            num = float(re.sub(r'\D', '', value))
            return str(int(round(num, -1)))
        except (ValueError, TypeError):
            return "[GENERALIZED]"
    
    def anonymize_text(
        self,
        text: str,
        strategy: Optional[AnonymizationStrategy] = None,
    ) -> Tuple[str, List[AnonymizationResult]]:
        """Anonymize all PII in text.
        
        Args:
            text: Text containing PII
            strategy: Strategy to use
            
        Returns:
            Tuple of (anonymized text, list of results)
        """
        strategy = strategy or self._config.strategy
        results: List[AnonymizationResult] = []
        result_text = text
        
        # Find all PII matches
        for pii_type, pattern in self._patterns.items():
            for match in pattern.finditer(result_text):
                original = match.group()
                result = self.anonymize(original, pii_type, strategy)
                results.append(result)
                result_text = result_text.replace(original, result.anonymized, 1)
        
        return result_text, results
    
    def batch_anonymize(
        self,
        records: List[Dict[str, Any]],
        field_mapping: Dict[str, PIIType],
        strategy: Optional[AnonymizationStrategy] = None,
    ) -> List[Dict[str, Any]]:
        """Batch anonymize records.
        
        Args:
            records: List of records
            field_mapping: Field name to PII type mapping
            strategy: Anonymization strategy
            
        Returns:
            Anonymized records
        """
        strategy = strategy or self._config.strategy
        anonymized: List[Dict[str, Any]] = []
        
        for record in records:
            new_record = dict(record)
            for field_name, pii_type in field_mapping.items():
                if field_name in new_record:
                    result = self.anonymize(
                        str(new_record[field_name]),
                        pii_type,
                        strategy,
                    )
                    new_record[field_name] = result.anonymized
            anonymized.append(new_record)
        
        return anonymized
