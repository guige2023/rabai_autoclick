"""
Data Enrichment Module.

Provides data enrichment capabilities including field augmentation,
external data lookup, data transformation, and quality enhancement
for downstream processing.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, TypeVar, Generic
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
import json
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EnrichmentType(Enum):
    """Types of data enrichment."""
    LOOKUP = auto()
    DERIVE = auto()
    TRANSFORM = auto()
    AUGMENT = auto()
    NORMALIZE = auto()
    CALCULATE = auto()
    VALIDATE = auto()


@dataclass
class EnrichmentRule:
    """Defines an enrichment rule."""
    name: str
    source_fields: List[str]
    enrichment_type: EnrichmentType
    transform_func: Callable[..., Any]
    target_field: str
    default_value: Any = None
    condition: Optional[Callable[[Dict], bool]] = None
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EnrichmentResult:
    """Result of enrichment operation."""
    original_record: Dict[str, Any]
    enriched_record: Dict[str, Any]
    applied_rules: List[str]
    failed_rules: List[Tuple[str, str]]
    skipped_rules: List[str]


class LookupEnricher:
    """Handles lookup-based enrichment."""
    
    def __init__(self) -> None:
        self._lookup_tables: Dict[str, Dict[str, Any]] = {}
        self._lookup_funcs: Dict[str, Callable[[str], Optional[Any]]]] = {}
    
    def add_lookup_table(
        self,
        table_name: str,
        data: Dict[str, Any]
    ) -> "LookupEnricher":
        """Add a lookup table."""
        self._lookup_tables[table_name] = data
        return self
    
    def add_lookup_func(
        self,
        func_name: str,
        func: Callable[[str], Optional[Any]]
    ) -> "LookupEnricher":
        """Add a lookup function for external lookups."""
        self._lookup_funcs[func_name] = func
        return self
    
    def lookup(
        self,
        lookup_name: str,
        key: str,
        return_field: Optional[str] = None
    ) -> Optional[Any]:
        """Perform lookup."""
        if lookup_name in self._lookup_tables:
            table = self._lookup_tables[lookup_name]
            record = table.get(key)
            
            if record is None:
                return None
            
            if return_field and isinstance(record, dict):
                return record.get(return_field)
            
            return record
        
        if lookup_name in self._lookup_funcs:
            return self._lookup_funcs[lookup_name](key)
        
        return None


class FieldDeriver:
    """Derives new fields from existing data."""
    
    @staticmethod
    def derive_initials(name: str) -> str:
        """Derive initials from name."""
        parts = name.split()
        return "".join(p[0].upper() for p in parts if p)
    
    @staticmethod
    def derive_full_name(first: str, last: str) -> str:
        """Derive full name from parts."""
        return f"{first} {last}".strip()
    
    @staticmethod
    def derive_age(birth_date: str) -> Optional[int]:
        """Derive age from birth date."""
        try:
            from datetime import date
            birth = date.fromisoformat(birth_date)
            today = date.today()
            return today.year - birth.year - (
                (today.month, today.day) < (birth.month, birth.day)
            )
        except Exception:
            return None
    
    @staticmethod
    def derive_domain(email: str) -> Optional[str]:
        """Extract domain from email."""
        if "@" in email:
            return email.split("@")[1]
        return None
    
    @staticmethod
    def derive_category_from_amount(amount: float) -> str:
        """Derive spending category from amount."""
        if amount < 10:
            return "micro"
        elif amount < 100:
            return "small"
        elif amount < 1000:
            return "medium"
        else:
            return "large"


class DataNormalizer:
    """Normalizes data values."""
    
    PHONE_PATTERN = re.compile(r"^\+?1?\d{9,15}$")
    EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    URL_PATTERN = re.compile(r"^https?://[^\s]+$")
    
    @staticmethod
    def normalize_phone(phone: str) -> Optional[str]:
        """Normalize phone number."""
        digits = re.sub(r"\D", "", phone)
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == "1":
            return f"+{digits}"
        return phone
    
    @staticmethod
    def normalize_email(email: str) -> str:
        """Normalize email address."""
        return email.lower().strip()
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """Normalize URL."""
        url = url.strip().lower()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url
    
    @staticmethod
    def normalize_date(date_str: str) -> Optional[str]:
        """Normalize date to ISO format."""
        try:
            from datetime import datetime
            formats = [
                "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d",
                "%m-%d-%Y", "%d-%m-%Y", "%b %d, %Y"
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.date().isoformat()
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    @staticmethod
    def normalize_state(state: str) -> str:
        """Normalize US state to abbreviation."""
        states = {
            "alabama": "AL", "alaska": "AK", "arizona": "AZ",
            "california": "CA", "colorado": "CO", "connecticut": "CT",
            "delaware": "DE", "florida": "FL", "georgia": "GA",
            "hawaii": "HI", "idaho": "ID", "illinois": "IL",
            "indiana": "IN", "iowa": "IA", "kansas": "KS",
            "kentucky": "KY", "louisiana": "LA", "maine": "ME",
            "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
            "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
            "montana": "MT", "nebraska": "NE", "nevada": "NV",
            "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
            "new york": "NY", "north carolina": "NC", "north dakota": "ND",
            "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
            "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
            "south dakota": "SD", "tennessee": "TN", "texas": "TX",
            "utah": "UT", "vermont": "VT", "virginia": "VA",
            "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
        }
        
        return states.get(state.lower(), state.upper())


class DataEnricher:
    """
    Comprehensive data enrichment engine.
    
    Applies enrichment rules to records including lookups,
    derivations, transformations, and normalization.
    """
    
    def __init__(self) -> None:
        self.rules: List[EnrichmentRule] = []
        self.lookup_enricher = LookupEnricher()
        self.normalizer = DataNormalizer()
        self._enrichment_cache: Dict[str, Any] = {}
    
    def add_rule(self, rule: EnrichmentRule) -> "DataEnricher":
        """Add an enrichment rule."""
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.priority)
        return self
    
    def add_derive_rule(
        self,
        name: str,
        source_fields: List[str],
        target_field: str,
        derive_func: Callable[..., Any]
    ) -> "DataEnricher":
        """Add a derivation rule."""
        rule = EnrichmentRule(
            name=name,
            source_fields=source_fields,
            enrichment_type=EnrichmentType.DERIVE,
            transform_func=derive_func,
            target_field=target_field
        )
        return self.add_rule(rule)
    
    def add_lookup_rule(
        self,
        name: str,
        source_field: str,
        lookup_table: str,
        target_field: str,
        return_field: Optional[str] = None
    ) -> "DataEnricher":
        """Add a lookup rule."""
        def lookup_func(values: List) -> Optional[Any]:
            key = values[0] if values else None
            return self.lookup_enricher.lookup(lookup_table, key, return_field)
        
        rule = EnrichmentRule(
            name=name,
            source_fields=[source_field],
            enrichment_type=EnrichmentType.LOOKUP,
            transform_func=lookup_func,
            target_field=target_field
        )
        return self.add_rule(rule)
    
    def add_normalize_rule(
        self,
        name: str,
        source_field: str,
        target_field: str,
        normalize_func: Callable[[str], Any]
    ) -> "DataEnricher":
        """Add a normalization rule."""
        rule = EnrichmentRule(
            name=name,
            source_fields=[source_field],
            enrichment_type=EnrichmentType.NORMALIZE,
            transform_func=normalize_func,
            target_field=target_field
        )
        return self.add_rule(rule)
    
    def enrich_record(self, record: Dict[str, Any]) -> EnrichmentResult:
        """
        Enrich a single record.
        
        Args:
            record: Input record
            
        Returns:
            EnrichmentResult with enriched data
        """
        enriched = dict(record)
        applied = []
        failed = []
        skipped = []
        
        for rule in self.rules:
            try:
                # Check condition
                if rule.condition and not rule.condition(enriched):
                    skipped.append(rule.name)
                    continue
                
                # Get source values
                source_values = []
                for field_name in rule.source_fields:
                    value = self._get_nested_value(enriched, field_name)
                    source_values.append(value)
                
                # Apply transformation
                result = rule.transform_func(*source_values)
                
                if result is None and rule.default_value is not None:
                    result = rule.default_value
                
                # Set target value
                self._set_nested_value(enriched, rule.target_field, result)
                applied.append(rule.name)
            
            except Exception as e:
                logger.warning(f"Rule {rule.name} failed: {e}")
                failed.append((rule.name, str(e)))
        
        return EnrichmentResult(
            original_record=record,
            enriched_record=enriched,
            applied_rules=applied,
            failed_rules=failed,
            skipped_rules=skipped
        )
    
    def enrich_batch(
        self,
        records: List[Dict[str, Any]],
        stop_on_error: bool = False
    ) -> List[EnrichmentResult]:
        """
        Enrich multiple records.
        
        Args:
            records: Input records
            stop_on_error: Stop on first error
            
        Returns:
            List of EnrichmentResults
        """
        results = []
        
        for record in records:
            try:
                result = self.enrich_record(record)
                results.append(result)
            except Exception as e:
                if stop_on_error:
                    raise
                logger.error(f"Failed to enrich record: {e}")
                results.append(EnrichmentResult(
                    original_record=record,
                    enriched_record=record,
                    applied_rules=[],
                    failed_rules=[("batch", str(e))],
                    skipped_rules=[]
                ))
        
        return results
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split(".")
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        
        return current
    
    def _set_nested_value(self, data: Dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        parts = path.split(".")
        current = data
        
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    enricher = DataEnricher()
    
    # Add lookup table
    enricher.lookup_enricher.add_lookup_table("user_tiers", {
        "alice@example.com": {"tier": "premium", "discount": 0.2},
        "bob@example.com": {"tier": "standard", "discount": 0.1},
        "carol@example.com": {"tier": "basic", "discount": 0.0}
    })
    
    # Add rules
    enricher.add_derive_rule(
        name="derive_initials",
        source_fields=["name"],
        target_field="initials",
        derive_func=lambda vals: FieldDeriver.derive_initials(vals[0]) if vals[0] else ""
    )
    
    enricher.add_derive_rule(
        name="derive_domain",
        source_fields=["email"],
        target_field="email_domain",
        derive_func=lambda vals: FieldDeriver.derive_domain(vals[0]) if vals[0] else ""
    )
    
    enricher.add_lookup_rule(
        name="lookup_tier",
        source_field="email",
        lookup_table="user_tiers",
        target_field="tier_info",
        return_field="tier"
    )
    
    enricher.add_normalize_rule(
        name="normalize_phone",
        source_field="phone",
        target_field="phone_normalized",
        normalize_func=DataNormalizer.normalize_phone
    )
    
    # Test records
    records = [
        {
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "phone": "555-123-4567",
            "amount": 150.00
        },
        {
            "name": "Bob Smith",
            "email": "bob@example.com",
            "phone": "5559876543",
            "amount": 75.00
        }
    ]
    
    print("=== Data Enrichment Demo ===\n")
    
    for record in records:
        result = enricher.enrich_record(record)
        
        print(f"Original: {result.original_record['name']}")
        print(f"  -> Enriched fields:")
        for key in result.enriched_record:
            if key not in result.original_record:
                print(f"    {key}: {result.enriched_record[key]}")
        print(f"  Applied: {len(result.applied_rules)} rules")
        print()
