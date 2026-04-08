"""Data Generator Action Module.

Provides synthetic data generation capabilities
for testing and simulation.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import random
import string
import hashlib
from datetime import datetime, timedelta


class DataType(Enum):
    """Types of data that can be generated."""
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    TEXT = "text"
    EMAIL = "email"
    PHONE = "phone"
    DATE = "date"
    DATETIME = "datetime"
    UUID = "uuid"
    ADDRESS = "address"
    NAME = "name"
    COMPANY = "company"
    URL = "url"
    IP_ADDRESS = "ip_address"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"


@dataclass
class FieldSpec:
    """Specification for a generated field."""
    name: str
    data_type: DataType
    min_value: Any = None
    max_value: Any = None
    length: int = 0
    pattern: str = ""
    choices: Optional[List[Any]] = None
    unique: bool = False
    null_ratio: float = 0.0

    def generate(self, context: Optional[Dict[str, Any]] = None) -> Any:
        """Generate a value based on spec."""
        if self.choices:
            return random.choice(self.choices)

        if random.random() < self.null_ratio:
            return None

        if self.data_type == DataType.INTEGER:
            min_val = self.min_value if self.min_value is not None else 0
            max_val = self.max_value if self.max_value is not None else 100
            return random.randint(min_val, max_val)

        elif self.data_type == DataType.FLOAT:
            min_val = self.min_value if self.min_value is not None else 0.0
            max_val = self.max_value if self.max_value is not None else 100.0
            return random.uniform(min_val, max_val)

        elif self.data_type == DataType.BOOLEAN:
            return random.choice([True, False])

        elif self.data_type == DataType.STRING:
            length = self.length or 10
            chars = self.pattern or string.ascii_letters + string.digits
            return ''.join(random.choice(chars) for _ in range(length))

        elif self.data_type == DataType.TEXT:
            length = self.length or 100
            words = [
                "lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
                "adipiscing", "elit", "sed", "do", "eiusmod", "tempor",
                "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua",
            ]
            text = ' '.join(random.choice(words) for _ in range(length // 5))
            return text[:length]

        elif self.data_type == DataType.EMAIL:
            domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
            username = ''.join(random.choices(string.ascii_lowercase, k=8))
            return f"{username}@{random.choice(domains)}"

        elif self.data_type == DataType.PHONE:
            return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

        elif self.data_type == DataType.DATE:
            start = self.min_value or (datetime.now() - timedelta(days=365))
            end = self.max_value or datetime.now()
            if isinstance(start, datetime):
                start = start.date()
            if isinstance(end, datetime):
                end = end.date()
            delta = end - start
            random_days = random.randint(0, delta.days)
            return start + timedelta(days=random_days)

        elif self.data_type == DataType.DATETIME:
            start = self.min_value or (datetime.now() - timedelta(days=365))
            end = self.max_value or datetime.now()
            delta = end - start
            random_seconds = random.randint(0, int(delta.total_seconds()))
            return start + timedelta(seconds=random_seconds)

        elif self.data_type == DataType.UUID:
            return str(uuid.uuid4())

        elif self.data_type == DataType.ADDRESS:
            streets = [
                "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd",
                "Elm St", "Washington Blvd", "Lincoln Ave", "Park Pl", "Lake Dr",
            ]
            cities = ["Springfield", "Riverside", "Fairview", "Georgetown", "Franklin"]
            states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
            street_num = random.randint(100, 9999)
            street = random.choice(streets)
            city = random.choice(cities)
            state = random.choice(states)
            zip_code = random.randint(10000, 99999)
            return f"{street_num} {street}, {city}, {state} {zip_code}"

        elif self.data_type == DataType.NAME:
            first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda"]
            last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
            return f"{random.choice(first_names)} {random.choice(last_names)}"

        elif self.data_type == DataType.COMPANY:
            prefixes = ["Acme", "Global", "Premier", "Apex", "Summit", "Dynamic"]
            suffixes = ["Corp", "Inc", "LLC", "Industries", "Solutions", "Systems"]
            return f"{random.choice(prefixes)} {random.choice(suffixes)}"

        elif self.data_type == DataType.URL:
            domains = ["example.com", "test.com", "demo.com"]
            paths = ["", "about", "products", "services", "contact"]
            return f"https://{random.choice(domains)}/{random.choice(paths)}"

        elif self.data_type == DataType.IP_ADDRESS:
            return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"

        elif self.data_type == DataType.CREDIT_CARD:
            return f"{random.randint(4000,4999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

        elif self.data_type == DataType.SSN:
            return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"

        return None


class DataSchema:
    """Schema for data generation."""

    def __init__(self, name: str):
        self.name = name
        self.fields: List[FieldSpec] = []
        self._unique_values: Dict[str, set] = {}

    def add_field(self, field_spec: FieldSpec):
        """Add a field specification."""
        self.fields.append(field_spec)
        if field_spec.unique:
            self._unique_values[field_spec.name] = set()

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate data based on schema."""
        results = []

        for _ in range(count):
            record = {}
            for field in self.fields:
                value = self._generate_unique(field)
                record[field.name] = value
            results.append(record)

        return results

    def _generate_unique(self, field: FieldSpec) -> Any:
        """Generate unique value for field."""
        if not field.unique:
            return field.generate()

        max_attempts = 100
        for _ in range(max_attempts):
            value = field.generate()
            if value not in self._unique_values[field.name]:
                self._unique_values[field.name].add(value)
                return value

        raise ValueError(f"Could not generate unique value for {field.name}")


class SequenceGenerator:
    """Generates sequential data."""

    def __init__(self, start: int = 1, step: int = 1):
        self.current = start
        self.step = step

    def next(self) -> int:
        """Get next value in sequence."""
        value = self.current
        self.current += self.step
        return value

    def reset(self, start: Optional[int] = None):
        """Reset sequence."""
        if start is not None:
            self.current = start


class FakerIntegration:
    """Wrapper for Faker library (if available)."""

    def __init__(self):
        self._faker = None
        self._available = False

    def _check_availability(self):
        """Check if Faker is available."""
        if self._faker is None:
            try:
                from faker import Faker
                self._faker = Faker()
                self._available = True
            except ImportError:
                self._available = False

    def name(self) -> str:
        """Generate a name."""
        self._check_availability()
        if self._available:
            return self._faker.name()
        return "Name"

    def email(self) -> str:
        """Generate an email."""
        self._check_availability()
        if self._available:
            return self._faker.email()
        return "email@example.com"

    def address(self) -> str:
        """Generate an address."""
        self._check_availability()
        if self._available:
            return self._faker.address()
        return "123 Main St"

    def text(self, length: int = 100) -> str:
        """Generate text."""
        self._check_availability()
        if self._available:
            return self._faker.text(max_nb_chars=length)
        return "Sample text"


class DataGeneratorAction:
    """High-level data generator action."""

    def __init__(self):
        self._schemas: Dict[str, DataSchema] = {}
        self._sequences: Dict[str, SequenceGenerator] = {}
        self.faker = FakerIntegration()

    def create_schema(self, name: str) -> DataSchema:
        """Create a new schema."""
        schema = DataSchema(name)
        self._schemas[name] = schema
        return schema

    def get_schema(self, name: str) -> Optional[DataSchema]:
        """Get schema by name."""
        return self._schemas.get(name)

    def add_field(
        self,
        schema_name: str,
        name: str,
        data_type: str,
        **kwargs,
    ) -> bool:
        """Add field to schema."""
        schema = self._schemas.get(schema_name)
        if not schema:
            return False

        field_spec = FieldSpec(
            name=name,
            data_type=DataType(data_type),
            **kwargs,
        )
        schema.add_field(field_spec)
        return True

    def generate(
        self,
        schema_name: str,
        count: int = 1,
    ) -> List[Dict[str, Any]]:
        """Generate data from schema."""
        schema = self._schemas.get(schema_name)
        if not schema:
            return []
        return schema.generate(count)

    def create_sequence(
        self,
        name: str,
        start: int = 1,
        step: int = 1,
    ) -> SequenceGenerator:
        """Create a sequence generator."""
        seq = SequenceGenerator(start, step)
        self._sequences[name] = seq
        return seq

    def next_sequence(self, name: str) -> Optional[int]:
        """Get next value from sequence."""
        seq = self._sequences.get(name)
        if seq:
            return seq.next()
        return None

    def generate_field(
        self,
        data_type: str,
        **kwargs,
    ) -> Any:
        """Generate a single field value."""
        field_spec = FieldSpec(
            name="",
            data_type=DataType(data_type),
            **kwargs,
        )
        return field_spec.generate()


import uuid

__all__ = [
    "DataGeneratorAction",
    "DataSchema",
    "FieldSpec",
    "SequenceGenerator",
    "FakerIntegration",
    "DataType",
]
