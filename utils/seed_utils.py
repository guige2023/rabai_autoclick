"""Random data seeding utilities for rabai_autoclick.

Provides realistic fake data generation for testing and development:
names, addresses, emails, UUIDs, timestamps, and weighted random choices.
"""

from __future__ import annotations

import random
import re
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Sequence

__all__ = [
    "Seeder",
    "SeedConfig",
    "fake_name",
    "fake_email",
    "fake_uuid",
    "fake_timestamp",
    "fake_choice",
    "weighted_choice",
    "random_string",
    "random_int",
    "random_float",
    "random_date",
    "FakeDataGenerator",
]


# --------------------------------------------------------------------------- #
# Static Helpers
# --------------------------------------------------------------------------- #

def random_string(
    length: int = 8,
    chars: str | None = None,
) -> str:
    """Return a random string of given length."""
    if chars is None:
        chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def random_int(
    low: int = 0,
    high: int = 100,
) -> int:
    """Return a random integer in [low, high]."""
    return random.randint(low, high)


def random_float(
    low: float = 0.0,
    high: float = 1.0,
    decimals: int = 2,
) -> float:
    """Return a random float in [low, high] rounded to ``decimals`` places."""
    val = random.uniform(low, high)
    return round(val, decimals)


def random_date(
    start: datetime | None = None,
    end: datetime | None = None,
) -> datetime:
    """Return a random datetime between ``start`` and ``end``."""
    if start is None:
        start = datetime.now(timezone.utc) - timedelta(days=365)
    if end is None:
        end = datetime.now(timezone.utc)
    delta = end - start
    offset = timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    return start + offset


def fake_uuid() -> str:
    """Return a random UUID string."""
    return str(uuid.uuid4())


def fake_timestamp(
    start: datetime | None = None,
    end: datetime | None = None,
) -> float:
    """Return a random Unix timestamp."""
    return random_date(start, end).timestamp()


def fake_choice(seq: Sequence[T]) -> T:
    """Return a random element from a sequence."""
    return random.choice(seq)


def weighted_choice(
    items: Sequence[T],
    weights: Sequence[float],
) -> T:
    """Return an item selected by weighted probability.

    Weights are normalized automatically.
    """
    return random.choices(items, weights=weights, k=1)[0]


# --------------------------------------------------------------------------- #
# Name / Email helpers
# --------------------------------------------------------------------------- #

_FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach", "Aria", "Blake", "Casey", "Dana", "Eli", "Faye",
    "Gavin", "Hannah", "Ivan", "Jade", "Kyle", "Luna", "Mason", "Nora",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
]

_EMAIL_DOMAINS = [
    "example.com", "test.org", "demo.net", "mail.io", "inbox.app",
    "acme.corp", "company.biz",
]

_PLURALS = {
    "address": "addresses",
    "city": "cities",
    "country": "countries",
    "phone": "phones",
    "entry": "entries",
    "record": "records",
}


def fake_name() -> str:
    """Return a random full name."""
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def fake_email(name: str | None = None, domain: str | None = None) -> str:
    """Return a plausible email address.

    If ``name`` is given, derives the local part from it.
    """
    if name:
        parts = name.lower().split()
        local = f"{parts[0]}.{parts[-1]}"
        local = re.sub(r"[^a-z.]", "", local)
    else:
        local = f"{random.choice(_FIRST_NAMES).lower()}.{random.choice(_LAST_NAMES).lower()}"
        local = re.sub(r"[^a-z.]", "", local)
        # Add random suffix to avoid duplicates
        local = f"{local}{random.randint(1, 999)}"

    if domain is None:
        domain = random.choice(_EMAIL_DOMAINS)
    return f"{local}@{domain}"


def fake_phone() -> str:
    """Return a random US-style phone number."""
    area = random.randint(200, 999)
    exchange = random.randint(200, 999)
    subscriber = random.randint(1000, 9999)
    return f"+1-{area}-{exchange}-{subscriber}"


# --------------------------------------------------------------------------- #
# Seeder Class
# --------------------------------------------------------------------------- #

@dataclass
class SeedConfig:
    """Configuration for the Seeder."""

    seed: int | None = None  # random seed; None = system entropy
    locale: str = "en_US"
    start_date: datetime | None = None
    end_date: datetime | None = None

    def __post_init__(self) -> None:
        if self.start_date is None:
            self.start_date = datetime.now() - timedelta(days=365)
        if self.end_date is None:
            self.end_date = datetime.now()


class Seeder:
    """Stateful seeded data generator.

    Provides consistent, reproducible fake data across runs when
    given the same seed.
    """

    def __init__(self, config: SeedConfig | None = None) -> None:
        self.config = config or SeedConfig()
        if self.config.seed is not None:
            random.seed(self.config.seed)

    def name(self) -> str:
        return fake_name()

    def email(self, name: str | None = None) -> str:
        return fake_email(name)

    def uuid(self) -> str:
        return fake_uuid()

    def timestamp(self) -> float:
        return fake_timestamp(self.config.start_date, self.config.end_date)

    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp())

    def int(self, low: int = 0, high: int = 100) -> int:
        return random.randint(low, high)

    def float(self, low: float = 0.0, high: float = 1.0, decimals: int = 2) -> float:
        return random_float(low, high, decimals)

    def string(self, length: int = 8, chars: str | None = None) -> str:
        return random_string(length, chars)

    def choice(self, seq: Sequence[T]) -> T:
        return random.choice(seq)

    def weighted_choice(
        self,
        items: Sequence[T],
        weights: Sequence[float],
    ) -> T:
        return random.choices(items, weights=weights, k=1)[0]

    def bool(self, true_probability: float = 0.5) -> bool:
        return random.random() < true_probability

    def phone(self) -> str:
        return fake_phone()

    def date(self) -> datetime:
        return random_date(self.config.start_date, self.config.end_date)

    def batch(self, factory: Callable[[], T], count: int) -> list[T]:
        return [factory() for _ in range(count)]


# --------------------------------------------------------------------------- #
# FakeDataGenerator (table-like generator)
# --------------------------------------------------------------------------- #

class FakeDataGenerator:
    """Generate batches of fake records with consistent field types.

    Usage::

        gen = FakeDataGenerator(seed=42)
        gen.define_field("id", gen.uuid)
        gen.define_field("name", gen.name)
        gen.define_field("email", gen.email)
        records = gen.generate(100)
    """

    def __init__(self, seed: int | None = None) -> None:
        self._fields: list[tuple[str, Callable[[], Any]]] = []
        self._seeder = Seeder(SeedConfig(seed=seed))

    def define_field(self, name: str, factory: Callable[[], Any]) -> "FakeDataGenerator":
        self._fields.append((name, factory))
        return self

    def define_field_from_seeder(
        self,
        name: str,
        method: str,
        **kwargs: Any,
    ) -> "FakeDataGenerator":
        factory = getattr(self._seeder, method)
        if kwargs:
            factory = lambda m=method, a=kwargs: getattr(self._seeder, m)(**a)  # noqa: E731
        return self.define_field(name, factory)

    def generate(self, count: int = 1) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for _ in range(count):
            record: dict[str, Any] = {}
            for name, factory in self._fields:
                record[name] = factory()
            records.append(record)
        return records

    def generate_dict(self) -> dict[str, Any]:
        """Generate a single record as a dict."""
        return self.generate(1)[0]
