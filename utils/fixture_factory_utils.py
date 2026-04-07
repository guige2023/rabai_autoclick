"""Test fixture factory utilities: model factories, random data generation, and sequences."""

from __future__ import annotations

import random
import string
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Type

__all__ = [
    "Field",
    "Factory",
    "FixtureFactory",
    "build",
    "build_many",
    "lazy",
    "sequence",
    "faker",
]


def sequence(n: int) -> int:
    """Sequence generator starting from n."""
    return n


def lazy(func: Callable[[], Any]) -> Callable[[], Any]:
    """Lazy evaluation wrapper - calls func each time."""
    return func


class Field:
    """Represents a factory field with optional default or callable."""

    def __init__(
        self,
        default: Any | None = None,
        factory: Callable[[], Any] | None = None,
    ) -> None:
        self.default = default
        self.factory = factory

    def resolve(self) -> Any:
        """Resolve the field value."""
        if self.factory is not None:
            return self.factory()
        if self.default is not None:
            if callable(self.default) and not isinstance(self.default, (str, int, float, bool, list, dict)):
                return self.default()
            return self.default
        return None


@dataclass
class Factory:
    """Base factory class for creating test fixtures."""

    class Meta:
        model: Type = object

    @classmethod
    def build(cls, **overrides: Any) -> Any:
        """Build a single instance with optional field overrides."""
        obj = cls.__new__(cls.Meta.model)
        for name, value in cls.__dict__.items():
            if isinstance(value, Field):
                setattr(obj, name, value.resolve())
        for name, value in overrides.items():
            setattr(obj, name, value)
        return obj

    @classmethod
    def build_many(cls, count: int, **overrides: Any) -> list[Any]:
        """Build multiple instances."""
        return [cls.build(**overrides) for _ in range(count)]


def faker() -> "Faker":
    """Get a shared Faker instance."""
    return _FAKER


class Faker:
    """Random data generation for test fixtures."""

    @staticmethod
    def uuid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def name() -> str:
        first = random.choice(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"])
        last = random.choice(["Smith", "Jones", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson"])
        return f"{first} {last}"

    @staticmethod
    def email() -> str:
        return f"{Faker.username()}@{Faker.domain()}"

    @staticmethod
    def username() -> str:
        chars = string.ascii_lowercase
        return "".join(random.choices(chars, k=8))

    @staticmethod
    def domain() -> str:
        return random.choice(["example.com", "test.org", "demo.net", "mail.io"])

    @staticmethod
    def url() -> str:
        return f"https://{Faker.domain()}/path/{Faker.uuid()[:8]}"

    @staticmethod
    def ip_v4() -> str:
        return ".".join(str(random.randint(1, 255)) for _ in range(4))

    @staticmethod
    def ip_v6() -> str:
        return ":".join(f"{random.randint(0, 65535):x}" for _ in range(8))

    @staticmethod
    def phone() -> str:
        return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

    @staticmethod
    def address() -> str:
        num = random.randint(100, 9999)
        street = random.choice(["Main", "Oak", "Elm", "Pine", "Maple", "Cedar", "Washington", "Park"])
        return f"{num} {street} St"

    @staticmethod
    def city() -> str:
        return random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"])

    @staticmethod
    def country() -> str:
        return random.choice(["United States", "Canada", "United Kingdom", "Germany", "France", "Japan", "Australia", "Brazil"])

    @staticmethod
    def text(max_chars: int = 200) -> str:
        words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod", "tempor"]
        return " ".join(random.choices(words, k=random.randint(10, max_chars // 5)))[:max_chars]

    @staticmethod
    def sentence(min_words: int = 5, max_words: int = 15) -> str:
        words = Faker.text(max_chars=max_words * 6).split()
        sentence_words = random.choices(words, k=random.randint(min_words, max_words))
        sentence_words[0] = sentence_words[0].capitalize()
        return " ".join(sentence_words) + "."

    @staticmethod
    def paragraph(min_sentences: int = 2, max_sentences: int = 5) -> str:
        return " ".join(
            Faker.sentence() for _ in range(random.randint(min_sentences, max_sentences))
        )

    @staticmethod
    def integer(min_val: int = 0, max_val: int = 1000) -> int:
        return random.randint(min_val, max_val)

    @staticmethod
    def float_(min_val: float = 0.0, max_val: float = 1000.0, decimals: int = 2) -> float:
        val = random.uniform(min_val, max_val)
        return round(val, decimals)

    @staticmethod
    def boolean() -> bool:
        return random.choice([True, False])

    @staticmethod
    def date(
        start_days_ago: int = 365,
        end_days_ago: int = 0,
    ) -> datetime:
        now = datetime.now()
        start = now - timedelta(days=start_days_ago)
        end = now - timedelta(days=end_days_ago)
        delta = end - start
        return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

    @staticmethod
    def date_iso(
        start_days_ago: int = 365,
        end_days_ago: int = 0,
    ) -> str:
        return Faker.date(start_days_ago, end_days_ago).isoformat()

    @staticmethod
    def choice(choices: list[Any]) -> Any:
        return random.choice(choices)

    @staticmethod
    def sample(population: list[Any], k: int) -> list[Any]:
        return random.sample(population, k=min(k, len(population)))


_FAKER = Faker()


class FixtureFactory:
    """Registry of named fixture factories."""

    _registry: dict[str, type[Factory]] = {}

    @classmethod
    def register(cls, name: str, factory: type[Factory]) -> None:
        cls._registry[name] = factory

    @classmethod
    def get(cls, name: str) -> type[Factory]:
        return cls._registry[name]

    @classmethod
    def build(cls, name: str, **overrides: Any) -> Any:
        return cls.get(name).build(**overrides)

    @classmethod
    def build_many(cls, name: str, count: int, **overrides: Any) -> list[Any]:
        return cls.get(name).build_many(count, **overrides)


def build(factory: type[Factory], **overrides: Any) -> Any:
    """Convenience function to build a fixture."""
    return factory.build(**overrides)


def build_many(factory: type[Factory], count: int, **overrides: Any) -> list[Any]:
    """Convenience function to build multiple fixtures."""
    return factory.build_many(count, **overrides)
