"""Random data seeding utilities.

Generate realistic fake data for testing, development, and demos.
Supports seeding for reproducibility and various data types.

Example:
    seeder = DataSeeder(random_seed=42)
    user = seeder.user()
    email = seeder.email()
    print(f"Name: {user['name']}, Email: {email}")
"""

from __future__ import annotations

import random
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False


@dataclass
class SeederConfig:
    """Configuration for data seeding."""
    random_seed: int | None = None
    locale: str = "en_US"
    include_none_chance: float = 0.0
    unique_suffix_length: int = 4


class DataSeeder:
    """Random data seeder with reproducible output.

    Generates realistic fake data for testing and development.
    Uses Faker library when available, falls back to built-in generators.
    """

    def __init__(self, config: SeederConfig | None = None) -> None:
        """Initialize data seeder.

        Args:
            config: Seeder configuration options.
        """
        self.config = config or SeederConfig()

        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)

        if FAKER_AVAILABLE:
            self._faker = Faker(self.config.locale)
            if self.config.random_seed is not None:
                Faker.seed(self.config.random_seed)
        else:
            self._faker = None

    def reset(self, seed: int | None = None) -> None:
        """Reset random state for reproducible output.

        Args:
            seed: New random seed. Uses existing if not provided.
        """
        actual_seed = seed if seed is not None else self.config.random_seed
        if actual_seed is not None:
            random.seed(actual_seed)
            if FAKER_AVAILABLE:
                Faker.seed(actual_seed)

    def user(self, **overrides: Any) -> dict[str, Any]:
        """Generate a fake user record.

        Returns:
            Dict with user fields.
        """
        data = {
            "id": self.uuid(),
            "name": self.name(),
            "email": self.email(),
            "username": self.username(),
            "phone": self.phone_number(),
            "created_at": self.past_datetime(),
            "updated_at": self.past_datetime(),
            "is_active": random.choice([True, True, True, False]),
            "is_verified": random.choice([True, False]),
            "avatar_url": self.avatar_url(),
            "bio": self.text(max_chars=200) if random.random() > 0.3 else None,
            "locale": random.choice(["en_US", "zh_CN", "ja_JP", "de_DE"]),
        }
        data.update(overrides)
        return self._apply_none_chance(data)

    def order(self, user_id: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Generate a fake order record.

        Returns:
            Dict with order fields.
        """
        data = {
            "id": self.uuid(),
            "order_number": self.order_number(),
            "user_id": user_id or self.uuid(),
            "status": random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
            "total_amount": self.decimal(min_val=10.0, max_val=5000.0, decimals=2),
            "currency": random.choice(["USD", "EUR", "CNY"]),
            "created_at": self.past_datetime(),
            "shipped_at": self.past_datetime() if random.random() > 0.5 else None,
            "delivered_at": self.past_datetime() if random.random() > 0.6 else None,
            "shipping_address": self.address(),
            "notes": self.text(max_chars=500) if random.random() > 0.7 else None,
        }
        data.update(overrides)
        return self._apply_none_chance(data)

    def product(self, **overrides: Any) -> dict[str, Any]:
        """Generate a fake product record."""
        data = {
            "id": self.uuid(),
            "sku": self.sku(),
            "name": self.product_name(),
            "description": self.text(max_chars=1000),
            "price": self.decimal(min_val=1.0, max_val=999.99, decimals=2),
            "cost": self.decimal(min_val=0.5, max_val=500.0, decimals=2),
            "category": random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"]),
            "stock_quantity": random.randint(0, 1000),
            "is_active": random.random() > 0.1,
            "weight_kg": round(random.uniform(0.1, 50.0), 2),
            "created_at": self.past_datetime(),
        }
        data.update(overrides)
        return self._apply_none_chance(data)

    def email(self) -> str:
        """Generate a random email address."""
        if self._faker:
            return self._faker.email()
        return f"{self.username()}@example.com"

    def name(self) -> str:
        """Generate a random full name."""
        if self._faker:
            return self._faker.name()
        first = random.choice(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Henry"])
        last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"])
        return f"{first} {last}"

    def username(self) -> str:
        """Generate a random username."""
        if self._faker:
            return self._faker.user_name()
        chars = string.ascii_lowercase + string.digits
        return "".join(random.choices(chars, k=random.randint(6, 12)))

    def phone_number(self) -> str:
        """Generate a random phone number."""
        if self._faker:
            return self._faker.phone_number()
        return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

    def address(self) -> dict[str, str]:
        """Generate a random address."""
        if self._faker:
            f = self._faker
            return {
                "street": f.street_address(),
                "city": f.city(),
                "state": f.state(),
                "postal_code": f.postcode(),
                "country": f.country(),
            }
        return {
            "street": f"{random.randint(1, 9999)} Main St",
            "city": "Springfield",
            "state": "CA",
            "postal_code": f"{random.randint(10000, 99999)}",
            "country": "USA",
        }

    def uuid(self) -> str:
        """Generate a random UUID."""
        return str(uuid.uuid4())

    def past_datetime(self, days_back: int = 365) -> datetime:
        """Generate a random datetime in the past."""
        if self._faker:
            return self._faker.date_time_between(
                start_date=f"-{days_back}d",
                end_date="now",
            )
        offset = timedelta(seconds=random.randint(0, days_back * 86400))
        return datetime.utcnow() - offset

    def future_datetime(self, days_forward: int = 30) -> datetime:
        """Generate a random datetime in the future."""
        offset = timedelta(seconds=random.randint(0, days_forward * 86400))
        return datetime.utcnow() + offset

    def decimal(self, min_val: float, max_val: float, decimals: int = 2) -> float:
        """Generate a random decimal in range."""
        val = random.uniform(min_val, max_val)
        return round(val, decimals)

    def integer(self, min_val: int, max_val: int) -> int:
        """Generate a random integer in range."""
        return random.randint(min_val, max_val)

    def text(self, max_chars: int = 200) -> str:
        """Generate random text."""
        if self._faker:
            return self._faker.text(max_nb_chars=max_chars)
        words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
                 "adipiscing", "elit", "sed", "do", "eiusmod"]
        text = " ".join(random.choices(words, k=random.randint(10, 50)))
        return text[:max_chars]

    def boolean(self) -> bool:
        """Generate a random boolean."""
        return random.choice([True, False])

    def choice(self, options: Sequence[Any]) -> Any:
        """Pick a random element from options."""
        return random.choice(list(options))

    def sku(self) -> str:
        """Generate a random SKU code."""
        prefix = random.choice(["SKU", "PRD", "ITM"])
        number = "".join(random.choices(string.digits, k=8))
        return f"{prefix}-{number}"

    def order_number(self) -> str:
        """Generate a random order number."""
        return f"ORD-{datetime.utcnow().year}-{random.randint(100000, 999999)}"

    def product_name(self) -> str:
        """Generate a random product name."""
        if self._faker:
            return self._faker.catch_phrase()
        adjectives = ["Premium", "Deluxe", "Ultra", "Professional", "Essential"]
        nouns = ["Widget", "Gadget", "Device", "Tool", "System"]
        return f"{random.choice(adjectives)} {random.choice(nouns)} {random.randint(100, 999)}"

    def avatar_url(self) -> str:
        """Generate a random avatar URL."""
        seed = random.randint(1, 70)
        return f"https://i.pravatar.cc/150?img={seed}"

    def company(self) -> dict[str, Any]:
        """Generate a fake company record."""
        return {
            "id": self.uuid(),
            "name": self.company_name(),
            "domain": self.domain(),
            "industry": random.choice(["Technology", "Finance", "Healthcare", "Retail", "Manufacturing"]),
            "employee_count": random.randint(5, 10000),
            "founded_year": random.randint(1950, 2023),
        }

    def company_name(self) -> str:
        """Generate a random company name."""
        if self._faker:
            return self._faker.company()
        suffixes = ["Inc", "LLC", "Corp", "Ltd", "Co"]
        return f"{self.name().split()[0]} {random.choice(suffixes)}"

    def domain(self) -> str:
        """Generate a random domain name."""
        if self._faker:
            return self._faker.domain_name()
        return f"{self.username()}.com"

    def _apply_none_chance(self, data: dict[str, Any]) -> dict[str, Any]:
        """Apply none chance to nullable fields."""
        chance = self.config.include_none_chance
        if chance <= 0:
            return data
        return {
            k: None if v is not None and random.random() < chance else v
            for k, v in data.items()
        }


class SeededSequence:
    """Deterministic infinite sequence with seeded randomness."""

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._index = 0

    def __iter__(self) -> "SeededSequence":
        return self

    def __next__(self) -> int:
        val = self._index
        self._index += 1
        return val

    def choice(self, options: Sequence[Any]) -> Any:
        return self._rng.choice(options)


def generate_batch(
    seeder: DataSeeder,
    factory: Callable[[], dict[str, Any]],
    count: int,
) -> list[dict[str, Any]]:
    """Generate a batch of seeded records.

    Args:
        seeder: DataSeeder instance.
        factory: Factory function that generates one record.
        count: Number of records to generate.

    Returns:
        List of generated records.
    """
    return [factory() for _ in range(count)]


def bulk_insert(
    connection: Any,
    table: str,
    records: list[dict[str, Any]],
    batch_size: int = 100,
) -> int:
    """Bulk insert records into database.

    Args:
        connection: Database connection.
        table: Target table name.
        records: List of dicts with column names as keys.
        batch_size: Records per INSERT statement.

    Returns:
        Total number of records inserted.
    """
    if not records:
        return 0

    cursor = connection.cursor()
    columns = list(records[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))

    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        values = [tuple(row.get(col) for col in columns) for row in batch]
        cursor.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )
        total += len(batch)

    connection.commit()
    return total
