"""ID generation utilities.

Provides various ID generation strategies including
UUIDs, sequential IDs, and custom generators.
"""

import hashlib
import secrets
import string
import time
import uuid as uuid_module
from typing import Any, Callable, Dict, Optional


class IDGenerator:
    """Base ID generator interface."""

    def generate(self) -> str:
        """Generate next ID."""
        raise NotImplementedError


class UUIDGenerator(IDGenerator):
    """UUID-based ID generator.

    Example:
        gen = UUIDGenerator()
        gen.generate()  # "550e8400-e29b-41d4-a716-446655440000"
    """

    def __init__(self, version: int = 4) -> None:
        self.version = version

    def generate(self) -> str:
        """Generate UUID."""
        if self.version == 1:
            return str(uuid_module.uuid1())
        elif self.version == 4:
            return str(uuid_module.uuid4())
        else:
            return str(uuid_module.uuid5(uuid_module.NAMESPACE_DNS, str(time.time())))

    def generate_many(self, count: int) -> list[str]:
        """Generate multiple UUIDs."""
        return [self.generate() for _ in range(count)]


class SequentialGenerator(IDGenerator):
    """Sequential integer ID generator.

    Example:
        gen = SequentialGenerator(prefix="user_")
        gen.generate()  # "user_000001"
        gen.generate()  # "user_000002"
    """

    def __init__(
        self,
        prefix: str = "",
        padding: int = 6,
        start: int = 1,
    ) -> None:
        self.prefix = prefix
        self.padding = padding
        self._current = start
        self._lock = None

    def generate(self) -> str:
        """Generate next sequential ID."""
        id_str = str(self._current).zfill(self.padding)
        self._current += 1
        return f"{self.prefix}{id_str}"

    def peek(self) -> str:
        """Peek at next ID without incrementing."""
        return f"{self.prefix}{str(self._current).zfill(self.padding)}"

    def reset(self, start: Optional[int] = None) -> None:
        """Reset counter."""
        if start is not None:
            self._current = start
        else:
            self._current = 1


class RandomStringGenerator(IDGenerator):
    """Random string ID generator.

    Example:
        gen = RandomStringGenerator(length=16, charset=string.ascii_letters)
        gen.generate()  # "aBcDeFgHiJkLmNoPq"
    """

    DEFAULT_CHARSETS = {
        "alphanumeric": string.ascii_letters + string.digits,
        "alpha": string.ascii_letters,
        "digits": string.digits,
        "hex": string.hexdigits.lower(),
        "safe": string.ascii_lowercase + string.digits + "_-",
    }

    def __init__(
        self,
        length: int = 16,
        charset: Optional[str] = None,
        prefix: str = "",
    ) -> None:
        self.length = length
        self.charset = charset or self.DEFAULT_CHARSETS["alphanumeric"]
        self.prefix = prefix

    def generate(self) -> str:
        """Generate random string ID."""
        chars = "".join(secrets.choice(self.charset) for _ in range(self.length))
        return f"{self.prefix}{chars}"

    def generate_many(self, count: int) -> list[str]:
        """Generate multiple random string IDs."""
        return [self.generate() for _ in range(count)]


class HashIDGenerator(IDGenerator):
    """Hash-based ID generator.

    Example:
        gen = HashIDGenerator(algorithm="sha256", length=12)
        gen.generate(input_data="some data")
    """

    def __init__(
        self,
        algorithm: str = "sha256",
        length: int = 16,
        prefix: str = "",
    ) -> None:
        self.algorithm = algorithm
        self.length = length
        self.prefix = prefix

    def generate(self, input_data: Any = None) -> str:
        """Generate hash-based ID."""
        if input_data is None:
            input_data = f"{time.time()}{secrets.token_hex(8)}"

        data = str(input_data).encode()
        hash_obj = hashlib.new(self.algorithm, data)
        hash_str = hash_obj.hexdigest()[:self.length]
        return f"{self.prefix}{hash_str}"


class TimeBasedGenerator(IDGenerator):
    """Time-based ID generator.

    Example:
        gen = TimeBasedGenerator(prefix="evt_")
        gen.generate()  # "evt_1704067200123abcd"
    """

    def __init__(
        self,
        prefix: str = "",
        random_suffix_length: int = 4,
    ) -> None:
        self.prefix = prefix
        self.random_suffix_length = random_suffix_length

    def generate(self) -> str:
        """Generate time-based ID."""
        timestamp = int(time.time() * 1000)
        random_part = secrets.token_hex(self.random_suffix_length)
        return f"{self.prefix}{timestamp}{random_part}"


class CompoundGenerator(IDGenerator):
    """Generator that combines multiple generators.

    Example:
        gen = CompoundGenerator(
            parts=[
                ("prefix", lambda: "user"),
                ("seq", SequentialGenerator(padding=4)),
                ("suffix", RandomStringGenerator(length=4))
            ],
            separator="_"
        )
        gen.generate()  # "user_0001_a3b2"
    """

    def __init__(
        self,
        parts: list[tuple[str, IDGenerator]],
        separator: str = "",
    ) -> None:
        self.parts = parts
        self.separator = separator

    def generate(self) -> str:
        """Generate compound ID."""
        parts = []
        for name, generator in self.parts:
            if callable(generator) and not isinstance(generator, IDGenerator):
                parts.append(str(generator()))
            else:
                parts.append(str(generator.generate()))
        return self.separator.join(parts)


class IDRegistry:
    """Registry for managing multiple ID generators.

    Example:
        registry = IDRegistry()
        registry.register("user", SequentialGenerator(prefix="u_"))
        registry.register("session", UUIDGenerator())
        user_id = registry.generate("user")
    """

    def __init__(self) -> None:
        self._generators: Dict[str, IDGenerator] = {}

    def register(self, name: str, generator: IDGenerator) -> None:
        """Register a generator."""
        self._generators[name] = generator

    def unregister(self, name: str) -> bool:
        """Unregister a generator."""
        if name in self._generators:
            del self._generators[name]
            return True
        return False

    def generate(self, name: str) -> str:
        """Generate ID using registered generator."""
        if name not in self._generators:
            raise KeyError(f"No generator registered for: {name}")
        return self._generators[name].generate()

    def has(self, name: str) -> bool:
        """Check if generator is registered."""
        return name in self._generators

    def list_generators(self) -> list[str]:
        """List all registered generator names."""
        return list(self._generators.keys())


def generate_short_code(length: int = 8) -> str:
    """Generate short alphanumeric code.

    Example:
        generate_short_code(6)  # "a3b5c7"
    """
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def generate_numeric_code(length: int = 6) -> str:
    """Generate numeric code.

    Example:
        generate_numeric_code(4)  # "7392"
    """
    return "".join(secrets.choice(string.digits) for _ in range(length))


def generate_invite_code() -> str:
    """Generate invite code with readable format.

    Example:
        generate_invite_code()  # "ABC-123-XYZ"
    """
    chars = string.ascii_uppercase.replace("O", "").replace("I", "")
    part1 = "".join(secrets.choice(chars) for _ in range(3))
    part2 = generate_numeric_code(3)
    part3 = "".join(secrets.choice(chars) for _ in range(3))
    return f"{part1}-{part2}-{part3}"


def generate_api_key() -> str:
    """Generate API key.

    Example:
        generate_api_key()  # "sk_live_a1b2c3d4e5f6..."
    """
    prefix = "sk_live_"
    random_part = secrets.token_urlsafe(32)
    return f"{prefix}{random_part}"


def generate_file_hash(data: bytes, algorithm: str = "sha256") -> str:
    """Generate hash of file data.

    Example:
        hash = generate_file_hash(file_content)
    """
    h = hashlib.new(algorithm)
    h.update(data)
    return h.hexdigest()


def ensure_uuid(value: str) -> str:
    """Ensure value is a valid UUID, convert if needed.

    Example:
        ensure_uuid("550e8400-e29b-41d4-a716-446655440000")
    """
    try:
        uuid_module.UUID(value)
        return value
    except ValueError:
        return str(uuid_module.uuid4())
