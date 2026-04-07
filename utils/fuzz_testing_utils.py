"""
Fuzz testing utilities for discovering bugs and edge cases.

Provides fuzzing strategies, input mutation, coverage-guided fuzzing,
corpus management, and test case minimization.
"""

from __future__ import annotations

import copy
import hashlib
import logging
import random
import string
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class FuzzResult:
    """Result of a fuzzing campaign."""
    total_iterations: int = 0
    crashes: list[tuple[str, bytes]] = field(default_factory=list)
    unique_hashes: set[str] = field(default_factory=set)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def crashes_found(self) -> int:
        return len(self.crashes)


class FuzzInputGenerator:
    """Generates fuzzed inputs for various data types."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)

    def string(self, min_len: int = 0, max_len: int = 100, charset: Optional[str] = None) -> str:
        """Generate a random string."""
        if charset is None:
            charset = string.ascii_letters + string.digits + string.punctuation + " \t\n"
        length = self.rng.randint(min_len, max_len)
        return "".join(self.rng.choice(charset) for _ in range(length))

    def bytes(self, min_len: int = 0, max_len: int = 1000) -> bytes:
        """Generate random bytes."""
        length = self.rng.randint(min_len, max_len)
        return bytes(self.rng.randint(0, 255) for _ in range(length))

    def integers(self, count: int, min_val: int = -10000, max_val: int = 10000) -> list[int]:
        """Generate random integers."""
        return [self.rng.randint(min_val, max_val) for _ in range(count)]

    def floats(self, count: int, min_val: float = -1e6, max_val: float = 1e6) -> list[float]:
        """Generate random floats."""
        return [self.rng.uniform(min_val, max_val) for _ in range(count)]

    def choice(self, options: list[Any]) -> Any:
        """Randomly pick from options."""
        return self.rng.choice(options)

    def ipv4(self) -> str:
        """Generate a random IPv4 address."""
        return ".".join(str(self.rng.randint(1, 254)) for _ in range(4))

    def email(self) -> str:
        """Generate a random email address."""
        user = self.string(3, 10)
        domain = self.choice(["example.com", "test.org", "mail.net"])
        return f"{user}@{domain}"

    def url(self) -> str:
        """Generate a random URL."""
        schemes = ["http", "https", "ftp", "file"]
        path = "/" + "/".join(self.string(3, 8) for _ in range(self.rng.randint(1, 4)))
        return f"{self.rng.choice(schemes)}://{self.ipv4()}{path}"

    def json(self, depth: int = 3) -> Any:
        """Generate a random JSON-like structure."""
        if depth <= 0:
            typ = self.rng.choice(["string", "int", "float", "bool", "null"])
        else:
            typ = self.rng.choice(["string", "int", "float", "bool", "null", "list", "dict"])

        if typ == "string":
            return self.string(0, 20)
        elif typ == "int":
            return self.rng.randint(-10000, 10000)
        elif typ == "float":
            return self.rng.uniform(-1e6, 1e6)
        elif typ == "bool":
            return self.rng.choice([True, False])
        elif typ == "null":
            return None
        elif typ == "list":
            return [self.json(depth - 1) for _ in range(self.rng.randint(0, 5))]
        elif typ == "dict":
            return {self.string(2, 6): self.json(depth - 1) for _ in range(self.rng.randint(0, 4))}

    def binary_struct(self, format_str: str) -> bytes:
        """Generate random binary data matching a struct format."""
        def gen_char(fm: str) -> bytes:
            if fm == "b":
                return struct.pack("b", self.rng.randint(-128, 127))
            elif fm == "B":
                return struct.pack("B", self.rng.randint(0, 255))
            elif fm == "h":
                return struct.pack("h", self.rng.randint(-32768, 32767))
            elif fm == "H":
                return struct.pack("H", self.rng.randint(0, 65535))
            elif fm == "i" or fm == "l":
                return struct.pack("i", self.rng.randint(-2147483648, 2147483647))
            elif fm == "I" or fm == "L":
                return struct.pack("I", self.rng.randint(0, 4294967295))
            elif fm == "f":
                return struct.pack("f", self.rng.uniform(-1e10, 1e10))
            elif fm == "d":
                return struct.pack("d", self.rng.uniform(-1e100, 1e100))
            elif fm == "?":
                return struct.pack("?", self.rng.choice([True, False]))
            elif fm == "s":
                return self.string(1, 10).encode()
            return b"\x00"

        result = b""
        i = 0
        fmt = format_str
        while i < len(fmt):
            if fmt[i] in "0123456789":
                count = int(fmt[i])
                if i + 1 < len(fmt) and fmt[i + 1] == "s":
                    result += self.string(count).encode()
                    i += 2
                    continue
            result += gen_char(fmt[i])
            i += 1
        return result


class MutationEngine:
    """Mutates input data to discover edge cases."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)

    def mutate_bytes(self, data: bytes) -> bytes:
        """Apply random mutations to bytes."""
        mutations = [
            self._flip_bit,
            self._insert_byte,
            self._delete_byte,
            self._swap_bytes,
            self._duplicate_section,
            self._insert_null,
            self._truncate,
            self._add_boundary_value,
        ]
        mutation = self.rng.choice(mutations)
        return mutation(data)

    def _flip_bit(self, data: bytes) -> bytes:
        if not data:
            return data
        arr = bytearray(data)
        byte_idx = self.rng.randint(0, len(arr) - 1)
        bit_idx = self.rng.randint(0, 7)
        arr[byte_idx] ^= (1 << bit_idx)
        return bytes(arr)

    def _insert_byte(self, data: bytes) -> bytes:
        arr = list(data)
        pos = self.rng.randint(0, len(arr))
        arr.insert(pos, self.rng.randint(0, 255))
        return bytes(arr)

    def _delete_byte(self, data: bytes) -> bytes:
        if len(data) <= 1:
            return data
        arr = list(data)
        del arr[self.rng.randint(0, len(arr) - 1)]
        return bytes(arr)

    def _swap_bytes(self, data: bytes) -> bytes:
        if len(data) < 2:
            return data
        arr = bytearray(data)
        i, j = self.rng.sample(range(len(arr)), 2)
        arr[i], arr[j] = arr[j], arr[i]
        return bytes(arr)

    def _duplicate_section(self, data: bytes) -> bytes:
        if len(data) < 2:
            return data
        size = self.rng.randint(1, min(10, len(data) // 2))
        pos = self.rng.randint(0, len(data) - size)
        section = data[pos:pos + size]
        insert_pos = self.rng.randint(0, len(data))
        arr = list(data)
        arr[insert_pos:insert_pos] = section
        return bytes(arr)

    def _insert_null(self, data: bytes) -> bytes:
        arr = list(data)
        pos = self.rng.randint(0, len(arr))
        arr.insert(pos, 0)
        return bytes(arr)

    def _truncate(self, data: bytes) -> bytes:
        if len(data) <= 1:
            return data
        new_len = self.rng.randint(0, len(data) - 1)
        return data[:new_len]

    def _add_boundary_value(self, data: bytes) -> bytes:
        boundary_values = [0, 127, 128, 255, 0xFF, 0x7F, 0x80]
        arr = list(data)
        pos = self.rng.randint(0, len(arr))
        arr.insert(pos, self.rng.choice(boundary_values))
        return bytes(arr)

    def mutate_string(self, s: str) -> str:
        """Apply random mutations to a string."""
        mutations = [
            lambda x: x + self.rng.choice(string.printable),
            lambda x: self.rng.choice(string.printable) + x,
            lambda x: x[:-1] if x else x,
            lambda x: x.replace(self.rng.choice(x) if x else "", self.rng.choice(string.printable)),
            lambda x: x[:len(x)//2] + self.rng.choice(string.whitespace) + x[len(x)//2:],
        ]
        mutation = self.rng.choice(mutations)
        return mutation(s)


class Fuzzer:
    """Main fuzzing engine for testing functions."""

    def __init__(
        self,
        target_func: Callable[..., Any],
        generator: FuzzInputGenerator,
        max_iterations: int = 10000,
        timeout_per_iteration: float = 1.0,
    ) -> None:
        self.target_func = target_func
        self.generator = generator
        self.max_iterations = max_iterations
        self.timeout_per_iteration = timeout_per_iteration
        self.mutator = MutationEngine()
        self.corpus: list[bytes] = []
        self.result = FuzzResult()

    def add_corpus(self, data: list[bytes]) -> None:
        """Add initial corpus samples."""
        self.corpus.extend(data)

    def fuzz(self) -> FuzzResult:
        """Run the fuzzing campaign."""
        self.result = FuzzResult()
        corpus = copy.deepcopy(self.corpus)

        for i in range(self.max_iterations):
            if corpus:
                base = self.rng.choice(corpus) if hasattr(self, "rng") else corpus[0]
                if isinstance(base, bytes):
                    input_data = self.mutator.mutate_bytes(base)
                else:
                    input_data = self.mutator.mutate_string(str(base))
            else:
                input_data = self.generator.bytes()

            self.result.total_iterations += 1
            self._test_input(input_data)

            if i % 1000 == 0 and i > 0:
                logger.info("Fuzzing progress: %d iterations, %d crashes", i, self.result.crashes_found)

        self.result.end_time = time.time()
        return self.result

    def _test_input(self, data: bytes) -> None:
        """Test a single input against the target function."""
        try:
            self.target_func(data)
        except Exception as e:
            crash_hash = hashlib.md5(data).hexdigest()
            if crash_hash not in self.result.unique_hashes:
                self.result.unique_hashes.add(crash_hash)
                self.result.crashes.append((str(e), data))
                logger.warning("Crash found: %s", e)

    @property
    def rng(self) -> random.Random:
        return self.generator.rng


def minimize_crash_input(crash_input: bytes, target_func: Callable[..., Any]) -> bytes:
    """Minimize a crash input to the smallest reproducing case."""
    current = bytearray(crash_input)
    changed = True

    while changed and len(current) > 1:
        changed = False
        original = bytes(current)
        for i in range(len(current) - 1, -1, -1):
            trial = bytearray(original)
            del trial[i]
            try:
                target_func(bytes(trial))
            except Exception:
                current = trial
                changed = True
                break

    return bytes(current)
