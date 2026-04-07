"""Random utilities for RabAI AutoClick.

Provides:
- Random generation helpers
- Random selection utilities
- Random string generation
"""

import random
import string
import uuid
import hashlib
from typing import Any, List, Callable, Optional, TypeVar

T = TypeVar('T')


def random_int(min_val: int = 0, max_val: int = 100) -> int:
    """Generate random integer.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random integer.
    """
    return random.randint(min_val, max_val)


def random_float(min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Generate random float.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random float.
    """
    return random.uniform(min_val, max_val)


def random_bool(probability: float = 0.5) -> bool:
    """Generate random boolean.

    Args:
        probability: Probability of True.

    Returns:
        Random boolean.
    """
    return random.random() < probability


def random_choice(items: List[T]) -> Optional[T]:
    """Choose random item from list.

    Args:
        items: List to choose from.

    Returns:
        Random item or None if empty.
    """
    return random.choice(items) if items else None


def random_sample(items: List[T], count: int) -> List[T]:
    """Sample random items from list.

    Args:
        items: List to sample from.
        count: Number of items to sample.

    Returns:
        List of sampled items.
    """
    if count >= len(items):
        return items[:]
    return random.sample(items, count)


def random_shuffle(items: List[T]) -> List[T]:
    """Shuffle list randomly.

    Args:
        items: List to shuffle.

    Returns:
        Shuffled list.
    """
    result = items[:]
    random.shuffle(result)
    return result


def random_string(length: int = 10, charset: str = None) -> str:
    """Generate random string.

    Args:
        length: Length of string.
        charset: Character set to use.

    Returns:
        Random string.
    """
    if charset is None:
        charset = string.ascii_letters + string.digits
    return ''.join(random.choice(charset) for _ in range(length))


def random_alphanumeric(length: int = 10) -> str:
    """Generate random alphanumeric string.

    Args:
        length: Length of string.

    Returns:
        Random alphanumeric string.
    """
    return random_string(length, string.ascii_letters + string.digits)


def random_alpha(length: int = 10) -> str:
    """Generate random alphabetic string.

    Args:
        length: Length of string.

    Returns:
        Random alphabetic string.
    """
    return random_string(length, string.ascii_letters)


def random_numeric(length: int = 10) -> str:
    """Generate random numeric string.

    Args:
        length: Length of string.

    Returns:
        Random numeric string.
    """
    return random_string(length, string.digits)


def random_uuid() -> str:
    """Generate random UUID.

    Returns:
        Random UUID string.
    """
    return str(uuid.uuid4())


def random_hex(length: int = 10) -> str:
    """Generate random hexadecimal string.

    Args:
        length: Length of string.

    Returns:
        Random hex string.
    """
    return random_string(length, string.hexdigits.lower())


def random_bytes(length: int = 32) -> bytes:
    """Generate random bytes.

    Args:
        length: Number of bytes.

    Returns:
        Random bytes.
    """
    return bytes(random.randint(0, 255) for _ in range(length))


def random_hash(data: str = None) -> str:
    """Generate random hash.

    Args:
        data: Optional data to hash.

    Returns:
        SHA256 hash string.
    """
    if data is None:
        data = str(random.random())
    return hashlib.sha256(data.encode()).hexdigest()


def weighted_choice(items: List[T], weights: List[float]) -> Optional[T]:
    """Choose random item with weighted probability.

    Args:
        items: List of items.
        weights: List of weights corresponding to items.

    Returns:
        Selected item or None if empty.
    """
    if not items or not weights:
        return None
    return random.choices(items, weights=weights, k=1)[0]


def random_date_year(year: int = None) -> str:
    """Generate random date string in given year.

    Args:
        year: Year to generate date in.

    Returns:
        Date string (YYYY-MM-DD).
    """
    if year is None:
        year = random.randint(2000, 2100)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year:04d}-{month:02d}-{day:02d}"


def random_ip_v4() -> str:
    """Generate random IPv4 address.

    Returns:
        IPv4 address string.
    """
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"


def random_port() -> int:
    """Generate random port number.

    Returns:
        Port number (0-65535).
    """
    return random.randint(0, 65535)


def random_bool_string(true_string: str = "true", false_string: str = "false", probability: float = 0.5) -> str:
    """Generate random boolean as string.

    Args:
        true_string: String for True.
        false_string: String for False.
        probability: Probability of True.

    Returns:
        Random boolean string.
    """
    return true_string if random_bool(probability) else false_string


def random_from_set(*items: T) -> Optional[T]:
    """Choose random item from set.

    Args:
        *items: Items to choose from.

    Returns:
        Random item or None if empty.
    """
    if not items:
        return None
    return random.choice(list(items))


def random_color_rgb() -> tuple:
    """Generate random RGB color.

    Returns:
        Tuple of (r, g, b).
    """
    return (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))


def random_color_hex() -> str:
    """Generate random hex color.

    Returns:
        Hex color string.
    """
    r, g, b = random_color_rgb()
    return f"#{r:02x}{g:02x}{b:02x}"


def random_email(domain: str = None) -> str:
    """Generate random email address.

    Args:
        domain: Domain to use.

    Returns:
        Random email address.
    """
    if domain is None:
        domain = random.choice(["example.com", "test.com", "demo.com"])
    username = random_alphanumeric(random.randint(5, 15))
    return f"{username}@{domain}"


def random_url(protocol: str = "https") -> str:
    """Generate random URL.

    Args:
        protocol: Protocol to use.

    Returns:
        Random URL.
    """
    domain = random_alphanumeric(random.randint(5, 15))
    tld = random.choice(["com", "net", "org", "io"])
    path = random_alphanumeric(random.randint(3, 10))
    return f"{protocol}://{domain}.{tld}/{path}"


def random_phone(format_string: str = None) -> str:
    """Generate random phone number.

    Args:
        format_string: Format string.

    Returns:
        Random phone number.
    """
    if format_string is None:
        format_string = "+1-###-###-####"
    result = []
    for char in format_string:
        if char == '#':
            result.append(str(random.randint(0, 9)))
        else:
            result.append(char)
    return ''.join(result)


def random_credit_card() -> str:
    """Generate random credit card number (not valid).

    Returns:
        Random credit card number.
    """
    return ''.join(str(random.randint(0, 9)) for _ in range(16))


def random_paragraph(word_count: int = 50) -> str:
    """Generate random paragraph.

    Args:
        word_count: Number of words.

    Returns:
        Random paragraph.
    """
    words = [random_alpha(random.randint(3, 12)).lower() for _ in range(word_count)]
    words[0] = words[0].capitalize()
    return ' '.join(words) + '.'


def random_sentence(word_count: int = 10) -> str:
    """Generate random sentence.

    Args:
        word_count: Number of words.

    Returns:
        Random sentence.
    """
    words = [random_alpha(random.randint(3, 12)).lower() for _ in range(word_count)]
    words[0] = words[0].capitalize()
    return ' '.join(words) + '.'


def random_name(first_only: bool = False) -> str:
    """Generate random name.

    Args:
        first_only: Only generate first name.

    Returns:
        Random name.
    """
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                   "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]
    first = random.choice(first_names)
    if first_only:
        return first
    last = random.choice(last_names)
    return f"{first} {last}"


def random_coordinates(lat_range: tuple = (-90, 90), lon_range: tuple = (-180, 180)) -> tuple:
    """Generate random coordinates.

    Args:
        lat_range: Latitude range.
        lon_range: Longitude range.

    Returns:
        Tuple of (latitude, longitude).
    """
    lat = random.uniform(lat_range[0], lat_range[1])
    lon = random.uniform(lon_range[0], lon_range[1])
    return (lat, lon)


def random_password(length: int = 16) -> str:
    """Generate random password.

    Args:
        length: Password length.

    Returns:
        Random password.
    """
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(random.choice(chars) for _ in range(length))


def random_token(length: int = 32) -> str:
    """Generate random token.

    Args:
        length: Token length.

    Returns:
        Random token.
    """
    return random_hex(length)


def random_json_friendly() -> Any:
    """Generate random JSON-friendly value.

    Returns:
        Random JSON value.
    """
    value_type = random.randint(0, 3)
    if value_type == 0:
        return random_int(0, 1000)
    elif value_type == 1:
        return random_float(0.0, 100.0)
    elif value_type == 2:
        return random_alphanumeric(10)
    else:
        return random_bool()
