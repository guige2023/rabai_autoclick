"""Random data generation utilities.

Provides random data generators for testing and
automation workflow initialization.
"""

import random
import string
import uuid
from typing import List, Optional, Tuple


def random_string(length: int = 10, charset: Optional[str] = None) -> str:
    """Generate random string.

    Args:
        length: Length of string.
        charset: Character set to use.

    Returns:
        Random string.
    """
    if charset is None:
        charset = string.ascii_letters + string.digits
    return "".join(random.choice(charset) for _ in range(length))


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


def random_digits(length: int = 10) -> str:
    """Generate random numeric string.

    Args:
        length: Length of string.

    Returns:
        Random numeric string.
    """
    return random_string(length, string.digits)


def random_hex(length: int = 10) -> str:
    """Generate random hexadecimal string.

    Args:
        length: Length of string.

    Returns:
        Random hex string.
    """
    return random_string(length, string.hexdigits.lower())


def random_int(min_val: int = 0, max_val: int = 100) -> int:
    """Generate random integer in range.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random integer.
    """
    return random.randint(min_val, max_val)


def random_float(min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Generate random float in range.

    Args:
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Random float.
    """
    return random.uniform(min_val, max_val)


def random_bool(prob_true: float = 0.5) -> bool:
    """Generate random boolean.

    Args:
        prob_true: Probability of True.

    Returns:
        Random boolean.
    """
    return random.random() < prob_true


def random_uuid() -> str:
    """Generate random UUID.

    Returns:
        UUID string.
    """
    return str(uuid.uuid4())


def random_choice(choices: List[T]) -> T:
    """Randomly select from choices.

    Args:
        choices: List of choices.

    Returns:
        Random choice.
    """
    return random.choice(choices)


def random_sample(population: List[T], k: int) -> List[T]:
    """Randomly sample k items from population.

    Args:
        population: Population to sample from.
        k: Number of items to sample.

    Returns:
        List of sampled items.
    """
    return random.sample(population, k)


def random_color_rgb() -> Tuple[int, int, int]:
    """Generate random RGB color.

    Returns:
        (R, G, B) tuple.
    """
    return (random_int(0, 255), random_int(0, 255), random_int(0, 255))


def random_color_hex() -> str:
    """Generate random hex color.

    Returns:
        Hex color string (e.g., "#A3F4C8").
    """
    return "#{:02X}{:02X}{:02X}".format(*random_color_rgb())


def random_date(
    start_year: int = 2000,
    end_year: int = 2030,
) -> Tuple[int, int, int]:
    """Generate random date.

    Args:
        start_year: Start year.
        end_year: End year.

    Returns:
        (year, month, day) tuple.
    """
    year = random_int(start_year, end_year)
    month = random_int(1, 12)
    day = random_int(1, 28)
    return (year, month, day)


def random_ip() -> str:
    """Generate random IPv4 address.

    Returns:
        IP address string.
    """
    return ".".join(str(random_int(0, 255)) for _ in range(4))


def random_email(domain: Optional[str] = None) -> str:
    """Generate random email address.

    Args:
        domain: Domain name. Random if None.

    Returns:
        Email address string.
    """
    if domain is None:
        domain = f"{random_alpha(6)}.com"
    username = random_alphanumeric(random_int(5, 12))
    return f"{username}@{domain}"


def random_name() -> str:
    """Generate random name.

    Returns:
        Random name string.
    """
    first = random.choice(["James", "John", "Mary", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"])
    last = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"])
    return f"{first} {last}"


def random_phone() -> str:
    """Generate random phone number.

    Returns:
        Phone number string.
    """
    return f"+1-{random_digits(3)}-{random_digits(3)}-{random_digits(4)}"


def shuffle_in_place(lst: List[T]) -> None:
    """Shuffle list in place.

    Args:
        lst: List to shuffle.
    """
    random.shuffle(lst)
