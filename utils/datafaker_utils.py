"""
datafaker_utils.py - Fake data generation utilities.

Provides comprehensive fake data generation for testing and development,
supporting multiple locales with type-safe generators.
"""

from __future__ import annotations

import random
import re
import string
import unicodedata
from datetime import date, datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")


class LocaleConfig:
    """Configuration for locale-specific fake data generation."""

    # Supported locales
    SUPPORTED_LOCALES = ["en_US", "zh_CN", "en_GB", "de_DE", "fr_FR", "ja_JP", "ko_KR"]

    # Locale-specific first name data
    FIRST_NAMES: Dict[str, List[str]] = {
        "en_US": ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                  "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica"],
        "zh_CN": ["伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "军", "洋", "勇", "艳", "杰", "涛", "明"],
        "en_GB": ["Oliver", "Harry", "Jack", "George", "Noah", "Charlie", "Archie", "Freddie",
                  "Amelia", "Olivia", "Isla", "Emily", "Poppy", "Ava", "Isabella", "Lily"],
        "de_DE": ["Anna", "Julia", "Katharina", "Martina", "Thomas", "Michael", "Peter", "Christian",
                  "Andreas", "Stefan", "Sebastian", "Alexander", "Daniel", "Marcel", "Patrick", "Frank"],
    }

    # Locale-specific last name data
    LAST_NAMES: Dict[str, List[str]] = {
        "en_US": ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"],
        "zh_CN": ["王", "李", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴", "徐", "孙", "马", "朱", "胡", "郭"],
        "en_GB": ["Smith", "Jones", "Williams", "Taylor", "Davies", "Brown", "Wilson", "Evans",
                  "Thomas", "Johnson", "Roberts", "Walker", "Wright", "Robinson", "Thompson", "White"],
        "de_DE": ["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker",
                  "Schulz", "Hoffmann", "Koch", "Bauer", "Richter", "Klein", "Wolf", "Schröder"],
    }

    # Locale-specific city data
    CITIES: Dict[str, List[str]] = {
        "en_US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                  "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Seattle", "Denver", "Boston"],
        "zh_CN": ["北京", "上海", "深圳", "广州", "杭州", "成都", "南京", "武汉", "西安", "苏州", "天津", "重庆"],
        "en_GB": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Leeds", "Sheffield",
                  "Edinburgh", "Bristol", "Cardiff", "Oxford", "Cambridge", "York", "Bath"],
        "de_DE": ["Berlin", "Hamburg", "München", "Köln", "Frankfurt", "Stuttgart", "Düsseldorf",
                  "Leipzig", "Dortmund", "Essen", "Bremen", "Dresden", "Hannover", "Nürnberg"],
    }

    # Locale-specific company suffixes
    COMPANY_SUFFIXES: Dict[str, List[str]] = {
        "en_US": ["Inc", "LLC", "Corp", "Group", "Solutions", "Systems", "Technologies", "Services"],
        "zh_CN": ["科技有限公司", "集团", "有限公司", "实业公司", "网络公司", "信息公司"],
        "en_GB": ["Ltd", "Plc", "Ltd", "Group", "Holdings", "Partners", "Associates", "Services"],
        "de_DE": ["GmbH", "AG", "Co. KG", "SE", "Holding", "Gruppe", "Service", "Technologie"],
    }

    # Locale-specific street suffixes
    STREET_SUFFIXES: Dict[str, List[str]] = {
        "en_US": ["St", "Ave", "Blvd", "Dr", "Rd", "Ln", "Way", "Pl", "Ct", "Cir"],
        "zh_CN": ["路", "街", "大道", "巷", "弄", "环路", "大街", "小巷"],
        "en_GB": ["Road", "Street", "Lane", "Avenue", "Drive", "Way", "Gardens", "Close", "Crescent"],
        "de_DE": ["straße", "weg", "platz", "allee", "ring", "gasse", "ufer", "berg"],
    }

    @classmethod
    def get_locale(cls, locale: Optional[str] = None) -> str:
        """Get a valid locale string, defaulting to en_US."""
        return locale if locale in cls.SUPPORTED_LOCALES else "en_US"


class DataFaker:
    """
    Main fake data generation class with locale support.

    Provides type-safe generators for common data types including
    names, addresses, companies, dates, and more.

    Example:
        >>> faker = DataFaker(locale="en_US")
        >>> faker.name()
        'James Smith'
        >>> faker.email()
        'james.smith@example.com'
        >>> faker.address()
        '123 Main St, New York, NY 10001'
    """

    # Email domain pools
    EMAIL_DOMAINS = [
        "example.com", "test.org", "demo.net", "sample.io",
        "mailinator.com", "guerrillamail.com", "tempmail.com",
    ]

    # URL TLDs
    URL_TLDS = [".com", ".org", ".net", ".io", ".co", ".app", ".dev", ".ai"]

    # Job titles
    JOB_TITLES = [
        "Software Engineer", "Product Manager", "Designer", "Data Scientist",
        "DevOps Engineer", "QA Engineer", "Technical Lead", "Architect",
        "Engineering Manager", "VP of Engineering", "CTO", "CEO", "CFO",
        "Marketing Manager", "Sales Representative", "HR Manager",
        "Business Analyst", "Project Manager", "Scrum Master",
    ]

    # Color names
    COLOR_NAMES = [
        "Red", "Blue", "Green", "Yellow", "Orange", "Purple", "Pink", "Brown",
        "Black", "White", "Gray", "Navy", "Teal", "Coral", "Crimson", "Gold",
    ]

    # Currency codes
    CURRENCY_CODES = ["USD", "EUR", "GBP", "JPY", "CNY", "AUD", "CAD", "CHF", "HKD", "SGD"]

    def __init__(
        self,
        locale: Optional[str] = None,
        seed: Optional[int] = None,
        unique_cache_size: int = 10000,
    ) -> None:
        """
        Initialize the DataFaker.

        Args:
            locale: Locale for generated data (default: en_US)
            seed: Random seed for reproducible generation
            unique_cache_size: Maximum size for uniqueness tracking cache
        """
        self._locale = LocaleConfig.get_locale(locale)
        self._random = random.Random(seed)
        self._unique_cache: Dict[str, set] = {}
        self._unique_cache_size = unique_cache_size
        self._counter: Dict[str, int] = {}

    @property
    def locale(self) -> str:
        """Current locale."""
        return self._locale

    def _unique(self, category: str, generator: Callable[[], T]) -> T:
        """
        Generate a unique value for the given category.

        Ensures uniqueness within the cache window.
        """
        if category not in self._unique_cache:
            self._unique_cache[category] = set()

        cache = self._unique_cache[category]
        max_attempts = 1000  # Prevent infinite loops

        for _ in range(max_attempts):
            value = generator()
            if value not in cache:
                if len(cache) >= self._unique_cache_size:
                    # Evict oldest entry
                    cache.pop()
                cache.add(value)
                return value

        # Fallback: return last generated (should be very rare)
        return value

    def _count(self, category: str) -> int:
        """Get and increment a counter for the given category."""
        count = self._counter.get(category, 0)
        self._counter[category] = count + 1
        return count

    def name(self) -> str:
        """
        Generate a random full name.

        Returns:
            Full name string (e.g., "James Smith")
        """
        first = self._random.choice(LocaleConfig.FIRST_NAMES.get(self._locale, LocaleConfig.FIRST_NAMES["en_US"]))
        last = self._random.choice(LocaleConfig.LAST_NAMES.get(self._locale, LocaleConfig.LAST_NAMES["en_US"]))
        return f"{first} {last}"

    def first_name(self) -> str:
        """Generate a random first name."""
        return self._random.choice(LocaleConfig.FIRST_NAMES.get(self._locale, LocaleConfig.FIRST_NAMES["en_US"]))

    def last_name(self) -> str:
        """Generate a random last name."""
        return self._random.choice(LocaleConfig.LAST_NAMES.get(self._locale, LocaleConfig.LAST_NAMES["en_US"]))

    def email(self, name: Optional[str] = None) -> str:
        """
        Generate a random email address.

        Args:
            name: Optional name to use for email local part
        """
        if name is None:
            name = self.name().lower().replace(" ", ".")

        # Sanitize for email
        name = re.sub(r"[^a-z0-9._-]", "", name)
        domain = self._random.choice(self.EMAIL_DOMAINS)
        return f"{name}@{domain}"

    def email_unique(self) -> str:
        """Generate a unique email address."""
        return self._unique("email", lambda: self.email())

    def phone(self, format: str = "US") -> str:
        """
        Generate a random phone number.

        Args:
            format: Phone format (US, UK, International)
        """
        if format == "US":
            area = self._random.randint(200, 999)
            exchange = self._random.randint(200, 999)
            subscriber = self._random.randint(1000, 9999)
            return f"({area}) {exchange}-{subscriber}"
        elif format == "UK":
            return f"+44 {self._random.randint(1000, 9999)} {self._random.randint(100, 999)}{self._random.randint(100, 999)}"
        else:  # International
            return f"+1-{self._random.randint(100, 999)}-{self._random.randint(100, 999)}-{self._random.randint(1000, 9999)}"

    def company(self) -> str:
        """
        Generate a random company name.

        Returns:
            Company name with locale-appropriate suffix
        """
        last_names = LocaleConfig.LAST_NAMES.get(self._locale, LocaleConfig.LAST_NAMES["en_US"])
        suffixes = LocaleConfig.COMPANY_SUFFIXES.get(self._locale, LocaleConfig.COMPANY_SUFFIXES["en_US"])

        founder = self._random.choice(last_names)
        suffix = self._random.choice(suffixes)

        return f"{founder} {suffix}"

    def address(self) -> str:
        """
        Generate a random street address.

        Returns:
            Street address string
        """
        street_num = self._random.randint(1, 9999)
        streets = LocaleConfig.STREET_SUFFIXES.get(self._locale, LocaleConfig.STREET_SUFFIXES["en_US"])
        street = self._random.choice(streets)

        return f"{street_num} {self._random.choice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm', 'Park', 'Lake'])} {street}"

    def city(self) -> str:
        """Generate a random city name."""
        return self._random.choice(LocaleConfig.CITIES.get(self._locale, LocaleConfig.CITIES["en_US"]))

    def country(self) -> str:
        """Generate a random country name."""
        countries = [
            "United States", "United Kingdom", "Canada", "Australia", "Germany",
            "France", "Japan", "China", "India", "Brazil", "Mexico", "Spain",
            "Italy", "Netherlands", "Sweden", "Norway", "Denmark", "Finland",
        ]
        return self._random.choice(countries)

    def zip_code(self) -> str:
        """Generate a random ZIP/postal code."""
        if self._locale == "zh_CN":
            return f"{self._random.randint(100000, 999999)}"
        elif self._locale.startswith("en"):
            return f"{self._random.randint(10000, 99999)}"
        else:
            return f"{self._random.randint(10000, 99999)}"

    def date_of_birth(self, min_age: int = 18, max_age: int = 80) -> date:
        """
        Generate a random date of birth.

        Args:
            min_age: Minimum age in years
            max_age: Maximum age in years
        """
        today = date.today()
        max_birth_date = date(today.year - min_age, today.month, today.day)
        min_birth_date = date(today.year - max_age, today.month, today.day)

        days_range = (max_birth_date - min_birth_date).days
        random_days = self._random.randint(0, days_range)

        return min_birth_date + timedelta(days=random_days)

    def date(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> date:
        """
        Generate a random date between start and end.

        Args:
            start: Start date (default: 1 year ago)
            end: End date (default: today)
        """
        if end is None:
            end = date.today()
        if start is None:
            start = end - timedelta(days=365)

        days_range = (end - start).days
        random_days = self._random.randint(0, days_range)

        return start + timedelta(days=random_days)

    def datetime_between(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> datetime:
        """Generate a random datetime between start and end."""
        if end is None:
            end = datetime.now()
        if start is None:
            start = end - timedelta(days=365)

        delta = end - start
        random_seconds = self._random.randint(0, int(delta.total_seconds()))

        return start + timedelta(seconds=random_seconds)

    def boolean(self, probability: float = 0.5) -> bool:
        """
        Generate a random boolean.

        Args:
            probability: Probability of True (0.0 to 1.0)
        """
        return self._random.random() < probability

    def integer(self, min_value: int = 0, max_value: int = 100) -> int:
        """Generate a random integer in range [min_value, max_value]."""
        return self._random.randint(min_value, max_value)

    def float(
        self,
        min_value: float = 0.0,
        max_value: float = 100.0,
        decimals: int = 2,
    ) -> float:
        """
        Generate a random float.

        Args:
            min_value: Minimum value
            max_value: Maximum value
            decimals: Number of decimal places
        """
        value = self._random.uniform(min_value, max_value)
        return round(value, decimals)

    def choice(self, items: List[T]) -> T:
        """Pick a random item from a list."""
        return self._random.choice(items)

    def choices(self, items: List[T], k: int) -> List[T]:
        """Pick k random items from a list (with replacement)."""
        return self._random.choices(items, k=k)

    def sample(self, items: List[T], k: int) -> List[T]:
        """Sample k unique items from a list (without replacement)."""
        return self._random.sample(items, k=min(k, len(items)))

    def url(self) -> str:
        """Generate a random URL."""
        protocols = ["http", "https"]
        domains = ["example", "demo", "test", "sample", "mysite", "app"]
        extensions = self.URL_TLDS

        protocol = self._random.choice(protocols)
        domain = self._random.choice(domains)
        ext = self._random.choice(extensions)
        path_segments = self._random.randint(0, 3)

        path = "/".join(
            "".join(self._random.choices(string.ascii_lowercase, k=self._random.randint(3, 12)))
            for _ in range(path_segments)
        )

        return f"{protocol}://{domain}{ext}/{path}" if path else f"{protocol}://{domain}{ext}"

    def ipv4(self) -> str:
        """Generate a random IPv4 address."""
        # Avoid reserved ranges
        first = self._random.choice([10, 172, 192])
        if first == 10:
            return f"10.{self._random.randint(0, 255)}.{self._random.randint(0, 255)}.{self._random.randint(1, 254)}"
        elif first == 172:
            return f"172.{self._random.randint(16, 31)}.{self._random.randint(0, 255)}.{self._random.randint(1, 254)}"
        else:
            return f"192.168.{self._random.randint(0, 255)}.{self._random.randint(1, 254)}"

    def uuid(self) -> str:
        """Generate a random UUID4 string."""
        import uuid
        return str(uuid.uuid4())

    def job_title(self) -> str:
        """Generate a random job title."""
        return self._random.choice(self.JOB_TITLES)

    def color_name(self) -> str:
        """Generate a random color name."""
        return self._random.choice(self.COLOR_NAMES)

    def currency_code(self) -> str:
        """Generate a random currency code."""
        return self._random.choice(self.CURRENCY_CODES)

    def credit_card_number(self, provider: str = "VISA") -> str:
        """
        Generate a fake credit card number.

        Note: This is NOT a valid card number - do NOT use for payments.
        Only for testing and development purposes.
        """
        if provider == "VISA":
            prefix = "4"
            length = 16
        elif provider == "MASTERCARD":
            prefix = self._random.choice(["51", "52", "53", "54", "55"])
            length = 16
        elif provider == "AMEX":
            prefix = self._random.choice(["34", "37"])
            length = 15
        else:
            prefix = "4"
            length = 16

        # Generate random digits
        remaining_length = length - len(prefix) - 1  # Leave room for check digit
        middle = "".join(str(self._random.randint(0, 9)) for _ in range(remaining_length))

        # Calculate Luhn check digit
        digits = [int(c) for c in prefix + middle]
        for i in range(len(digits) - 1, -1, -2):
            digits[i] *= 2
            if digits[i] > 9:
                digits[i] -= 9

        total = sum(digits)
        check_digit = (10 - (total % 10)) % 10

        return f"{prefix}{middle}{check_digit}"

    def sentence(self, word_count: Optional[int] = None) -> str:
        """
        Generate a random sentence.

        Args:
            word_count: Number of words (random if None)
        """
        if word_count is None:
            word_count = self._random.randint(6, 15)

        words = [
            "the", "be", "to", "of", "and", "a", "in", "that", "have", "it",
            "for", "not", "on", "with", "he", "as", "you", "do", "at", "this",
            "but", "his", "by", "from", "they", "we", "say", "her", "she", "or",
            "an", "will", "my", "one", "all", "would", "there", "their", "what",
            "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
        ]

        selected = self._random.choices(words, k=word_count)
        selected[0] = selected[0].capitalize()
        return " ".join(selected) + "."

    def paragraph(self, sentence_count: Optional[int] = None) -> str:
        """Generate a random paragraph."""
        if sentence_count is None:
            sentence_count = self._random.randint(3, 8)

        sentences = [self.sentence() for _ in range(sentence_count)]
        return " ".join(sentences)

    def word(self) -> str:
        """Generate a single random word."""
        words = [
            "apple", "banana", "cherry", "dog", "elephant", "flower", "garden",
            "happy", "island", "jungle", "kitchen", "lemon", "mountain", "nature",
            "ocean", "panda", "quiet", "river", "sunset", "tree", "umbrella",
            "valley", "window", "yellow", "zebra", "automation", "bot", "click",
            "driver", "engine", "framework", "generator", "helper", "instance",
        ]
        return self._random.choice(words)

    def password(
        self,
        length: int = 16,
        include_uppercase: bool = True,
        include_lowercase: bool = True,
        include_digits: bool = True,
        include_special: bool = True,
    ) -> str:
        """
        Generate a random password.

        Args:
            length: Total password length
            include_uppercase: Include uppercase letters
            include_lowercase: Include lowercase letters
            include_digits: Include digits
            include_special: Include special characters
        """
        chars = ""
        if include_lowercase:
            chars += string.ascii_lowercase
        if include_uppercase:
            chars += string.ascii_uppercase
        if include_digits:
            chars += string.digits
        if include_special:
            chars += "!@#$%^&*()_+-=[]{}|;:,.<>?"

        if not chars:
            chars = string.ascii_letters

        return "".join(self._random.choice(chars) for _ in range(length))

    def username(self, name: Optional[str] = None) -> str:
        """
        Generate a random username.

        Args:
            name: Optional name to base username on
        """
        if name is None:
            name = self.name().lower().replace(" ", "")

        suffixes = [
            "", "123", "1234", "_2024", "_dev", "99", "88",
            str(self._random.randint(1, 999)),
        ]

        base = re.sub(r"[^a-z0-9]", "", name)
        suffix = self._random.choice(suffixes)

        return f"{base}{suffix}"

    def mac_address(self) -> str:
        """Generate a random MAC address."""
        return ":".join(f"{self._random.randint(0, 255):02x}" for _ in range(6))

    def generate_batch(self, generator: Callable[[], T], count: int) -> List[T]:
        """
        Generate a batch of values using a generator function.

        Args:
            generator: Callable that generates a single value
            count: Number of values to generate
        """
        return [generator() for _ in range(count)]

    def generate_dict(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a dictionary from a schema definition.

        Args:
            schema: Dict mapping field names to generator functions or values
        """
        result = {}
        for key, value in schema.items():
            if callable(value):
                result[key] = value(self)
            elif isinstance(value, list) and len(value) == 2 and callable(value[0]):
                # (generator_func, count) tuple
                result[key] = [value[0](self) for _ in range(value[1])]
            else:
                result[key] = value
        return result


# Factory function
def create_faker(locale: Optional[str] = None, seed: Optional[int] = None) -> DataFaker:
    """
    Create a DataFaker instance.

    Args:
        locale: Locale for generated data
        seed: Random seed for reproducibility

    Returns:
        DataFaker instance
    """
    return DataFaker(locale=locale, seed=seed)
