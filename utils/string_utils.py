"""String manipulation and text processing utilities.

Provides text transformation, sanitization, and
common string operations for automation.
"""

import hashlib
import re
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Pattern, Union


def slugify(
    text: str,
    max_length: int = 0,
    lowercase: bool = True,
    separator: str = "-",
) -> str:
    """Convert text to URL-friendly slug.

    Example:
        slugify("Hello World!")  # "hello-world"
    """
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", separator, text).strip(separator)

    if lowercase:
        text = text.lower()

    if max_length > 0 and len(text) > max_length:
        text = text[:max_length].rsplit(separator, 1)[0]

    return text


def camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case.

    Example:
        camel_to_snake("camelCase")  # "camel_case"
    """
    text = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub("([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower()


def snake_to_camel(text: str, capitalize_first: bool = False) -> str:
    """Convert snake_case to camelCase.

    Example:
        snake_to_camel("snake_case")  # "snakeCase"
        snake_to_camel("snake_case", capitalize_first=True)  # "SnakeCase"
    """
    components = text.split("_")
    result = components[0]
    if not capitalize_first:
        result = components[0]
    result += "".join(x.title() for x in components[1:])
    return result


def pascal_case(text: str) -> str:
    """Convert text to PascalCase.

    Example:
        pascal_case("hello world")  # "HelloWorld"
    """
    words = re.findall(r"[A-Z][a-z]*|[a-z]+", text.title())
    return "".join(words)


def kebab_case(text: str) -> str:
    """Convert text to kebab-case.

    Example:
        kebab_case("Hello World")  # "hello-world"
    """
    return slugify(text, separator="-")


def title_case(text: str) -> str:
    """Convert text to Title Case.

    Example:
        title_case("hello world")  # "Hello World"
    """
    return text.title()


def truncate(
    text: str,
    max_length: int,
    suffix: str = "...",
) -> str:
    """Truncate text with suffix.

    Example:
        truncate("Hello World", 8)  # "Hello..."
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def strip_html(text: str) -> str:
    """Remove HTML tags from text.

    Example:
        strip_html("<p>Hello <b>World</b></p>")  # "Hello World"
    """
    return re.sub(r"<[^>]+>", "", text)


def remove_whitespace(text: str) -> str:
    """Remove all whitespace from text.

    Example:
        remove_whitespace("hello   world")  # "helloworld"
    """
    return re.sub(r"\s+", "", text)


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace to single spaces.

    Example:
        normalize_whitespace("hello    world")  # "hello world"
    """
    return re.sub(r"\s+", " ", text).strip()


def remove_accents(text: str) -> str:
    """Remove accents from text.

    Example:
        remove_accents("café")  # "cafe"
    """
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def extract_numbers(text: str) -> List[float]:
    """Extract all numbers from text.

    Example:
        extract_numbers("abc 123 def 45.6")  # [123.0, 45.6]
    """
    return [float(x) for x in re.findall(r"-?\d+\.?\d*", text)]


def extract_words(text: str, min_length: int = 1) -> List[str]:
    """Extract words from text.

    Example:
        extract_words("Hello, World!")  # ["Hello", "World"]
    """
    words = re.findall(r"\b[a-zA-Z]+\b", text)
    if min_length > 1:
        words = [w for w in words if len(w) >= min_length]
    return words


def mask_sensitive(
    text: str,
    pattern: str = r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
    replacement: str = "****-****-****-****",
) -> str:
    """Mask sensitive patterns like credit card numbers.

    Example:
        mask_sensitive("Card: 1234-5678-9012-3456")  # "Card: ****-****-****-****"
    """
    return re.sub(pattern, replacement, text)


def hash_string(text: str, algorithm: str = "md5") -> str:
    """Hash string using specified algorithm.

    Example:
        hash_string("password")  # "5f4dcc3b5aa765d61d8327deb882cf99"
    """
    h = hashlib.new(algorithm)
    h.update(text.encode())
    return h.hexdigest()


def repeat_string(text: str, count: int, separator: str = "") -> str:
    """Repeat string with separator.

    Example:
        repeat_string("hi", 3)  # "hihihi"
        repeat_string("hi", 3, "-")  # "hi-hi-hi"
    """
    return separator.join([text] * count)


def reverse_string(text: str) -> str:
    """Reverse a string.

    Example:
        reverse_string("hello")  # "olleh"
    """
    return text[::-1]


def is_palindrome(text: str, ignore_case: bool = True, ignore_spaces: bool = True) -> bool:
    """Check if text is a palindrome.

    Example:
        is_palindrome("A man a plan a canal Panama")  # True
    """
    if ignore_spaces:
        text = text.replace(" ", "")
    if ignore_case:
        text = text.lower()
    return text == text[::-1]


def word_count(text: str) -> int:
    """Count words in text."""
    return len(re.findall(r"\b\w+\b", text))


def char_count(text: str, include_spaces: bool = True) -> int:
    """Count characters in text."""
    if include_spaces:
        return len(text)
    return len(text.replace(" ", ""))


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Example:
        levenshtein_distance("kitten", "sitting")  # 3
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def highlight_matches(
    text: str,
    pattern: Union[str, Pattern[str]],
    prefix: str = "**",
    suffix: str = "**",
) -> str:
    """Highlight pattern matches in text.

    Example:
        highlight_matches("error occurred", "error")  # "**error** occurred"
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern, re.IGNORECASE)
    return pattern.sub(f"{prefix}\\{pattern.pattern}\\{suffix}", text)


def capitalize_words(text: str, exceptions: Optional[List[str]] = None) -> str:
    """Capitalize first letter of each word with exceptions.

    Example:
        capitalize_words("the quick brown fox")  # "The Quick Brown Fox"
    """
    exceptions = exceptions or ["a", "an", "the", "and", "but", "or", "in", "on", "at"]
    words = text.split()
    result = []
    for i, word in enumerate(words):
        if i == 0 or word.lower() not in exceptions:
            result.append(word.capitalize())
        else:
            result.append(word.lower())
    return " ".join(result)


def replace_multiple(
    text: str,
    replacements: Dict[str, str],
) -> str:
    """Replace multiple substrings in text.

    Example:
        replace_multiple("hello world", {"hello": "hi", "world": "there"})
        # "hi there"
    """
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text
