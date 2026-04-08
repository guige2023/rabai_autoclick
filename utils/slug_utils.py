"""
Slug generation and URL-safe string utilities.

Provides functions for converting text to URL slugs,
with support for various languages and customization options.

Example:
    >>> from utils.slug_utils import slugify, SlugGenerator
    >>> slugify("Hello World!")
    'hello-world'
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional


class SlugGenerator:
    """
    Configurable slug generator with Unicode support.

    Converts text strings to URL-safe slugs with customizable
    transliteration, separator, and length limits.

    Attributes:
        separator: Character to use between words.
        lowercase: Convert to lowercase.
        max_length: Maximum slug length.
        trim: Trim repeated separators.
    """

    def __init__(
        self,
        separator: str = "-",
        lowercase: bool = True,
        max_length: Optional[int] = None,
        trim_repeated: bool = True,
        strip_html: bool = False,
    ) -> None:
        """
        Initialize the slug generator.

        Args:
            separator: Word separator character.
            lowercase: Convert to lowercase.
            max_length: Maximum slug length.
            trim_repeated: Remove repeated separators.
            strip_html: Remove HTML tags before processing.
        """
        self.separator = separator
        self.lowercase = lowercase
        self.max_length = max_length
        self.trim_repeated = trim_repeated
        self.strip_html = strip_html

        self._transliteration_map: dict[int, str] = {}

    def slugify(self, text: str) -> str:
        """
        Generate a slug from text.

        Args:
            text: Input text to convert.

        Returns:
            URL-safe slug.
        """
        if not text:
            return ""

        if self.strip_html:
            text = re.sub(r"<[^>]+>", "", text)

        text = unicodedata.normalize("NFKD", text)

        if self.lowercase:
            text = text.lower()

        text = self._transliterate(text)

        text = re.sub(r"[^\w\s" + re.escape(self.separator) + r"]", "", text)

        if self.trim_repeated:
            pattern = re.escape(self.separator) + r"+"
            text = re.sub(pattern, self.separator, text)

        text = text.strip(self.separator + " \t\n")

        if self.max_length is not None:
            text = self._truncate(text)

        return text

    def _transliterate(self, text: str) -> str:
        """Transliterate Unicode characters to ASCII equivalents."""
        result = []
        for char in text:
            code_point = ord(char)
            if code_point in self._transliteration_map:
                result.append(self._transliteration_map[code_point])
            elif unicodedata.category(char) == "Mn":
                continue
            else:
                result.append(char)
        return "".join(result)

    def add_transliteration(self, from_char: str, to_char: str) -> None:
        """
        Add a custom transliteration mapping.

        Args:
            from_char: Source character.
            to_char: Target character.
        """
        self._transliteration_map[ord(from_char)] = to_char

    def _truncate(self, text: str) -> str:
        """Truncate slug to max length."""
        if len(text) <= self.max_length:
            return text

        truncated = text[: self.max_length]

        if self.separator in truncated:
            truncated = truncated.rsplit(self.separator, 1)[0]

        return truncated

    def __call__(self, text: str) -> str:
        """Allow calling the generator as a function."""
        return self.slugify(text)


def slugify(
    text: str,
    separator: str = "-",
    lowercase: bool = True,
    max_length: Optional[int] = None,
) -> str:
    """
    Convenience function to generate a slug.

    Args:
        text: Input text to convert.
        separator: Word separator character.
        lowercase: Convert to lowercase.
        max_length: Maximum slug length.

    Returns:
        URL-safe slug.
    """
    generator = SlugGenerator(
        separator=separator,
        lowercase=lowercase,
        max_length=max_length,
    )
    return generator.slugify(text)


def slugify_file_path(path: str) -> str:
    """
    Convert a file path to a safe filename slug.

    Args:
        path: File path to convert.

    Returns:
        Safe filename slug.
    """
    path_obj = Path(path)
    name = path_obj.stem
    ext = path_obj.suffix

    slug_name = slugify(name)
    if ext:
        ext = re.sub(r"[^\w]", "", ext)
        return f"{slug_name}.{ext}"
    return slug_name


def slugify_directory_path(path: str) -> str:
    """
    Convert a directory path to a safe directory name.

    Args:
        path: Directory path to convert.

    Returns:
        Safe directory name slug.
    """
    path_obj = Path(path)
    return slugify(path_obj.name)


def validate_slug(slug: str, separator: str = "-") -> bool:
    """
    Check if a string is a valid slug.

    Args:
        slug: String to validate.
        separator: Expected separator character.

    Returns:
        True if valid slug, False otherwise.
    """
    if not slug:
        return False

    pattern = r"^[a-z0-9]+" + re.escape(separator) + r"[a-z0-9]*$"
    if re.match(pattern, slug, re.IGNORECASE):
        return True

    pattern = r"^[a-z0-9]+$"
    return bool(re.match(pattern, slug, re.IGNORECASE))


def deslugify(slug: str, separator: str = "-") -> str:
    """
    Convert a slug back to a human-readable title.

    Args:
        slug: Slug to convert.
        separator: Separator used in the slug.

    Returns:
        Human-readable title.
    """
    if not slug:
        return ""

    words = slug.replace(separator, " ")
    words = words.replace("_", " ")

    words = re.sub(r"\s+", " ", words).strip()

    return words.title()


def increment_slug(slug: str, separator: str = "-") -> str:
    """
    Generate an incremented version of a slug.

    Args:
        slug: Base slug.
        separator: Separator used in the slug.

    Returns:
        Incremented slug (e.g., "my-post" -> "my-post-2").
    """
    pattern = re.escape(separator) + r"(\d+)$"
    match = re.search(pattern, slug)

    if match:
        num = int(match.group(1)) + 1
        return re.sub(pattern, f"{separator}{num}", slug)

    return f"{slug}{separator}2"


def slug_to_snake(slug: str, separator: str = "-") -> str:
    """
    Convert a slug to snake_case.

    Args:
        slug: Slug to convert.
        separator: Separator used in the slug.

    Returns:
        Snake_case string.
    """
    return slug.replace(separator, "_")


def snake_to_slug(snake: str) -> str:
    """
    Convert a snake_case string to a slug.

    Args:
        snake: Snake_case string to convert.

    Returns:
        Slug string.
    """
    return snake.replace("_", "-")


class SlugCache:
    """
    Cache for generating unique slugs.

    Ensures uniqueness when multiple inputs may
    produce the same slug.
    """

    def __init__(self, separator: str = "-") -> None:
        """Initialize the cache."""
        self.separator = separator
        self._seen: dict[str, int] = {}

    def get_unique(self, text: str) -> str:
        """
        Get a unique slug for text.

        Args:
            text: Input text to convert.

        Returns:
            Unique slug.
        """
        slug = slugify(text, separator=self.separator)

        if slug not in self._seen:
            self._seen[slug] = 1
            return slug

        self._seen[slug] += 1
        return increment_slug(slug, self.separator)

    def reset(self) -> None:
        """Reset the cache."""
        self._seen.clear()
