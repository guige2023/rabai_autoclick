"""UI Localization utilities for internationalization support.

This module provides utilities for working with localized UI content,
including text translation, locale detection, and format conversion.
"""

from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
import re
import json
from datetime import datetime, date
from decimal import Decimal


class Locale(Enum):
    """Supported locales."""
    EN_US = "en-US"
    ZH_CN = "zh-CN"
    ZH_TW = "zh-TW"
    JA_JP = "ja-JP"
    KO_KR = "ko-KR"
    DE_DE = "de-DE"
    FR_FR = "fr-FR"
    ES_ES = "es-ES"
    IT_IT = "it-IT"
    PT_BR = "pt-BR"
    RU_RU = "ru-RU"
    AR_SA = "ar-SA"


@dataclass
class LocaleConfig:
    """Configuration for a locale."""
    locale: Locale
    language_code: str
    country_code: str
    text_direction: str = "ltr"  # ltr or rtl
    date_format: str = "%Y-%m-%d"
    time_format: str = "%H:%M"
    datetime_format: str = "%Y-%m-%d %H:%M"
    decimal_separator: str = "."
    thousands_separator: str = ","
    currency_symbol: str = "$"


@dataclass
class TranslationEntry:
    """A single translation entry."""
    key: str
    source_text: str
    translations: Dict[str, str] = field(default_factory=dict)
    description: str = ""
    context: str = ""
    plural_form: Optional[str] = None

    def translate(self, locale: str, count: Optional[int] = None) -> str:
        """Get translation for locale."""
        if locale in self.translations:
            text = self.translations[locale]
            if self.plural_form and count is not None:
                return self._apply_plural(text, count)
            return text
        return self.source_text

    def _apply_plural(self, text: str, count: int) -> str:
        """Apply plural rules to text."""
        if "{count}" in text:
            return text.replace("{count}", str(count))
        if self.plural_form:
            rules = self.plural_form.split("|")
            index = self._get_plural_index(count, len(rules))
            if index < len(rules):
                return rules[index].replace("{n}", str(count))
        return text

    def _get_plural_index(self, n: int, forms: int) -> int:
        """Get plural form index based on CLDR rules."""
        if forms == 2:
            return 1 if n != 1 else 0
        if forms == 3:
            if n == 0:
                return 2
            if n == 1:
                return 0
            return 1
        return 0 if n == 1 else 1


class LocalizationManager:
    """Manages localization and translations."""

    def __init__(self, default_locale: Locale = Locale.EN_US):
        self.default_locale = default_locale
        self.current_locale = default_locale
        self.translations: Dict[str, TranslationEntry] = {}
        self.fallback_locale = Locale.EN_US
        self._interpolators: Dict[str, Callable[[str, Any], str]] = {}

    def add_translation(self, entry: TranslationEntry) -> None:
        """Add a translation entry."""
        self.translations[entry.key] = entry

    def add_translations_from_dict(self, data: Dict[str, Dict[str, str]],
                                  base_locale: str = "en") -> None:
        """Add translations from nested dictionary."""
        for key, translations in data.items():
            entry = TranslationEntry(
                key=key,
                source_text=translations.get(base_locale, key),
            )
            for locale, text in translations.items():
                entry.translations[locale] = text
            self.translations[key] = entry

    def add_translations_from_json(self, json_str: str) -> None:
        """Add translations from JSON string."""
        data = json.loads(json_str)
        if isinstance(data, dict):
            for key, translations in data.items():
                if isinstance(translations, dict):
                    entry = TranslationEntry(
                        key=key,
                        source_text=translations.get("en", key),
                    )
                    for locale, text in translations.items():
                        entry.translations[locale] = text
                    self.translations[key] = entry

    def set_locale(self, locale: Locale) -> None:
        """Set current locale."""
        self.current_locale = locale

    def get_text(self, key: str, locale: Optional[str] = None,
                 count: Optional[int] = None, **kwargs) -> str:
        """Get translated text for key."""
        target_locale = locale or self.current_locale.value

        if key not in self.translations:
            return key

        entry = self.translations[key]
        text = entry.translate(target_locale, count)

        if kwargs:
            text = self._interpolate(text, kwargs)

        return text

    def _interpolate(self, text: str, values: Dict[str, Any]) -> str:
        """Interpolate values into text."""
        for key, value in values.items():
            placeholder = f"{{{key}}}"
            if placeholder in text:
                text = text.replace(placeholder, str(value))
        return text

    def register_interpolator(self, name: str,
                             func: Callable[[str, Any], str]) -> None:
        """Register a custom interpolator."""
        self._interpolators[name] = func

    def get_plural(self, key: str, count: int,
                   locale: Optional[str] = None) -> str:
        """Get pluralized text."""
        return self.get_text(key, locale, count, count=count)

    def has_translation(self, key: str, locale: Optional[str] = None) -> bool:
        """Check if translation exists for key."""
        target_locale = locale or self.current_locale.value
        if key not in self.translations:
            return False
        return target_locale in self.translations[key].translations

    def get_missing_translations(self, locale: Locale) -> List[str]:
        """Get list of keys missing translation for locale."""
        missing = []
        for key, entry in self.translations.items():
            if locale.value not in entry.translations:
                missing.append(key)
        return missing

    def export_for_locale(self, locale: Locale) -> Dict[str, str]:
        """Export all translations for a locale."""
        result = {}
        for key, entry in self.translations.items():
            if locale.value in entry.translations:
                result[key] = entry.translations[locale.value]
            else:
                result[key] = entry.source_text
        return result


class LocaleFormatter:
    """Formats values according to locale conventions."""

    def __init__(self, locale: Locale):
        self.locale = locale
        self.config = self._get_locale_config(locale)

    def _get_locale_config(self, locale: Locale) -> LocaleConfig:
        """Get configuration for locale."""
        configs = {
            Locale.EN_US: LocaleConfig(
                locale=Locale.EN_US,
                language_code="en",
                country_code="US",
                currency_symbol="$",
            ),
            Locale.ZH_CN: LocaleConfig(
                locale=Locale.ZH_CN,
                language_code="zh",
                country_code="CN",
                text_direction="ltr",
                currency_symbol="¥",
            ),
            Locale.ZH_TW: LocaleConfig(
                locale=Locale.ZH_TW,
                language_code="zh",
                country_code="TW",
                text_direction="ltr",
                currency_symbol="NT$",
            ),
            Locale.JA_JP: LocaleConfig(
                locale=Locale.JA_JP,
                language_code="ja",
                country_code="JP",
                text_direction="ltr",
                currency_symbol="¥",
            ),
            Locale.DE_DE: LocaleConfig(
                locale=Locale.DE_DE,
                language_code="de",
                country_code="DE",
                decimal_separator=",",
                thousands_separator=".",
                currency_symbol="€",
            ),
            Locale.FR_FR: LocaleConfig(
                locale=Locale.FR_FR,
                language_code="fr",
                country_code="FR",
                decimal_separator=",",
                thousands_separator=" ",
                currency_symbol="€",
            ),
        }
        return configs.get(locale, LocaleConfig(
            locale=locale,
            language_code=locale.value.split("-")[0],
            country_code=locale.value.split("-")[1] if "-" in locale.value else "",
        ))

    def format_number(self, value: float, decimals: int = 2) -> str:
        """Format number according to locale."""
        parts = str(value).split(".")
        integer_part = parts[0]
        decimal_part = parts[1] if len(parts) > 1 else ""

        integer_part = integer_part.replace("-", "")
        formatted = ""
        for i, digit in enumerate(reversed(integer_part)):
            if i > 0 and i % 3 == 0:
                formatted = self.config.thousands_separator + formatted
            formatted = digit + formatted

        if decimal_part or decimals > 0:
            decimal_part = decimal_part.ljust(decimals, "0")[:decimals]
            return f"{'-' if value < 0 else ''}{formatted}{self.config.decimal_separator}{decimal_part}"
        return f"{'-' if value < 0 else ''}{formatted}"

    def format_currency(self, value: float, show_symbol: bool = True) -> str:
        """Format currency according to locale."""
        formatted = self.format_number(abs(value))
        if show_symbol:
            return f"{self.config.currency_symbol}{formatted}"
        return formatted

    def format_date(self, dt: datetime, format_type: str = "medium") -> str:
        """Format date according to locale."""
        format_map = {
            "short": "%m/%d/%y" if self.config.language_code == "en" else "%y/%m/%d",
            "medium": "%b %d, %Y" if self.config.language_code == "en" else "%Y/%m/%d",
            "long": "%B %d, %Y" if self.config.language_code == "en" else "%Y年%m月%d日",
            "full": "%A, %B %d, %Y" if self.config.language_code == "en" else "%Y年%m月%d日%A",
        }
        fmt = format_map.get(format_type, self.config.date_format)
        return dt.strftime(fmt)

    def format_time(self, dt: datetime, use_24h: bool = False) -> str:
        """Format time according to locale."""
        if use_24h or self.config.language_code != "en":
            return dt.strftime("%H:%M")
        return dt.strftime("%I:%M %p")

    def format_datetime(self, dt: datetime) -> str:
        """Format datetime according to locale."""
        return f"{self.format_date(dt)} {self.format_time(dt)}"

    def format_percent(self, value: float, decimals: int = 1) -> str:
        """Format percentage according to locale."""
        formatted = self.format_number(value * 100, decimals)
        return f"{formatted}%"


def detect_locale_from_text(text: str) -> Optional[Locale]:
    """Attempt to detect locale from text sample."""
    cjk_ranges = [
        (0x4E00, 0x9FFF),  # CJK Unified Ideographs
        (0x3400, 0x4DBF),  # CJK Extension A
    ]

    japanese_ranges = [
        (0x3040, 0x309F),  # Hiragana
        (0x30A0, 0x30FF),  # Katakana
    ]

    korean_ranges = [
        (0xAC00, 0xD7AF),  # Hangul Syllables
        (0x1100, 0x11FF),  # Hangul Jamo
    ]

    arabic_ranges = [
        (0x0600, 0x06FF),  # Arabic
        (0x0750, 0x077F),  # Arabic Supplement
    ]

    def count_chars_in_ranges(text: str, ranges: List[Tuple[int, int]]) -> int:
        count = 0
        for char in text:
            code = ord(char)
            for start, end in ranges:
                if start <= code <= end:
                    count += 1
                    break
        return count

    cjk_count = count_chars_in_ranges(text, cjk_ranges)
    japanese_count = count_chars_in_ranges(text, japanese_ranges)
    korean_count = count_chars_in_ranges(text, korean_ranges)
    arabic_count = count_chars_in_ranges(text, arabic_ranges)

    total = len(text.strip())
    if total == 0:
        return None

    if arabic_count / total > 0.3:
        return Locale.AR_SA
    if korean_count / total > 0.3:
        return Locale.KO_KR
    if japanese_count / total > 0.2:
        return Locale.JA_JP
    if cjk_count / total > 0.3:
        return Locale.ZH_CN

    return None


def normalize_text_for_comparison(text: str) -> str:
    """Normalize text for locale-agnostic comparison."""
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    return text
