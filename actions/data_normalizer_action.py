"""
Data Normalizer Action Module.

Normalize data formats, units, and representations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class NormalizationRule:
    """A normalization rule."""
    field: str
    transform: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None


class DataNormalizerAction:
    """
    Data normalization utilities.

    Handles units, formats, casing, and custom transformations.
    """

    UNIT_CONVERSIONS = {
        "km": {"m": 1000, "cm": 100000, "mm": 1000000, "mi": 0.621371, "ft": 3280.84, "yd": 1093.61},
        "m": {"km": 0.001, "cm": 100, "mm": 1000, "mi": 0.000621371, "ft": 3.28084, "yd": 1.09361},
        "kg": {"g": 1000, "mg": 1000000, "lb": 2.20462, "oz": 35.274, "t": 0.001},
        "g": {"kg": 0.001, "mg": 1000, "lb": 0.00220462, "oz": 0.035274},
        "c": {"f": lambda c: c * 9/5 + 32, "k": lambda c: c + 273.15},
        "f": {"c": lambda f: (f - 32) * 5/9, "k": lambda f: (f - 32) * 5/9 + 273.15},
    }

    CASE_TRANSFORMS = {
        "snake": lambda s: re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower(),
        "camel": lambda s: s[0].lower() + re.sub(r"_([a-z])", lambda m: m.group(1).upper(), s[1:]) if "_" in s else s,
        "pascal": lambda s: "".join(word.capitalize() for word in s.split("_")),
        "kebab": lambda s: re.sub(r"_", "-", s.lower()),
    }

    def __init__(self) -> None:
        self._rules: List[NormalizationRule] = []

    def add_rule(
        self,
        field: str,
        transform: Callable[[Any], Any],
        condition: Optional[Callable[[Any], bool]] = None,
    ) -> "DataNormalizerAction":
        """
        Add a normalization rule.

        Args:
            field: Field name to normalize
            transform: Transformation function
            condition: Optional condition for when to apply

        Returns:
            Self for chaining
        """
        self._rules.append(NormalizationRule(
            field=field,
            transform=transform,
            condition=condition,
        ))
        return self

    def normalize(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Normalize data according to rules.

        Args:
            data: Data to normalize

        Returns:
            Normalized data
        """
        result = data.copy()

        for rule in self._rules:
            if rule.field not in result:
                continue

            value = result[rule.field]

            if rule.condition is not None and not rule.condition(value):
                continue

            try:
                result[rule.field] = rule.transform(value)
            except Exception:
                pass

        return result

    def convert_unit(
        self,
        value: float,
        from_unit: str,
        to_unit: str,
    ) -> Optional[float]:
        """
        Convert between units.

        Args:
            value: Numeric value
            from_unit: Source unit
            to_unit: Target unit

        Returns:
            Converted value or None if not possible
        """
        from_unit = from_unit.lower()
        to_unit = to_unit.lower()

        if from_unit not in self.UNIT_CONVERSIONS:
            return None

        conversions = self.UNIT_CONVERSIONS[from_unit]

        if to_unit not in conversions:
            return None

        conversion = conversions[to_unit]

        if callable(conversion):
            return conversion(value)

        return value * conversion

    def transform_case(
        self,
        value: str,
        case_type: str,
    ) -> str:
        """
        Transform string case.

        Args:
            value: String to transform
            case_type: Target case (snake, camel, pascal, kebab)

        Returns:
            Transformed string
        """
        transform = self.CASE_TRANSFORMS.get(case_type)
        if transform:
            return transform(value)
        return value

    def normalize_string(
        self,
        value: str,
        strip: bool = True,
        lowercase: bool = False,
        remove_accents: bool = False,
    ) -> str:
        """
        Normalize a string.

        Args:
            value: String to normalize
            strip: Strip whitespace
            lowercase: Convert to lowercase
            remove_accents: Remove accent marks

        Returns:
            Normalized string
        """
        result = value

        if strip:
            result = result.strip()

        if remove_accents:
            result = self._remove_accents(result)

        if lowercase:
            result = result.lower()

        return result

    def _remove_accents(self, text: str) -> str:
        """Remove accent marks from text."""
        import unicodedata
        nfd = unicodedata.normalize("NFD", text)
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn")

    def normalize_email(self, email: str) -> str:
        """Normalize email address."""
        email = email.lower().strip()
        if "@" in email:
            local, domain = email.rsplit("@", 1)
            local = local.replace(".", "").split("+")[0]
            return f"{local}@{domain}"
        return email

    def normalize_phone(
        self,
        phone: str,
        country_code: str = "US",
    ) -> Optional[str]:
        """
        Normalize phone number.

        Args:
            phone: Phone number string
            country_code: Country code for formatting

        Returns:
            Normalized phone or None
        """
        digits = re.sub(r"\D", "", phone)

        if country_code == "US" and len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) >= 10:
            return f"+{digits}"

        return None

    def normalize_json(
        self,
        data: Union[Dict, List],
        case: Optional[str] = None,
    ) -> Union[Dict, List]:
        """
        Recursively normalize JSON data.

        Args:
            data: JSON data
            case: Optional case transformation for keys

        Returns:
            Normalized data
        """
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if case:
                    key = self.transform_case(key, case)
                result[key] = self.normalize_json(value, case)
            return result
        elif isinstance(data, list):
            return [self.normalize_json(item, case) for item in data]
        return data
