"""
HID Report Utilities for Human Interface Devices.

This module provides utilities for parsing and generating
Human Interface Device reports in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class HIDReportType(Enum):
    """HID report types."""
    INPUT = "input"
    OUTPUT = "output"
    FEATURE = "feature"


class HIDItemType(Enum):
    """HID item types."""
    MAIN = "main"
    GLOBAL = "global"
    LOCAL = "local"
    RESERVED = "reserved"


@dataclass
class HIDReportDescriptor:
    """HID report descriptor."""
    report_id: int
    report_type: HIDReportType
    size: int
    items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class HIDReport:
    """Parsed HID report."""
    report_id: int
    report_type: HIDReportType
    data: bytes
    timestamp: float
    parsed_values: Dict[str, int] = field(default_factory=dict)


class HIDReportParser:
    """
    Parse HID reports from raw bytes.
    """

    def __init__(self):
        """Initialize HID report parser."""
        self._report_map: Dict[int, HIDReportDescriptor] = {}

    def register_descriptor(self, descriptor: HIDReportDescriptor) -> None:
        """Register a report descriptor."""
        self._report_map[descriptor.report_id] = descriptor

    def parse(self, data: bytes) -> Optional[HIDReport]:
        """
        Parse raw HID report data.

        Args:
            data: Raw report bytes

        Returns:
            Parsed HIDReport or None
        """
        if not data:
            return None

        report_id = data[0] if data[0] != 0 else 0
        descriptor = self._report_map.get(report_id)

        if descriptor:
            return self._parse_with_descriptor(data, descriptor)
        else:
            return self._parse_generic(data)

    def _parse_with_descriptor(
        self,
        data: bytes,
        descriptor: HIDReportDescriptor
    ) -> HIDReport:
        """Parse using known descriptor."""
        values = {}

        for item in descriptor.items:
            field_name = item.get("name", f"field_{len(values)}")
            bit_offset = item.get("bit_offset", 0)
            bit_size = item.get("bit_size", 8)

            value = self._extract_bits(data, bit_offset, bit_size)
            values[field_name] = value

        return HIDReport(
            report_id=descriptor.report_id,
            report_type=descriptor.report_type,
            data=data,
            timestamp=0.0,
            parsed_values=values
        )

    def _parse_generic(self, data: bytes) -> HIDReport:
        """Generic parsing without descriptor."""
        return HIDReport(
            report_id=data[0] if len(data) > 0 else 0,
            report_type=HIDReportType.INPUT,
            data=data,
            timestamp=0.0,
            parsed_values={"raw_length": len(data)}
        )

    def _extract_bits(
        self,
        data: bytes,
        bit_offset: int,
        bit_size: int
    ) -> int:
        """Extract bits from report data."""
        value = 0
        for i in range(bit_size):
            byte_idx = (bit_offset + i) // 8
            bit_idx = (bit_offset + i) % 8
            if byte_idx < len(data):
                if (data[byte_idx] >> bit_idx) & 1:
                    value |= (1 << i)
        return value


class HIDReportBuilder:
    """
    Build HID reports from structured data.
    """

    def __init__(self, report_id: int = 0):
        """
        Initialize HID report builder.

        Args:
            report_id: Report ID for this builder
        """
        self.report_id = report_id
        self._fields: List[Dict[str, Any]] = []

    def add_field(
        self,
        name: str,
        bit_size: int,
        default_value: int = 0
    ) -> 'HIDReportBuilder':
        """
        Add a field to the report.

        Args:
            name: Field name
            bit_size: Field size in bits
            default_value: Default value

        Returns:
            Self for chaining
        """
        self._fields.append({
            "name": name,
            "bit_size": bit_size,
            "default_value": default_value,
            "bit_offset": self._calculate_offset()
        })
        return self

    def _calculate_offset(self) -> int:
        """Calculate bit offset for next field."""
        return sum(f["bit_size"] for f in self._fields[:-1])

    def build(self, values: Optional[Dict[str, int]] = None) -> bytes:
        """
        Build HID report bytes.

        Args:
            values: Field values to set

        Returns:
            Report bytes
        """
        values = values or {}
        total_bits = sum(f["bit_size"] for f in self._fields)
        total_bytes = (total_bits + 7) // 8 + (1 if self.report_id > 0 else 0)

        report = bytearray(total_bytes)

        byte_idx = 1 if self.report_id > 0 else 0
        for field in self._fields:
            field_name = field["name"]
            bit_size = field["bit_size"]
            bit_offset = field["bit_offset"]
            value = values.get(field_name, field["default_value"])

            self._set_bits(report, byte_idx + bit_offset // 8, bit_offset % 8, bit_size, value)

        return bytes(report)

    def _set_bits(
        self,
        data: bytearray,
        byte_idx: int,
        bit_offset: int,
        bit_size: int,
        value: int
    ) -> None:
        """Set bits in report data."""
        for i in range(bit_size):
            if byte_idx + (bit_offset + i) // 8 >= len(data):
                break
            idx = byte_idx + (bit_offset + i) // 8
            b = (bit_offset + i) % 8
            if (value >> i) & 1:
                data[idx] |= (1 << b)
            else:
                data[idx] &= ~(1 << b)


def parse_keyboard_report(data: bytes) -> Dict[str, Any]:
    """
    Parse standard keyboard HID report.

    Args:
        data: Raw HID report bytes

    Returns:
        Parsed keyboard report
    """
    if len(data) < 3:
        return {"error": "invalid_length"}

    modifiers = data[0]
    reserved = data[1]
    key_codes = data[2:8]

    return {
        "modifiers": modifiers,
        "reserved": reserved,
        "key_codes": list(key_codes),
        "key_count": sum(1 for k in key_codes if k != 0),
        "left_control": bool(modifiers & 0x01),
        "left_shift": bool(modifiers & 0x02),
        "left_alt": bool(modifiers & 0x04),
        "left_gui": bool(modifiers & 0x08),
        "right_control": bool(modifiers & 0x10),
        "right_shift": bool(modifiers & 0x20),
        "right_alt": bool(modifiers & 0x40),
        "right_gui": bool(modifiers & 0x80),
    }


def parse_mouse_report(data: bytes) -> Dict[str, Any]:
    """
    Parse standard mouse HID report.

    Args:
        data: Raw HID report bytes

    Returns:
        Parsed mouse report
    """
    if len(data) < 3:
        return {"error": "invalid_length"}

    buttons = data[0]
    x_delta = data[1] if data[1] < 128 else data[1] - 256
    y_delta = data[2] if data[2] < 128 else data[2] - 256

    return {
        "buttons": buttons,
        "button_1": bool(buttons & 0x01),
        "button_2": bool(buttons & 0x02),
        "button_3": bool(buttons & 0x04),
        "x_delta": x_delta,
        "y_delta": y_delta,
    }
