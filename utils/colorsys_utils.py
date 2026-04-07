"""
Color conversion utilities between RGB, HSV, HSL, HEX, and more.
"""

import colorsys
from typing import Tuple, Union, Optional, List, Dict
from dataclasses import dataclass


RGB = Tuple[int, int, int]
HSV = Tuple[float, float, float]
HSL = Tuple[float, float, float]
ColorInput = Union[RGB, str]


@dataclass(frozen=True)
class Color:
    """Immutable color container with multiple format accessors."""
    r: int
    g: int
    b: int
    a: int = 255

    def __post_init__(self) -> None:
        for val in (self.r, self.g, self.b, self.a):
            if not 0 <= val <= 255:
                raise ValueError(f\"Color values must be in [0, 255], got {val}\")

    @property
    def rgb(self) -> RGB:
        return (self.r, self.g, self.b)

    @property
    def hex(self) -> str:
        return f\"#{self.r:02x}{self.g:02x}{self.b:02x}\"

    @property
    def hsv(self) -> HSV:
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        return (round(h, 6), round(s, 6), round(v, 6))

    @property
    def hsl(self) -> HSL:
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        return (round(h, 6), round(s, 6), round(l, 6))

    def brightness(self) -> float:
        \"\"\"Perceived brightness [0, 255].\"\"\"\n        return 0.299 * self.r + 0.587 * self.g + 0.114 * self.b

    def is_dark(self, threshold: float = 127.5) -> bool:
        return self.brightness() < threshold

    def is_light(self, threshold: float = 127.5) -> bool:
        return self.brightness() >= threshold

    def contrast_color(self) -> \"Color\":
        \"\"\"Return black or white depending on which contrasts better.\"\"\"\n        return Color(0, 0, 0) if self.is_dark() else Color(255, 255, 255)

    def blend_with(self, other: \"Color\", factor: float = 0.5) -> \"Color\":
        \"\"\"Blend this color with another.\"\"\"\n        f = max(0, min(1, factor))\n        return Color(\n            r=int(self.r + (other.r - self.r) * f),\n            g=int(self.g + (other.g - self.g) * f),\n            b=int(self.b + (other.b - self.b) * f),\n            a=int(self.a + (other.a - self.a) * f),\n        )

    def to_dict(self) -> Dict[str, any]:
        return {\"rgb\": self.rgb, \"hex\": self.hex, \"hsv\": self.hsv, \"hsl\": self.hsl}
