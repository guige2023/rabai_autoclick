"""Color and Theme Utilities.

This module provides color manipulation, theme management, and accessibility
color utilities for macOS desktop applications including color spaces,
color harmony, and dynamic theme switching.

Example:
    >>> from color_theme_utils import Color, ThemeManager
    >>> color = Color.from_hex('#3498db')
    >>> darker = color.darken(0.2)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple, Union


class ColorSpace(Enum):
    """Supported color spaces."""
    RGB = auto()
    HSL = auto()
    HSV = auto()
    HSB = auto()
    CMYK = auto()
    HEX = auto()


@dataclass
class RGBA:
    """RGBA color representation."""
    r: float
    g: float
    b: float
    a: float = 1.0
    
    def __post_init__(self):
        self.r = max(0.0, min(1.0, self.r))
        self.g = max(0.0, min(1.0, self.g))
        self.b = max(0.0, min(1.0, self.b))
        self.a = max(0.0, min(1.0, self.a))
    
    @property
    def hex(self) -> str:
        """Get hex string representation."""
        r = int(self.r * 255)
        g = int(self.g * 255)
        b = int(self.b * 255)
        return f'#{r:02x}{g:02x}{b:02x}'
    
    @property
    def hex_alpha(self) -> str:
        """Get hex string with alpha."""
        r = int(self.r * 255)
        g = int(self.g * 255)
        b = int(self.b * 255)
        a = int(self.a * 255)
        return f'#{r:02x}{g:02x}{b:02x}{a:02x}'
    
    def to_hsl(self) -> Tuple[float, float, float]:
        """Convert to HSL."""
        max_c = max(self.r, self.g, self.b)
        min_c = min(self.r, self.g, self.b)
        l = (max_c + min_c) / 2
        
        if max_c == min_c:
            h = s = 0.0
        else:
            d = max_c - min_c
            s = l > 0.5 and d / (2 - max_c - min_c) or d / (max_c + min_c)
            
            if max_c == self.r:
                h = (self.g - self.b) / d + (self.g < self.b and 6 or 0)
            elif max_c == self.g:
                h = (self.b - self.r) / d + 2
            else:
                h = (self.r - self.g) / d + 4
            
            h /= 6
        
        return (h, s, l)
    
    def to_hsv(self) -> Tuple[float, float, float]:
        """Convert to HSV/HSB."""
        max_c = max(self.r, self.g, self.b)
        min_c = min(self.r, self.g, self.b)
        v = max_c
        
        d = max_c - min_c
        s = max_c == 0 and 0 or d / max_c
        
        if max_c == min_c:
            h = 0.0
        else:
            if max_c == self.r:
                h = (self.g - self.b) / d + (self.g < self.b and 6 or 0)
            elif max_c == self.g:
                h = (self.b - self.r) / d + 2
            else:
                h = (self.r - self.g) / d + 4
            
            h /= 6
        
        return (h, s, v)
    
    def distance_to(self, other: RGBA) -> float:
        """Calculate color distance (Euclidean in RGB space)."""
        dr = self.r - other.r
        dg = self.g - other.g
        db = self.b - other.b
        da = self.a - other.a
        return math.sqrt(dr*dr + dg*dg + db*db + da*da)
    
    def blend_with(self, other: RGBA, ratio: float = 0.5) -> RGBA:
        """Blend with another color."""
        r = self.r * (1 - ratio) + other.r * ratio
        g = self.g * (1 - ratio) + other.g * ratio
        b = self.b * (1 - ratio) + other.b * ratio
        a = self.a * (1 - ratio) + other.a * ratio
        return RGBA(r, g, b, a)
    
    def darken(self, amount: float) -> RGBA:
        """Darken color by amount (0.0 to 1.0)."""
        h, s, l = self.to_hsl()
        l = max(0.0, l - amount)
        return self._from_hsl(h, s, l)
    
    def lighten(self, amount: float) -> RGBA:
        """Lighten color by amount (0.0 to 1.0)."""
        h, s, l = self.to_hsl()
        l = min(1.0, l + amount)
        return self._from_hsl(h, s, l)
    
    def saturate(self, amount: float) -> RGBA:
        """Increase saturation by amount."""
        h, s, l = self.to_hsl()
        s = min(1.0, s + amount)
        return self._from_hsl(h, s, l)
    
    def desaturate(self, amount: float) -> RGBA:
        """Decrease saturation by amount."""
        h, s, l = self.to_hsl()
        s = max(0.0, s - amount)
        return self._from_hsl(h, s, l)
    
    def _from_hsl(self, h: float, s: float, l: float) -> RGBA:
        """Create RGBA from HSL."""
        if s == 0:
            return RGBA(l, l, l, self.a)
        
        q = l < 0.5 and l * (1 + s) or l + s - l * s
        p = 2 * l - q
        
        r = self._hue_to_rgb(p, q, h + 1/3)
        g = self._hue_to_rgb(p, q, h)
        b = self._hue_to_rgb(p, q, h - 1/3)
        
        return RGBA(r, g, b, self.a)
    
    @staticmethod
    def _hue_to_rgb(p: float, q: float, t: float) -> float:
        """Convert hue to RGB component."""
        if t < 0:
            t += 1
        if t > 1:
            t -= 1
        if t < 1/6:
            return p + (q - p) * 6 * t
        if t < 1/2:
            return q
        if t < 2/3:
            return p + (q - p) * (2/3 - t) * 6
        return p


class Color:
    """Color representation and manipulation."""
    
    def __init__(self, r: float, g: float, b: float, a: float = 1.0):
        self._rgba = RGBA(r, g, b, a)
    
    @classmethod
    def from_rgb(cls, r: float, g: float, b: float, a: float = 1.0) -> Color:
        """Create from RGB values (0-255)."""
        return cls(r/255, g/255, b/255, a)
    
    @classmethod
    def from_hex(cls, hex_string: str) -> Color:
        """Create from hex string (#RGB, #RGBA, #RRGGBB, #RRGGBBAA)."""
        hex_string = hex_string.lstrip('#')
        
        if len(hex_string) == 3:
            hex_string = ''.join(c*2 for c in hex_string)
        elif len(hex_string) == 4:
            hex_string = ''.join(c*2 for c in hex_string)
        
        if len(hex_string) == 6:
            r = int(hex_string[0:2], 16) / 255
            g = int(hex_string[2:4], 16) / 255
            b = int(hex_string[4:6], 16) / 255
            return cls(r, g, b, 1.0)
        elif len(hex_string) == 8:
            r = int(hex_string[0:2], 16) / 255
            g = int(hex_string[2:4], 16) / 255
            b = int(hex_string[4:6], 16) / 255
            a = int(hex_string[6:8], 16) / 255
            return cls(r, g, b, a)
        
        raise ValueError(f"Invalid hex color: {hex_string}")
    
    @classmethod
    def from_hsl(cls, h: float, s: float, l: float, a: float = 1.0) -> Color:
        """Create from HSL values (h: 0-360, s: 0-1, l: 0-1)."""
        h_norm = h / 360.0
        
        if s == 0:
            return cls(l, l, l, a)
        
        q = l < 0.5 and l * (1 + s) or l + s - l * s
        p = 2 * l - q
        
        r = cls._hue_to_rgb(p, q, h_norm + 1/3)
        g = cls._hue_to_rgb(p, q, h_norm)
        b = cls._hue_to_rgb(p, q, h_norm - 1/3)
        
        return cls(r, g, b, a)
    
    @staticmethod
    def _hue_to_rgb(p: float, q: float, t: float) -> float:
        if t < 0: t += 1
        if t > 1: t -= 1
        if t < 1/6: return p + (q - p) * 6 * t
        if t < 1/2: return q
        if t < 2/3: return p + (q - p) * (2/3 - t) * 6
        return p
    
    def to_rgba(self) -> RGBA:
        return self._rgba
    
    def to_hex(self) -> str:
        return self._rgba.hex
    
    def to_rgb_string(self) -> str:
        r, g, b = int(self._rgba.r * 255), int(self._rgba.g * 255), int(self._rgba.b * 255)
        return f'rgb({r}, {g}, {b})'
    
    def to_rgba_string(self) -> str:
        r, g, b, a = (int(self._rgba.r * 255), int(self._rgba.g * 255), 
                      int(self._rgba.b * 255), self._rgba.a)
        return f'rgba({r}, {g}, {b}, {a})'


class ColorPalette:
    """Color palette with semantic color names."""
    
    def __init__(self):
        self._colors: Dict[str, Color] = {}
        self._build_default_palette()
    
    def _build_default_palette(self) -> None:
        """Build default color palette."""
        self.add('primary', Color.from_hex('#3498db'))
        self.add('secondary', Color.from_hex('#2ecc71'))
        self.add('accent', Color.from_hex('#e74c3c'))
        self.add('background', Color.from_hex('#ffffff'))
        self.add('surface', Color.from_hex('#f5f5f5'))
        self.add('text', Color.from_hex('#333333'))
        self.add('text_secondary', Color.from_hex('#666666'))
        self.add('border', Color.from_hex('#dddddd'))
        self.add('error', Color.from_hex('#e74c3c'))
        self.add('warning', Color.from_hex('#f39c12'))
        self.add('success', Color.from_hex('#27ae60'))
        self.add('info', Color.from_hex('#3498db'))
    
    def add(self, name: str, color: Color) -> None:
        self._colors[name] = color
    
    def get(self, name: str) -> Optional[Color]:
        return self._colors.get(name)
    
    def get_or_raise(self, name: str) -> Color:
        if name not in self._colors:
            raise KeyError(f"Color '{name}' not found in palette")
        return self._colors[name]
    
    def names(self) -> List[str]:
        return list(self._colors.keys())


class ThemeMode(Enum):
    """Theme mode (light/dark/system)."""
    LIGHT = auto()
    DARK = auto()
    SYSTEM = auto()


class Theme:
    """Application theme with colors and styling."""
    
    def __init__(self, name: str, mode: ThemeMode):
        self.name = name
        self.mode = mode
        self.palette = ColorPalette()
        self._custom_styles: Dict[str, Dict] = {}
    
    def get_color(self, name: str) -> Optional[Color]:
        return self.palette.get(name)
    
    def set_color(self, name: str, color: Color) -> None:
        self.palette.add(name, color)
    
    def get_style(self, element: str) -> Dict:
        return self._custom_styles.get(element, {})
    
    def set_style(self, element: str, styles: Dict) -> None:
        self._custom_styles[element] = styles


class ThemeManager:
    """Manages application themes and theme switching."""
    
    def __init__(self):
        self._themes: Dict[str, Theme] = {}
        self._current_theme_name: str = "default"
        self._system_mode: ThemeMode = ThemeMode.LIGHT
        self._override_mode: Optional[ThemeMode] = None
        self._listeners: List[callable] = []
        self._build_default_themes()
    
    def _build_default_themes(self) -> None:
        """Build default light and dark themes."""
        light = Theme("light", ThemeMode.LIGHT)
        light.palette.add('background', Color.from_hex('#ffffff'))
        light.palette.add('surface', Color.from_hex('#f5f5f5'))
        light.palette.add('text', Color.from_hex('#333333'))
        light.palette.add('text_secondary', Color.from_hex('#666666'))
        self._themes['light'] = light
        
        dark = Theme("dark", ThemeMode.DARK)
        dark.palette.add('background', Color.from_hex('#1e1e1e'))
        dark.palette.add('surface', Color.from_hex('#2d2d2d'))
        dark.palette.add('text', Color.from_hex('#ffffff'))
        dark.palette.add('text_secondary', Color.from_hex('#aaaaaa'))
        self._themes['dark'] = dark
        
        self._current_theme_name = 'light'
    
    @property
    def current_theme(self) -> Theme:
        """Get the current active theme."""
        return self._themes[self._current_theme_name]
    
    def get_theme(self, name: str) -> Optional[Theme]:
        return self._themes.get(name)
    
    def add_theme(self, theme: Theme) -> None:
        self._themes[theme.name] = theme
    
    def set_theme(self, name: str) -> None:
        if name in self._themes:
            self._current_theme_name = name
            self._notify_listeners()
    
    def set_mode(self, mode: ThemeMode) -> None:
        """Set theme mode (light/dark/system)."""
        self._override_mode = mode
        self._update_theme_for_mode()
        self._notify_listeners()
    
    def _update_theme_for_mode(self) -> None:
        """Update current theme based on mode."""
        if self._override_mode == ThemeMode.LIGHT:
            self._current_theme_name = 'light'
        elif self._override_mode == ThemeMode.DARK:
            self._current_theme_name = 'dark'
        elif self._override_mode == ThemeMode.SYSTEM:
            self._current_theme_name = 'dark' if self._is_system_dark() else 'light'
    
    def _is_system_dark(self) -> bool:
        """Check if system is in dark mode."""
        return False
    
    def add_listener(self, callback: callable) -> None:
        self._listeners.append(callback)
    
    def remove_listener(self, callback: callable) -> None:
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def _notify_listeners(self) -> None:
        for listener in self._listeners:
            try:
                listener(self.current_theme)
            except Exception:
                pass
