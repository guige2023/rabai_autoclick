"""UI contrast utilities for accessibility and automation.

Provides utilities for analyzing contrast ratios,
detecting visibility issues, and ensuring accessible UI elements.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class Color:
    """Represents a color in RGB format."""
    r: float
    g: float
    b: float
    a: float = 1.0
    
    @classmethod
    def from_hex(cls, hex_str: str) -> "Color":
        """Create color from hex string.
        
        Args:
            hex_str: Hex color string (e.g., "#FF5733" or "FF5733").
            
        Returns:
            Color object.
        """
        hex_str = hex_str.lstrip('#')
        
        if len(hex_str) == 6:
            r = int(hex_str[0:2], 16) / 255.0
            g = int(hex_str[2:4], 16) / 255.0
            b = int(hex_str[4:6], 16) / 255.0
            return cls(r=r, g=g, b=b)
        elif len(hex_str) == 8:
            r = int(hex_str[0:2], 16) / 255.0
            g = int(hex_str[2:4], 16) / 255.0
            b = int(hex_str[4:6], 16) / 255.0
            a = int(hex_str[6:8], 16) / 255.0
            return cls(r=r, g=g, b=b, a=a)
        
        return cls(r=0, g=0, b=0)
    
    def to_hex(self) -> str:
        """Convert to hex string.
        
        Returns:
            Hex color string.
        """
        r = int(self.r * 255)
        g = int(self.g * 255)
        b = int(self.b * 255)
        return f"#{r:02X}{g:02X}{b:02X}"
    
    def to_luminance(self) -> float:
        """Calculate relative luminance.
        
        Returns:
            Luminance value between 0 and 1.
        """
        def linearize(c: float) -> float:
            if c <= 0.03928:
                return c / 12.92
            return math.pow((c + 0.055) / 1.055, 2.4)
        
        r = linearize(self.r)
        g = linearize(self.g)
        b = linearize(self.b)
        
        return 0.2126 * r + 0.7152 * g + 0.0722 * b


@dataclass
class ContrastResult:
    """Result of a contrast ratio calculation."""
    ratio: float
    passes_aa_normal: bool
    passes_aa_large: bool
    passes_aaa_normal: bool
    passes_aaa_large: bool
    foreground: Color
    background: Color
    
    @property
    def rating(self) -> str:
        """Get a human-readable rating.
        
        Returns:
            Rating string.
        """
        if self.passes_aaa_normal:
            return "Excellent (AAA)"
        elif self.passes_aa_normal:
            return "Good (AA)"
        elif self.passes_aa_large:
            return "Fair (AA Large)"
        else:
            return "Poor"


class ContrastAnalyzer:
    """Analyzes contrast ratios for accessibility.
    
    Calculates contrast ratios between foreground and background
    colors and checks against WCAG guidelines.
    """
    
    WCAG_AA_NORMAL = 4.5
    WCAG_AA_LARGE = 3.0
    WCAG_AAA_NORMAL = 7.0
    WCAG_AAA_LARGE = 4.5
    
    def __init__(self) -> None:
        """Initialize the contrast analyzer."""
        pass
    
    def calculate_ratio(
        self,
        foreground: Color,
        background: Color
    ) -> float:
        """Calculate contrast ratio between two colors.
        
        Args:
            foreground: Foreground color.
            background: Background color.
            
        Returns:
            Contrast ratio (1 to 21).
        """
        l1 = foreground.to_luminance()
        l2 = background.to_luminance()
        
        lighter = max(l1, l2)
        darker = min(l1, l2)
        
        return (lighter + 0.05) / (darker + 0.05)
    
    def analyze(
        self,
        foreground: Color,
        background: Color
    ) -> ContrastResult:
        """Analyze contrast between two colors.
        
        Args:
            foreground: Foreground color.
            background: Background color.
            
        Returns:
            ContrastResult with analysis.
        """
        ratio = self.calculate_ratio(foreground, background)
        
        return ContrastResult(
            ratio=ratio,
            passes_aa_normal=ratio >= self.WCAG_AA_NORMAL,
            passes_aa_large=ratio >= self.WCAG_AA_LARGE,
            passes_aaa_normal=ratio >= self.WCAG_AAA_NORMAL,
            passes_aaa_large=ratio >= self.WCAG_AAA_LARGE,
            foreground=foreground,
            background=background
        )
    
    def suggest_adjustment(
        self,
        foreground: Color,
        background: Color,
        target_ratio: float = 4.5
    ) -> Color:
        """Suggest a foreground color that meets target ratio.
        
        Args:
            foreground: Original foreground color.
            background: Background color.
            target_ratio: Target contrast ratio.
            
        Returns:
            Adjusted foreground color.
        """
        current_ratio = self.calculate_ratio(foreground, background)
        
        if current_ratio >= target_ratio:
            return foreground
        
        bg_luminance = background.to_luminance()
        
        if bg_luminance > 0.5:
            target_luminance = (bg_luminance + 0.05) / target_ratio - 0.05
        else:
            target_luminance = bg_luminance * target_ratio + 0.05 * (target_ratio - 1)
        
        target_luminance = max(0, min(1, target_luminance))
        
        adjustment = 0.2
        current_luminance = foreground.to_luminance()
        
        if current_luminance < target_luminance:
            while current_luminance < target_luminance and adjustment > 0.01:
                current_luminance += adjustment
                adjustment /= 2
        else:
            while current_luminance > target_luminance and adjustment > 0.01:
                current_luminance -= adjustment
                adjustment /= 2
        
        ratio = self.calculate_ratio(Color(r=current_luminance, g=current_luminance, b=current_luminance), background)
        
        if ratio >= target_ratio:
            return Color(r=current_luminance, g=current_luminance, b=current_luminance)
        
        return foreground
    
    def find_best_contrast(
        self,
        candidates: List[Color],
        background: Color,
        target_ratio: float = 4.5
    ) -> Optional[Color]:
        """Find the best contrasting color from candidates.
        
        Args:
            candidates: List of candidate colors.
            background: Background color.
            target_ratio: Target contrast ratio.
            
        Returns:
            Best contrasting color or None.
        """
        best = None
        best_ratio = 0.0
        
        for candidate in candidates:
            ratio = self.calculate_ratio(candidate, background)
            if ratio > best_ratio:
                best_ratio = ratio
                best = candidate
        
        if best and best_ratio >= target_ratio:
            return best
        
        return best


class VisibilityChecker:
    """Checks UI element visibility and contrast.
    
    Provides utilities for detecting potential visibility
    issues in UI elements.
    """
    
    def __init__(self, analyzer: Optional[ContrastAnalyzer] = None) -> None:
        """Initialize the visibility checker.
        
        Args:
            analyzer: Contrast analyzer to use.
        """
        self.analyzer = analyzer or ContrastAnalyzer()
    
    def check_text_visibility(
        self,
        text_color: Color,
        background_color: Color,
        font_size: float,
        is_bold: bool = False
    ) -> Dict[str, any]:
        """Check if text is visible on background.
        
        Args:
            text_color: Text color.
            background_color: Background color.
            font_size: Font size in points.
            is_bold: Whether text is bold.
            
        Returns:
            Dictionary with visibility analysis.
        """
        result = self.analyzer.analyze(text_color, background_color)
        
        large_threshold = 18.0 if is_bold else 24.0
        is_large_text = font_size >= large_threshold
        
        passes = result.passes_aa_normal if not is_large_text else result.passes_aa_large
        
        return {
            "visible": passes,
            "ratio": result.ratio,
            "rating": result.rating,
            "is_large_text": is_large_text,
            "foreground": text_color.to_hex(),
            "background": background_color.to_hex()
        }
    
    def check_icon_visibility(
        self,
        icon_color: Color,
        background_color: Color,
        icon_size: float
    ) -> Dict[str, any]:
        """Check if an icon is visible on background.
        
        Args:
            icon_color: Icon color.
            background_color: Background color.
            icon_size: Icon size in pixels.
            
        Returns:
            Dictionary with visibility analysis.
        """
        result = self.analyzer.analyze(icon_color, background_color)
        
        is_large_icon = icon_size >= 24
        
        passes = result.passes_aa_large if is_large_icon else result.passes_aa_normal
        
        return {
            "visible": passes,
            "ratio": result.ratio,
            "rating": result.rating,
            "is_large_icon": is_large_icon,
            "foreground": icon_color.to_hex(),
            "background": background_color.to_hex()
        }
    
    def check_ui_component(
        self,
        component_bg: Color,
        parent_bg: Color,
        border_color: Optional[Color] = None
    ) -> Dict[str, any]:
        """Check if a UI component is distinguishable from parent.
        
        Args:
            component_bg: Component background color.
            parent_bg: Parent background color.
            border_color: Optional border color.
            
        Returns:
            Dictionary with visibility analysis.
        """
        result = self.analyzer.analyze(component_bg, parent_bg)
        
        has_border = border_color is not None
        border_passes = False
        
        if border_color:
            border_result = self.analyzer.analyze(border_color, component_bg)
            border_passes = border_result.ratio >= 3.0
        
        distinguishable = result.ratio >= 3.0 or has_border and border_passes
        
        return {
            "distinguishable": distinguishable,
            "component_ratio": result.ratio,
            "rating": result.rating,
            "has_border": has_border,
            "border_passes": border_passes,
            "component_bg": component_bg.to_hex(),
            "parent_bg": parent_bg.to_hex()
        }


def calculate_relative_luminance(color: Color) -> float:
    """Calculate relative luminance of a color.
    
    Args:
        color: Color to analyze.
        
    Returns:
        Luminance value between 0 and 1.
    """
    return color.to_luminance()


def contrast_ratio(color1: Color, color2: Color) -> float:
    """Calculate contrast ratio between two colors.
    
    Args:
        color1: First color.
        color2: Second color.
        
    Returns:
        Contrast ratio (1 to 21).
    """
    l1 = color1.to_luminance()
    l2 = color2.to_luminance()
    
    lighter = max(l1, l2)
    darker = min(l1, l2)
    
    return (lighter + 0.05) / (darker + 0.05)


def meets_wcag_aa(
    foreground: Color,
    background: Color,
    is_large_text: bool = False
) -> bool:
    """Check if colors meet WCAG AA standard.
    
    Args:
        foreground: Foreground color.
        background: Background color.
        is_large_text: Whether this is large text (18pt+ bold or 24pt+).
        
    Returns:
        True if meets AA standard.
    """
    analyzer = ContrastAnalyzer()
    result = analyzer.analyze(foreground, background)
    
    if is_large_text:
        return result.passes_aa_large
    
    return result.passes_aa_normal


def meets_wcag_aaa(
    foreground: Color,
    background: Color,
    is_large_text: bool = False
) -> bool:
    """Check if colors meet WCAG AAA standard.
    
    Args:
        foreground: Foreground color.
        background: Background color.
        is_large_text: Whether this is large text (18pt+ bold or 24pt+).
        
    Returns:
        True if meets AAA standard.
    """
    analyzer = ContrastAnalyzer()
    result = analyzer.analyze(foreground, background)
    
    if is_large_text:
        return result.passes_aaa_large
    
    return result.passes_aaa_normal
