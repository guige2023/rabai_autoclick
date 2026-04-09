"""
Animation Curve Utilities for UI Automation.

This module provides easing functions and animation curves for smooth
UI interactions, transitions, and visual feedback effects.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from typing import Callable


# Type alias for animation functions
AnimationFunc = Callable[[float], float]  # Input: 0.0-1.0, Output: 0.0-1.0


def linear(t: float) -> float:
    """Linear interpolation (no easing)."""
    return t


def ease_in_quad(t: float) -> float:
    """Ease-in quadratic: starts slow, ends fast."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Ease-out quadratic: starts fast, ends slow."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Ease-in-out quadratic: slow start and end."""
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Ease-in cubic."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Ease-out cubic."""
    t -= 1
    return t * t * t + 1


def ease_in_out_cubic(t: float) -> float:
    """Ease-in-out cubic."""
    if t < 0.5:
        return 4 * t * t * t
    t = (2 * t) - 2
    return (t * t * t + 2) / 2


def ease_in_quart(t: float) -> float:
    """Ease-in quartic."""
    return t * t * t * t


def ease_out_quart(t: float) -> float:
    """Ease-out quartic."""
    t -= 1
    return 1 - t * t * t * t


def ease_in_out_quart(t: float) -> float:
    """Ease-in-out quartic."""
    if t < 0.5:
        return 8 * t * t * t * t
    t -= 1
    return 1 - 8 * t * t * t * t


def ease_in_quint(t: float) -> float:
    """Ease-in quintic."""
    return t * t * t * t * t


def ease_out_quint(t: float) -> float:
    """Ease-out quintic."""
    t -= 1
    return t * t * t * t * t + 1


def ease_in_out_quint(t: float) -> float:
    """Ease-in-out quintic."""
    if t < 0.5:
        return 16 * t * t * t * t * t
    t = (2 * t) - 2
    return ((t * t * t * t * t) + 2) / 2


def ease_in_sine(t: float) -> float:
    """Ease-in sine curve."""
    import math
    return 1 - math.cos((t * math.pi) / 2)


def ease_out_sine(t: float) -> float:
    """Ease-out sine curve."""
    import math
    return math.sin((t * math.pi) / 2)


def ease_in_out_sine(t: float) -> float:
    """Ease-in-out sine curve."""
    import math
    return -(math.cos(math.pi * t) - 1) / 2


def ease_in_expo(t: float) -> float:
    """Ease-in exponential."""
    if t == 0:
        return 0
    return 2 ** (10 * (t - 1))


def ease_out_expo(t: float) -> float:
    """Ease-out exponential."""
    if t == 1:
        return 1
    return 1 - 2 ** (-10 * t)


def ease_in_out_expo(t: float) -> float:
    """Ease-in-out exponential."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return 2 ** (20 * t - 10) / 2
    return (2 - 2 ** (-20 * t + 10)) / 2


def ease_in_circ(t: float) -> float:
    """Ease-in circular."""
    import math
    return 1 - math.sqrt(1 - t * t)


def ease_out_circ(t: float) -> float:
    """Ease-out circular."""
    import math
    t -= 1
    return math.sqrt(1 - t * t)


def ease_in_out_circ(t: float) -> float:
    """Ease-in-out circular."""
    import math
    if t < 0.5:
        return (1 - math.sqrt(1 - 4 * t * t)) / 2
    t = (2 * t) - 2
    return (math.sqrt(1 - t * t) + 1) / 2


def ease_in_back(t: float) -> float:
    """Ease-in with overshoot (backward movement)."""
    import math
    c1 = 1.70158
    c3 = c1 + 1
    return c3 * t * t * t - c1 * t * t


def ease_out_back(t: float) -> float:
    """Ease-out with overshoot (forward movement)."""
    import math
    c1 = 1.70158
    c3 = c1 + 1
    t -= 1
    return 1 + c3 * t * t * t + c1 * t * t


def ease_in_out_back(t: float) -> float:
    """Ease-in-out with overshoot."""
    import math
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) / 2
    t = (2 * t) - 2
    return ((t ** 2) * ((c2 + 1) * t + c2) + 2) / 2


def ease_in_elastic(t: float) -> float:
    """Ease-in with elastic effect."""
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -(2 ** (10 * t - 10)) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


def ease_out_elastic(t: float) -> float:
    """Ease-out with elastic effect."""
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1


def ease_in_out_elastic(t: float) -> float:
    """Ease-in-out with elastic effect."""
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2
    return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2 + 1


def ease_in_bounce(t: float) -> float:
    """Ease-in with bounce effect."""
    return 1 - ease_out_bounce(1 - t)


def ease_out_bounce(t: float) -> float:
    """Ease-out with bounce effect."""
    import math
    n1 = 7.5625
    d1 = 2.75
    
    if t < 1 / d1:
        return n1 * t * t
    elif t < 2 / d1:
        t -= 1.5 / d1
        return n1 * t * t + 0.75
    elif t < 2.5 / d1:
        t -= 2.25 / d1
        return n1 * t * t + 0.9375
    else:
        t -= 2.625 / d1
        return n1 * t * t + 0.984375


def ease_in_out_bounce(t: float) -> float:
    """Ease-in-out with bounce effect."""
    if t < 0.5:
        return (1 - ease_out_bounce(1 - 2 * t)) / 2
    return (1 + ease_out_bounce(2 * t - 1)) / 2


class AnimationCurve:
    """
    Represents an animation curve with easing function.
    
    Example:
        curve = AnimationCurve(ease_in_out_quad)
        value = curve.interpolate(0.5)  # Get value at 50%
    """
    
    def __init__(self, func: AnimationFunc = linear):
        self.func = func
    
    def interpolate(self, t: float) -> float:
        """
        Get the eased value for a normalized input.
        
        Args:
            t: Input value between 0.0 and 1.0
            
        Returns:
            Eased value between 0.0 and 1.0
        """
        t = max(0.0, min(1.0, t))
        return self.func(t)
    
    def interpolate_pair(
        self, 
        start: float, 
        end: float, 
        t: float
    ) -> float:
        """
        Interpolate between two values using this curve.
        
        Args:
            start: Starting value
            end: Ending value
            t: Input value between 0.0 and 1.0
            
        Returns:
            Interpolated value
        """
        eased_t = self.interpolate(t)
        return start + (end - start) * eased_t


# Pre-defined commonly used curves
CURVES = {
    "linear": AnimationCurve(linear),
    "ease_in_quad": AnimationCurve(ease_in_quad),
    "ease_out_quad": AnimationCurve(ease_out_quad),
    "ease_in_out_quad": AnimationCurve(ease_in_out_quad),
    "ease_in_cubic": AnimationCurve(ease_in_cubic),
    "ease_out_cubic": AnimationCurve(ease_out_cubic),
    "ease_in_out_cubic": AnimationCurve(ease_in_out_cubic),
    "ease_in_sine": AnimationCurve(ease_in_sine),
    "ease_out_sine": AnimationCurve(ease_out_sine),
    "ease_in_out_sine": AnimationCurve(ease_in_out_sine),
    "ease_in_elastic": AnimationCurve(ease_in_elastic),
    "ease_out_elastic": AnimationCurve(ease_out_elastic),
    "ease_in_out_elastic": AnimationCurve(ease_in_out_elastic),
    "ease_in_bounce": AnimationCurve(ease_in_bounce),
    "ease_out_bounce": AnimationCurve(ease_out_bounce),
    "ease_in_out_bounce": AnimationCurve(ease_in_out_bounce),
}


def get_curve(name: str) -> AnimationCurve:
    """
    Get a named animation curve.
    
    Args:
        name: Curve name (e.g., "ease_in_out_quad")
        
    Returns:
        AnimationCurve instance
        
    Raises:
        KeyError: If curve name is not found
    """
    if name not in CURVES:
        raise KeyError(f"Unknown curve: {name}. Available: {list(CURVES.keys())}")
    return CURVES[name]


class BezierCurve:
    """
    Custom bezier curve for animations.
    
    Example:
        # Cubic bezier with control points
        curve = BezierCurve(0.25, 0.1, 0.25, 1.0)
        value = curve.get_value(0.5)
    """
    
    def __init__(self, x1: float, y1: float, x2: float, y2: float):
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2
    
    def get_value(self, t: float) -> float:
        """
        Get Y value for given X using cubic bezier.
        
        Args:
            t: X value between 0 and 1
            
        Returns:
            Y value between 0 and 1
        """
        import math
        
        # Binary search for t given x
        t = max(0.0, min(1.0, t))
        
        def sample(t_val: float) -> float:
            # De Casteljau's algorithm
            p0 = 0.0
            p1 = self.y1
            p2 = self.y2
            p3 = 1.0
            u = 1 - t_val
            return u*u*u*p0 + 3*u*u*t_val*p1 + 3*u*t_val*t_val*p2 + t_val*t_val*t_val*p3
        
        # Use Newton-Raphson to find parameter
        t_val = t
        for _ in range(20):
            x_val = self._get_x_for_t(t_val)
            if abs(x_val - t) < 1e-6:
                break
            t_val -= (x_val - t) / self._get_dx_for_t(t_val)
        
        return sample(max(0.0, min(1.0, t_val)))
    
    def _get_x_for_t(self, t: float) -> float:
        """Get X value for parameter t using De Casteljau."""
        u = 1 - t
        return 3*u*u*t*self.x1 + 3*u*t*t*self.x2 + t*t*t
    
    def _get_dx_for_t(self, t: float) -> float:
        """Get derivative of X with respect to t."""
        u = 1 - t
        return 3*u*u*self.x1 + 6*u*t*(self.x2 - self.x1) + 3*t*t*(1 - self.x2)
