"""
Mouse profile management utilities for automation.

Provides mouse speed, acceleration, and behavior profiles
for consistent automation execution.
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum


@dataclass
class MouseProfile:
    """Mouse behavior profile."""
    name: str
    speed: float
    acceleration: bool
    scroll_speed: float
    double_click_interval: float
    dragging: bool


class MouseProfileManager:
    """Manages mouse behavior profiles."""
    
    PROFILES = {
        "default": MouseProfile(
            name="default",
            speed=1.0,
            acceleration=True,
            scroll_speed=1.0,
            double_click_interval=0.5,
            dragging=True
        ),
        "slow": MouseProfile(
            name="slow",
            speed=0.5,
            acceleration=False,
            scroll_speed=0.5,
            double_click_interval=0.5,
            dragging=True
        ),
        "fast": MouseProfile(
            name="fast",
            speed=2.0,
            acceleration=False,
            scroll_speed=1.5,
            double_click_interval=0.3,
            dragging=True
        ),
        "automation": MouseProfile(
            name="automation",
            speed=1.0,
            acceleration=False,
            scroll_speed=1.0,
            double_click_interval=0.25,
            dragging=True
        ),
    }
    
    def __init__(self):
        self._current: Optional[MouseProfile] = None
        self._saved_profile: Optional[Dict[str, Any]] = None
    
    def get_profile(self, name: str) -> Optional[MouseProfile]:
        """Get profile by name."""
        return self.PROFILES.get(name)
    
    def list_profiles(self) -> List[str]:
        """List available profile names."""
        return list(self.PROFILES.keys())
    
    def apply_profile(self, profile: MouseProfile) -> bool:
        """
        Apply mouse profile settings.
        
        Args:
            profile: MouseProfile to apply.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            self._saved_profile = self._get_current_settings()
            
            if profile.acceleration:
                subprocess.run(
                    ["defaults", "write", "-g", "com.apple.mouse.acceleration", "-int", "1"],
                    capture_output=True
                )
            else:
                subprocess.run(
                    ["defaults", "write", "-g", "com.apple.mouse.acceleration", "-int", "0"],
                    capture_output=True
                )
            
            subprocess.run(
                ["defaults", "write", "-g", "com.apple.mouse.doubleClickInterval", "-float", str(profile.double_click_interval)],
                capture_output=True
            )
            
            self._current = profile
            return True
        except Exception:
            return False
    
    def restore(self) -> bool:
        """
        Restore saved profile.
        
        Returns:
            True if successful, False otherwise.
        """
        if not self._saved_profile:
            return False
        
        try:
            if 'acceleration' in self._saved_profile:
                subprocess.run(
                    ["defaults", "write", "-g", "com.apple.mouse.acceleration", "-int",
                     str(self._saved_profile['acceleration'])],
                    capture_output=True
                )
            
            if 'double_click' in self._saved_profile:
                subprocess.run(
                    ["defaults", "write", "-g", "com.apple.mouse.doubleClickInterval", "-float",
                     str(self._saved_profile['double_click'])],
                    capture_output=True
                )
            
            self._current = None
            return True
        except Exception:
            return False
    
    def _get_current_settings(self) -> Dict[str, Any]:
        """Get current mouse settings."""
        settings = {}
        
        try:
            result = subprocess.run(
                ["defaults", "read", "-g", "com.apple.mouse.acceleration"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                settings['acceleration'] = int(result.stdout.strip())
        except Exception:
            pass
        
        try:
            result = subprocess.run(
                ["defaults", "read", "-g", "com.apple.mouse.doubleClickInterval"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                settings['double_click'] = float(result.stdout.strip())
        except Exception:
            pass
        
        return settings
    
    def create_custom_profile(self,
                             name: str,
                             speed: float = 1.0,
                             acceleration: bool = False,
                             scroll_speed: float = 1.0,
                             double_click_interval: float = 0.5,
                             dragging: bool = True) -> MouseProfile:
        """
        Create custom profile.
        
        Args:
            name: Profile name.
            speed: Mouse speed multiplier.
            acceleration: Enable acceleration.
            scroll_speed: Scroll speed multiplier.
            double_click_interval: Double click interval.
            dragging: Enable drag behavior.
            
        Returns:
            New MouseProfile.
        """
        profile = MouseProfile(
            name=name,
            speed=speed,
            acceleration=acceleration,
            scroll_speed=scroll_speed,
            double_click_interval=double_click_interval,
            dragging=dragging
        )
        self.PROFILES[name] = profile
        return profile
    
    def get_current(self) -> Optional[MouseProfile]:
        """Get currently applied profile."""
        return self._current


def get_current_mouse_speed() -> float:
    """
    Get current mouse speed setting.
    
    Returns:
        Mouse speed value.
    """
    try:
        result = subprocess.run(
            ["defaults", "read", "-g", "com.apple.mouse.acceleration"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return float(result.stdout.strip())
    except Exception:
        pass
    return 1.0


def set_mouse_speed(speed: float) -> bool:
    """
    Set mouse speed.
    
    Args:
        speed: Speed value.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        subprocess.run(
            ["defaults", "write", "-g", "com.apple.mouse.acceleration", "-int", str(int(speed))],
            capture_output=True
        )
        return True
    except Exception:
        return False


def get_double_click_interval() -> float:
    """
    Get system double-click interval.
    
    Returns:
        Interval in seconds.
    """
    try:
        result = subprocess.run(
            ["defaults", "read", "-g", "com.apple.mouse.doubleClickInterval"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return float(result.stdout.strip())
    except Exception:
        pass
    return 0.5
