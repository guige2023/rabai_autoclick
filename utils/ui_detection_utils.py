"""
UI detection utilities for identifying and validating UI states.

Provides UI element detection, state validation, and
readiness checks for automation workflows.
"""

from __future__ import annotations

import time
import subprocess
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum


class UIState(Enum):
    """UI state conditions."""
    EXISTS = "exists"
    VISIBLE = "visible"
    ENABLED = "enabled"
    FOCUSED = "focused"
    DISABLED = "disabled"
    HIDDEN = "hidden"
    LOADING = "loading"
    READY = "ready"


@dataclass
class DetectionResult:
    """Result of UI detection."""
    found: bool
    element: Optional[Dict[str, Any]] = None
    attempts: int = 0
    duration: float = 0.0
    error: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of UI state validation."""
    valid: bool
    expected_state: UIState
    actual_state: Optional[UIState] = None
    message: str = ""
    details: Dict[str, Any] = None


class UIDetector:
    """Detects UI elements and states."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """
        Initialize UI detector.
        
        Args:
            app_bundle_id: Optional app to scope detection.
        """
        self.app_bundle_id = app_bundle_id
    
    def detect(self, role: str, title: Optional[str] = None,
               timeout: float = 5.0) -> DetectionResult:
        """
        Detect UI element.
        
        Args:
            role: Element role to find.
            title: Optional title filter.
            timeout: Max wait time.
            
        Returns:
            DetectionResult.
        """
        start = time.time()
        attempts = 0
        
        while time.time() - start < timeout:
            attempts += 1
            
            element = self._find_element(role, title)
            if element:
                return DetectionResult(
                    found=True,
                    element=element,
                    attempts=attempts,
                    duration=time.time() - start
                )
            
            time.sleep(0.1)
        
        return DetectionResult(
            found=False,
            attempts=attempts,
            duration=time.time() - start,
            error="Element not found"
        )
    
    def _find_element(self, role: str, title: Optional[str]) -> Optional[Dict[str, Any]]:
        """Find element via accessibility."""
        title_cond = f'and title contains "{title}"' if title else ''
        
        script = f'''
        tell application "System Events"
            {"set targetApp to first process whose bundle identifier is \"" + self.app_bundle_id + "\"" if self.app_bundle_id else "set targetApp to first process whose frontmost is true"}
            set elemList to every UI element whose role is "{role}" {title_cond}
            if (count of elemList) > 0 then
                set elem to first item of elemList
                set elemTitle to title of elem
                set elemValue to value of elem
                set elemEnabled to enabled of elem
                set elemFocused to focused of elem
                set elemPos to position of elem
                set elemSize to size of elem
                return {{elemTitle, elemValue, elemEnabled, elemFocused, elemPos, elemSize}}
            end if
        end tell
        '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.stdout.strip():
                parts = result.stdout.strip().split(',')
                if len(parts) >= 6:
                    return {
                        'title': parts[0].strip(),
                        'value': parts[1].strip(),
                        'enabled': parts[2].strip() == 'true',
                        'focused': parts[3].strip() == 'true',
                        'position': self._parse_pos(parts[4]),
                        'size': self._parse_pos(parts[5])
                    }
        except Exception:
            pass
        
        return None
    
    def _parse_pos(self, s: str) -> tuple:
        """Parse position string."""
        cleaned = s.strip().replace('{', '').replace('}', '')
        parts = cleaned.split(',')
        return (int(parts[0].strip()), int(parts[1].strip()))
    
    def wait_until_ready(self, role: str, title: Optional[str] = None,
                        timeout: float = 10.0) -> bool:
        """
        Wait until element is ready (visible and enabled).
        
        Args:
            role: Element role.
            title: Optional title.
            timeout: Max wait time.
            
        Returns:
            True if ready, False otherwise.
        """
        start = time.time()
        
        while time.time() - start < timeout:
            element = self._find_element(role, title)
            if element and element.get('enabled'):
                return True
            time.sleep(0.1)
        
        return False
    
    def detect_with_retry(self, role: str, title: Optional[str] = None,
                         retries: int = 3,
                         delay: float = 0.5) -> DetectionResult:
        """
        Detect with automatic retry.
        
        Args:
            role: Element role.
            title: Optional title.
            retries: Number of retries.
            delay: Delay between retries.
            
        Returns:
            DetectionResult.
        """
        for attempt in range(retries):
            result = self.detect(role, title, timeout=5.0)
            if result.found:
                return result
            time.sleep(delay)
        
        return DetectionResult(
            found=False,
            attempts=retries,
            error="Detection failed after retries"
        )


def validate_ui_state(element: Dict[str, Any], expected: UIState) -> ValidationResult:
    """
    Validate element matches expected state.
    
    Args:
        element: Element dict from detector.
        expected: Expected UIState.
        
    Returns:
        ValidationResult.
    """
    if element is None:
        return ValidationResult(
            valid=False,
            expected_state=expected,
            actual_state=None,
            message="Element is None"
        )
    
    actual = UIState.EXISTS
    
    if expected == UIState.EXISTS:
        valid = True
    elif expected == UIState.ENABLED:
        valid = element.get('enabled', False)
        actual = UIState.ENABLED if valid else UIState.DISABLED
    elif expected == UIState.DISABLED:
        valid = not element.get('enabled', True)
        actual = UIState.DISABLED if valid else UIState.ENABLED
    elif expected == UIState.FOCUSED:
        valid = element.get('focused', False)
        actual = UIState.FOCUSED if valid else UIState.EXISTS
    elif expected == UIState.VISIBLE:
        pos = element.get('position', (0, 0))
        size = element.get('size', (0, 0))
        valid = pos[0] >= 0 and pos[1] >= 0 and size[0] > 0 and size[1] > 0
        actual = UIState.VISIBLE if valid else UIState.HIDDEN
    else:
        valid = False
    
    return ValidationResult(
        valid=valid,
        expected_state=expected,
        actual_state=actual,
        message="State matches" if valid else f"Expected {expected.value}, got {actual.value}"
    )


def wait_for_condition(condition: Callable[[], bool],
                      timeout: float = 10.0,
                      poll_interval: float = 0.1) -> bool:
    """
    Wait for a condition to become true.
    
    Args:
        condition: Callable returning bool.
        timeout: Max wait time.
        poll_interval: Check interval.
        
    Returns:
        True if condition met, False on timeout.
    """
    start = time.time()
    
    while time.time() - start < timeout:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(poll_interval)
    
    return False
