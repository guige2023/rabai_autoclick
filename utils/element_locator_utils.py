"""
Element locator utilities for finding UI elements.

Provides multiple locator strategies including XPath, CSS selectors,
and accessibility-based locators for automation.
"""

from __future__ import annotations

import subprocess
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class LocatorType(Enum):
    """UI element locator strategies."""
    ACCESSIBILITY = "accessibility"
    TITLE = "title"
    ROLE = "role"
    VALUE = "value"
    POSITION = "position"
    XPATH = "xpath"


@dataclass
class Locator:
    """Element locator."""
    type: LocatorType
    value: str
    index: int = 0
    timeout: float = 5.0


@dataclass
class LocatorResult:
    """Result of locator search."""
    found: bool
    element: Optional[Dict[str, Any]] = None
    locator: Locator
    attempts: int = 0
    duration: float = 0.0
    error: Optional[str] = None


class ElementLocator:
    """Locates UI elements using various strategies."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """
        Initialize element locator.
        
        Args:
            app_bundle_id: Optional app scope.
        """
        self.app_bundle_id = app_bundle_id
    
    def find(self, locator: Locator) -> LocatorResult:
        """
        Find element using locator.
        
        Args:
            locator: Locator to use.
            
        Returns:
            LocatorResult.
        """
        import time
        start = time.time()
        attempts = 0
        
        while time.time() - start < locator.timeout:
            attempts += 1
            
            element = self._find_element(locator)
            if element:
                return LocatorResult(
                    found=True,
                    element=element,
                    locator=locator,
                    attempts=attempts,
                    duration=time.time() - start
                )
            
            time.sleep(0.1)
        
        return LocatorResult(
            found=False,
            locator=locator,
            attempts=attempts,
            duration=time.time() - start,
            error="Element not found"
        )
    
    def _find_element(self, locator: Locator) -> Optional[Dict[str, Any]]:
        """Find element based on locator type."""
        if locator.type == LocatorType.ACCESSIBILITY:
            return self._find_by_accessibility(locator.value)
        elif locator.type == LocatorType.TITLE:
            return self._find_by_title(locator.value, locator.index)
        elif locator.type == LocatorType.ROLE:
            return self._find_by_role(locator.value, locator.index)
        elif locator.type == LocatorType.POSITION:
            return self._find_by_position(locator.value)
        return None
    
    def _find_by_accessibility(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Find by accessibility identifier."""
        script = f'''
        tell application "System Events"
            {"set targetApp to first process whose bundle identifier is \"" + self.app_bundle_id + "\"" if self.app_bundle_id else "set targetApp to first process whose frontmost is true"}
            set elemList to every UI element whose accessibility description is "{identifier}" or identifier is "{identifier}"
            if (count of elemList) > 0 then
                set elem to first item of elemList
                return element_info(elem)
            end if
        end tell
        '''
        return self._run_script(script)
    
    def _find_by_title(self, title: str, index: int = 0) -> Optional[Dict[str, Any]]:
        """Find by element title."""
        script = f'''
        tell application "System Events"
            {"set targetApp to first process whose bundle identifier is \"" + self.app_bundle_id + "\"" if self.app_bundle_id else "set targetApp to first process whose frontmost is true"}
            set elemList to every UI element whose title contains "{title}"
            if (count of elemList) > {index} then
                set elem to item {index + 1} of elemList
                return element_info(elem)
            end if
        end tell
        '''
        return self._run_script(script)
    
    def _find_by_role(self, role: str, index: int = 0) -> Optional[Dict[str, Any]]:
        """Find by element role."""
        script = f'''
        tell application "System Events"
            {"set targetApp to first process whose bundle identifier is \"" + self.app_bundle_id + "\"" if self.app_bundle_id else "set targetApp to first process whose frontmost is true"}
            set elemList to every UI element whose role is "{role}"
            if (count of elemList) > {index} then
                set elem to item {index + 1} of elemList
                return element_info(elem)
            end if
        end tell
        '''
        return self._run_script(script)
    
    def _find_by_position(self, pos_spec: str) -> Optional[Dict[str, Any]]:
        """Find element at screen position."""
        parts = pos_spec.strip().replace('(', '').replace(')', '').split(',')
        if len(parts) != 2:
            return None
        
        x, y = int(parts[0].strip()), int(parts[1].strip())
        
        script = f'''
        tell application "System Events"
            set elem to UI element at position {x}, {y}
            if exists elem then
                return element_info(elem)
            end if
        end tell
        '''
        return self._run_script(script)
    
    def _run_script(self, script: str) -> Optional[Dict[str, Any]]:
        """Run AppleScript and parse result."""
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.stdout.strip() and ',' in result.stdout:
                parts = result.stdout.strip().split(',')
                if len(parts) >= 6:
                    return {
                        'role': parts[0].strip(),
                        'title': parts[1].strip(),
                        'value': parts[2].strip(),
                        'enabled': parts[3].strip() == 'true',
                        'focused': parts[4].strip() == 'true',
                        'position': self._parse_pos(parts[5])
                    }
        except Exception:
            pass
        return None
    
    def _parse_pos(self, s: str) -> Tuple[int, int]:
        """Parse position string."""
        cleaned = s.strip().replace('{', '').replace('}', '')
        parts = cleaned.split(',')
        return (int(parts[0].strip()), int(parts[1].strip()))
    
    def find_all(self, locator: Locator) -> List[Dict[str, Any]]:
        """
        Find all elements matching locator.
        
        Args:
            locator: Locator to use.
            
        Returns:
            List of matching elements.
        """
        elements = []
        
        if locator.type == LocatorType.ROLE:
            script = f'''
            tell application "System Events"
                {"set targetApp to first process whose bundle identifier is \"" + self.app_bundle_id + "\"" if self.app_bundle_id else "set targetApp to first process whose frontmost is true"}
                set elemList to every UI element whose role is "{locator.value}"
                set resultList to {{}}
                repeat with elem in elemList
                    set end of resultList to element_info(elem)
                end repeat
                return resultList
            end tell
            '''
            
            try:
                result = subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                if result.stdout.strip():
                    for line in result.stdout.strip().split('\n'):
                        if ',' in line:
                            parts = line.split(',')
                            if len(parts) >= 6:
                                elements.append({
                                    'role': parts[0].strip(),
                                    'title': parts[1].strip(),
                                    'value': parts[2].strip(),
                                    'enabled': parts[3].strip() == 'true',
                                    'focused': parts[4].strip() == 'true',
                                    'position': self._parse_pos(parts[5])
                                })
            except Exception:
                pass
        
        return elements


def accessibility_id(identifier: str) -> Locator:
    """Create accessibility ID locator."""
    return Locator(type=LocatorType.ACCESSIBILITY, value=identifier)


def title(title: str, index: int = 0) -> Locator:
    """Create title-based locator."""
    return Locator(type=LocatorType.TITLE, value=title, index=index)


def role(role: str, index: int = 0) -> Locator:
    """Create role-based locator."""
    return Locator(type=LocatorType.ROLE, value=role, index=index)


def at_position(x: int, y: int) -> Locator:
    """Create position-based locator."""
    return Locator(type=LocatorType.POSITION, value=f"({x},{y})")
