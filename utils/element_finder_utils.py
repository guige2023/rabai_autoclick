"""
UI element finder utilities for macOS automation.

Provides element location via accessibility APIs, coordinate-based
detection, and visual matching for GUI automation.
"""

from __future__ import annotations

import subprocess
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class ElementRole(Enum):
    """macOS accessibility element roles."""
    BUTTON = "AXButton"
    CHECKBOX = "AXCheckBox"
    TEXT_FIELD = "AXTextField"
    TEXT_AREA = "AXTextArea"
    MENU_ITEM = "AXMenuItem"
    POP_UP_BUTTON = "AXPopUpButton"
    TABLE = "AXTable"
    ROW = "AXRow"
    CELL = "AXCell"
    GROUP = "AXGroup"
    WINDOW = "AXWindow"
    APPLICATION = "AXApplication"


@dataclass
class UIElement:
    """UI element representation."""
    role: str
    title: str
    value: str
    description: str
    position: Tuple[int, int]
    size: Tuple[int, int]
    enabled: bool
    focused: bool
    parent: Optional[str] = None
    children: List[str] = None
    identifier: Optional[str] = None
    subrole: Optional[str] = None


class ElementFinder:
    """Finder for UI elements using accessibility APIs."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """
        Initialize element finder.
        
        Args:
            app_bundle_id: Optional bundle ID to scope searches.
        """
        self.app_bundle_id = app_bundle_id
        self._cached_app_name: Optional[str] = None
    
    def _get_app_script(self) -> str:
        """Get app targeting script."""
        if self.app_bundle_id:
            return f'''
            tell application "System Events"
                set targetApp to first process whose bundle identifier is "{self.app_bundle_id}"
            end tell
            '''
        return '''
        tell application "System Events"
            set targetApp to first process whose frontmost is true
        end tell
        '''
    
    def find_element_by_role(self, role: ElementRole,
                            title: Optional[str] = None) -> Optional[UIElement]:
        """
        Find element by role, optionally filtered by title.
        
        Args:
            role: Element role to search for.
            title: Optional title to filter by.
            
        Returns:
            UIElement if found, None otherwise.
        """
        script = f'''
        {self._get_app_script()}
        tell targetApp
            set elemList to every UI element whose role is "{role.value}"
            if (count of elemList) > 0 then
                set elem to first item of elemList
                set elemTitle to title of elem
                set elemValue to value of elem
                set elemDesc to description of elem
                set elemPos to position of elem
                set elemSize to size of elem
                set elemEnabled to enabled of elem
                set elemFocused to focused of elem
                return {{elemTitle, elemValue, elemDesc, elemPos, elemSize, elemEnabled, elemFocused}}
            end if
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
                return self._parse_element(result.stdout, role.value)
        except Exception:
            pass
        return None
    
    def find_all_by_role(self, role: ElementRole) -> List[UIElement]:
        """
        Find all elements matching role.
        
        Args:
            role: Element role to search for.
            
        Returns:
            List of UIElement matching role.
        """
        elements = []
        script = f'''
        {self._get_app_script()}
        tell targetApp
            set elemList to every UI element whose role is "{role.value}"
            set resultList to {{}}
            repeat with elem in elemList
                set elemTitle to title of elem
                set elemValue to value of elem
                set elemDesc to description of elem
                set elemPos to position of elem
                set elemSize to size of elem
                set elemEnabled to enabled of elem
                set elemFocused to focused of elem
                set end of resultList to {{elemTitle, elemValue, elemDesc, elemPos, elemSize, elemEnabled, elemFocused}}
            end repeat
            return resultList
        end tell
        '''
        
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    elements.append(self._parse_element(line, role.value))
        except Exception:
            pass
        return elements
    
    def find_by_title(self, title: str, role: Optional[ElementRole] = None) -> Optional[UIElement]:
        """
        Find element by title.
        
        Args:
            title: Title/subtitle to search for.
            role: Optional role filter.
            
        Returns:
            UIElement if found, None otherwise.
        """
        role_cond = f'and role is "{role.value}"' if role else ''
        script = f'''
        {self._get_app_script()}
        tell targetApp
            set elemList to every UI element whose title contains "{title}" {role_cond}
            if (count of elemList) > 0 then
                set elem to first item of elemList
                set elemRole to role of elem
                set elemValue to value of elem
                set elemDesc to description of elem
                set elemPos to position of elem
                set elemSize to size of elem
                set elemEnabled to enabled of elem
                set elemFocused to focused of elem
                return {{elemRole, elemValue, elemDesc, elemPos, elemSize, elemEnabled, elemFocused}}
            end if
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
                parts = result.stdout.strip().split(',')
                if len(parts) >= 7:
                    return UIElement(
                        role=parts[0].strip(),
                        title=title,
                        value=parts[1].strip(),
                        description=parts[2].strip(),
                        position=self._parse_pos(parts[3]),
                        size=self._parse_pos(parts[4]),
                        enabled=parts[5].strip() == 'true',
                        focused=parts[6].strip() == 'true'
                    )
        except Exception:
            pass
        return None
    
    def _parse_element(self, data: str, role: str) -> UIElement:
        """Parse element data from AppleScript output."""
        parts = data.strip().split(',')
        if len(parts) >= 7:
            return UIElement(
                role=role,
                title=parts[0].strip(),
                value=parts[1].strip(),
                description=parts[2].strip(),
                position=self._parse_pos(parts[3]),
                size=self._parse_pos(parts[4]),
                enabled=parts[5].strip() == 'true',
                focused=parts[6].strip() == 'true'
            )
        return UIElement(
            role=role, title='', value='', description='',
            position=(0, 0), size=(0, 0), enabled=False, focused=False
        )
    
    def _parse_pos(self, s: str) -> Tuple[int, int]:
        """Parse position/size from string."""
        cleaned = s.strip().replace('{', '').replace('}', '')
        parts = cleaned.split(',')
        return int(parts[0].strip()), int(parts[1].strip())
    
    def get_element_at(self, x: int, y: int) -> Optional[UIElement]:
        """
        Get element at screen coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            UIElement at position, None if none found.
        """
        script = f'''
        tell application "System Events"
            set elem to UI element at position {x}, {y}
            if exists elem then
                set elemRole to role of elem
                set elemTitle to title of elem
                set elemValue to value of elem
                set elemDesc to description of elem
                set elemPos to position of elem
                set elemSize to size of elem
                set elemEnabled to enabled of elem
                set elemFocused to focused of elem
                return {{elemRole, elemTitle, elemValue, elemDesc, elemPos, elemSize, elemEnabled, elemFocused}}
            end if
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
                parts = result.stdout.strip().split(',')
                if len(parts) >= 8:
                    return UIElement(
                        role=parts[0].strip(),
                        title=parts[1].strip(),
                        value=parts[2].strip(),
                        description=parts[3].strip(),
                        position=self._parse_pos(parts[4]),
                        size=self._parse_pos(parts[5]),
                        enabled=parts[6].strip() == 'true',
                        focused=parts[7].strip() == 'true'
                    )
        except Exception:
            pass
        return None
