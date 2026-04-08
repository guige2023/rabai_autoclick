"""
UI inspector utilities for debugging and inspecting UI elements.

Provides element tree inspection, property dumping,
and accessibility analysis for automation debugging.
"""

from __future__ import annotations

import subprocess
import json
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum


class ElementProperty(Enum):
    """UI element properties."""
    ROLE = "AXRole"
    TITLE = "AXTitle"
    VALUE = "AXValue"
    DESCRIPTION = "AXDescription"
    POSITION = "AXPosition"
    SIZE = "AXSize"
    ENABLED = "AXEnabled"
    FOCUSED = "AXFocused"
    VISIBLE = "AXVisible"
    HELP = "AXHelp"
    IDENTIFIER = "AXIdentifier"


@dataclass
class InspectResult:
    """UI inspection result."""
    element: Dict[str, Any]
    path: List[str]
    depth: int


@dataclass
class ElementTree:
    """UI element tree structure."""
    role: str
    title: str
    children: List['ElementTree']
    properties: Dict[str, Any]
    position: Tuple[int, int]
    size: Tuple[int, int]


class UIInspector:
    """Inspects UI element hierarchy."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """
        Initialize UI inspector.
        
        Args:
            app_bundle_id: Optional app bundle ID.
        """
        self.app_bundle_id = app_bundle_id
    
    def inspect(self, x: int, y: int) -> Optional[InspectResult]:
        """
        Inspect element at position.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            InspectResult or None.
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
                return self._parse_result(result.stdout, [(x, y)])
        except Exception:
            pass
        return None
    
    def _parse_result(self, output: str, path: List) -> Optional[InspectResult]:
        """Parse inspection result."""
        parts = output.strip().split(',')
        if len(parts) >= 8:
            elem = {
                'role': parts[0].strip(),
                'title': parts[1].strip(),
                'value': parts[2].strip(),
                'description': parts[3].strip(),
                'position': self._parse_pos(parts[4]),
                'size': self._parse_pos(parts[5]),
                'enabled': parts[6].strip() == 'true',
                'focused': parts[7].strip() == 'true',
            }
            return InspectResult(
                element=elem,
                path=[f"({p[0]},{p[1]})" for p in path],
                depth=len(path)
            )
        return None
    
    def _parse_pos(self, s: str) -> Tuple[int, int]:
        """Parse position string."""
        cleaned = s.strip().replace('{', '').replace('}', '')
        parts = cleaned.split(',')
        return (int(parts[0].strip()), int(parts[1].strip()))
    
    def dump_element_tree(self, max_depth: int = 5) -> Optional[ElementTree]:
        """
        Dump full element tree for frontmost app.
        
        Args:
            max_depth: Maximum tree depth.
            
        Returns:
            ElementTree or None.
        """
        if self.app_bundle_id:
            script = f'''
            tell application "System Events"
                set targetApp to first process whose bundle identifier is "{self.app_bundle_id}"
                set frontWin to first window of targetApp
                return dump_element(frontWin, 0, {max_depth})
            end tell
            '''
        else:
            script = f'''
            tell application "System Events"
                set targetApp to first process whose frontmost is true
                set frontWin to first window of targetApp
                return dump_element(frontWin, 0, {max_depth})
            end tell
            '''
        
        return None
    
    def get_all_elements(self) -> List[Dict[str, Any]]:
        """
        Get all UI elements from frontmost app.
        
        Returns:
            List of element dicts.
        """
        elements = []
        
        script = '''
        tell application "System Events"
            set targetApp to first process whose frontmost is true
            set elemList to every UI element of frontWin
            set resultList to {}
            repeat with elem in elemList
                set elemRole to role of elem
                set elemTitle to title of elem
                set elemValue to value of elem
                set elemEnabled to enabled of elem
                set end of resultList to {elemRole, elemTitle, elemValue, elemEnabled}
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
                    if ',' in line:
                        parts = line.split(',')
                        if len(parts) >= 4:
                            elements.append({
                                'role': parts[0].strip(),
                                'title': parts[1].strip(),
                                'value': parts[2].strip(),
                                'enabled': parts[3].strip() == 'true',
                            })
        except Exception:
            pass
        
        return elements
    
    def find_by_role(self, role: str) -> List[Dict[str, Any]]:
        """
        Find all elements with role.
        
        Args:
            role: Element role.
            
        Returns:
            List of matching elements.
        """
        elements = []
        
        script = f'''
        tell application "System Events"
            set elemList to every UI element whose role is "{role}"
            set resultList to {{}}
            repeat with elem in elemList
                set elemTitle to title of elem
                set elemPos to position of elem
                set elemSize to size of elem
                set end of resultList to {{elemTitle, elemPos, elemSize}}
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
                    if ',' in line:
                        parts = line.split(',')
                        if len(parts) >= 3:
                            elements.append({
                                'title': parts[0].strip(),
                                'position': self._parse_pos(parts[1]),
                                'size': self._parse_pos(parts[2]),
                            })
        except Exception:
            pass
        
        return elements
    
    def export_json(self, path: str) -> bool:
        """
        Export all elements to JSON.
        
        Args:
            path: Output file path.
            
        Returns:
            True if successful.
        """
        elements = self.get_all_elements()
        
        try:
            with open(path, 'w') as f:
                json.dump(elements, f, indent=2)
            return True
        except Exception:
            return False


def quick_inspect(x: int, y: int) -> Optional[Dict[str, Any]]:
    """
    Quick inspect element at position.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        
    Returns:
        Element dict or None.
    """
    inspector = UIInspector()
    result = inspector.inspect(x, y)
    return result.element if result else None
