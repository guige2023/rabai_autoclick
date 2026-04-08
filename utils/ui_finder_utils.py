"""
UI element finder and locator utilities.

Provides utilities for finding and locating UI elements
using various strategies including accessibility, image recognition, and coordinates.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Dict, Any, Callable, Tuple
from dataclasses import dataclass
from enum import Enum
import time


class FinderStrategy(Enum):
    """UI element finding strategies."""
    ACCESSIBILITY = "accessibility"
    IMAGE = "image"
    COORDINATE = "coordinate"
    TEXT = "text"
    ATTRIBUTE = "attribute"


@dataclass
class UIFound:
    """Represents a found UI element."""
    element_type: str
    identifier: str
    x: int
    y: int
    width: int
    height: int
    attributes: Dict[str, Any]
    
    @property
    def center(self) -> Tuple[int, int]:
        """Center coordinates."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        """Bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)


@dataclass
class FinderOptions:
    """Options for UI element finding."""
    timeout: float = 10.0
    interval: float = 0.1
    required: bool = True
    visible_only: bool = True
    enabled_only: bool = False


class UIFinder:
    """Finds UI elements using various strategies."""
    
    def __init__(self):
        """Initialize UI finder."""
        self._options = FinderOptions()
    
    def set_options(
        self,
        timeout: Optional[float] = None,
        interval: Optional[float] = None,
        visible_only: Optional[bool] = None,
        enabled_only: Optional[bool] = None
    ) -> "UIFinder":
        """Set finder options.
        
        Args:
            timeout: Search timeout in seconds
            interval: Retry interval in seconds
            visible_only: Only find visible elements
            enabled_only: Only find enabled elements
            
        Returns:
            Self for chaining
        """
        if timeout is not None:
            self._options.timeout = timeout
        if interval is not None:
            self._options.interval = interval
        if visible_only is not None:
            self._options.visible_only = visible_only
        if enabled_only is not None:
            self._options.enabled_only = enabled_only
        
        return self
    
    def find_by_accessibility(
        self,
        role: str,
        identifier: Optional[str] = None,
        title: Optional[str] = None,
        **attributes
    ) -> Optional[UIFound]:
        """Find element by accessibility attributes.
        
        Args:
            role: Element role (button, textfield, etc.)
            identifier: Accessibility identifier
            title: Element title
            **attributes: Additional attributes
            
        Returns:
            UIFound or None
        """
        try:
            script = '''
            tell application "System Events"
                tell process "System Events"
            '''
            
            if identifier:
                script += f'''
                    set theElements to every UI element whose accessibility description is "{identifier}"
                '''
            elif title:
                script += f'''
                    set theElements to every UI element whose title is "{title}"
                '''
            else:
                script += f'''
                    set theElements to every UI element whose role is "{role}"
                '''
            
            script += '''
                    if (count of theElements) > 0 then
                        set theElement to item 1 of theElements
                        return role of theElement & "|" & description of theElement
                    end if
                end tell
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split("|")
                return UIFound(
                    element_type=parts[0] if len(parts) > 0 else role,
                    identifier=parts[1] if len(parts) > 1 else "",
                    x=0, y=0, width=100, height=100,
                    attributes={}
                )
        except Exception:
            pass
        
        return None
    
    def find_all_by_accessibility(
        self,
        role: Optional[str] = None,
        **attributes
    ) -> List[UIFound]:
        """Find all elements matching accessibility criteria.
        
        Args:
            role: Optional element role filter
            **attributes: Accessibility attributes
            
        Returns:
            List of UIFound elements
        """
        results = []
        
        try:
            script = '''
            tell application "System Events"
                tell process "System Events"
                    set allElements to every UI element
            '''
            
            if role:
                script = script.replace("every UI element", f'every UI element whose role is "{role}"')
            
            script += '''
                    set resultList to {}
                    repeat with theElement in allElements
                        try
                            set elemRole to role of theElement
                            set elemDesc to description of theElement
                            set elemPos to position of theElement
                            set elemSize to size of theElement
                            set end of resultList to elemRole & "|" & elemDesc & "|" & (item 1 of elemPos as string) & "," & (item 2 of elemPos as string) & "|" & (item 1 of elemSize as string) & "," & (item 2 of elemSize as string)
                        end try
                    end repeat
                    return resultList
                end tell
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    parts = line.split("|")
                    if len(parts) >= 3:
                        pos_parts = parts[2].split(",")
                        size_parts = parts[3].split(",") if len(parts) > 3 else ["100", "100"]
                        
                        try:
                            results.append(UIFound(
                                element_type=parts[0],
                                identifier=parts[1],
                                x=int(pos_parts[0]) if pos_parts else 0,
                                y=int(pos_parts[1]) if pos_parts else 0,
                                width=int(size_parts[0]) if size_parts else 100,
                                height=int(size_parts[1]) if size_parts else 100,
                                attributes={}
                            ))
                        except (ValueError, IndexError):
                            pass
        except Exception:
            pass
        
        return results
    
    def find_by_text(
        self,
        text: str,
        element_type: Optional[str] = None
    ) -> Optional[UIFound]:
        """Find element containing specific text.
        
        Args:
            text: Text to search for
            element_type: Optional element type filter
            
        Returns:
            UIFound or None
        """
        try:
            script = f'''
            tell application "System Events"
                tell process "System Events"
                    set allElements to every UI element
                    repeat with theElement in allElements
                        try
                            set elemValue to value of theElement as string
                            if elemValue contains "{text}" then
                                return "found"
                            end if
                        end try
                    end repeat
                end tell
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0 and "found" in result.stdout.lower():
                return UIFound(
                    element_type="unknown",
                    identifier=text,
                    x=0, y=0, width=100, height=100,
                    attributes={"text": text}
                )
        except Exception:
            pass
        
        return None
    
    def wait_for_element(
        self,
        finder_func: Callable[[], Optional[UIFound]],
        timeout: Optional[float] = None,
        interval: Optional[float] = None
    ) -> Optional[UIFound]:
        """Wait for an element to appear.
        
        Args:
            finder_func: Function that returns UIFound or None
            timeout: Maximum wait time
            interval: Retry interval
            
        Returns:
            UIFound when found, or None on timeout
        """
        timeout = timeout or self._options.timeout
        interval = interval or self._options.interval
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = finder_func()
            if result:
                return result
            time.sleep(interval)
        
        return None
    
    def find_with_retry(
        self,
        strategy: FinderStrategy,
        max_retries: int = 3,
        **kwargs
    ) -> Optional[UIFound]:
        """Find element with retry logic.
        
        Args:
            strategy: Finding strategy to use
            max_retries: Maximum number of retries
            **kwargs: Strategy-specific arguments
            
        Returns:
            UIFound or None
        """
        for attempt in range(max_retries):
            if strategy == FinderStrategy.ACCESSIBILITY:
                result = self.find_by_accessibility(**kwargs)
                if result:
                    return result
            elif strategy == FinderStrategy.TEXT:
                result = self.find_by_text(kwargs.get("text", ""), kwargs.get("element_type"))
                if result:
                    return result
            
            time.sleep(0.1 * (attempt + 1))
        
        return None


class UIFinderBuilder:
    """Builder for complex UI finding operations."""
    
    def __init__(self):
        """Initialize builder."""
        self._finder = UIFinder()
        self._predicates: List[Callable[[UIFound], bool]] = []
    
    def with_role(self, role: str) -> "UIFinderBuilder":
        """Add role filter.
        
        Args:
            role: Element role
            
        Returns:
            Self for chaining
        """
        def predicate(element: UIFound) -> bool:
            return element.element_type.lower() == role.lower()
        
        self._predicates.append(predicate)
        return self
    
    def with_identifier(self, identifier: str) -> "UIFinderBuilder":
        """Add identifier filter.
        
        Args:
            identifier: Accessibility identifier
            
        Returns:
            Self for chaining
        """
        def predicate(element: UIFound) -> bool:
            return identifier.lower() in element.identifier.lower()
        
        self._predicates.append(predicate)
        return self
    
    def with_text(self, text: str) -> "UIFinderBuilder":
        """Add text filter.
        
        Args:
            text: Text that element should contain
            
        Returns:
            Self for chaining
        """
        def predicate(element: UIFound) -> bool:
            return text.lower() in str(element.attributes.get("text", "")).lower()
        
        self._predicates.append(predicate)
        return self
    
    def with_bounds(self, x: int, y: int, width: int, height: int) -> "UIFinderBuilder":
        """Add bounds filter.
        
        Args:
            x: X coordinate
            y: Y coordinate
            width: Width
            height: Height
            
        Returns:
            Self for chaining
        """
        def predicate(element: UIFound) -> bool:
            return (element.x == x and element.y == y and
                   element.width == width and element.height == height)
        
        self._predicates.append(predicate)
        return self
    
    def find(self) -> Optional[UIFound]:
        """Execute the find operation.
        
        Returns:
            First matching UIFound or None
        """
        elements = self._finder.find_all_by_accessibility()
        
        for element in elements:
            if all(predicate(element) for predicate in self._predicates):
                return element
        
        return None
    
    def find_all(self) -> List[UIFound]:
        """Execute the find all operation.
        
        Returns:
            All matching UIFound elements
        """
        elements = self._finder.find_all_by_accessibility()
        
        return [e for e in elements if all(p(e) for p in self._predicates)]


def find_button(title: str, timeout: float = 10.0) -> Optional[UIFound]:
    """Find a button by title.
    
    Args:
        title: Button title
        timeout: Search timeout
        
    Returns:
        UIFound or None
    """
    finder = UIFinder()
    return finder.wait_for_element(
        lambda: finder.find_by_accessibility("button", title=title),
        timeout=timeout
    )


def find_textfield(identifier: Optional[str] = None, timeout: float = 10.0) -> Optional[UIFound]:
    """Find a text field by identifier.
    
    Args:
        identifier: Text field identifier
        timeout: Search timeout
        
    Returns:
        UIFound or None
    """
    finder = UIFinder()
    return finder.wait_for_element(
        lambda: finder.find_by_accessibility("textfield", identifier=identifier),
        timeout=timeout
    )


def find_menu_item(name: str, timeout: float = 10.0) -> Optional[UIFound]:
    """Find a menu item by name.
    
    Args:
        name: Menu item name
        timeout: Search timeout
        
    Returns:
        UIFound or None
    """
    finder = UIFinder()
    return finder.wait_for_element(
        lambda: finder.find_by_accessibility("menu item", title=name),
        timeout=timeout
    )


def find_checkbox(identifier: Optional[str] = None, timeout: float = 10.0) -> Optional[UIFound]:
    """Find a checkbox by identifier.
    
    Args:
        identifier: Checkbox identifier
        timeout: Search timeout
        
    Returns:
        UIFound or None
    """
    finder = UIFinder()
    return finder.wait_for_element(
        lambda: finder.find_by_accessibility("checkbox", identifier=identifier),
        timeout=timeout
    )
