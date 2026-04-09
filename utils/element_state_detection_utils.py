"""Element state detection utilities for UI automation.

This module provides utilities for detecting UI element states like
hover, active, disabled, selected, and visibility, useful for
conditional automation logic in workflow execution.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class ElementState(Enum):
    """Possible element states."""
    VISIBLE = auto()
    INVISIBLE = auto()
    ENABLED = auto()
    DISABLED = auto()
    HOVERED = auto()
    ACTIVE = auto()
    SELECTED = auto()
    UNSELECTED = auto()
    CHECKED = auto()
    UNCHECKED = auto()
    FOCUSED = auto()
    BLURRED = auto()


@dataclass
class ElementStateInfo:
    """State information for a UI element."""
    element_id: str
    states: List[ElementState]
    bounding_box: Tuple[int, int, int, int]
    confidence: float
    
    @property
    def is_visible(self) -> bool:
        return ElementState.VISIBLE in self.states
    
    @property
    def is_enabled(self) -> bool:
        return ElementState.ENABLED in self.states
    
    @property
    def is_interactive(self) -> bool:
        return self.is_visible and self.is_enabled


@dataclass
class StateDetectionConfig:
    """Configuration for state detection."""
    check_visibility: bool = True
    check_hover: bool = True
    check_focus: bool = True
    check_enabled: bool = True
    confidence_threshold: float = 0.7


def detect_element_state(
    image_data: bytes,
    element_region: Tuple[int, int, int, int],
    config: Optional[StateDetectionConfig] = None,
) -> ElementStateInfo:
    """Detect the state of a UI element.
    
    Args:
        image_data: Screenshot bytes.
        element_region: (x, y, width, height) of element.
        config: Detection configuration.
    
    Returns:
        ElementStateInfo with detected states.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        config = config or StateDetectionConfig()
        x, y, w, h = element_region
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        element_img = img.crop((x, y, x + w, y + h))
        element_array = np.array(element_img)
        
        gray = np.mean(element_array, axis=2)
        mean_brightness = float(gray.mean() / 255.0)
        std_brightness = float(gray.std() / 255.0)
        
        states = []
        
        if mean_brightness > 0.1:
            states.append(ElementState.VISIBLE)
        else:
            states.append(ElementState.INVISIBLE)
        
        if std_brightness > 0.05:
            states.append(ElementState.ENABLED)
        else:
            states.append(ElementState.DISABLED)
        
        if mean_brightness > 0.3:
            states.append(ElementState.ENABLED)
        
        if std_brightness > 0.15:
            states.append(ElementState.ACTIVE)
        
        return ElementStateInfo(
            element_id="element",
            states=states,
            bounding_box=element_region,
            confidence=0.75,
        )
    except ImportError:
        raise ImportError("numpy and PIL are required for state detection")


def detect_button_state(
    image_data: bytes,
    button_region: Tuple[int, int, int, int],
) -> str:
    """Detect button state (normal, hover, pressed, disabled).
    
    Args:
        image_data: Screenshot bytes.
        button_region: (x, y, width, height) of button.
    
    Returns:
        State string: "normal", "hover", "pressed", or "disabled".
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        x, y, w, h = button_region
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        button_img = img.crop((x, y, x + w, y + h))
        button_array = np.array(button_img)
        
        brightness = float(np.mean(button_array) / 255.0)
        contrast = float(np.std(button_array) / 255.0)
        
        if brightness < 0.15:
            return "disabled"
        elif contrast > 0.2 and brightness > 0.5:
            return "pressed"
        elif brightness > 0.3:
            return "hover"
        else:
            return "normal"
    except ImportError:
        raise ImportError("numpy and PIL are required for button state detection")


def detect_checkbox_state(
    image_data: bytes,
    checkbox_region: Tuple[int, int, int, int],
) -> bool:
    """Detect if checkbox is checked.
    
    Args:
        image_data: Screenshot bytes.
        checkbox_region: (x, y, width, height) of checkbox.
    
    Returns:
        True if checked, False otherwise.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        x, y, w, h = checkbox_region
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        cb_img = img.crop((x, y, x + w, y + h))
        cb_array = np.array(cb_img)
        
        gray = np.mean(cb_array, axis=2)
        
        center_region = gray[int(h * 0.3):int(h * 0.7), int(w * 0.3):int(w * 0.7)]
        center_brightness = float(np.mean(center_region) / 255.0)
        
        return center_brightness > 0.3
    except ImportError:
        raise ImportError("numpy and PIL is required for checkbox detection")


def batch_detect_states(
    image_data: bytes,
    elements: List[Tuple[str, Tuple[int, int, int, int]]],
    config: Optional[StateDetectionConfig] = None,
) -> List[ElementStateInfo]:
    """Detect states for multiple elements.
    
    Args:
        image_data: Screenshot bytes.
        elements: List of (element_id, region) tuples.
        config: Detection configuration.
    
    Returns:
        List of ElementStateInfo for each element.
    """
    results = []
    for element_id, region in elements:
        state_info = detect_element_state(image_data, region, config)
        state_info.element_id = element_id
        results.append(state_info)
    return results


def filter_visible_elements(
    elements: List[ElementStateInfo],
) -> List[ElementStateInfo]:
    """Filter to only visible elements.
    
    Args:
        elements: List of element states.
    
    Returns:
        List of only visible elements.
    """
    return [e for e in elements if e.is_visible]


def filter_interactive_elements(
    elements: List[ElementStateInfo],
) -> List[ElementStateInfo]:
    """Filter to only interactive elements.
    
    Args:
        elements: List of element states.
    
    Returns:
        List of only interactive elements.
    """
    return [e for e in elements if e.is_interactive]
